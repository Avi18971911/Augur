package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	logHelper "github.com/Avi18971911/Augur/internal/otel_server/log/helper"
	spanHelper "github.com/Avi18971911/Augur/internal/otel_server/trace/helper"
	analyticsModel "github.com/Avi18971911/Augur/internal/pipeline/analytics/model"
	analyticsService "github.com/Avi18971911/Augur/internal/pipeline/analytics/service"
	"github.com/Avi18971911/Augur/internal/pipeline/count/service"
	inferenceModel "github.com/Avi18971911/Augur/internal/query_server/service/inference/model"
	"go.uber.org/zap"
	"math"
	"time"
)

const timeout = 10 * time.Second
const querySize = 100

type InferenceQueryService interface {
	GetChainOfEvents(
		ctx context.Context,
		input inferenceModel.LogOrSpanData,
	) (mostLikelySequence map[string]*inferenceModel.ClusterNode, err error)
	GetSpanOrLogData(ctx context.Context, id string) (inferenceModel.LogOrSpanData, error)
}

type InferenceQueryServiceImpl struct {
	ac     client.AugurClient
	logger *zap.Logger
}

func NewAnalyticsQueryService(ac client.AugurClient, logger *zap.Logger) InferenceQueryService {
	return &InferenceQueryServiceImpl{
		ac:     ac,
		logger: logger,
	}
}

func (as *InferenceQueryServiceImpl) GetSpanOrLogData(ctx context.Context, id string) (inferenceModel.LogOrSpanData, error) {
	query := getLogOrSpanQuery(id)
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return inferenceModel.LogOrSpanData{}, fmt.Errorf("failed to marshal span or log query: %w", err)
	}
	var localQuerySize = querySize
	searchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	docs, err := as.ac.Search(
		searchCtx,
		string(queryJSON),
		[]string{bootstrapper.SpanIndexName, bootstrapper.LogIndexName},
		&localQuerySize,
	)
	if err != nil {
		return inferenceModel.LogOrSpanData{}, fmt.Errorf("failed to search for span or log: %w", err)
	}
	logOrSpanData, err := convertDocsToLogOrSpanData(docs)
	if err != nil {
		return inferenceModel.LogOrSpanData{}, fmt.Errorf("failed to convert docs to log or span data: %w", err)
	}
	if len(logOrSpanData) != 1 {
		return inferenceModel.LogOrSpanData{}, fmt.Errorf("expected 1 log or span data, got %d", len(logOrSpanData))
	}
	return logOrSpanData[0], nil
}

func (as *InferenceQueryServiceImpl) GetChainOfEvents(
	ctx context.Context,
	logOrSpanData inferenceModel.LogOrSpanData,
) (mleSequence map[string]*inferenceModel.ClusterNode, err error) {
	clusterToSearchOn := logOrSpanData.ClusterId
	clusterGraph, err := as.getClusterGraph(ctx, clusterToSearchOn)
	if err != nil {
		as.logger.Error("failed to get cluster graph", zap.Error(err))
		return nil, err
	}
	if logOrSpanData.SpanDetails != nil {
		clusterGraph[clusterToSearchOn].LogOrSpanData = inferenceModel.LogOrSpanData{
			Id:          logOrSpanData.Id,
			ClusterId:   logOrSpanData.ClusterId,
			SpanDetails: logOrSpanData.SpanDetails,
		}
	} else if logOrSpanData.LogDetails != nil {
		clusterGraph[clusterToSearchOn].LogOrSpanData = inferenceModel.LogOrSpanData{
			Id:         logOrSpanData.Id,
			ClusterId:  logOrSpanData.ClusterId,
			LogDetails: logOrSpanData.LogDetails,
		}
	} else {
		return nil, fmt.Errorf("log or span data is nil for cluster: %s", clusterToSearchOn)
	}
	mleSequence, err = as.getMostLikelySequence(
		ctx,
		clusterToSearchOn,
		clusterGraph,
	)
	if err != nil {
		as.logger.Error("failed to get most likely sequence", zap.Error(err))
		return nil, err
	}
	return mleSequence, nil
}

func (as *InferenceQueryServiceImpl) getClusterGraph(
	ctx context.Context,
	clusterToSearchOn string,
) (clustersInGraph map[string]*inferenceModel.ClusterNode, err error) {
	clusterStack := []string{clusterToSearchOn}
	visitedClusters := map[string]*inferenceModel.ClusterNode{
		clusterToSearchOn: {
			Successors:   make([]inferenceModel.SimpleClusterNode, 0),
			Predecessors: make([]inferenceModel.SimpleClusterNode, 0),
		},
	}
	for {
		if len(clusterStack) == 0 {
			break
		}
		currentCluster := clusterStack[0]
		clusterStack = clusterStack[1:]
		currentClusterNode := visitedClusters[currentCluster]
		if currentClusterNode == nil {
			return nil, fmt.Errorf("current cluster node is nil: %v", currentCluster)
		}

		succeedingClusters, err := as.getSucceedingClusters(ctx, currentCluster)
		if err != nil {
			return nil, fmt.Errorf("failed to get succeeding clusters: %w", err)
		}
		precedingClusters, err := as.getPrecedingClusters(ctx, currentCluster)
		if err != nil {
			return nil, fmt.Errorf("failed to get preceding clusters: %w", err)
		}

		for _, succeedingCluster := range succeedingClusters {
			simpleClusterNode := inferenceModel.SimpleClusterNode{
				Id:        succeedingCluster.LogOrSpanData.Id,
				ClusterId: succeedingCluster.LogOrSpanData.ClusterId,
			}
			currentClusterNode.Successors = append(currentClusterNode.Successors, simpleClusterNode)
			if _, ok := visitedClusters[succeedingCluster.LogOrSpanData.ClusterId]; !ok {
				clusterStack = append(clusterStack, succeedingCluster.LogOrSpanData.ClusterId)
				visitedClusters[succeedingCluster.LogOrSpanData.ClusterId] = &succeedingCluster
			}
		}
		for _, precedingCluster := range precedingClusters {
			simpleClusterNode := inferenceModel.SimpleClusterNode{
				Id:        precedingCluster.LogOrSpanData.Id,
				ClusterId: precedingCluster.LogOrSpanData.ClusterId,
			}
			currentClusterNode.Predecessors = append(currentClusterNode.Predecessors, simpleClusterNode)
			if _, ok := visitedClusters[precedingCluster.LogOrSpanData.ClusterId]; !ok {
				clusterStack = append(clusterStack, precedingCluster.LogOrSpanData.ClusterId)
				visitedClusters[precedingCluster.LogOrSpanData.ClusterId] = &precedingCluster
			}
		}
	}
	return visitedClusters, nil
}

func (as *InferenceQueryServiceImpl) getSucceedingClusters(
	ctx context.Context,
	clusterId string,
) ([]inferenceModel.ClusterNode, error) {
	query := getSucceedingClusterIdsQuery(clusterId)
	clusters, err := as.getClusterSubGraph(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster sub graph: %w", err)
	}
	if len(clusters) == 0 {
		return nil, fmt.Errorf("no clusters found for cluster id: %s", clusterId)
	} else if len(clusters) != 1 {
		return nil, fmt.Errorf("expected 1 cluster, got %d", len(clusters))
	}
	cluster := clusters[0]
	clusterNodes := make([]inferenceModel.ClusterNode, len(cluster.CausesClusters))
	for i, causeCluster := range cluster.CausesClusters {
		clusterNodes[i] = inferenceModel.ClusterNode{
			Successors:   make([]inferenceModel.SimpleClusterNode, 0),
			Predecessors: make([]inferenceModel.SimpleClusterNode, 0),
			LogOrSpanData: inferenceModel.LogOrSpanData{
				ClusterId: causeCluster,
			},
		}
	}
	return clusterNodes, nil
}

func (as *InferenceQueryServiceImpl) getPrecedingClusters(
	ctx context.Context,
	clusterId string,
) ([]inferenceModel.ClusterNode, error) {
	query := getPrecedingClusterIdsQuery(clusterId)
	clusters, err := as.getClusterSubGraph(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster sub graph: %w", err)
	}
	clusterNodes := make([]inferenceModel.ClusterNode, len(clusters))
	for i, cluster := range clusters {
		clusterNodes[i] = inferenceModel.ClusterNode{
			Successors:   make([]inferenceModel.SimpleClusterNode, 0),
			Predecessors: make([]inferenceModel.SimpleClusterNode, 0),
			LogOrSpanData: inferenceModel.LogOrSpanData{
				ClusterId: cluster.ClusterId,
			},
		}
	}
	return clusterNodes, nil
}

func (as *InferenceQueryServiceImpl) getClusterSubGraph(
	ctx context.Context,
	query map[string]interface{},
) ([]analyticsModel.Cluster, error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal preceding cluster query: %w", err)
	}
	var localQuerySize = querySize
	searchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	docs, err := as.ac.Search(searchCtx, string(queryJSON), []string{bootstrapper.ClusterIndexName}, &localQuerySize)
	if err != nil {
		return nil, fmt.Errorf("failed to search for preceding clusters: %w", err)
	}
	clusters, err := convertDocsToClusterNodes(docs)
	if err != nil {
		return nil, fmt.Errorf("failed to convert docs to cluster nodes: %w", err)
	}
	return clusters, nil
}

func (as *InferenceQueryServiceImpl) getMostLikelySequence(
	ctx context.Context,
	clusterIdToSearchOn string,
	clusterGraph map[string]*inferenceModel.ClusterNode,
) (map[string]*inferenceModel.ClusterNode, error) {
	startNode := clusterGraph[clusterIdToSearchOn]
	nodesToDoMLEOn := []inferenceModel.MostLikelyEstimatorPair{
		{
			PreviousNode: startNode,
			NextNodes:    make([]inferenceModel.SimpleClusterNode, 0),
		},
	}
	visitedNodes := map[string]bool{clusterIdToSearchOn: true}
	nodesToDoMLEOn[0].NextNodes = append(nodesToDoMLEOn[0].NextNodes, clusterGraph[clusterIdToSearchOn].Predecessors...)
	nodesToDoMLEOn[0].NextNodes = append(nodesToDoMLEOn[0].NextNodes, clusterGraph[clusterIdToSearchOn].Successors...)
	for _, node := range nodesToDoMLEOn[0].NextNodes {
		visitedNodes[node.ClusterId] = true
	}

	for {
		if len(nodesToDoMLEOn) == 0 {
			break
		}
		currentPair := nodesToDoMLEOn[0]
		nodesToDoMLEOn = nodesToDoMLEOn[1:]
		previousNode := currentPair.PreviousNode
		nextNodes := currentPair.NextNodes
		for _, currentNode := range nextNodes {
			countClusterDetails, err := as.getCountClusterDetails(
				ctx,
				previousNode.LogOrSpanData.ClusterId,
				currentNode.ClusterId,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to get cluster details: %w", err)
			}
			spanOrLogDetailsOfCurrentNode, err := as.getSpanOrLogDetails(
				ctx,
				currentNode.ClusterId,
				previousNode.LogOrSpanData,
				countClusterDetails,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to get span or log details: %w", err)
			}
			mostLikelyLogOrSpan := as.getMostLikelyLogOrSpan(
				spanOrLogDetailsOfCurrentNode,
				previousNode.LogOrSpanData,
				countClusterDetails,
			)
			if mostLikelyLogOrSpan == nil {
				as.logger.Warn(
					"Could not find suitable candidate for MLE",
					zap.String("cluster_id", currentNode.ClusterId),
					zap.String("preceding_cluster_id", previousNode.LogOrSpanData.ClusterId),
				)
				continue
			} else {
				clusterGraph[currentNode.ClusterId].LogOrSpanData = *mostLikelyLogOrSpan
			}

			successorsNextNodes := make([]inferenceModel.SimpleClusterNode, 0)
			for _, successor := range clusterGraph[currentNode.ClusterId].Successors {
				if _, ok := visitedNodes[successor.ClusterId]; !ok {
					successorsNextNodes = append(successorsNextNodes, successor)
					visitedNodes[successor.ClusterId] = true
				}
			}
			if len(successorsNextNodes) > 0 {
				nodesToDoMLEOn = append(nodesToDoMLEOn, inferenceModel.MostLikelyEstimatorPair{
					PreviousNode: clusterGraph[currentNode.ClusterId],
					NextNodes:    successorsNextNodes,
				})
			}

			predecessorsNextNodes := make([]inferenceModel.SimpleClusterNode, 0)
			for _, predecessor := range clusterGraph[currentNode.ClusterId].Predecessors {
				if _, ok := visitedNodes[predecessor.ClusterId]; !ok {
					predecessorsNextNodes = append(predecessorsNextNodes, predecessor)
					visitedNodes[predecessor.ClusterId] = true
				}
			}
			if len(predecessorsNextNodes) > 0 {
				nodesToDoMLEOn = append(nodesToDoMLEOn, inferenceModel.MostLikelyEstimatorPair{
					PreviousNode: clusterGraph[currentNode.ClusterId],
					NextNodes:    predecessorsNextNodes,
				})
			}
		}
	}
	return clusterGraph, nil
}

func (as *InferenceQueryServiceImpl) getCountClusterDetails(
	ctx context.Context,
	previousClusterId string,
	nextClusterId string,
) (inferenceModel.CountCluster, error) {
	countId := service.GetIDFromConstituents(previousClusterId, nextClusterId)
	query := getCountClusterDetailsQuery(countId)
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return inferenceModel.CountCluster{}, fmt.Errorf("failed to marshal cluster details query: %w", err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	docs, err := as.ac.Search(queryCtx, string(queryJSON), []string{bootstrapper.CountIndexName}, nil)
	if err != nil {
		return inferenceModel.CountCluster{}, fmt.Errorf("failed to search for cluster details: %w", err)
	}
	countClusters, err := analyticsService.ParseClusters(docs)
	if err != nil {
		return inferenceModel.CountCluster{}, fmt.Errorf("failed to convert docs to count clusters: %w", err)
	}
	if len(countClusters) != 1 {
		return inferenceModel.CountCluster{}, fmt.Errorf("expected 1 count cluster from composite ID, got %d", len(countClusters))
	}
	return countClusters[0], nil
}

func (as *InferenceQueryServiceImpl) getSpanOrLogDetails(
	ctx context.Context,
	clusterId string,
	previousLogOrSpanData inferenceModel.LogOrSpanData,
	countClusterDetails inferenceModel.CountCluster,
) ([]inferenceModel.LogOrSpanData, error) {
	var timeStart time.Time
	if previousLogOrSpanData.SpanDetails != nil {
		timeStart = previousLogOrSpanData.SpanDetails.StartTime
	} else {
		timeStart = previousLogOrSpanData.LogDetails.Timestamp
	}
	// positive because countClusters TDOA is defined as CURRENT - PREVIOUS
	timeToSearchAround := timeStart.Add(time.Duration(countClusterDetails.MeanTDOA) * time.Second)
	startTime, endTime := getSearchStartAndEnd(timeToSearchAround, countClusterDetails.VarianceTDOA)
	query := getLogsAndSpansAroundTimeQuery(clusterId, startTime, endTime)
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal logs and spans query: %w", err)
	}
	var localQuerySize = querySize
	searchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	docs, err := as.ac.Search(
		searchCtx,
		string(queryJSON),
		[]string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName},
		&localQuerySize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search for logs and spans: %w", err)
	}
	logOrSpanData, err := convertDocsToLogOrSpanData(docs)
	if err != nil {
		return nil, fmt.Errorf("failed to convert docs to log or span data: %w", err)
	}
	return logOrSpanData, nil
}

func (as *InferenceQueryServiceImpl) getMostLikelyLogOrSpan(
	spanOrLogDetails []inferenceModel.LogOrSpanData,
	previousSpanOrLogDetails inferenceModel.LogOrSpanData,
	clusterDetails inferenceModel.CountCluster,
) *inferenceModel.LogOrSpanData {
	probabilities := make([]float64, len(spanOrLogDetails))
	var previousTime time.Time
	var TDOA float64
	if previousSpanOrLogDetails.SpanDetails != nil {
		previousTime = previousSpanOrLogDetails.SpanDetails.StartTime
	} else {
		previousTime = previousSpanOrLogDetails.LogDetails.Timestamp
	}
	for i, logOrSpanData := range spanOrLogDetails {
		if logOrSpanData.SpanDetails != nil {
			span := logOrSpanData.SpanDetails
			TDOA = span.StartTime.Sub(previousTime).Seconds()
		} else {
			log := logOrSpanData.LogDetails
			TDOA = log.Timestamp.Sub(previousTime).Seconds()
		}
		isCausal, probability := DetermineCausality(
			TDOA,
			clusterDetails.MeanTDOA,
			clusterDetails.VarianceTDOA,
			clusterDetails.CoOccurrences,
			clusterDetails.Occurrences,
		)
		if !isCausal {
			probabilities[i] = 0.0
		} else {
			probabilities[i] = probability
		}
	}
	maxProbability := 0.0
	maxIndex := -1
	for i, probability := range probabilities {
		if probability > maxProbability {
			maxProbability = probability
			maxIndex = i
		}
	}
	if maxIndex == -1 {
		return nil
	} else {
		return &spanOrLogDetails[maxIndex]
	}
}

func getSearchStartAndEnd(searchTime time.Time, varianceTDOA float64) (time.Time, time.Time) {
	std := math.Sqrt(varianceTDOA)
	const multiplier = 50.0
	const millisecondsInSecond = 1000.0
	earliestTime := searchTime.Add(-time.Duration(multiplier*std*millisecondsInSecond) * time.Millisecond)
	latestTime := searchTime.Add(time.Duration(multiplier*std*millisecondsInSecond) * time.Millisecond)
	return earliestTime, latestTime
}

func convertDocsToClusterNodes(docs []map[string]interface{}) ([]analyticsModel.Cluster, error) {
	clusters := make([]analyticsModel.Cluster, len(docs))
	for i, doc := range docs {
		clusterId, ok := doc["_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert cluster_id to string: %v", doc)
		}
		causesClusters, ok := doc["causes_clusters"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to convert causes_clusters to []string: %v", doc)
		}
		causesClustersString := make([]string, len(causesClusters))
		for j, causeCluster := range causesClusters {
			causeClusterString, ok := causeCluster.(string)
			if !ok {
				return nil, fmt.Errorf("failed to convert cause_cluster to string: %v", doc)
			}
			causesClustersString[j] = causeClusterString
		}
		cluster := analyticsModel.Cluster{
			ClusterId:      clusterId,
			CausesClusters: causesClustersString,
		}
		clusters[i] = cluster
	}
	return clusters, nil
}

func convertDocsToLogOrSpanData(docs []map[string]interface{}) ([]inferenceModel.LogOrSpanData, error) {
	logOrSpanData := make([]inferenceModel.LogOrSpanData, len(docs))
	if len(docs) == 0 {
		return nil, nil
	}
	if _, ok := docs[0]["timestamp"]; ok {
		logs, err := logHelper.ConvertFromDocuments(docs)
		if err != nil {
			return nil, fmt.Errorf("failed to convert docs to logs: %w", err)
		}
		for i, log := range logs {
			logOrSpanData[i] = inferenceModel.LogOrSpanData{
				Id:         log.Id,
				ClusterId:  log.ClusterId,
				LogDetails: &log,
			}
		}
	} else {
		spans, err := spanHelper.ConvertFromDocuments(docs)
		if err != nil {
			return nil, fmt.Errorf("failed to convert docs to spans: %w", err)
		}
		for i, span := range spans {
			logOrSpanData[i] = inferenceModel.LogOrSpanData{
				Id:          span.Id,
				ClusterId:   span.ClusterId,
				SpanDetails: &span,
			}
		}

	}
	return logOrSpanData, nil
}
