package service

import (
	"context"
	"encoding/json"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	"github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/inference/model"
	logHelper "github.com/Avi18971911/Augur/pkg/log/helper"
	spanHelper "github.com/Avi18971911/Augur/pkg/trace/helper"
	"go.uber.org/zap"
	"time"
)

const timeout = 10 * time.Second
const minimumRatio = 0.6
const querySize = 1000
const bucket countModel.Bucket = 100

type AnalyticsService struct {
	ac     augurElasticsearch.AugurClient
	logger *zap.Logger
}

func NewAnalyticsService(
	ac augurElasticsearch.AugurClient,
	logger *zap.Logger,
) *AnalyticsService {
	return &AnalyticsService{
		ac:     ac,
		logger: logger,
	}
}

func (as *AnalyticsService) UpdateAnalytics(
	ctx context.Context,
	clusterIds []string,
) error {
	for _, clusterId := range clusterIds {
		stack := []string{clusterId}
		clusterToSucceedingClusters := make(map[string][]string)
		visitedClusters := map[string]bool{clusterId: true}
		for {
			if len(stack) == 0 {
				break
			}
			currentClusterId := stack[0]
			stack = stack[1:]
			if _, ok := clusterToSucceedingClusters[currentClusterId]; !ok {
				clusterToSucceedingClusters[currentClusterId] = make([]string, 0)
			}
			relatedClusters, err := as.getRelatedClusters(ctx, currentClusterId)
			if err != nil {
				return fmt.Errorf("failed to get related clusters: %w", err)
			}
			for _, relatedCluster := range relatedClusters {
				if relatedCluster.MeanTDOA > 0 {
					clusterToSucceedingClusters[relatedCluster.ClusterId] =
						append(clusterToSucceedingClusters[relatedCluster.ClusterId], currentClusterId)
				} else {
					clusterToSucceedingClusters[currentClusterId] =
						append(clusterToSucceedingClusters[currentClusterId], relatedCluster.ClusterId)
				}
				if _, ok := visitedClusters[relatedCluster.ClusterId]; !ok {
					stack = append(stack, relatedCluster.ClusterId)
					visitedClusters[relatedCluster.ClusterId] = true
				}
			}
		}
		metaUpdate, documentUpdate := getAnalyticsUpdateStatement(clusterToSucceedingClusters)
		updateCtx, cancel := context.WithTimeout(ctx, timeout)
		err := as.ac.BulkUpdate(updateCtx, metaUpdate, documentUpdate, bootstrapper.ClusterIndexName)
		cancel()
		if err != nil {
			return fmt.Errorf("failed to bulk update analytics: %w", err)
		}
	}
	return nil
}

func (as *AnalyticsService) getRelatedClusters(
	ctx context.Context,
	clusterId string,
) ([]model.CountCluster, error) {
	queryJSON, err := json.Marshal(buildGetRelatedClustersQuery(clusterId))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal get related cluster query: %w", err)
	}
	var querySize = 1000
	searchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	docs, err := as.ac.Search(searchCtx, string(queryJSON), []string{bootstrapper.CountIndexName}, &querySize)
	if err != nil {
		return nil, fmt.Errorf("failed to search for related clusters: %w", err)
	}
	clusters, err := parseClusters(docs)
	prunedClusters := pruneClusters(clusters)
	return prunedClusters, nil
}

func parseClusters(docs []map[string]interface{}) ([]model.CountCluster, error) {
	clusters := make([]model.CountCluster, len(docs))
	for i, doc := range docs {
		clusterId, ok := doc["cluster_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert cluster_id to string: %v", doc)
		}
		occurrences, ok := doc["occurrences"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert occurrences to float64: %v", doc)
		}
		coOccurrences, ok := doc["co_occurrences"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert co_occurrences to float64: %v", doc)
		}
		meanTDOA, ok := doc["mean_TDOA"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert mean_tdoa to float64: %v", doc)
		}
		varianceTDOA, ok := doc["variance_TDOA"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert variance_tdoa to float64: %v", doc)
		}
		cluster := model.CountCluster{
			ClusterId:     clusterId,
			Occurrences:   int64(occurrences),
			CoOccurrences: int64(coOccurrences),
			MeanTDOA:      meanTDOA,
			VarianceTDOA:  varianceTDOA,
		}
		clusters[i] = cluster
	}
	return clusters, nil
}

func pruneClusters(clusters []model.CountCluster) []model.CountCluster {
	prunedClusters := make([]model.CountCluster, 0)
	for _, cluster := range clusters {
		sampleRatio := float64(cluster.CoOccurrences) / float64(cluster.Occurrences)
		if sampleRatio >= minimumRatio {
			prunedClusters = append(prunedClusters, cluster)
		}
	}
	return prunedClusters
}

func getAnalyticsUpdateStatement(
	clusterToSucceedingClusters map[string][]string,
) ([]map[string]interface{}, []map[string]interface{}) {
	metaUpdates := make([]map[string]interface{}, len(clusterToSucceedingClusters))
	documentUpdates := make([]map[string]interface{}, len(clusterToSucceedingClusters))
	i := 0
	for clusterId, succeedingClusterIds := range clusterToSucceedingClusters {
		metaUpdate := map[string]interface{}{
			"update": map[string]interface{}{
				"_id": clusterId,
			},
		}
		documentUpdate := map[string]interface{}{
			"doc": map[string]interface{}{
				"causes_clusters": succeedingClusterIds,
			},
			"doc_as_upsert": true,
		}
		metaUpdates[i] = metaUpdate
		documentUpdates[i] = documentUpdate
		i++
	}
	return metaUpdates, documentUpdates
}

func (as *AnalyticsService) GetChainOfEvents(
	ctx context.Context,
	logOrSpanData model.LogOrSpanData,
) (mleSequence map[string]*model.ClusterNode, err error) {
	clusterToSearchOn := logOrSpanData.ClusterId
	clusterGraph, err := as.getClusterGraph(ctx, clusterToSearchOn)
	if err != nil {
		as.logger.Error("failed to get cluster graph", zap.Error(err))
		return nil, err
	}
	if logOrSpanData.SpanDetails != nil {
		clusterGraph[clusterToSearchOn].LogOrSpanData.SpanDetails = logOrSpanData.SpanDetails
	} else if logOrSpanData.LogDetails != nil {
		clusterGraph[clusterToSearchOn].LogOrSpanData.LogDetails = logOrSpanData.LogDetails
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

func (as *AnalyticsService) getClusterGraph(
	ctx context.Context,
	clusterToSearchOn string,
) (clustersInGraph map[string]*model.ClusterNode, err error) {
	clusterStack := []string{clusterToSearchOn}
	visitedClusters := map[string]*model.ClusterNode{
		clusterToSearchOn: {
			ClusterId:    clusterToSearchOn,
			Successors:   make([]*model.ClusterNode, 0),
			Predecessors: make([]*model.ClusterNode, 0),
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
			if _, ok := visitedClusters[succeedingCluster.ClusterId]; !ok {
				clusterStack = append(clusterStack, succeedingCluster.ClusterId)
				currentClusterNode.Successors = append(currentClusterNode.Successors, &succeedingCluster)
				visitedClusters[succeedingCluster.ClusterId] = &succeedingCluster
			}
		}
		for _, precedingCluster := range precedingClusters {
			if _, ok := visitedClusters[precedingCluster.ClusterId]; !ok {
				clusterStack = append(clusterStack, precedingCluster.ClusterId)
				currentClusterNode.Predecessors = append(currentClusterNode.Predecessors, &precedingCluster)
				visitedClusters[precedingCluster.ClusterId] = &precedingCluster
			}
		}
	}
	return visitedClusters, nil
}

func (as *AnalyticsService) getSucceedingClusters(
	ctx context.Context,
	clusterId string,
) ([]model.ClusterNode, error) {
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
	clusterNodes := make([]model.ClusterNode, len(cluster.CausesClusters))
	for i, causeCluster := range cluster.CausesClusters {
		clusterNodes[i] = model.ClusterNode{
			ClusterId:    causeCluster,
			Successors:   make([]*model.ClusterNode, 0),
			Predecessors: make([]*model.ClusterNode, 0),
		}
	}
	return clusterNodes, nil
}

func (as *AnalyticsService) getPrecedingClusters(
	ctx context.Context,
	clusterId string,
) ([]model.ClusterNode, error) {
	query := getPrecedingClusterIdsQuery(clusterId)
	clusters, err := as.getClusterSubGraph(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster sub graph: %w", err)
	}
	clusterNodes := make([]model.ClusterNode, len(clusters))
	for i, cluster := range clusters {
		clusterNodes[i] = model.ClusterNode{
			ClusterId:    cluster.ClusterId,
			Successors:   make([]*model.ClusterNode, 0),
			Predecessors: make([]*model.ClusterNode, 0),
		}
	}
	return clusterNodes, nil
}

func (as *AnalyticsService) getClusterSubGraph(
	ctx context.Context,
	query map[string]interface{},
) ([]model.Cluster, error) {
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

func (as *AnalyticsService) getMostLikelySequence(
	ctx context.Context,
	clusterIdToSearchOn string,
	clusterGraph map[string]*model.ClusterNode,
) (map[string]*model.ClusterNode, error) {
	startNode := clusterGraph[clusterIdToSearchOn]
	nodesToDoMLEOn := []model.MostLikelyEstimatorPair{
		{
			PreviousNode: startNode,
			NextNodes:    make([]*model.ClusterNode, 0),
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
			countClusterDetails, err := as.getCountClusterDetails(ctx, previousNode.ClusterId, currentNode.ClusterId)
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
			if mostLikelyLogOrSpan != nil {
				as.logger.Warn(
					"Could not find suitable candidate for MLE",
					zap.String("cluster_id", currentNode.ClusterId),
					zap.String("preceding_cluster_id", previousNode.ClusterId),
				)
			} else {
				currentNode.LogOrSpanData = *mostLikelyLogOrSpan
			}

			for _, successor := range currentNode.Successors {
				if _, ok := visitedNodes[successor.ClusterId]; !ok {
					nodesToDoMLEOn = append(nodesToDoMLEOn, model.MostLikelyEstimatorPair{
						PreviousNode: currentNode,
						NextNodes:    currentNode.Successors,
					})
					visitedNodes[successor.ClusterId] = true
				}
			}

			for _, predecessor := range currentNode.Predecessors {
				if _, ok := visitedNodes[predecessor.ClusterId]; !ok {
					nodesToDoMLEOn = append(nodesToDoMLEOn, model.MostLikelyEstimatorPair{
						PreviousNode: currentNode,
						NextNodes:    currentNode.Predecessors,
					})
					visitedNodes[predecessor.ClusterId] = true
				}
			}
		}
	}
	return clusterGraph, nil
}

func (as *AnalyticsService) getCountClusterDetails(
	ctx context.Context,
	previousClusterId string,
	nextClusterId string,
) (model.CountCluster, error) {
	countId := service.GetIDFromConstituents(previousClusterId, nextClusterId)
	query := getCountClusterDetailsQuery(countId)
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return model.CountCluster{}, fmt.Errorf("failed to marshal cluster details query: %w", err)
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	docs, err := as.ac.Search(queryCtx, string(queryJSON), []string{bootstrapper.CountIndexName}, nil)
	if err != nil {
		return model.CountCluster{}, fmt.Errorf("failed to search for cluster details: %w", err)
	}
	countClusters, err := parseClusters(docs)
	if err != nil {
		return model.CountCluster{}, fmt.Errorf("failed to convert docs to count clusters: %w", err)
	}
	if len(countClusters) != 1 {
		return model.CountCluster{}, fmt.Errorf("expected 1 count cluster from composite ID, got %d", len(countClusters))
	}
	return countClusters[0], nil
}

func (as *AnalyticsService) getSpanOrLogDetails(
	ctx context.Context,
	clusterId string,
	previousLogOrSpanData model.LogOrSpanData,
	countClusterDetails model.CountCluster,
) ([]model.LogOrSpanData, error) {
	var timeStart time.Time
	if previousLogOrSpanData.SpanDetails != nil {
		timeStart = previousLogOrSpanData.SpanDetails.StartTime
	} else {
		timeStart = previousLogOrSpanData.LogDetails.Timestamp
	}
	// positive because countClusters TDOA is defined as CURRENT - PREVIOUS
	timeToSearchAround := timeStart.Add(time.Duration(countClusterDetails.MeanTDOA) * time.Millisecond)
	query := getLogsAndSpansAroundTimeQuery(clusterId, timeToSearchAround, time.Duration(bucket)*time.Millisecond)
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

func (as *AnalyticsService) getMostLikelyLogOrSpan(
	spanOrLogDetails []model.LogOrSpanData,
	previousSpanOrLogDetails model.LogOrSpanData,
	clusterDetails model.CountCluster,
) *model.LogOrSpanData {
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

func convertDocsToClusterNodes(docs []map[string]interface{}) ([]model.Cluster, error) {
	clusters := make([]model.Cluster, len(docs))
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
		cluster := model.Cluster{
			ClusterId:      clusterId,
			CausesClusters: causesClustersString,
		}
		clusters[i] = cluster
	}
	return clusters, nil
}

func convertDocsToLogOrSpanData(docs []map[string]interface{}) ([]model.LogOrSpanData, error) {
	logOrSpanData := make([]model.LogOrSpanData, len(docs))
	if len(docs) == 0 {
		return nil, nil
	}
	if _, ok := docs[0]["timestamp"]; ok {
		logs, err := logHelper.ConvertFromDocuments(docs)
		if err != nil {
			return nil, fmt.Errorf("failed to convert docs to logs: %w", err)
		}
		for i, log := range logs {
			logOrSpanData[i] = model.LogOrSpanData{
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
			logOrSpanData[i] = model.LogOrSpanData{
				Id:          span.Id,
				ClusterId:   span.ClusterId,
				SpanDetails: &span,
			}
		}

	}
	return logOrSpanData, nil
}
