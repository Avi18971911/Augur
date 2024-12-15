package service

import (
	"context"
	"encoding/json"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/inference/model"
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
) ([]CountCluster, error) {
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

func parseClusters(docs []map[string]interface{}) ([]CountCluster, error) {
	clusters := make([]CountCluster, len(docs))
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
		cluster := CountCluster{
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

func pruneClusters(clusters []CountCluster) []CountCluster {
	prunedClusters := make([]CountCluster, 0)
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

func (as *AnalyticsService) GetChainOfEvents(ctx context.Context, logOrSpanData model.LogOrSpanData) (mleSequence, error) {
	clusterToSearchOn := logOrSpanData.ClusterId
	clusterGraph, err := as.getClusterGraph(ctx, clusterToSearchOn)
	if err != nil {
		as.logger.Error("failed to get cluster graph", zap.Error(err))
		return nil, err
	}
	mleSequence := getMostLikelySequence(
		clusterToSearchOn,
		clusterGraph,
		logOrSpanData.SpanTimeDetails,
		logOrSpanData.LogTimeDetails,
	)
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
	query := getSuceedingClusterIdsQuery(clusterId)
	return as.getClusterSubGraph(ctx, query)
}

func (as *AnalyticsService) getPrecedingClusters(
	ctx context.Context,
	clusterId string,
) ([]model.ClusterNode, error) {
	query := getPreceedingClusterIdsQuery(clusterId)
	return as.getClusterSubGraph(ctx, query)
}

func (as *AnalyticsService) getClusterSubGraph(
	ctx context.Context,
	query map[string]interface{},
) ([]model.ClusterNode, error) {
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

func convertDocsToClusterNodes(docs []map[string]interface{}) ([]model.ClusterNode, error) {
	clusters := make([]model.ClusterNode, len(docs))
	for i, doc := range docs {
		clusterId, ok := doc["cluster_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert cluster_id to string: %v", doc)
		}
		cluster := model.ClusterNode{
			ClusterId: clusterId,
		}
		clusters[i] = cluster
	}
	return clusters, nil
}

func getMostLikelySequence(
	clusterGraph []model.ClusterNode,
	spanTimeDetails *model.SpanTimeDetails,
	logTimeDetails *model.LogTimeDetails,
) []model.LogOrSpanData {

}
