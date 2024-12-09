package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"go.uber.org/zap"
	"time"
)

const timeout = 10 * time.Second

type AnalyticsService struct {
	ac     augurElasticsearch.AugurClient
	logger *zap.Logger
}

func NewAnalyticsService(ac augurElasticsearch.AugurClient, logger *zap.Logger) *InferenceService {
	return &AnalyticsService{
		ac:     ac,
		logger: logger,
	}
}

func (as *AnalyticsService) UpdateAnalytics(
	ctx context.Context,
	clusterId string,
) {
	stack := []string{clusterId}
	var clusterToSucceedingClusters map[string][]string
	visitedClusters := map[string]bool{clusterId: true}
	for {
		if len(stack) == 0 {
			break
		}
		currentClusterId := stack[0]
		stack = stack[1:]
		relatedClusters, err := as.getRelatedClusters(ctx, clusterId)
		if err != nil {
			as.logger.Error("failed to get related clusters", zap.Error(err))
			return
		}
		for _, cluster := range relatedClusters {
			if cluster.MeanTDOA < 0 {
				clusterToSucceedingClusters[cluster.ClusterId] =
					append(clusterToSucceedingClusters[cluster.ClusterId], currentClusterId)
			} else {
				clusterToSucceedingClusters[cluster.ClusterId] =
					append(clusterToSucceedingClusters[cluster.ClusterId], cluster.ClusterId)
			}
			if _, ok := visitedClusters[cluster.ClusterId]; !ok {
				stack = append(stack, cluster.ClusterId)
				visitedClusters[cluster.ClusterId] = true
			}
		}
	}
	metaUpdate, documentUpdate := getAnalyticsUpdate(clusterToSucceedingClusters)
}

func (as *AnalyticsService) getRelatedClusters(
	ctx context.Context,
	clusterId string,
) ([]Cluster, error) {
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

func parseClusters(docs []map[string]interface{}) ([]Cluster, error) {
	clusters := make([]Cluster, 0)
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
		cluster := Cluster{
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
