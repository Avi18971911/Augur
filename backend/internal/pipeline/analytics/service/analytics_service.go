package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	totalCountModel "github.com/Avi18971911/Augur/internal/pipeline/cluster_count/model"
	"github.com/Avi18971911/Augur/internal/pipeline/cluster_count/service"
	"github.com/Avi18971911/Augur/internal/query_server/service/inference/model"
	"go.uber.org/zap"
	"time"
)

const timeout = 10 * time.Second
const minimumRatio = 0.6
const querySize = 1000

type AnalyticsService struct {
	ac     client.AugurClient
	logger *zap.Logger
}

func NewAnalyticsService(
	ac client.AugurClient,
	logger *zap.Logger,
) *AnalyticsService {
	return &AnalyticsService{
		ac:     ac,
		logger: logger,
	}
}

func (as *AnalyticsService) UpdateAnalytics(
	ctx context.Context,
	clusterIdsToAnalyze []string,
) error {
	clusterToSucceedingClusters := make(map[string]map[string]bool)
	visitedClusters := map[string]bool{}
	for _, clusterId := range clusterIdsToAnalyze {
		if _, ok := visitedClusters[clusterId]; ok {
			continue
		}
		visitedClusters[clusterId] = true
		stack := []string{clusterId}
		for {
			if len(stack) == 0 {
				break
			}
			currentClusterId := stack[0]
			stack = stack[1:]
			// always create a new map for the current cluster to make sure it gets added later even if there are no related clusters
			if _, ok := clusterToSucceedingClusters[currentClusterId]; !ok {
				clusterToSucceedingClusters[currentClusterId] = make(map[string]bool)
			}
			relatedClusters, err := as.getRelatedClusters(ctx, currentClusterId)
			if err != nil {
				return fmt.Errorf("failed to get related clusters: %w", err)
			}
			for _, relatedCluster := range relatedClusters {
				// MeanTDOA > 0 means relatedCluster -> currentCluster w.r.t. causation
				if relatedCluster.MeanTDOA > 0 {
					if _, ok := clusterToSucceedingClusters[relatedCluster.ClusterId]; !ok {
						clusterToSucceedingClusters[relatedCluster.ClusterId] = map[string]bool{currentClusterId: true}
					} else {
						clusterToSucceedingClusters[relatedCluster.ClusterId][currentClusterId] = true
					}
				} else {
					clusterToSucceedingClusters[currentClusterId][relatedCluster.ClusterId] = true
				}
				if _, ok := visitedClusters[relatedCluster.ClusterId]; !ok {
					stack = append(stack, relatedCluster.ClusterId)
					visitedClusters[relatedCluster.ClusterId] = true
				}
			}
		}
	}
	as.pruneCycleClusters(clusterToSucceedingClusters)
	metaUpdate, documentUpdate := getAnalyticsUpdateStatement(clusterToSucceedingClusters)
	updateCtx, cancel := context.WithTimeout(ctx, timeout)
	err := as.ac.BulkUpdate(updateCtx, metaUpdate, documentUpdate, bootstrapper.ClusterGraphNodeIndexName)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to bulk update analytics: %w", err)
	}
	return nil
}

func (as *AnalyticsService) pruneCycleClusters(clusterToSucceedingClusters map[string]map[string]bool) {
	if len(clusterToSucceedingClusters) == 0 {
		return
	}
	visitedClusters := make(map[string]bool)
	inCurrentPath := make(map[string]bool)

	for startCluster := range clusterToSucceedingClusters {
		if visitedClusters[startCluster] {
			continue
		}
		type StackFrame struct {
			clusterID     string
			successorIter []string
		}
		stack := []StackFrame{
			{
				clusterID:     startCluster,
				successorIter: getKeys(clusterToSucceedingClusters[startCluster]),
			},
		}
		inCurrentPath[startCluster] = true

		for len(stack) > 0 {
			frame := &stack[len(stack)-1]
			currentCluster := frame.clusterID

			if len(frame.successorIter) == 0 {
				stack = stack[:len(stack)-1]
				delete(inCurrentPath, currentCluster)
				continue
			}

			nextSuccessor := frame.successorIter[0]
			frame.successorIter = frame.successorIter[1:]

			if inCurrentPath[nextSuccessor] {
				delete(clusterToSucceedingClusters[currentCluster], nextSuccessor)
				continue
			}

			if visitedClusters[nextSuccessor] {
				continue
			}

			inCurrentPath[nextSuccessor] = true
			visitedClusters[nextSuccessor] = true

			stack = append(stack, StackFrame{
				clusterID:     nextSuccessor,
				successorIter: getKeys(clusterToSucceedingClusters[nextSuccessor]),
			})
		}
	}
}

func (as *AnalyticsService) getRelatedClusters(
	ctx context.Context,
	clusterId string,
) ([]model.CountCluster, error) {
	totalCountDetails, err := as.getRelatedClusterTotalCountDetails(ctx, clusterId)
	if err != nil {
		return nil, err
	}
	totalWindowDetails, err := as.getRelatedClusterWindowCountDetails(ctx, clusterId)
	if err != nil {
		return nil, err
	}

	clusters, err := mergeCountAndWindowDetails(totalCountDetails, totalWindowDetails)
	prunedClusters := pruneClusters(clusters)
	return prunedClusters, nil
}

func (as *AnalyticsService) getRelatedClusterTotalCountDetails(
	ctx context.Context,
	clusterId string,
) ([]totalCountModel.ClusterTotalCountEntry, error) {
	queryJSON, err := json.Marshal(buildGetRelatedClustersQuery(clusterId))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal get related cluster query: %w", err)
	}
	var localQuerySize = querySize
	searchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	docs, err := as.ac.Search(
		searchCtx,
		string(queryJSON),
		[]string{bootstrapper.ClusterTotalCountIndexName},
		&localQuerySize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search for related clusters: %w", err)
	}
	clusterTotalCountDetails, err := service.ConvertCountDocsToCountEntries(docs)
	if err != nil {
		return nil, fmt.Errorf("failed to convert count docs to count entries: %w", err)
	}
	return clusterTotalCountDetails, nil
}

func (as *AnalyticsService) getRelatedClusterWindowCountDetails(
	ctx context.Context,
	clusterId string,
) ([]totalCountModel.ClusterWindowCountEntry, error) {
	queryJSON, err := json.Marshal(buildGetRelatedClusterWindowsQuery(clusterId, querySize))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal get related cluster window query: %w", err)
	}
	var localQuerySize = querySize
	searchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	docs, err := as.ac.SearchAggregation(
		searchCtx,
		string(queryJSON),
		bootstrapper.ClusterWindowCountIndexName,
		&localQuerySize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search for related clusters: %w", err)
	}
	clusterWindowCountDetails, err := service.ConvertCountDocsToWindowCountEntries(docs)
	if err != nil {
		return nil, fmt.Errorf("failed to convert count docs to window count entries: %w", err)
	}
	return clusterWindowCountDetails, nil
}

func mergeCountAndWindowDetails(
	totalCountDetails []totalCountModel.ClusterTotalCountEntry,
	totalWindowDetails []totalCountModel.ClusterWindowCountEntry,
) ([]model.CountCluster, error) {
	clusterMap := make(map[string]model.CountCluster)
	for _, totalCountDetail := range totalCountDetails {
		clusterMap[totalCountDetail.ClusterId] = model.CountCluster{
			ClusterId:     totalCountDetail.ClusterId,
			Occurrences:   totalCountDetail.TotalInstances,
			CoOccurrences: totalCountDetail.TotalInstancesWithCoCluster,
		}
	}
	for _, totalWindowDetail := range totalWindowDetails {
		if cluster, ok := clusterMap[totalWindowDetail.ClusterId]; !ok {
			return nil, fmt.Errorf("cluster not found in total count details: %s", totalWindowDetail.ClusterId)
		} else {
			clusterMap[totalWindowDetail.ClusterId] = model.CountCluster{
				ClusterId:     cluster.ClusterId,
				Occurrences:   cluster.Occurrences,
				CoOccurrences: cluster.CoOccurrences,
				MeanTDOA:      totalWindowDetail.MeanTDOA,
				VarianceTDOA:  totalWindowDetail.VarianceTDOA,
			}
		}
	}
	clusters := make([]model.CountCluster, len(clusterMap))
	i := 0
	for _, cluster := range clusterMap {
		clusters[i] = cluster
		i++
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
	clusterToSucceedingClusters map[string]map[string]bool,
) ([]map[string]interface{}, []map[string]interface{}) {
	metaUpdates := make([]map[string]interface{}, len(clusterToSucceedingClusters))
	documentUpdates := make([]map[string]interface{}, len(clusterToSucceedingClusters))
	i := 0
	for clusterId, succeedingClusterIdsMap := range clusterToSucceedingClusters {
		metaUpdate := map[string]interface{}{
			"update": map[string]interface{}{
				"_id": clusterId,
			},
		}
		succeedingClusteridsList := make([]string, len(succeedingClusterIdsMap))
		j := 0
		for succeedingClusterId := range succeedingClusterIdsMap {
			succeedingClusteridsList[j] = succeedingClusterId
			j++
		}
		documentUpdate := map[string]interface{}{
			"doc": map[string]interface{}{
				"causes_clusters": succeedingClusteridsList,
			},
			"doc_as_upsert": true,
		}
		metaUpdates[i] = metaUpdate
		documentUpdates[i] = documentUpdate
		i++
	}
	return metaUpdates, documentUpdates
}

func getKeys(m map[string]bool) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}
