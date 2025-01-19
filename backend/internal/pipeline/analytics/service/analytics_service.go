package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	totalCountModel "github.com/Avi18971911/Augur/internal/pipeline/count/model"
	"github.com/Avi18971911/Augur/internal/pipeline/count/service"
	"github.com/Avi18971911/Augur/internal/query_server/service/inference/model"
	"go.uber.org/zap"
	"time"
)

const timeout = 10 * time.Second
const minimumRatio = 0.6
const querySize = 10000

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
		[]string{bootstrapper.ClusterTotalCountIndexName, bootstrapper.ClusterWindowCountIndexName},
		&localQuerySize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search for related clusters: %w", err)
	}
	clusters, err := ParseClusters(docs)
	prunedClusters := pruneClusters(clusters)
	return prunedClusters, nil
}

func ParseClusters(docs []map[string]interface{}) ([]model.CountCluster, error) {
	totalCountDocs := make(map[string]totalCountModel.ClusterTotalCountEntry)
	windowCountDocs := make(map[string][]totalCountModel.ClusterWindowCountEntry)
	for _, doc := range docs {
		if docIsTotalCount(doc) {
			resultList, err := service.ConvertCountDocsToCountEntries([]map[string]interface{}{doc})
			if err != nil {
				return nil, fmt.Errorf("failed to convert count docs to count entries: %w", err)
			}
			totalCountDoc := resultList[0]
			totalCountDocs[totalCountDoc.ClusterId] = totalCountDoc
		} else {
			resultList, err := service.ConvertCountDocsToWindowCountEntries([]map[string]interface{}{doc})
			if err != nil {
				return nil, fmt.Errorf("failed to convert count docs to window count entries: %w", err)
			}
			windowCountDoc := resultList[0]
			if _, ok := windowCountDocs[windowCountDoc.ClusterId]; !ok {
				windowCountDocs[windowCountDoc.ClusterId] = make([]totalCountModel.ClusterWindowCountEntry, 0)
			}
			windowCountDocs[windowCountDoc.ClusterId] = append(
				windowCountDocs[windowCountDoc.ClusterId],
				windowCountDoc,
			)
		}
	}
	clusters, err := FindMostProbableMatchingWindow(totalCountDocs, windowCountDocs)
	if err != nil {
		return nil, fmt.Errorf("failed to find most probable matching window: %w", err)
	}
	return clusters, nil
}

func FindMostProbableMatchingWindow(
	totalCountDocs map[string]totalCountModel.ClusterTotalCountEntry,
	windowCountDocs map[string][]totalCountModel.ClusterWindowCountEntry,
) ([]model.CountCluster, error) {
	clusters := make([]model.CountCluster, 0, len(totalCountDocs))
	for clusterId, totalCountDoc := range totalCountDocs {
		windowCountDoc, ok := windowCountDocs[clusterId]
		if !ok {
			continue
		}
		if len(windowCountDoc) == 0 {
			return nil, fmt.Errorf("no window count docs found for cluster: %s", clusterId)
		}
		maxIndexOfOccurrences := 0
		for i, windowDoc := range windowCountDoc {
			if windowDoc.Occurrences > windowCountDoc[maxIndexOfOccurrences].Occurrences {
				maxIndexOfOccurrences = i
			}
		}
		clusters = append(clusters, model.CountCluster{
			ClusterId:     clusterId,
			Occurrences:   totalCountDoc.TotalInstances,
			CoOccurrences: totalCountDoc.TotalInstancesWithCoCluster,
			MeanTDOA:      windowCountDoc[maxIndexOfOccurrences].MeanTDOA,
			VarianceTDOA:  windowCountDoc[maxIndexOfOccurrences].VarianceTDOA,
		})
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

func docIsTotalCount(doc map[string]interface{}) bool {
	_, ok := doc["total_instances"]
	return ok
}
