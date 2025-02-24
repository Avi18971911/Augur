package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/otel_server/log/helper"
	"github.com/Avi18971911/Augur/internal/pipeline/analytics/service"
	clusterService "github.com/Avi18971911/Augur/internal/pipeline/cluster/service"
	totalCountModel "github.com/Avi18971911/Augur/internal/pipeline/cluster_count/model"
	countService "github.com/Avi18971911/Augur/internal/pipeline/cluster_count/service"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
)

const createdAt = "2021-08-01T00:00:00.000Z"

func TestUpdateAnalytics(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Immediate)
	logger, err := zap.NewProduction()
	if err != nil {
		t.Errorf("Failed to create logger: %v", err)
	}
	as := service.NewAnalyticsService(
		ac,
		logger,
	)

	bucket := totalCountModel.Bucket(1000 * 30)
	indices := []string{bootstrapper.LogIndexName}
	cls := clusterService.NewClusterService(ac, logger)
	wc := countService.NewClusterWindowCountService(ac, 50, logger)
	cs := countService.NewClusterTotalCountService(ac, wc, logger)
	cdp := clusterService.NewClusterDataProcessor(ac, cls, logger)
	csp := countService.NewCountDataProcessorService(ac, cs, bucket, indices, logger)

	t.Run("should insert the entire graph if it doesn't exist", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}

		totalCountInput := []totalCountModel.ClusterTotalCountEntry{
			{
				CreatedAt:                   createdAt,
				ClusterId:                   "2",
				CoClusterId:                 "1",
				TotalInstances:              7,
				TotalInstancesWithCoCluster: 5,
			},
			{
				CreatedAt:                   createdAt,
				ClusterId:                   "3",
				CoClusterId:                 "2",
				TotalInstances:              12,
				TotalInstancesWithCoCluster: 10,
			},
			{
				CreatedAt:                   createdAt,
				ClusterId:                   "4",
				CoClusterId:                 "1",
				TotalInstances:              24,
				TotalInstancesWithCoCluster: 18,
			},
		}

		windowCountInput := []totalCountModel.ClusterWindowCountEntry{
			{
				ClusterId:    "2",
				CoClusterId:  "1",
				Start:        0,
				End:          1,
				Occurrences:  7,
				MeanTDOA:     5.0,
				VarianceTDOA: 0.0,
			},
			{
				ClusterId:    "3",
				CoClusterId:  "2",
				Start:        0,
				End:          1,
				Occurrences:  12,
				MeanTDOA:     5.0,
				VarianceTDOA: 0.0,
			},
			{
				ClusterId:    "4",
				CoClusterId:  "1",
				Start:        0,
				End:          1,
				Occurrences:  24,
				MeanTDOA:     -5.0,
				VarianceTDOA: 0.0,
			},
		}

		err = loadDataIntoElasticsearch(ac, totalCountInput, bootstrapper.ClusterTotalCountIndexName)
		assert.NoError(t, err)
		err = loadDataIntoElasticsearch(ac, windowCountInput, bootstrapper.ClusterWindowCountIndexName)
		assert.NoError(t, err)
		err = as.UpdateAnalytics(context.Background(), []string{"1"})
		assert.NoError(t, err)
		queryString, err := json.Marshal(getAllQuery())
		assert.NoError(t, err)
		allClusterDocs, err := ac.Search(
			context.Background(),
			string(queryString),
			[]string{bootstrapper.ClusterGraphNodeIndexName},
			nil,
		)
		if err != nil {
			t.Errorf("Failed to search: %v", err)
		}
		clusters, err := convertClusterDocs(allClusterDocs)
		if err != nil {
			t.Errorf("Failed to convert cluster docs: %v", err)
		}
		assert.Equal(t, 4, len(clusters))
		var clusterOneCauses, clusterTwoCauses, clusterThreeCauses, clusterFourCauses []string
		for _, cluster := range clusters {
			if cluster.ClusterId == "1" {
				clusterOneCauses = cluster.CausedClusters
			} else if cluster.ClusterId == "2" {
				clusterTwoCauses = cluster.CausedClusters
			} else if cluster.ClusterId == "3" {
				clusterThreeCauses = cluster.CausedClusters
			} else if cluster.ClusterId == "4" {
				clusterFourCauses = cluster.CausedClusters
			}
		}
		assert.ElementsMatch(t, []string{"4"}, clusterOneCauses)
		assert.ElementsMatch(t, []string{"1"}, clusterTwoCauses)
		assert.ElementsMatch(t, []string{"2"}, clusterThreeCauses)
		assert.ElementsMatch(t, []string{}, clusterFourCauses)
	})

	t.Run("Should update and prune invalid edges", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}

		countInput := []totalCountModel.ClusterTotalCountEntry{
			{
				CreatedAt:                   createdAt,
				ClusterId:                   "2",
				CoClusterId:                 "1",
				TotalInstances:              7,
				TotalInstancesWithCoCluster: 5,
			},
			{
				CreatedAt:                   createdAt,
				ClusterId:                   "3",
				CoClusterId:                 "2",
				TotalInstances:              12,
				TotalInstancesWithCoCluster: 10,
			},
			{
				CreatedAt:                   createdAt,
				ClusterId:                   "4",
				CoClusterId:                 "1",
				TotalInstances:              24,
				TotalInstancesWithCoCluster: 18,
			},
		}

		windowCountInput := []totalCountModel.ClusterWindowCountEntry{
			{
				ClusterId:    "2",
				CoClusterId:  "1",
				Start:        0,
				End:          1,
				Occurrences:  7,
				MeanTDOA:     5.0,
				VarianceTDOA: 0.0,
			},
			{
				ClusterId:    "3",
				CoClusterId:  "2",
				Start:        0,
				End:          1,
				Occurrences:  12,
				MeanTDOA:     5.0,
				VarianceTDOA: 0.0,
			},
			{
				ClusterId:    "4",
				CoClusterId:  "1",
				Start:        0,
				End:          1,
				Occurrences:  24,
				MeanTDOA:     -5.0,
				VarianceTDOA: 0.0,
			},
		}

		err = loadDataIntoElasticsearch(ac, countInput, bootstrapper.ClusterTotalCountIndexName)
		assert.NoError(t, err)
		err = loadDataIntoElasticsearch(ac, windowCountInput, bootstrapper.ClusterWindowCountIndexName)
		assert.NoError(t, err)
		err = as.UpdateAnalytics(context.Background(), []string{"1"})
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}

		err = deleteAllDocumentsFromIndex(es, bootstrapper.ClusterTotalCountIndexName)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		newCountInput := []totalCountModel.ClusterTotalCountEntry{
			{
				CreatedAt:                   createdAt,
				ClusterId:                   "2",
				CoClusterId:                 "1",
				TotalInstances:              58,
				TotalInstancesWithCoCluster: 5,
			},
			{
				CreatedAt:                   createdAt,
				ClusterId:                   "3",
				CoClusterId:                 "2",
				TotalInstances:              49,
				TotalInstancesWithCoCluster: 10,
			},
			{
				CreatedAt:                   createdAt,
				ClusterId:                   "4",
				CoClusterId:                 "1",
				TotalInstances:              47,
				TotalInstancesWithCoCluster: 18,
			},
		}
		newWindowCountInput := []totalCountModel.ClusterWindowCountEntry{
			{
				ClusterId:    "2",
				CoClusterId:  "1",
				Start:        0,
				End:          1,
				Occurrences:  58,
				MeanTDOA:     5.0,
				VarianceTDOA: 0.0,
			},
			{
				ClusterId:    "3",
				CoClusterId:  "2",
				Start:        0,
				End:          1,
				Occurrences:  49,
				MeanTDOA:     5.0,
				VarianceTDOA: 0.0,
			},
			{
				ClusterId:    "4",
				CoClusterId:  "1",
				Start:        0,
				End:          1,
				Occurrences:  47,
				MeanTDOA:     -5.0,
				VarianceTDOA: 0.0,
			},
		}
		err = loadDataIntoElasticsearch(ac, newCountInput, bootstrapper.ClusterTotalCountIndexName)
		assert.NoError(t, err)
		err = loadDataIntoElasticsearch(ac, newWindowCountInput, bootstrapper.ClusterWindowCountIndexName)
		assert.NoError(t, err)
		err = as.UpdateAnalytics(context.Background(), []string{"1"})
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}

		queryString, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("Failed to marshal query: %v", err)
		}
		allClusterDocs, err := ac.Search(
			context.Background(),
			string(queryString),
			[]string{bootstrapper.ClusterGraphNodeIndexName},
			nil,
		)
		if err != nil {
			t.Errorf("Failed to search: %v", err)
		}
		clusters, err := convertClusterDocs(allClusterDocs)
		if err != nil {
			t.Errorf("Failed to convert cluster docs: %v", err)
		}
		assert.Equal(t, 4, len(clusters))
		var clusterOneCauses, clusterTwoCauses, clusterThreeCauses, clusterFourCauses []string
		for _, cluster := range clusters {
			if cluster.ClusterId == "1" {
				clusterOneCauses = cluster.CausedClusters
			} else if cluster.ClusterId == "2" {
				clusterTwoCauses = cluster.CausedClusters
			} else if cluster.ClusterId == "3" {
				clusterThreeCauses = cluster.CausedClusters
			} else if cluster.ClusterId == "4" {
				clusterFourCauses = cluster.CausedClusters
			}
		}
		assert.ElementsMatch(t, []string{}, clusterOneCauses)
		assert.ElementsMatch(t, []string{"1"}, clusterTwoCauses)
		assert.ElementsMatch(t, []string{"2"}, clusterThreeCauses)
		assert.ElementsMatch(t, []string{}, clusterFourCauses)
	})

	t.Run("Should work in a difficult real-world scenario and produce no cycles", func(t *testing.T) {
		err := deleteAllDocuments(es)
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.LogIndexName, "data/difficult_inference/log_index.json")
		assert.NoError(t, err)

		const maxLogsInChain = 8
		stringQuery, err := json.Marshal(getAllQuery())
		assert.NoError(t, err)
		var querySize = 10000
		logDocs, err := ac.Search(
			context.Background(),
			string(stringQuery),
			[]string{bootstrapper.LogIndexName},
			&querySize,
		)
		logs, err := helper.ConvertFromDocuments(logDocs)
		spanOrLogData := convertLogToSpanOrLogData(logs)
		clusterOutput, err := cdp.ClusterData(context.Background(), spanOrLogData)
		assert.NoError(t, err)
		countOutput, err := csp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterOutput)
		assert.NoError(t, err)

		clusterIdsSet := make(map[string]bool)
		for _, cluster := range countOutput {
			clusterIdsSet[cluster] = true
		}

		clusterIds := make([]string, 0, len(clusterIdsSet))
		for clusterId := range clusterIdsSet {
			clusterIds = append(clusterIds, clusterId)
		}
		err = as.UpdateAnalytics(context.Background(), clusterIds)
		assert.NoError(t, err)

		queryString, err := json.Marshal(getAllQuery())
		assert.NoError(t, err)
		allClusterDocs, err := ac.Search(
			context.Background(),
			string(queryString),
			[]string{bootstrapper.ClusterGraphNodeIndexName},
			nil,
		)
		assert.NoError(t, err)
		clusters, err := convertClusterDocs(allClusterDocs)
		assert.NoError(t, err)
		assert.Equal(t, len(clusterIds), len(clusters))
		graph := make(map[string]map[string]bool)
		causesClustersMap := make(map[int]bool)
		// check for a basic cycle
		for _, cluster := range clusters {
			causesClustersMap[len(cluster.CausedClusters)] = true
			graph[cluster.ClusterId] = make(map[string]bool)
			for _, causedCluster := range cluster.CausedClusters {
				graph[cluster.ClusterId][causedCluster] = true
				assert.False(t, graph[causedCluster][cluster.ClusterId])
				if _, ok := graph[causedCluster][cluster.ClusterId]; ok {
					logger.Error(
						"Cycle detected",
						zap.String("cluster", cluster.ClusterId),
						zap.String("caused_cluster", causedCluster),
					)
				}
			}
		}
		// ensure that there is at least 1 of each cluster causing 'n' number of other clusters
		for i := range maxLogsInChain {
			assert.True(t, causesClustersMap[i])
		}
	})
}

type AnalyticsCluster struct {
	ClusterId      string
	CausedClusters []string
}

func convertClusterDocs(docs []map[string]interface{}) ([]AnalyticsCluster, error) {
	clusters := make([]AnalyticsCluster, len(docs))
	for i, doc := range docs {
		clusterId, ok := doc["_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert cluster_id to string: %v", doc)
		}
		causesClusters, ok := doc["causes_clusters"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to convert causes_clusters to []string: %v", doc)
		}
		stringCausesClusters := make([]string, len(causesClusters))
		for j, causeCluster := range causesClusters {
			stringCauseCluster, ok := causeCluster.(string)
			if !ok {
				return nil, fmt.Errorf("failed to convert cause_cluster to string: %v", doc)
			}
			stringCausesClusters[j] = stringCauseCluster
		}
		clusters[i] = AnalyticsCluster{
			ClusterId:      clusterId,
			CausedClusters: stringCausesClusters,
		}
	}
	return clusters, nil
}
