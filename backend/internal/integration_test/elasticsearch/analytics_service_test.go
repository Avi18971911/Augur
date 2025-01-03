package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/pipeline/analytics/service"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
)

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

	t.Run("should insert the entire graph if it doesn't exist", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}

		countInput := []AnalyticsTestCluster{
			{
				ClusterId:     "2",
				CoClusterId:   "1",
				Occurrences:   7,
				CoOccurrences: 5,
				MeanTDOA:      5.0,
				VarianceTDOA:  0.0,
			},
			{
				ClusterId:     "3",
				CoClusterId:   "2",
				Occurrences:   12,
				CoOccurrences: 10,
				MeanTDOA:      5.0,
				VarianceTDOA:  0.0,
			},
			{
				ClusterId:     "4",
				CoClusterId:   "1",
				Occurrences:   24,
				CoOccurrences: 18,
				MeanTDOA:      -5.0,
				VarianceTDOA:  0.0,
			},
		}
		err = loadDataIntoElasticsearch(ac, countInput, bootstrapper.CountIndexName)
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
			[]string{bootstrapper.ClusterIndexName},
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

		countInput := []AnalyticsTestCluster{
			{
				ClusterId:     "2",
				CoClusterId:   "1",
				Occurrences:   7,
				CoOccurrences: 5,
				MeanTDOA:      5.0,
				VarianceTDOA:  0.0,
			},
			{
				ClusterId:     "3",
				CoClusterId:   "2",
				Occurrences:   12,
				CoOccurrences: 10,
				MeanTDOA:      5.0,
				VarianceTDOA:  0.0,
			},
			{
				ClusterId:     "4",
				CoClusterId:   "1",
				Occurrences:   24,
				CoOccurrences: 18,
				MeanTDOA:      -5.0,
				VarianceTDOA:  0.0,
			},
		}
		err = loadDataIntoElasticsearch(ac, countInput, bootstrapper.CountIndexName)
		err = as.UpdateAnalytics(context.Background(), []string{"1"})
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}

		err = deleteAllDocumentsFromIndex(es, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		newCountInput := []AnalyticsTestCluster{
			{
				ClusterId:     "2",
				CoClusterId:   "1",
				Occurrences:   58,
				CoOccurrences: 5,
				MeanTDOA:      5.0,
				VarianceTDOA:  0.0,
			},
			{
				ClusterId:     "3",
				CoClusterId:   "2",
				Occurrences:   49,
				CoOccurrences: 10,
				MeanTDOA:      5.0,
				VarianceTDOA:  0.0,
			},
			{
				ClusterId:     "4",
				CoClusterId:   "1",
				Occurrences:   47,
				CoOccurrences: 18,
				MeanTDOA:      -5.0,
				VarianceTDOA:  0.0,
			},
		}
		err = loadDataIntoElasticsearch(ac, newCountInput, bootstrapper.CountIndexName)
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
			[]string{bootstrapper.ClusterIndexName},
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
		err = loadTestDataFromFile(es, bootstrapper.CountIndexName, "data/difficult_inference/count_index.json")
		assert.NoError(t, err)

		clusterIds := []string{
			"15d28b23-8ac5-4097-9292-62294611d2b0",
			"df3d8241-5da4-48e0-b5dc-fcd33c1744de",
			"8cb0cdfd-5ddd-463e-ae61-e0e1efd1978e",
			"593bdd0c-eba9-4878-a6f6-86a59ce78edb",
			"5fc97189-716d-4d79-a157-b6a8261c501e",
			"7ce1d5fb-1349-4a41-8181-d54e3e69a1ea",
			"009c18b9-afd1-40cc-8d83-fd810a5ce4ce",
			"2d0ea4ac-61ce-439d-8b83-4804557f523e",
		}
		err = as.UpdateAnalytics(context.Background(), clusterIds)
		assert.NoError(t, err)

		queryString, err := json.Marshal(getAllQuery())
		assert.NoError(t, err)
		allClusterDocs, err := ac.Search(
			context.Background(),
			string(queryString),
			[]string{bootstrapper.ClusterIndexName},
			nil,
		)
		assert.NoError(t, err)
		clusters, err := convertClusterDocs(allClusterDocs)
		assert.NoError(t, err)
		assert.Equal(t, 8, len(clusters))
		graph := make(map[string]map[string]bool)
		for _, cluster := range clusters {
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
	})
}

type AnalyticsTestCluster struct {
	ClusterId     string  `json:"cluster_id"`
	CoClusterId   string  `json:"co_cluster_id"`
	Occurrences   int64   `json:"occurrences"`
	CoOccurrences int64   `json:"co_occurrences"`
	MeanTDOA      float64 `json:"mean_TDOA"`
	VarianceTDOA  float64 `json:"variance_TDOA"`
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
