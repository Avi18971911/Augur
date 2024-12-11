package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/inference/service"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestUpdateAnalytics(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Immediate)
	as := service.NewAnalyticsService(ac, logger)

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
		meta, documents, err := as.UpdateAnalytics(context.Background(), "1")
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}
		updateCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var index = bootstrapper.ClusterIndexName
		defer cancel()
		err = ac.BulkIndex(updateCtx, meta, documents, &index)
		if err != nil {
			t.Errorf("Failed to bulk index: %v", err)
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
		meta, documents, err := as.UpdateAnalytics(context.Background(), "1")
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}
		updateCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var index = bootstrapper.ClusterIndexName
		defer cancel()
		err = ac.BulkIndex(updateCtx, meta, documents, &index)
		if err != nil {
			t.Errorf("Failed to bulk index: %v", err)
		}

		err = deleteAllDocuments(es)
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
		meta, documents, err = as.UpdateAnalytics(context.Background(), "1")
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}
		err = ac.BulkIndex(updateCtx, meta, documents, &index)
		if err != nil {
			t.Errorf("Failed to bulk index: %v", err)
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
