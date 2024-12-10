package elasticsearch

import (
	"context"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/inference/service"
	"go.uber.org/zap"
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
		updateCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		meta, documents, err := as.UpdateAnalytics(updateCtx, "1")
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}
		logger.Info("Meta", zap.Any("meta", meta), zap.Any("documents", documents))
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
