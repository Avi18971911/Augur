package elasticsearch

import (
	"context"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/Avi18971911/Augur/pkg/log/service"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestUpdates(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	logProcessor := service.NewLogClusterService(ac, logger)
	t.Run("should be able to process and update logs of the same type", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		err = loadTestDataFromFile(es, "log_index", "data_dump/log_index_array.json")
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		logEntry := model.LogEntry{
			Service: "kafka.cluster.Partition",
			Message: "[Partition __consumer_offsets-99 broker=2] Log loaded for partition " +
				"__consumer_offsets-55 with initial high watermark 0",
			Timestamp: time.Date(2021, 1, 1, 0, 0, 0, 8748, time.UTC),
			Id:        "test",
		}
		ids, docs, err := logProcessor.ClusterLog(ctx, logEntry)
		if err != nil {
			t.Errorf("Failed to parse log with message: %v", err)
		}
		assert.NotZero(t, len(docs))
		assert.NotEqual(t, 1, len(docs))
		var equalClusterId = docs[0]["cluster_id"]
		var testClusterId string
		if ids[0] == "test" {
			testClusterId = docs[0]["cluster_id"].(string)
		}
		for i, doc := range docs[1:] {
			assert.Equal(t, equalClusterId, doc["cluster_id"])
			if ids[i+1] == "test" {
				testClusterId = doc["cluster_id"].(string)
			}
		}
		assert.NotEqual(t, "", testClusterId)
	})
}
