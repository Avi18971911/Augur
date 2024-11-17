package elasticsearch

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/pkg/elasticsearch"
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
	ac := elasticsearch.NewAugurClientImpl(es, elasticsearch.Wait)
	logProcessor := service.NewLogProcessorService(ac, logger)
	t.Run("should be able to process and update logs of the same type", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		err = loadTestDataFromFile(es, "log_index", "data_dump/log_index_array.json")
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}
		logService := "kafka.cluster.Partition"
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		logEntry := model.LogEntry{
			Service: "kafka.cluster.Partition",
			Message: "[Partition __consumer_offsets-99 broker=2] Log loaded for partition " +
				"__consumer_offsets-55 with initial high watermark 0",
			Timestamp: time.Date(2021, 1, 1, 0, 0, 0, 8748, time.UTC),
		}
		newLog, err := logProcessor.ParseLogWithMessage(logService, logEntry, ctx)
		if err != nil {
			t.Errorf("Failed to parse log with message: %v", err)
		}
		assert.NotEqual(t, "", newLog.ClusterId)
		logsQuery := getLogsWithClusterIdQuery(newLog.ClusterId)
		var querySize = 100
		docs, err := ac.Search(ctx, logsQuery, "log_index", &querySize)
		logDocs, err := elasticsearch.ConvertToLogDocuments(docs)
		assert.Equal(t, 10, len(logDocs))
		for _, doc := range logDocs {
			assert.Equal(t, newLog.ClusterId, doc.ClusterId)
		}
	})
}

func getLogsWithClusterIdQuery(clusterId string) string {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"cluster_id": clusterId,
			},
		},
	}
	queryBody, err := json.Marshal(query)
	if err != nil {
		panic(err)
	}
	return string(queryBody)
}
