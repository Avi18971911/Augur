package elasticsearch

import (
	"github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/Avi18971911/Augur/pkg/log/service"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCount(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := elasticsearch.NewAugurClientImpl(es)
	countService := service.NewCountService(ac, logger)
	t.Run("should be able to count co-occurrences within the smallest bucket", func(t *testing.T) {
		numInitialLogs := 1
		numWithinBucket := 4
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		logsOfInitialClusterId := makeLogsOfSameClusterId("initialClusterId", initialTime, numInitialLogs)
		logsOfDifferentTime := makeLogsOfSameClusterId("differentTime", initialTime.Add(time.Second), numWithinBucket)
		logsOfDifferentTime = append(
			logsOfDifferentTime,
			makeLogsOfSameClusterId(
				"differentTime",
				initialTime.Add(time.Second*2),
				numWithinBucket,
			)...,
		)
		err := loadLogsIntoElasticsearch(ac, logsOfInitialClusterId)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadLogsIntoElasticsearch(ac, logsOfDifferentTime)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		time.Sleep(1 * time.Second)
		buckets := []service.Bucket{2500}
		countInfo, err := countService.CountOccurrences("initialClusterId", buckets)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		relevantCountInfo := countInfo["differentTime"]
		assert.Equal(t, int64(numInitialLogs), relevantCountInfo.Occurrences)
		assert.Equal(t, int64(numWithinBucket*2), relevantCountInfo.CoOccurrences)
		err = deleteAllDocuments(es, "log_index")
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
	})
}

func makeLogsOfSameClusterId(clusterId string, timestamp time.Time, numberOfLogs int) []model.LogEntry {
	logs := make([]model.LogEntry, numberOfLogs)
	for i := 0; i < numberOfLogs; i++ {
		logs[i] = model.LogEntry{
			ClusterId: clusterId,
			Timestamp: timestamp,
		}
	}
	return logs
}

func loadLogsIntoElasticsearch(ac elasticsearch.AugurClient, logs []model.LogEntry) error {
	genericInput := make([]interface{}, len(logs))
	for i, log := range logs {
		genericInput[i] = log
	}
	err := ac.BulkIndex(genericInput, nil, "log_index")
	if err != nil {
		return err
	}
	return nil
}
