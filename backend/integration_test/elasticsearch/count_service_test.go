package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
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

	ac := elasticsearch.NewAugurClientImpl(es, elasticsearch.Wait)
	countService := service.NewCountService(ac, logger)
	t.Run("should be able to count co-occurrences within the smallest bucket", func(t *testing.T) {
		err := deleteAllDocuments(es, elasticsearch.LogIndexName)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: initialTime,
		}
		logsOfDifferentTime := makeLogsOfSameClusterId("differentTime", initialTime.Add(time.Second), numWithinBucket)
		logsOfDifferentTime = append(
			logsOfDifferentTime,
			makeLogsOfSameClusterId(
				"differentTime",
				initialTime.Add(time.Second*2),
				numWithinBucket,
			)...,
		)
		err = loadLogsIntoElasticsearch(ac, []model.LogEntry{newLog})
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadLogsIntoElasticsearch(ac, logsOfDifferentTime)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []service.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		countInfo, err := countService.CountOccurrencesAndCoOccurrencesByCoClusterId(ctx, newLog, buckets)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		relevantCountInfo := countInfo["differentTime"]
		assert.Equal(t, int64(len(logsOfDifferentTime)), relevantCountInfo.Occurrences)
		assert.Equal(t, int64(numWithinBucket), relevantCountInfo.CoOccurrences)
		err = deleteAllDocuments(es, elasticsearch.LogIndexName)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
	})

	t.Run("should store new entries into the database if nothing else is there", func(t *testing.T) {
		err := deleteAllDocuments(es, elasticsearch.LogIndexName)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: initialTime,
		}
		logsOfDifferentTime := makeLogsOfSameClusterId("differentTime", initialTime.Add(time.Second), numWithinBucket)
		err = loadLogsIntoElasticsearch(ac, logsOfDifferentTime)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadLogsIntoElasticsearch(ac, []model.LogEntry{newLog})
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []service.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = countService.CountAndUpdateOccurrences(ctx, newLog, buckets)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, elasticsearch.CountIndexName, 100)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		countEntry := countEntries[0]
		assert.Equal(t, int64(numWithinBucket), countEntry.Occurrences)
		assert.Equal(t, int64(numWithinBucket), countEntry.CoOccurrences)
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i, log := range logs {
		genericInput[i] = log
	}
	err := ac.BulkIndex(ctx, genericInput, nil, elasticsearch.LogIndexName)
	if err != nil {
		return err
	}
	return nil
}

func countQuery(clusterId string) string {
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

func convertCountDocsToCountEntries(docs []map[string]interface{}) ([]countModel.CountEntry, error) {
	var countEntries []countModel.CountEntry
	for _, doc := range docs {
		countEntry := countModel.CountEntry{}
		coClusterId, ok := doc["co_cluster_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert co_cluster_id to string")
		}
		countEntry.CoClusterId = coClusterId
		clusterId, ok := doc["cluster_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert cluster_id to string")
		}
		countEntry.ClusterId = clusterId
		occurrences, ok := doc["occurrences"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert occurrences to int")
		}
		countEntry.Occurrences = int64(occurrences)
		coOccurrences, ok := doc["co_occurrences"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert co_occurrences to int")
		}
		countEntry.CoOccurrences = int64(coOccurrences)
		countEntries = append(countEntries, countEntry)
	}
	return countEntries, nil
}
