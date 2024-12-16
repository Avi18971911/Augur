package elasticsearch

import (
	"context"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logHelper "github.com/Avi18971911/Augur/pkg/log/helper"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSearchAfter(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Immediate)

	t.Run("should be able to paginate through log records", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		logSize := 30
		querySize := 10
		indices := []string{"log_index"}
		logs := make([]logModel.LogEntry, logSize)
		beginTime := time.Date(2021, 1, 1, 0, 0, 0, 32, time.UTC)
		for i := 0; i < logSize; i++ {
			logs[i] = logModel.LogEntry{
				CreatedAt: beginTime.Add(time.Duration(i) * time.Second),
				Timestamp: beginTime.Add(time.Duration(i) * time.Second),
			}
		}
		err = loadDataIntoElasticsearch(ac, logs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load data into Elasticsearch: %v", err)
		}
		searchCtx, searchCancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer searchCancel()
		channel := ac.SearchAfter(searchCtx, getAllQuery(), indices, nil, &querySize)
		var results []map[string]interface{}
		for result := range channel {
			if result.Error != nil {
				t.Errorf("Error in search after: %v", result.Error)
			} else if result.Success == nil {
				t.Error("Result is nil")
			} else {
				results = append(results, result.Success.Result...)
			}
		}
		assert.Equal(t, logSize, len(results))
		logDocs, err := logHelper.ConvertFromDocuments(results)
		if err != nil {
			t.Errorf("Failed to convert search results to log documents: %v", err)
		}
		for i, log := range logDocs {
			assert.Equal(t, logs[i].Timestamp, log.Timestamp)
			assert.Equal(t, logs[i].CreatedAt, log.CreatedAt)
		}
	})

	t.Run("should be able to continuously paginate with search after", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		logSize := 30
		cutOffSize := 10
		querySize := 10
		indices := []string{"log_index"}
		logs := make([]logModel.LogEntry, logSize)
		beginTime := time.Date(2021, 1, 1, 0, 0, 0, 32, time.UTC)
		for i := 0; i < logSize; i++ {
			logs[i] = logModel.LogEntry{
				CreatedAt: beginTime.Add(time.Duration(i) * time.Second),
				Timestamp: beginTime.Add(time.Duration(i) * time.Second),
			}
		}
		err = loadDataIntoElasticsearch(ac, logs[:cutOffSize], bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load data into Elasticsearch: %v", err)
		}
		searchCtx, searchCancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer searchCancel()
		channel := ac.SearchAfter(searchCtx, getAllQuery(), indices, nil, &querySize)
		var results []map[string]interface{}
		for result := range channel {
			if result.Error != nil {
				t.Errorf("Error in search after: %v", result.Error)
			} else if result.Success == nil {
				t.Error("Result is nil")
			} else {
				results = append(results, result.Success.Result...)
			}
		}
		lastCreatedAt := results[len(results)-1]["created_at"].(string)
		lastCreatedAtTime, err := client.NormalizeTimestampToNanoseconds(lastCreatedAt)
		if err != nil {
			t.Errorf("Failed to convert timestamp '%s' to time.Time: %v", lastCreatedAt, err)
		}
		err = loadDataIntoElasticsearch(ac, logs[cutOffSize:], bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load data into Elasticsearch: %v", err)
		}
		newSearchParams := &client.SearchAfterParams{
			CreatedAt: lastCreatedAtTime,
		}
		channel = ac.SearchAfter(searchCtx, getAllQuery(), indices, newSearchParams, &querySize)
		for result := range channel {
			if result.Error != nil {
				t.Errorf("Error in search after: %v", result.Error)
			} else if result.Success == nil {
				t.Error("Result is nil")
			} else {
				results = append(results, result.Success.Result...)
			}
		}
		assert.Equal(t, logSize, len(results))
		logDocs, err := logHelper.ConvertFromDocuments(results)
		if err != nil {
			t.Errorf("Failed to convert search results to log documents: %v", err)
		}
		for i, log := range logDocs {
			assert.Equal(t, logs[i].Timestamp, log.Timestamp)
			assert.Equal(t, logs[i].CreatedAt, log.CreatedAt)
		}
	})

}
