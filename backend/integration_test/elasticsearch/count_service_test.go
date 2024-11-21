package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/Avi18971911/Augur/pkg/log/model"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLogCount(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	var querySize = 100

	ac := elasticsearch.NewAugurClientImpl(es, elasticsearch.Wait)
	cs := countService.NewCountService(ac, logger)
	t.Run("should be able to count co-occurrences within the smallest bucket", func(t *testing.T) {
		err := deleteAllDocuments(es)
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
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog})
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, logsOfDifferentTime)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countService.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		countInfo, err := cs.CountOccurrencesAndCoOccurrencesByCoClusterId(
			ctx,
			newLog.ClusterId,
			countService.TimeInfo{LogInfo: &countService.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		relevantCountInfo := countInfo["differentTime"]
		assert.Equal(t, int64(len(logsOfDifferentTime)), relevantCountInfo.Occurrences)
		assert.Equal(t, int64(numWithinBucket), relevantCountInfo.CoOccurrences)
	})

	t.Run("should store new entries into the database if nothing else is there", func(t *testing.T) {
		err := deleteAllDocuments(es)
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
		err = loadDataIntoElasticsearch(ac, logsOfDifferentTime)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog})
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countService.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = cs.CountAndUpdateOccurrences(
			ctx,
			newLog.ClusterId,
			countService.TimeInfo{LogInfo: &countService.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{elasticsearch.CountIndexName}, &querySize)
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

	t.Run("should update existing entries in the database", func(t *testing.T) {
		err := deleteAllDocuments(es)
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
		err = loadDataIntoElasticsearch(ac, logsOfDifferentTime)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog})
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countService.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = cs.CountAndUpdateOccurrences(
			ctx,
			newLog.ClusterId,
			countService.TimeInfo{LogInfo: &countService.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		err = cs.CountAndUpdateOccurrences(
			ctx,
			newLog.ClusterId,
			countService.TimeInfo{LogInfo: &countService.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{elasticsearch.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		countEntry := countEntries[0]
		assert.Equal(t, int64(numWithinBucket*2), countEntry.Occurrences)
		assert.Equal(t, int64(numWithinBucket*2), countEntry.CoOccurrences)
	})
}

func TestSpanCount(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := elasticsearch.NewAugurClientImpl(es, elasticsearch.Wait)
	cs := countService.NewCountService(ac, logger)
	t.Run("should be able to count co-occurrences of logs within", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newSpan := spanModel.Span{
			EndTime:   initialTime.Add(time.Second * 2),
			StartTime: initialTime.Add(-time.Second * 2),
			ClusterId: "initialClusterId",
		}
		logsOfDifferentTime := makeLogsOfSameClusterId("differentTime", initialTime.Add(time.Second), numWithinBucket)
		logsOfDifferentTime = append(
			logsOfDifferentTime,
			makeLogsOfSameClusterId(
				"differentTime",
				initialTime.Add(time.Second*4),
				numWithinBucket,
			)...,
		)
		err = loadDataIntoElasticsearch(ac, []spanModel.Span{newSpan})
		if err != nil {
			t.Error("Failed to load span into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, logsOfDifferentTime)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countService.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		countInfo, err := cs.CountOccurrencesAndCoOccurrencesByCoClusterId(
			ctx,
			newSpan.ClusterId,
			countService.TimeInfo{
				SpanInfo: &countService.SpanInfo{
					FromTime: newSpan.StartTime, ToTime: newSpan.EndTime,
				},
			},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		relevantCountInfo := countInfo["differentTime"]
		assert.Equal(t, int64(len(logsOfDifferentTime)), relevantCountInfo.Occurrences)
		assert.Equal(t, int64(numWithinBucket), relevantCountInfo.CoOccurrences)
	})

	t.Run("should be able to count co-occurrences of overlapping spans", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newSpan := spanModel.Span{
			EndTime:   initialTime.Add(time.Second * 2),
			StartTime: initialTime.Add(-time.Second * 2),
			ClusterId: "initialClusterId",
		}
		overlappingSpans := makeSpansOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second),
			initialTime.Add(time.Second*4),
			numWithinBucket,
		)
		nonOverlappingSpans := makeSpansOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second*3),
			initialTime.Add(time.Second*9),
			numWithinBucket,
		)
		err = loadDataIntoElasticsearch(ac, []spanModel.Span{newSpan})
		if err != nil {
			t.Error("Failed to load span into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, append(overlappingSpans, nonOverlappingSpans...))
		if err != nil {
			t.Error("Failed to load overlapping spans into elasticsearch")
		}
		buckets := []countService.Bucket{1000}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		countInfo, err := cs.CountOccurrencesAndCoOccurrencesByCoClusterId(
			ctx,
			newSpan.ClusterId,
			countService.TimeInfo{
				SpanInfo: &countService.SpanInfo{
					FromTime: newSpan.StartTime, ToTime: newSpan.EndTime,
				},
			},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		relevantCountInfo := countInfo["differentTime"]
		assert.Equal(t, int64(len(overlappingSpans)+len(nonOverlappingSpans)), relevantCountInfo.Occurrences)
		assert.Equal(t, int64(len(overlappingSpans)), relevantCountInfo.CoOccurrences)
	})

	t.Run("should update existing entries in the database", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newSpan := spanModel.Span{
			EndTime:   initialTime.Add(time.Second * 2),
			StartTime: initialTime.Add(-time.Second * 2),
			ClusterId: "initialClusterId",
		}
		overlappingSpans := makeSpansOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second),
			initialTime.Add(time.Second*4),
			numWithinBucket,
		)
		nonOverlappingSpans := makeSpansOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second*3),
			initialTime.Add(time.Second*9),
			numWithinBucket,
		)
		err = loadDataIntoElasticsearch(ac, []spanModel.Span{newSpan})
		if err != nil {
			t.Error("Failed to load span into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, append(overlappingSpans, nonOverlappingSpans...))
		if err != nil {
			t.Error("Failed to load overlapping spans into elasticsearch")
		}
		buckets := []countService.Bucket{1000}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = cs.CountAndUpdateOccurrences(
			ctx,
			newSpan.ClusterId,
			countService.TimeInfo{
				SpanInfo: &countService.SpanInfo{
					FromTime: newSpan.StartTime, ToTime: newSpan.EndTime,
				},
			},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		err = cs.CountAndUpdateOccurrences(
			ctx,
			newSpan.ClusterId,
			countService.TimeInfo{
				SpanInfo: &countService.SpanInfo{
					FromTime: newSpan.StartTime, ToTime: newSpan.EndTime,
				},
			},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		searchQueryBody := countQuery(newSpan.ClusterId)
		var querySize = 100
		docs, err := ac.Search(ctx, searchQueryBody, []string{elasticsearch.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		countEntry := countEntries[0]
		assert.Equal(t, int64(len(overlappingSpans)+len(nonOverlappingSpans))*2, countEntry.Occurrences)
		assert.Equal(t, int64(len(overlappingSpans)*2), countEntry.CoOccurrences)
	})
}

func loadDataIntoElasticsearch[Data any](ac elasticsearch.AugurClient, data []Data) error {
	metaMap, dataMap, err := elasticsearch.ToMetaAndDataMap(data)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = ac.BulkIndex(ctx, dataMap, metaMap, elasticsearch.LogIndexName)
	if err != nil {
		return err
	}
	return nil
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

func makeSpansOfSameClusterId(
	clusterId string,
	startTime time.Time,
	endTime time.Time,
	numberOfSpans int,
) []spanModel.Span {
	spans := make([]spanModel.Span, numberOfSpans)
	for i := 0; i < numberOfSpans; i++ {
		spans[i] = spanModel.Span{
			ClusterId: clusterId,
			StartTime: startTime,
			EndTime:   endTime,
		}
	}
	return spans
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
