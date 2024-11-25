package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/log/model"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
	"time"
)

func TestLogCount(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	var querySize = 100

	ac := client.NewAugurClientImpl(es, client.Immediate)
	cs := countService.NewCountService(ac, logger)
	t.Run("should be able to count co-occurrences of logs within the same timespan", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		numOutsideBucket := 2
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: initialTime,
		}
		logsOfSameTime := makeLogsOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second),
			numWithinBucket,
		)
		logsOfDifferentTime := makeLogsOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second*2),
			numOutsideBucket,
		)
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog})
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, append(logsOfSameTime, logsOfDifferentTime...))
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countService.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		countInfo, err := cs.CountOccurrencesAndCoOccurrencesByCoClusterId(
			ctx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		relevantCountInfo := countInfo["differentTime"]
		assert.Equal(t, int64(len(logsOfSameTime)), relevantCountInfo.Occurrences)
		assert.Equal(t, int64(len(logsOfSameTime)), relevantCountInfo.CoOccurrences)
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
		logsOfDifferentTime := makeLogsOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second),
			numWithinBucket,
		)
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
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		err = ac.BulkIndex(ctx, res.MetaMapList, res.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.CountIndexName}, &querySize)
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
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		err = ac.BulkIndex(ctx, res.MetaMapList, res.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		res, err = cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		err = ac.BulkIndex(ctx, res.MetaMapList, res.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.CountIndexName}, &querySize)
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

	t.Run("should add occurrences for misses if co-occurrences have been found", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		numOutsideBucket := 2
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: initialTime,
		}
		logsOfSameTime := makeLogsOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second),
			numWithinBucket,
		)
		logsOfDifferentTime := makeLogsOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second*2),
			numOutsideBucket,
		)
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog})
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, append(logsOfSameTime, logsOfDifferentTime...))
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countService.Bucket{2500}
		firstCtx, firstCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer firstCancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			firstCtx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences for successes: %v", err)
		}
		err = ac.BulkIndex(firstCtx, res.MetaMapList, res.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		secondCtx, secondCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer secondCancel()
		fakeInput := countModel.IncreaseMissesInput{
			ClusterId:    newLog.ClusterId,
			CoClusterIds: []string{},
		}
		missesRes, err := cs.GetIncrementMissesQueryInfo(
			secondCtx,
			fakeInput,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences for misses: %v", err)
		}
		err = ac.BulkIndex(secondCtx, missesRes.MetaMapList, missesRes.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		queryCtx, queryCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer queryCancel()
		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(queryCtx, searchQueryBody, []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		countEntry := countEntries[0]
		assert.Equal(t, int64(numWithinBucket+1), countEntry.Occurrences)
		assert.Equal(t, int64(numWithinBucket), countEntry.CoOccurrences)
	})

	t.Run("should not add occurrences for misses if no co-occurrences have been found", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		numOutsideBucket := 2
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: initialTime,
		}
		logsOfSameTime := makeLogsOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second),
			numWithinBucket,
		)
		logsOfDifferentTime := makeLogsOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second*2),
			numOutsideBucket,
		)
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog})
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, append(logsOfSameTime, logsOfDifferentTime...))
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countService.Bucket{2500}
		missCtx, missCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer missCancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			missCtx,
			logsOfDifferentTime[0].ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: logsOfDifferentTime[0].Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences for misses: %v", err)
		}
		insertCtx, insertCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer insertCancel()
		err = ac.BulkIndex(insertCtx, res.MetaMapList, res.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		hitCtx, hitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer hitCancel()
		res, err = cs.GetCountAndUpdateOccurrencesQueryConstituents(
			hitCtx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences for successes: %v", err)
		}
		err = ac.BulkIndex(insertCtx, res.MetaMapList, res.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		queryCtx, queryCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer queryCancel()
		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(queryCtx, searchQueryBody, []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		countEntry := countEntries[0]
		assert.Equal(t, int64(len(logsOfSameTime)), countEntry.Occurrences)
	})
}

func TestSpanCount(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Immediate)
	cs := countService.NewCountService(ac, logger)
	t.Run("should be able to count co-occurrences of logs within the same timespan", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		numOutsideBucket := 2
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newSpan := spanModel.Span{
			EndTime:   initialTime.Add(time.Second * 2),
			StartTime: initialTime.Add(-time.Second * 2),
			ClusterId: "initialClusterId",
		}
		logsOfSameTimeSpan := makeLogsOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second),
			numWithinBucket,
		)
		logsOfDifferentTime := makeLogsOfSameClusterId(
			"differentTime",
			initialTime.Add(time.Second*4),
			numOutsideBucket,
		)
		err = loadDataIntoElasticsearch(ac, []spanModel.Span{newSpan})
		if err != nil {
			t.Error("Failed to load span into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, append(logsOfSameTimeSpan, logsOfDifferentTime...))
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countService.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		countInfo, err := cs.CountOccurrencesAndCoOccurrencesByCoClusterId(
			ctx,
			newSpan.ClusterId,
			countModel.TimeInfo{
				SpanInfo: &countModel.SpanInfo{
					FromTime: newSpan.StartTime, ToTime: newSpan.EndTime,
				},
			},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		relevantCountInfo := countInfo["differentTime"]
		assert.Equal(t, int64(len(logsOfSameTimeSpan)), relevantCountInfo.Occurrences)
		assert.Equal(t, int64(len(logsOfSameTimeSpan)), relevantCountInfo.CoOccurrences)
	})

	t.Run("should be able to count co-occurrences of overlapping spans", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		numOutsideBucket := 3
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
			numOutsideBucket,
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
			countModel.TimeInfo{
				SpanInfo: &countModel.SpanInfo{
					FromTime: newSpan.StartTime, ToTime: newSpan.EndTime,
				},
			},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		relevantCountInfo := countInfo["differentTime"]
		assert.Equal(t, int64(len(overlappingSpans)), relevantCountInfo.Occurrences)
		assert.Equal(t, int64(len(overlappingSpans)), relevantCountInfo.CoOccurrences)
	})

	t.Run("should update existing entries in the database", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		numWithinBucket := 4
		numOutsideBucket := 3
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
			numOutsideBucket,
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
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newSpan.ClusterId,
			countModel.TimeInfo{
				SpanInfo: &countModel.SpanInfo{
					FromTime: newSpan.StartTime, ToTime: newSpan.EndTime,
				},
			},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		err = ac.BulkIndex(ctx, res.MetaMapList, res.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		res, err = cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newSpan.ClusterId,
			countModel.TimeInfo{
				SpanInfo: &countModel.SpanInfo{
					FromTime: newSpan.StartTime, ToTime: newSpan.EndTime,
				},
			},
			buckets,
		)
		err = ac.BulkIndex(ctx, res.MetaMapList, res.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		searchQueryBody := countQuery(newSpan.ClusterId)
		var querySize = 100
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		countEntry := countEntries[0]
		assert.Equal(t, int64(len(overlappingSpans)*2), countEntry.Occurrences)
		assert.Equal(t, int64(len(overlappingSpans)*2), countEntry.CoOccurrences)
	})
}

func TestAlgorithm(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Wait)
	cs := countService.NewCountService(ac, logger)
	t.Run("should insert two different matches into the database", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: initialTime,
		}
		logWithClusterId1 := model.LogEntry{
			ClusterId: "clusterId1",
			Timestamp: initialTime.Add(time.Second),
		}
		logWithClusterId2 := model.LogEntry{
			ClusterId: "clusterId2",
			Timestamp: initialTime.Add(time.Second),
		}
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog, logWithClusterId1, logWithClusterId2})
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countService.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		err = ac.BulkIndex(ctx, res.MetaMapList, res.DocumentMapList, bootstrapper.CountIndexName)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		searchQueryBody := countQuery(newLog.ClusterId)
		var querySize = 100
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		assert.Equal(t, 2, len(countEntries))
		coClusters := []string{countEntries[0].CoClusterId, countEntries[1].CoClusterId}
		slices.Sort(coClusters)
		expectedResult := []string{"clusterId1", "clusterId2"}
		slices.Sort(expectedResult)
		assert.EqualValues(t, expectedResult, coClusters)
	})
}

func loadDataIntoElasticsearch[Data any](ac client.AugurClient, data []Data) error {
	metaMap, dataMap, err := client.ToMetaAndDataMap(data)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = ac.BulkIndex(ctx, metaMap, dataMap, bootstrapper.LogIndexName)
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
