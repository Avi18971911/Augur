package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/otel_server/log/model"
	spanModel "github.com/Avi18971911/Augur/internal/otel_server/trace/model"
	countModel "github.com/Avi18971911/Augur/internal/pipeline/count/model"
	countService "github.com/Avi18971911/Augur/internal/pipeline/count/service"
	"github.com/stretchr/testify/assert"
	"slices"
	"testing"
	"time"
)

var indices = []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName}

func TestLogCount(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	var querySize = 100

	ac := client.NewAugurClientImpl(es, client.Immediate)
	wc := countService.NewClusterWindowCountService(ac, logger)
	cs := countService.NewClusterTotalCountService(ac, wc, logger)

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
		err = loadDataIntoElasticsearch(ac, logsOfDifferentTime, bootstrapper.LogIndexName)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog}, bootstrapper.LogIndexName)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countModel.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			indices,
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		index := bootstrapper.ClusterTotalCountIndexName
		err = ac.BulkIndex(ctx, res.TotalCountMetaMapList, res.TotalCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		res, err = cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			indices,
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		err = ac.BulkIndex(ctx, res.TotalCountMetaMapList, res.TotalCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		countEntry := countEntries[0]
		assert.Equal(t, int64(2), countEntry.TotalInstances)
		assert.Equal(t, int64(2), countEntry.TotalInstancesWithCoCluster)
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
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog}, bootstrapper.LogIndexName)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, append(logsOfSameTime, logsOfDifferentTime...), bootstrapper.LogIndexName)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countModel.Bucket{2500}
		firstCtx, firstCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer firstCancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			firstCtx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			indices,
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences for successes: %v", err)
		}
		index := bootstrapper.ClusterTotalCountIndexName
		err = ac.BulkIndex(firstCtx, res.TotalCountMetaMapList, res.TotalCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		secondCtx, secondCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer secondCancel()
		fakeInput := countModel.IncreaseMissesInput{
			ClusterId:                 newLog.ClusterId,
			CoClusterDetailsToExclude: []string{},
		}
		missesRes, err := cs.GetIncrementMissesQueryInfo(
			secondCtx,
			fakeInput,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences for misses: %v", err)
		}
		err = ac.BulkIndex(secondCtx, missesRes.MetaMapList, missesRes.DocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		queryCtx, queryCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer queryCancel()
		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(queryCtx, searchQueryBody, []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		countEntry := countEntries[0]
		assert.Equal(t, int64(2), countEntry.TotalInstances)
		assert.Equal(t, int64(1), countEntry.TotalInstancesWithCoCluster)
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
		err = loadDataIntoElasticsearch(ac, []model.LogEntry{newLog}, bootstrapper.LogIndexName)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		err = loadDataIntoElasticsearch(ac, append(logsOfSameTime, logsOfDifferentTime...), bootstrapper.LogIndexName)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		missCtx, missCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer missCancel()
		fakeInput := countModel.IncreaseMissesInput{
			ClusterId:                 newLog.ClusterId,
			CoClusterDetailsToExclude: []string{},
		}
		missRes, err := cs.GetIncrementMissesQueryInfo(
			missCtx,
			fakeInput,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences for misses: %v", err)
		}
		assert.Nil(t, missRes)
	})
}

func TestSpanCount(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Immediate)
	wc := countService.NewClusterWindowCountService(ac, logger)
	cs := countService.NewClusterTotalCountService(ac, wc, logger)

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
		err = loadDataIntoElasticsearch(ac, []spanModel.Span{newSpan}, bootstrapper.SpanIndexName)
		if err != nil {
			t.Error("Failed to load span into elasticsearch")
		}
		err = loadDataIntoElasticsearch(
			ac, append(overlappingSpans, nonOverlappingSpans...), bootstrapper.SpanIndexName,
		)
		if err != nil {
			t.Error("Failed to load overlapping spans into elasticsearch")
		}
		buckets := []countModel.Bucket{1000}
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
			indices,
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		index := bootstrapper.ClusterTotalCountIndexName
		err = ac.BulkIndex(ctx, res.TotalCountMetaMapList, res.TotalCountDocumentMapList, &index)
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
			indices,
			buckets,
		)
		err = ac.BulkIndex(ctx, res.TotalCountMetaMapList, res.TotalCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		searchQueryBody := countQuery(newSpan.ClusterId)
		var querySize = 100
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}
		countEntry := countEntries[0]
		assert.Equal(t, int64(2), countEntry.TotalInstances)
		assert.Equal(t, int64(2), countEntry.TotalInstancesWithCoCluster)
	})
}

func TestAlgorithm(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Wait)
	wc := countService.NewClusterWindowCountService(ac, logger)
	cs := countService.NewClusterTotalCountService(ac, wc, logger)
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
		err = loadDataIntoElasticsearch(
			ac,
			[]model.LogEntry{newLog, logWithClusterId1, logWithClusterId2},
			bootstrapper.LogIndexName,
		)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countModel.Bucket{2500}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			indices,
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		index := bootstrapper.ClusterTotalCountIndexName
		err = ac.BulkIndex(ctx, res.TotalCountMetaMapList, res.TotalCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		searchQueryBody := countQuery(newLog.ClusterId)
		var querySize = 100
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
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

func convertCountDocsToCountEntries(docs []map[string]interface{}) ([]countModel.ClusterTotalCountEntry, error) {
	var countEntries []countModel.ClusterTotalCountEntry
	for _, doc := range docs {
		countEntry := countModel.ClusterTotalCountEntry{}
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
		totalInstances, ok := doc["total_instances"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert total_instances to float64")
		}
		countEntry.TotalInstances = int64(totalInstances)
		totalInstancesWithCoCluster, ok := doc["total_instances_with_co_cluster"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert total_instances_with_co_cluster to float64")
		}
		countEntry.TotalInstancesWithCoCluster = int64(totalInstancesWithCoCluster)
		countEntries = append(countEntries, countEntry)
	}
	return countEntries, nil
}

func convertCountDocsToWindowCountEntries(docs []map[string]interface{}) ([]countModel.ClusterWindowCountEntry, error) {
	var countEntries []countModel.ClusterWindowCountEntry
	for _, doc := range docs {
		countEntry := countModel.ClusterWindowCountEntry{}
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
		start, ok := doc["start"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert start to float64")
		}
		countEntry.Start = start
		end, ok := doc["end"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert end to float64")
		}
		countEntry.End = end
		occurrences, ok := doc["occurrences"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert occurrences to float64")
		}
		countEntry.Occurrences = int64(occurrences)
		meanTDOA, ok := doc["mean_TDOA"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert mean_tdoa to float64")
		}
		countEntry.MeanTDOA = meanTDOA
		varianceTDOA, ok := doc["variance_TDOA"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert variance_tdoa to float64")
		}
		countEntry.VarianceTDOA = varianceTDOA
		countEntries = append(countEntries, countEntry)
	}
	return countEntries, nil
}
