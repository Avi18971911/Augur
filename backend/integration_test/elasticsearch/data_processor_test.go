package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/data_processor/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

var dpIndices = []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName}

func TestDataProcessor(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	cs := countService.NewCountService(ac, logger)

	t.Run("should increment both co-occurring clusters, and misses", func(t *testing.T) {
		dp := service.NewDataProcessorService(ac, cs, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		clusterA := "clusterA"
		clusterB := "clusterB"
		firstTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		overlapWithFirstTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 500, time.UTC)

		secondTimeStamp := time.Date(2021, 2, 1, 0, 0, 1, 0, time.UTC)
		overlapWithSecondTimeStamp := time.Date(2021, 2, 1, 0, 0, 1, 500, time.UTC)

		clusterAMiss := time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC)

		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)

		coOccurringLogs := []logModel.LogEntry{
			{
				ClusterId: clusterA,
				Timestamp: firstTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterA,
				Timestamp: secondTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithSecondTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterA,
				Timestamp: clusterAMiss,
				CreatedAt: createdAt,
			},
		}

		err = loadDataIntoElasticsearch(ac, coOccurringLogs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}

		buckets := []countModel.Bucket{100}
		_, errors := dp.ProcessData(context.Background(), buckets, dpIndices)

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterAEntries := make([]countModel.CountEntry, 0)
		clusterBEntries := make([]countModel.CountEntry, 0)

		for _, entry := range countEntries {
			if entry.ClusterId == clusterA {
				clusterAEntries = append(clusterAEntries, entry)
			} else if entry.ClusterId == clusterB {
				clusterBEntries = append(clusterBEntries, entry)
			}
		}

		assertAllErrorsAreNil(t, errors)
		assert.Equal(t, int64(2), clusterAEntries[0].CoOccurrences)
		assert.Equal(t, int64(2), clusterBEntries[0].CoOccurrences)

		assert.Equal(t, int64(3), clusterAEntries[0].Occurrences)
		assert.Equal(t, int64(2), clusterBEntries[0].Occurrences)
	})

	t.Run("should increment asymmetrically with multiple overlaps on the same period", func(t *testing.T) {
		dp := service.NewDataProcessorService(ac, cs, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		clusterA := "clusterA"
		clusterB := "clusterB"

		firstTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		overlapWithFirstTimeStampOne := time.Date(2021, 1, 1, 0, 0, 0, 500, time.UTC)
		overlapWithFirstTimeStampTwo := time.Date(2021, 1, 1, 0, 0, 0, 750, time.UTC)
		overlapWithFirstTimeStampThree := time.Date(2021, 1, 1, 0, 0, 0, 900, time.UTC)

		clusterBMiss := time.Date(2023, 1, 1, 0, 0, 1, 0, time.UTC)

		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)

		coOccurringLogs := []logModel.LogEntry{
			{
				ClusterId: clusterA,
				Timestamp: firstTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStampOne,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStampTwo,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStampThree,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: clusterBMiss,
				CreatedAt: createdAt,
			},
		}

		err = loadDataIntoElasticsearch(ac, coOccurringLogs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}

		buckets := []countModel.Bucket{100}
		_, errors := dp.ProcessData(context.Background(), buckets, dpIndices)

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterAEntries := make([]countModel.CountEntry, 0)
		clusterBEntries := make([]countModel.CountEntry, 0)

		for _, entry := range countEntries {
			if entry.ClusterId == clusterA {
				clusterAEntries = append(clusterAEntries, entry)
			} else if entry.ClusterId == clusterB {
				clusterBEntries = append(clusterBEntries, entry)
			}
		}

		assertAllErrorsAreNil(t, errors)
		assert.Equal(t, int64(1), clusterAEntries[0].CoOccurrences)
		assert.Equal(t, int64(3), clusterBEntries[0].CoOccurrences)

		assert.Equal(t, int64(1), clusterAEntries[0].Occurrences)
		assert.Equal(t, int64(4), clusterBEntries[0].Occurrences)
	})

	t.Run("should be able to scroll through a huge list of logs/spans", func(t *testing.T) {
		dp := service.NewDataProcessorService(ac, cs, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		logSize := 30000
		logs := make([]logModel.LogEntry, logSize)
		overlapSize := 50
		beginTime := time.Date(2021, 1, 1, 0, 0, 0, 32, time.UTC)
		for i := 0; i < logSize; i++ {
			clusterId := "cluster" + strconv.Itoa(i)
			logs[i] = logModel.LogEntry{
				CreatedAt: beginTime.Add(time.Duration(i) * time.Millisecond * time.Duration(overlapSize)),
				Timestamp: beginTime.Add(time.Duration(i) * time.Millisecond * time.Duration(overlapSize)),
				ClusterId: clusterId,
			}
		}
		err = loadDataIntoElasticsearch(ac, logs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load data into Elasticsearch: %v", err)
		}
		buckets := []countModel.Bucket{countModel.Bucket(overlapSize * 2)}
		_, errors := dp.ProcessData(context.Background(), buckets, dpIndices)

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		count, err := ac.Count(searchCtx, string(stringQuery), []string{bootstrapper.CountIndexName})
		if err != nil {
			t.Errorf("Failed to count: %v", err)
		}
		assertAllErrorsAreNil(t, errors)
		// every log sequentially overlaps with another, creating two co-occurrences per log (previous and next log)
		// except for the first and last log
		assert.Equal(t, int64(logSize*2-2), count)
	})

	t.Run(
		"should be able to process spans completely unrelated to each other without error", func(t *testing.T) {
			dp := service.NewDataProcessorService(ac, cs, logger)
			err := deleteAllDocuments(es)
			if err != nil {
				t.Errorf("Failed to delete all documents: %v", err)
			}
			beginTime := time.Date(2021, 1, 1, 0, 0, 0, 32, time.UTC)
			spanSize := 100
			spans := createSpans(beginTime, time.Millisecond*50, spanSize)
			overlapSize := 2

			err = loadDataIntoElasticsearch(ac, spans, bootstrapper.SpanIndexName)
			if err != nil {
				t.Errorf("Failed to load data into Elasticsearch: %v", err)
			}
			buckets := []countModel.Bucket{countModel.Bucket(overlapSize)}
			_, errors := dp.ProcessData(context.Background(), buckets, dpIndices)

			stringQuery, err := json.Marshal(getAllQuery())
			if err != nil {
				t.Errorf("failed to marshal query: %v", err)
			}

			searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			count, err := ac.Count(searchCtx, string(stringQuery), []string{bootstrapper.CountIndexName})
			if err != nil {
				t.Errorf("Failed to count: %v", err)
			}
			assertAllErrorsAreNil(t, errors)
			assert.Equal(t, int64(0), count)
		},
	)

	t.Run("overlapping spans and logs should be processed correctly", func(t *testing.T) {
		dp := service.NewDataProcessorService(ac, cs, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		clusterA := "clusterA"
		clusterB := "clusterB"

		firstTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 500, time.UTC)
		overlapWithFirstTimeStampOne := time.Date(2021, 1, 1, 0, 0, 0, 100, time.UTC)
		overlapWithFirstTimeStampTwo := time.Date(2021, 1, 1, 0, 0, 0, 200, time.UTC)
		overlapWithFirstTimeStampThree := time.Date(2021, 1, 1, 0, 0, 0, 300, time.UTC)

		clusterBMiss := time.Date(2023, 1, 1, 0, 0, 1, 200, time.UTC)

		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)

		clusterALogs := []logModel.LogEntry{
			{
				ClusterId: clusterA,
				Timestamp: firstTimeStamp,
				CreatedAt: createdAt,
			},
		}

		clusterBSpans := []spanModel.Span{
			createSpan(clusterB, overlapWithFirstTimeStampOne, overlapWithFirstTimeStampOne.Add(time.Millisecond*50)),
			createSpan(clusterB, overlapWithFirstTimeStampTwo, overlapWithFirstTimeStampTwo.Add(time.Millisecond*50)),
			createSpan(
				clusterB, overlapWithFirstTimeStampThree, overlapWithFirstTimeStampThree.Add(time.Millisecond*50),
			),
			createSpan(clusterB, clusterBMiss, clusterBMiss.Add(time.Millisecond*50)),
		}

		err = loadDataIntoElasticsearch(ac, clusterALogs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		err = loadDataIntoElasticsearch(ac, clusterBSpans, bootstrapper.SpanIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}

		buckets := []countModel.Bucket{100}
		_, errors := dp.ProcessData(
			context.Background(),
			buckets,
			[]string{bootstrapper.LogIndexName,
				bootstrapper.SpanIndexName},
		)

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterAEntries := make([]countModel.CountEntry, 0)
		clusterBEntries := make([]countModel.CountEntry, 0)

		for _, entry := range countEntries {
			if entry.ClusterId == clusterA {
				clusterAEntries = append(clusterAEntries, entry)
			} else if entry.ClusterId == clusterB {
				clusterBEntries = append(clusterBEntries, entry)
			}
		}

		assertAllErrorsAreNil(t, errors)
		assert.Equal(t, int64(1), clusterAEntries[0].CoOccurrences)
		assert.Equal(t, int64(3), clusterBEntries[0].CoOccurrences)
		assert.Equal(t, int64(1), clusterAEntries[0].Occurrences)
		assert.Equal(t, int64(4), clusterBEntries[0].Occurrences)
	})
}

func createSpan(
	clusterId string,
	startTime time.Time,
	endTime time.Time,
) spanModel.Span {
	return spanModel.Span{
		CreatedAt:    startTime,
		SpanID:       clusterId,
		ParentSpanID: clusterId,
		TraceID:      "trace-12345",
		ServiceName:  "serviceName",
		StartTime:    startTime,
		EndTime:      endTime,
		ActionName:   "actionName",
		SpanKind:     "spanKind",
		ClusterEvent: fmt.Sprintf(
			"service=%s,operation=%s,kind=%s", "serviceName", "actionName", "spanKind",
		),
		ClusterId: clusterId,
		Attributes: map[string]string{
			"http.method": "GET",
			"user.id":     clusterId,
		},
		Events: []spanModel.SpanEvent{},
	}
}

func createSpans(
	baseTime time.Time,
	interval time.Duration,
	numSpans int,
) []spanModel.Span {
	spans := make([]spanModel.Span, numSpans)
	for i := 0; i < numSpans; i++ {
		startTime := baseTime.Add(time.Duration(i) * interval)
		endTime := startTime.Add(interval / 2)

		span := spanModel.Span{
			Id:           fmt.Sprintf("span-%d", i+1),
			CreatedAt:    startTime,
			SpanID:       fmt.Sprintf("span-%d", i+1),
			ParentSpanID: fmt.Sprintf("span-%d", i),
			TraceID:      "trace-12345",
			ServiceName:  "serviceName",
			StartTime:    startTime,
			EndTime:      endTime,
			ActionName:   "actionName",
			SpanKind:     "spanKind",
			ClusterEvent: fmt.Sprintf(
				"service=%s,operation=%s,kind=%s", "serviceName", "actionName", "spanKind",
			),
			ClusterId: fmt.Sprintf("cluster-%d", i+1),
			Attributes: map[string]string{
				"http.method": "GET",
				"user.id":     fmt.Sprintf("user-%d", i+1),
			},
			Events: []spanModel.SpanEvent{},
		}
		spans[i] = span
	}
	return spans
}

func assertAllErrorsAreNil(t *testing.T, errors []error) {
	for _, err := range errors {
		assert.Nil(t, err)
	}
}
