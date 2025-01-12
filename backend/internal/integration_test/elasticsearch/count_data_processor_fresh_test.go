package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/internal/otel_server/log/model"
	spanModel "github.com/Avi18971911/Augur/internal/otel_server/trace/model"
	countModel "github.com/Avi18971911/Augur/internal/pipeline/count/model"
	countService "github.com/Avi18971911/Augur/internal/pipeline/count/service"
	"github.com/Avi18971911/Augur/internal/pipeline/data_processor/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var cdpIndices = []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName}

func TestCountDataProcessor(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	wc := countService.NewClusterWindowCountService(ac, 50, logger)
	cs := countService.NewClusterTotalCountService(ac, wc, logger)
	bucket := countModel.Bucket(100)

	t.Run("should increment both co-occurring clusters, and misses", func(t *testing.T) {
		dp := countService.NewCountDataProcessorService(ac, cs, bucket, cdpIndices, logger)
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
		clusterOutput := createClusterOutputFromLogs(coOccurringLogs)

		err = loadDataIntoElasticsearch(ac, coOccurringLogs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}

		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterOutput)
		if err != nil {
			t.Errorf("Failed to process data: %v", err)
		}

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := countService.ConvertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterAEntries := make([]countModel.ClusterTotalCountEntry, 0)
		clusterBEntries := make([]countModel.ClusterTotalCountEntry, 0)

		for _, entry := range countEntries {
			if entry.ClusterId == clusterA {
				clusterAEntries = append(clusterAEntries, entry)
			} else if entry.ClusterId == clusterB {
				clusterBEntries = append(clusterBEntries, entry)
			}
		}

		assert.Equal(t, int64(2), clusterAEntries[0].TotalInstancesWithCoCluster)
		assert.Equal(t, int64(2), clusterBEntries[0].TotalInstancesWithCoCluster)

		assert.Equal(t, int64(3), clusterAEntries[0].TotalInstances)
		assert.Equal(t, int64(2), clusterBEntries[0].TotalInstances)
	})

	t.Run("should increment asymmetrically with multiple overlaps on the same period", func(t *testing.T) {
		dp := countService.NewCountDataProcessorService(ac, cs, bucket, cdpIndices, logger)
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

		clusterOutput := createClusterOutputFromLogs(coOccurringLogs)

		err = loadDataIntoElasticsearch(ac, coOccurringLogs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}

		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterOutput)
		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := countService.ConvertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterAEntries := make([]countModel.ClusterTotalCountEntry, 0)
		clusterBEntries := make([]countModel.ClusterTotalCountEntry, 0)

		for _, entry := range countEntries {
			if entry.ClusterId == clusterA {
				clusterAEntries = append(clusterAEntries, entry)
			} else if entry.ClusterId == clusterB {
				clusterBEntries = append(clusterBEntries, entry)
			}
		}

		assert.Equal(t, int64(1), clusterAEntries[0].TotalInstancesWithCoCluster)
		assert.Equal(t, int64(3), clusterBEntries[0].TotalInstancesWithCoCluster)

		assert.Equal(t, int64(1), clusterAEntries[0].TotalInstances)
		assert.Equal(t, int64(4), clusterBEntries[0].TotalInstances)
	})

	t.Run(
		"should be able to process spans completely unrelated to each other without error", func(t *testing.T) {
			dp := countService.NewCountDataProcessorService(ac, cs, countModel.Bucket(10), cdpIndices, logger)
			err := deleteAllDocuments(es)
			if err != nil {
				t.Errorf("Failed to delete all documents: %v", err)
			}
			beginTime := time.Date(2021, 1, 1, 0, 0, 0, 32, time.UTC)
			spanSize := 100
			spans, clusterOutput := createSpans(beginTime, time.Millisecond*50, spanSize)

			err = loadDataIntoElasticsearch(ac, spans, bootstrapper.SpanIndexName)
			if err != nil {
				t.Errorf("Failed to load data into Elasticsearch: %v", err)
			}

			_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterOutput)
			if err != nil {
				t.Errorf("Failed to process data: %v", err)
			}

			stringQuery, err := json.Marshal(getAllQuery())
			if err != nil {
				t.Errorf("failed to marshal query: %v", err)
			}

			searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			count, err := ac.Count(searchCtx, string(stringQuery), []string{bootstrapper.ClusterTotalCountIndexName})
			if err != nil {
				t.Errorf("Failed to count: %v", err)
			}
			assert.Equal(t, int64(0), count)
		},
	)

	t.Run("overlapping spans and logs should be processed correctly", func(t *testing.T) {
		dp := countService.NewCountDataProcessorService(ac, cs, countModel.Bucket(10), cdpIndices, logger)
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

		clusterAOutput := createClusterOutputFromLogs(clusterALogs)
		clusterBOutput := createClusterOutputFromSpans(clusterBSpans)

		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterAOutput)
		if err != nil {
			t.Errorf("Failed to process data: %v", err)
		}
		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterBOutput)
		if err != nil {
			t.Errorf("Failed to process data: %v", err)
		}

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := countService.ConvertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterAEntries := make([]countModel.ClusterTotalCountEntry, 0)
		clusterBEntries := make([]countModel.ClusterTotalCountEntry, 0)

		for _, entry := range countEntries {
			if entry.ClusterId == clusterA {
				clusterAEntries = append(clusterAEntries, entry)
			} else if entry.ClusterId == clusterB {
				clusterBEntries = append(clusterBEntries, entry)
			}
		}

		assert.Equal(t, int64(1), clusterAEntries[0].TotalInstancesWithCoCluster)
		assert.Equal(t, int64(3), clusterBEntries[0].TotalInstancesWithCoCluster)
		assert.Equal(t, int64(1), clusterAEntries[0].TotalInstances)
		assert.Equal(t, int64(4), clusterBEntries[0].TotalInstances)
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
		Service:      "serviceName",
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
) ([]spanModel.Span, []model.ClusterOutput) {
	spans := make([]spanModel.Span, numSpans)
	clusterOutput := make([]model.ClusterOutput, numSpans)
	for i := 0; i < numSpans; i++ {
		startTime := baseTime.Add(time.Duration(i) * interval)
		endTime := startTime.Add(interval / 2)

		span := spanModel.Span{
			Id:           fmt.Sprintf("span-%d", i+1),
			CreatedAt:    startTime,
			SpanID:       fmt.Sprintf("span-%d", i+1),
			ParentSpanID: fmt.Sprintf("span-%d", i),
			TraceID:      "trace-12345",
			Service:      "serviceName",
			StartTime:    startTime,
			EndTime:      endTime,
			ActionName:   "actionName",
			SpanKind:     "spanKind",
			ClusterEvent: fmt.Sprintf(
				"service=%s,operation=%s,kind=%s,number=%d", "serviceName", "actionName", "spanKind", i,
			),
			ClusterId: fmt.Sprintf("cluster-%d", i+1),
			Attributes: map[string]string{
				"http.method": "GET",
				"user.id":     fmt.Sprintf("user-%d", i+1),
			},
			Events: []spanModel.SpanEvent{},
		}

		cluster := model.ClusterOutput{
			ClusterId:       fmt.Sprintf("cluster-%d", i+1),
			ClusterDataType: model.SpanClusterType,
			SpanTimeDetails: model.SpanDetails{
				StartTime: startTime,
				EndTime:   endTime,
			},
		}

		spans[i] = span
		clusterOutput[i] = cluster
	}
	return spans, clusterOutput
}

func assertAllErrorsAreNil(t *testing.T, errors []error) {
	for _, err := range errors {
		assert.Nil(t, err)
	}
}

func createClusterOutputFromLogs(logs []logModel.LogEntry) []model.ClusterOutput {
	clusterOutput := make([]model.ClusterOutput, len(logs))
	for i, log := range logs {
		clusterOutput[i] = model.ClusterOutput{
			ClusterId:       log.ClusterId,
			ClusterDataType: model.LogClusterType,
			LogTimeDetails: model.LogDetails{
				Timestamp: log.Timestamp,
			},
		}
	}
	return clusterOutput
}

func createClusterOutputFromSpans(spans []spanModel.Span) []model.ClusterOutput {
	clusterOutput := make([]model.ClusterOutput, len(spans))
	for i, span := range spans {
		clusterOutput[i] = model.ClusterOutput{
			ClusterId:       span.ClusterId,
			ClusterDataType: model.SpanClusterType,
			SpanTimeDetails: model.SpanDetails{
				StartTime: span.StartTime,
				EndTime:   span.EndTime,
			},
		}
	}
	return clusterOutput
}
