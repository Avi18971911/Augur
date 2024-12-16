package elasticsearch

import (
	"context"
	"encoding/json"
	clusterService "github.com/Avi18971911/Augur/pkg/cluster/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logHelper "github.com/Avi18971911/Augur/pkg/log/helper"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	spanHelper "github.com/Avi18971911/Augur/pkg/trace/helper"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestClusterDataProcessorDataInDB(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	cs := clusterService.NewClusterService(ac, logger)

	t.Run("should cluster new groups with the old", func(t *testing.T) {
		dp := clusterService.NewClusterDataProcessor(ac, cs, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		const clusterAMessage = "Message in common with Cluster A"
		const clusterBMessage = "B's message, random filler words"
		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		onlyTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		firstBatch := []logModel.LogEntry{
			{
				Id:        "1",
				Message:   clusterAMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt.Add(-time.Second * 500),
				ClusterId: clusterService.DefaultClusterId,
			},
			{
				Id:        "2",
				Message:   clusterBMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt.Add(-time.Second * 500),
				ClusterId: clusterService.DefaultClusterId,
			},
		}
		firstBatchData := convertLogToSpanOrLogData(firstBatch)

		err = loadDataIntoElasticsearch(ac, firstBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.ClusterData(context.Background(), firstBatchData)
		if err != nil {
			t.Errorf("failed to cluster data: %v", err)
		}

		secondBatch := []logModel.LogEntry{
			{
				Id:        "3",
				Message:   clusterAMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt,
				ClusterId: clusterService.DefaultClusterId,
			},
			{
				Id:        "4",
				Message:   clusterBMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt,
				ClusterId: clusterService.DefaultClusterId,
			},
			{
				Id:        "5",
				Message:   clusterBMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt,
				ClusterId: clusterService.DefaultClusterId,
			},
		}
		secondBatchData := convertLogToSpanOrLogData(secondBatch)

		err = loadDataIntoElasticsearch(ac, secondBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.ClusterData(context.Background(), secondBatchData)
		if err != nil {
			t.Errorf("failed to cluster data: %v", err)
		}

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.LogIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		logEntries, err := logHelper.ConvertFromDocuments(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterALogs := make([]logModel.LogEntry, 0)
		clusterBLogs := make([]logModel.LogEntry, 0)

		for _, entry := range logEntries {
			if entry.Message == clusterAMessage {
				clusterALogs = append(clusterALogs, entry)
			} else if entry.Message == clusterBMessage {
				clusterBLogs = append(clusterBLogs, entry)
			}
		}

		assert.Equal(t, 2, len(clusterALogs))
		assert.Equal(t, 3, len(clusterBLogs))
		assert.Equal(t, len(logEntries), len(clusterALogs)+len(clusterBLogs))

		clusterAId := clusterALogs[0].ClusterId
		clusterBId := clusterBLogs[0].ClusterId

		for _, entry := range clusterALogs {
			assert.Equal(t, clusterAId, entry.ClusterId)
			assert.NotEqual(t, clusterBId, entry.ClusterId)
			assert.NotEqual(t, clusterService.DefaultClusterId, entry.ClusterId)
		}
		for _, entry := range clusterBLogs {
			assert.Equal(t, clusterBId, entry.ClusterId)
			assert.NotEqual(t, clusterAId, entry.ClusterId)
			assert.NotEqual(t, clusterService.DefaultClusterId, entry.ClusterId)
		}
	})

	t.Run("should add a new cluster if there are no entries of the old one", func(t *testing.T) {
		dp := clusterService.NewClusterDataProcessor(ac, cs, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		const clusterAMessage = "Message in common with Cluster A"
		const clusterBMessage = "B's message, random filler words"

		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		onlyTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		firstBatch := []logModel.LogEntry{
			{
				Id:        "1",
				Message:   clusterAMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt.Add(-time.Second * 500),
				ClusterId: clusterService.DefaultClusterId,
			},
			{
				Id:        "2",
				Message:   clusterAMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt.Add(-time.Second * 500),
				ClusterId: clusterService.DefaultClusterId,
			},
		}
		firstBatchData := convertLogToSpanOrLogData(firstBatch)
		secondBatch := []logModel.LogEntry{
			{
				Id:        "3",
				Message:   clusterBMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt,
				ClusterId: clusterService.DefaultClusterId,
			},
			{
				Id:        "4",
				Message:   clusterBMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt,
				ClusterId: clusterService.DefaultClusterId,
			},
			{
				Id:        "5",
				Message:   clusterAMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt,
				ClusterId: clusterService.DefaultClusterId,
			},
		}
		secondBatchData := convertLogToSpanOrLogData(secondBatch)
		err = loadDataIntoElasticsearch(ac, firstBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.ClusterData(context.Background(), firstBatchData)
		if err != nil {
			t.Errorf("failed to cluster data: %v", err)
		}

		err = loadDataIntoElasticsearch(ac, secondBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.ClusterData(context.Background(), secondBatchData)
		if err != nil {
			t.Errorf("failed to cluster data: %v", err)
		}

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.LogIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		logEntries, err := logHelper.ConvertFromDocuments(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterALogs := make([]logModel.LogEntry, 0)
		clusterBLogs := make([]logModel.LogEntry, 0)

		for _, entry := range logEntries {
			if entry.Message == clusterAMessage {
				clusterALogs = append(clusterALogs, entry)
			} else if entry.Message == clusterBMessage {
				clusterBLogs = append(clusterBLogs, entry)
			}
		}

		assert.Equal(t, 3, len(clusterALogs))
		assert.Equal(t, 2, len(clusterBLogs))
		assert.Equal(t, len(logEntries), len(clusterALogs)+len(clusterBLogs))

		clusterAId := clusterALogs[0].ClusterId
		clusterBId := clusterBLogs[0].ClusterId

		for _, entry := range clusterALogs {
			assert.Equal(t, clusterAId, entry.ClusterId)
			assert.NotEqual(t, clusterBId, entry.ClusterId)
			assert.NotEqual(t, clusterService.DefaultClusterId, entry.ClusterId)
		}
		for _, entry := range clusterBLogs {
			assert.Equal(t, clusterBId, entry.ClusterId)
			assert.NotEqual(t, clusterAId, entry.ClusterId)
			assert.NotEqual(t, clusterService.DefaultClusterId, entry.ClusterId)
		}
	})

	t.Run("should be able to cluster both logs and spans at the same time", func(t *testing.T) {
		dp := clusterService.NewClusterDataProcessor(ac, cs, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		const clusterAMessage = "Message in common with Cluster A"
		const clusterBMessage = "B's message, random filler words"
		const clusterCEvent = "Precise 100% match for Cluster C"
		const clusterDEvent = "Precise 100% match for Cluster D"

		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		onlyTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		firstBatchLogs := []logModel.LogEntry{
			{
				Id:        "1",
				Message:   clusterAMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt.Add(-time.Second * 500),
				ClusterId: clusterService.DefaultClusterId,
			},
			{
				Id:        "2",
				Message:   clusterAMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt.Add(-time.Second * 500),
				ClusterId: clusterService.DefaultClusterId,
			},
		}
		firstBatchLogData := convertLogToSpanOrLogData(firstBatchLogs)
		firstBatchSpans := []spanModel.Span{
			{
				Id:           "3",
				ClusterId:    clusterService.DefaultClusterId,
				StartTime:    onlyTimeStamp,
				EndTime:      onlyTimeStamp,
				CreatedAt:    createdAt.Add(-time.Second * 500),
				ClusterEvent: clusterCEvent,
				Events:       make([]spanModel.SpanEvent, 0),
				Attributes:   make(map[string]string),
			},
		}
		firstBatchSpanData := convertSpanToSpanOrLogData(firstBatchSpans)
		err = loadDataIntoElasticsearch(ac, firstBatchLogs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.ClusterData(context.Background(), firstBatchLogData)
		if err != nil {
			t.Errorf("failed to cluster data: %v", err)
		}
		err = loadDataIntoElasticsearch(ac, firstBatchSpans, bootstrapper.SpanIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.ClusterData(context.Background(), firstBatchSpanData)
		if err != nil {
			t.Errorf("failed to cluster data: %v", err)
		}

		secondBatchLogs := []logModel.LogEntry{
			{
				Id:        "4",
				Message:   clusterBMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt,
				ClusterId: clusterService.DefaultClusterId,
			},
			{
				Id:        "5",
				Message:   clusterBMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt,
				ClusterId: clusterService.DefaultClusterId,
			},
			{
				Id:        "6",
				Message:   clusterAMessage,
				Timestamp: onlyTimeStamp,
				CreatedAt: createdAt,
				ClusterId: clusterService.DefaultClusterId,
			},
		}
		secondBatchLogData := convertLogToSpanOrLogData(secondBatchLogs)
		secondBatchSpans := []spanModel.Span{
			{
				Id:           "7",
				ClusterId:    clusterService.DefaultClusterId,
				StartTime:    onlyTimeStamp,
				EndTime:      onlyTimeStamp,
				CreatedAt:    createdAt,
				ClusterEvent: clusterDEvent,
				Events:       make([]spanModel.SpanEvent, 0),
				Attributes:   make(map[string]string),
			},
			{
				Id:           "8",
				ClusterId:    clusterService.DefaultClusterId,
				StartTime:    onlyTimeStamp,
				EndTime:      onlyTimeStamp,
				CreatedAt:    createdAt,
				ClusterEvent: clusterDEvent,
				Events:       make([]spanModel.SpanEvent, 0),
				Attributes:   make(map[string]string),
			},
			{
				Id:           "9",
				ClusterId:    clusterService.DefaultClusterId,
				StartTime:    onlyTimeStamp,
				EndTime:      onlyTimeStamp,
				CreatedAt:    createdAt,
				ClusterEvent: clusterCEvent,
				Events:       make([]spanModel.SpanEvent, 0),
				Attributes:   make(map[string]string),
			},
		}
		secondBatchSpanData := convertSpanToSpanOrLogData(secondBatchSpans)
		err = loadDataIntoElasticsearch(ac, secondBatchLogs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.ClusterData(context.Background(), secondBatchLogData)
		if err != nil {
			t.Errorf("failed to cluster data: %v", err)
		}
		err = loadDataIntoElasticsearch(ac, secondBatchSpans, bootstrapper.SpanIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.ClusterData(context.Background(), secondBatchSpanData)
		if err != nil {
			t.Errorf("failed to cluster data: %v", err)
		}

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		logDocs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.LogIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for logs: %v", err)
		}
		logEntries, err := logHelper.ConvertFromDocuments(logDocs)
		if err != nil {
			t.Errorf("Failed to convert log docs to log entries: %v", err)
		}

		spanDocs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.SpanIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for spans: %v", err)
		}
		spanEntries, err := spanHelper.ConvertFromDocuments(spanDocs)
		if err != nil {
			t.Errorf("Failed to convert span docs to span entries: %v", err)
		}

		clusterALogs := make([]logModel.LogEntry, 0)
		clusterBLogs := make([]logModel.LogEntry, 0)
		clusterCSpans := make([]spanModel.Span, 0)
		clusterDSpans := make([]spanModel.Span, 0)

		for _, entry := range logEntries {
			if entry.Message == clusterAMessage {
				clusterALogs = append(clusterALogs, entry)
			} else if entry.Message == clusterBMessage {
				clusterBLogs = append(clusterBLogs, entry)
			}
		}

		for _, entry := range spanEntries {
			if entry.ClusterEvent == clusterCEvent {
				clusterCSpans = append(clusterCSpans, entry)
			} else if entry.ClusterEvent == clusterDEvent {
				clusterDSpans = append(clusterDSpans, entry)
			}
		}

		assert.Equal(t, 3, len(clusterALogs))
		assert.Equal(t, 2, len(clusterBLogs))
		assert.Equal(t, 2, len(clusterCSpans))
		assert.Equal(t, 2, len(clusterDSpans))
		assert.Equal(t, len(logEntries), len(clusterALogs)+len(clusterBLogs))
		assert.Equal(t, len(spanEntries), len(clusterCSpans)+len(clusterDSpans))

		clusterAId := clusterALogs[0].ClusterId
		clusterBId := clusterBLogs[0].ClusterId
		clusterCId := clusterCSpans[0].ClusterId
		clusterDId := clusterDSpans[0].ClusterId

		for _, entry := range clusterALogs {
			assert.Equal(t, clusterAId, entry.ClusterId)
			assert.NotEqual(t, clusterBId, entry.ClusterId)
			assert.NotEqual(t, clusterService.DefaultClusterId, entry.ClusterId)
		}
		for _, entry := range clusterBLogs {
			assert.Equal(t, clusterBId, entry.ClusterId)
			assert.NotEqual(t, clusterAId, entry.ClusterId)
			assert.NotEqual(t, clusterService.DefaultClusterId, entry.ClusterId)
		}
		for _, entry := range clusterCSpans {
			assert.Equal(t, clusterCId, entry.ClusterId)
			assert.NotEqual(t, clusterDId, entry.ClusterId)
			assert.NotEqual(t, clusterService.DefaultClusterId, entry.ClusterId)
		}
		for _, entry := range clusterDSpans {
			assert.Equal(t, clusterDId, entry.ClusterId)
			assert.NotEqual(t, clusterCId, entry.ClusterId)
			assert.NotEqual(t, clusterService.DefaultClusterId, entry.ClusterId)
		}
	})
}

func convertLogToSpanOrLogData(logs []logModel.LogEntry) []map[string]interface{} {
	result := make([]map[string]interface{}, len(logs))
	for i, log := range logs {
		timeStampString := log.Timestamp.Format(time.RFC3339Nano)
		result[i] = map[string]interface{}{
			"_id":        log.Id,
			"service":    log.Service,
			"message":    log.Message,
			"timestamp":  timeStampString,
			"created_at": log.CreatedAt,
			"cluster_id": log.ClusterId,
		}
	}
	return result
}

func convertSpanToSpanOrLogData(spans []spanModel.Span) []map[string]interface{} {
	result := make([]map[string]interface{}, len(spans))
	for i, span := range spans {
		result[i] = map[string]interface{}{
			"_id":           span.Id,
			"service_name":  span.ServiceName,
			"cluster_event": span.ClusterEvent,
			"start_time":    span.StartTime,
			"end_time":      span.EndTime,
			"created_at":    span.CreatedAt,
			"cluster_id":    span.ClusterId,
		}
	}
	return result
}
