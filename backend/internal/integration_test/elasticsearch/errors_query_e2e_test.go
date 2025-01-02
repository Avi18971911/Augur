package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/internal/otel_server/log/model"
	"github.com/Avi18971911/Augur/internal/otel_server/trace/model"
	"github.com/Avi18971911/Augur/internal/query_server/handler"
	"github.com/Avi18971911/Augur/internal/query_server/service/log_and_span"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"net/http/httptest"
	"testing"
	"time"
)

func TestErrorsQuery(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Immediate)
	logger, err := zap.NewProduction()
	if err != nil {
		t.Errorf("Failed to create logger: %v", err)
	}

	qs := log_and_span.NewLogAndSpanService(
		ac,
		logger,
	)

	t.Run("should be able to detect all errors in logs", func(t *testing.T) {
		err := deleteAllDocuments(es)
		assert.NoError(t, err)
		timestamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		notWithinTimestampLength := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		createdAt := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		service := "service1"

		logs := []logModel.LogEntry{
			{
				Id:        "1",
				Severity:  logModel.ErrorLevel,
				Message:   "Error message 1",
				Timestamp: timestamp,
				CreatedAt: createdAt,
				Service:   service,
				ClusterId: "cluster1",
			},
			{
				Id:        "2",
				Severity:  logModel.ErrorLevel,
				Message:   "Error message 2",
				Timestamp: timestamp,
				CreatedAt: createdAt,
				Service:   "service2",
				ClusterId: "cluster1",
			},
			{
				Id:        "3",
				Severity:  logModel.InfoLevel,
				Message:   "Info message 1",
				Timestamp: timestamp,
				CreatedAt: createdAt,
				Service:   service,
				ClusterId: "cluster1",
			},
			{
				Id:        "4",
				Severity:  logModel.ErrorLevel,
				Message:   "Error message 3",
				Timestamp: timestamp,
				CreatedAt: createdAt,
				Service:   service,
				ClusterId: "cluster1",
			},
			{
				Id:        "5",
				Severity:  logModel.ErrorLevel,
				Message:   "Error message 4",
				Timestamp: notWithinTimestampLength,
				CreatedAt: createdAt,
				Service:   service,
				ClusterId: "cluster1",
			},
		}

		err = loadDataIntoElasticsearch(ac, logs, bootstrapper.LogIndexName)

		startTime := timestamp.Add(-time.Hour).Format(time.RFC3339)
		endTime := timestamp.Add(time.Hour).Format(time.RFC3339)

		input := log_and_span.SearchParams{
			Service:   &service,
			StartTime: &startTime,
			EndTime:   &endTime,
			Types:     []log_and_span.Type{log_and_span.Error},
		}
		body, err := json.Marshal(input)
		assert.NoError(t, err)
		req := httptest.NewRequest("GET", "/error", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		var response handler.DataResponseDTO
		handler.LogAndSpanHandler(context.Background(), qs, logger)(w, req)

		err = json.NewDecoder(w.Result().Body).Decode(&response)
		assert.NoError(t, err)
		expectedLogIds := []string{logs[0].Id, logs[3].Id}
		actualLogIds := make([]string, 0)
		for _, log := range response.Data {
			actualLogIds = append(actualLogIds, log.LogDTO.Id)
		}
		assert.EqualValues(t, expectedLogIds, actualLogIds)
	})

	t.Run("should be able to detect all errors in spans", func(t *testing.T) {
		err := deleteAllDocuments(es)
		assert.NoError(t, err)
		startTime := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		endTime := time.Date(2021, 1, 1, 1, 0, 0, 0, time.UTC)
		createdAt := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		notWithinStartTime := time.Date(2021, 1, 1, 2, 0, 0, 0, time.UTC)
		notWithinEndTime := time.Date(2021, 1, 1, 3, 0, 0, 0, time.UTC)
		service := "service1"

		spans := []model.Span{
			{
				Id:           "1",
				CreatedAt:    createdAt,
				StartTime:    startTime,
				EndTime:      endTime,
				Service:      service,
				ClusterId:    "cluster1",
				ClusterEvent: "cluster_event1",
				SpanID:       "span1",
				ParentSpanID: "parent_span1",
				TraceID:      "trace1",
				ActionName:   "action1",
				Status: model.Status{
					Message: "error",
					Code:    model.ERROR,
				},
				Events:     make([]model.SpanEvent, 0),
				Attributes: make(map[string]string),
			},
			{
				Id:           "2",
				CreatedAt:    createdAt,
				StartTime:    startTime,
				EndTime:      endTime,
				Service:      "service2",
				ClusterId:    "cluster1",
				ClusterEvent: "cluster_event2",
				SpanID:       "span2",
				ParentSpanID: "parent_span2",
				TraceID:      "trace2",
				ActionName:   "action2",
				Status: model.Status{
					Message: "error",
					Code:    model.ERROR,
				},
				Events:     make([]model.SpanEvent, 0),
				Attributes: make(map[string]string),
			},
			{
				Id:           "3",
				CreatedAt:    createdAt,
				StartTime:    startTime,
				EndTime:      endTime,
				Service:      service,
				ClusterId:    "cluster1",
				ClusterEvent: "cluster_event3",
				SpanID:       "span3",
				ParentSpanID: "parent_span3",
				TraceID:      "trace3",
				ActionName:   "action3",
				Status: model.Status{
					Message: "ok",
					Code:    model.OK,
				},
				Events:     make([]model.SpanEvent, 0),
				Attributes: make(map[string]string),
			},
			{
				Id:           "4",
				CreatedAt:    createdAt,
				StartTime:    startTime,
				EndTime:      endTime,
				Service:      service,
				ClusterId:    "cluster1",
				ClusterEvent: "cluster_event4",
				SpanID:       "span4",
				ParentSpanID: "parent_span4",
				TraceID:      "trace4",
				ActionName:   "action4",
				Status: model.Status{
					Message: "error",
					Code:    model.ERROR,
				},
				Events:     make([]model.SpanEvent, 0),
				Attributes: make(map[string]string),
			},
			{
				Id:           "5",
				CreatedAt:    createdAt,
				StartTime:    notWithinStartTime,
				EndTime:      notWithinEndTime,
				Service:      service,
				ClusterId:    "cluster1",
				ClusterEvent: "cluster_event5",
				SpanID:       "span5",
				ParentSpanID: "parent_span5",
				TraceID:      "trace5",
				ActionName:   "action5",
				Status: model.Status{
					Message: "error",
					Code:    model.ERROR,
				},
				Events:     make([]model.SpanEvent, 0),
				Attributes: make(map[string]string),
			},
		}

		err = loadDataIntoElasticsearch(ac, spans, bootstrapper.SpanIndexName)
		assert.NoError(t, err)

		startTimeStr := startTime.Format(time.RFC3339)
		endTimeStr := endTime.Format(time.RFC3339)
		input := log_and_span.SearchParams{
			Service:   &service,
			StartTime: &startTimeStr,
			EndTime:   &endTimeStr,
			Types:     []log_and_span.Type{log_and_span.Error},
		}
		body, err := json.Marshal(input)
		assert.NoError(t, err)
		req := httptest.NewRequest("GET", "/error", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		var response handler.DataResponseDTO
		handler.LogAndSpanHandler(context.Background(), qs, logger)(w, req)

		err = json.NewDecoder(w.Result().Body).Decode(&response)
		assert.NoError(t, err)
		expectedSpanIds := []string{spans[0].Id, spans[3].Id}
		actualSpanIds := make([]string, 0)
		for _, span := range response.Data {
			actualSpanIds = append(actualSpanIds, span.SpanDTO.Id)
		}
		assert.EqualValues(t, expectedSpanIds, actualSpanIds)
	})

	t.Run("should return all possible errors with empty params", func(t *testing.T) {
		err := deleteAllDocuments(es)
		assert.NoError(t, err)

		timestamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		createdAt := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		startTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
		endTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

		logs := []logModel.LogEntry{
			{
				Id:        "1",
				Severity:  logModel.ErrorLevel,
				Message:   "Error message 1",
				Timestamp: timestamp,
				CreatedAt: createdAt,
				Service:   "service1",
				ClusterId: "cluster1",
			},
			{
				Id:        "2",
				Severity:  logModel.InfoLevel,
				Message:   "Info message 1",
				Timestamp: timestamp,
				CreatedAt: createdAt,
				Service:   "service1",
				ClusterId: "cluster1",
			},
		}

		err = loadDataIntoElasticsearch(ac, logs, bootstrapper.LogIndexName)
		assert.NoError(t, err)

		spans := []model.Span{
			{
				Id:           "3",
				CreatedAt:    createdAt,
				StartTime:    startTime,
				EndTime:      endTime,
				Service:      "service1",
				ClusterId:    "cluster1",
				ClusterEvent: "cluster_event1",
				SpanID:       "span1",
				ParentSpanID: "parent_span1",
				TraceID:      "trace1",
				ActionName:   "action1",
				Status: model.Status{
					Message: "error",
					Code:    model.ERROR,
				},
				Events:     make([]model.SpanEvent, 0),
				Attributes: make(map[string]string),
			},
			{
				Id:           "4",
				CreatedAt:    createdAt,
				StartTime:    startTime,
				EndTime:      endTime,
				Service:      "service1",
				ClusterId:    "cluster1",
				ClusterEvent: "cluster_event2",
				SpanID:       "span2",
				ParentSpanID: "parent_span2",
				TraceID:      "trace2",
				ActionName:   "action2",
				Status: model.Status{
					Message: "ok",
					Code:    model.OK,
				},
				Events:     make([]model.SpanEvent, 0),
				Attributes: make(map[string]string),
			},
		}

		err = loadDataIntoElasticsearch(ac, spans, bootstrapper.SpanIndexName)
		assert.NoError(t, err)

		input := log_and_span.SearchParams{
			Types: []log_and_span.Type{log_and_span.Error},
		}
		body, err := json.Marshal(input)
		assert.NoError(t, err)
		req := httptest.NewRequest("GET", "/error", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		var response handler.DataResponseDTO
		handler.LogAndSpanHandler(context.Background(), qs, logger)(w, req)

		err = json.NewDecoder(w.Result().Body).Decode(&response)
		assert.NoError(t, err)
		expectedLogIds := []string{logs[0].Id}
		expectedSpanIds := []string{spans[0].Id}
		actualSpanIds := make([]string, 0)
		actualLogIds := make([]string, 0)
		for _, logOrSpan := range response.Data {
			if logOrSpan.LogDTO != nil {
				actualLogIds = append(actualLogIds, logOrSpan.LogDTO.Id)
			} else {
				actualSpanIds = append(actualSpanIds, logOrSpan.SpanDTO.Id)
			}
		}

		assert.EqualValues(t, expectedLogIds, actualLogIds)
		assert.EqualValues(t, expectedSpanIds, actualSpanIds)
	})
}
