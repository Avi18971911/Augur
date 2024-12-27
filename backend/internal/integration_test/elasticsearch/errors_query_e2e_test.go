package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/internal/otel_server/log/model"
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

		input := log_and_span.ErrorSearchParams{
			Service:   &service,
			StartTime: &startTime,
			EndTime:   &endTime,
		}
		body, err := json.Marshal(input)
		assert.NoError(t, err)
		req := httptest.NewRequest("GET", "/error", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		var response handler.ErrorResponseDTO
		handler.ErrorHandler(context.Background(), qs, logger)(w, req)

		err = json.NewDecoder(w.Result().Body).Decode(&response)
		assert.NoError(t, err)
		expectedLogIds := []string{logs[0].Id, logs[3].Id}
		actualLogIds := make([]string, 0)
		for _, log := range response.Errors {
			actualLogIds = append(actualLogIds, log.LogDTO.Id)
		}
		assert.EqualValues(t, expectedLogIds, actualLogIds)
	})
}
