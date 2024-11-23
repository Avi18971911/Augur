package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	count "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	logService "github.com/Avi18971911/Augur/pkg/log/service"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	spanService "github.com/Avi18971911/Augur/pkg/trace/service"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"
	"time"
)

func getAllDocumentsQuery() map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
}

func deleteAllDocuments(es *elasticsearch.Client) error {
	indexes := []string{bootstrapper.CountIndexName}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	queryJSON, _ := json.Marshal(query)
	res, err := es.DeleteByQuery(indexes, bytes.NewReader(queryJSON), es.DeleteByQuery.WithRefresh(true))
	if err != nil {
		return fmt.Errorf("failed to delete documents by query: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to delete documents in index %s", res.String())
	}
	return nil
}

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		logger.Error("Failed to create elasticsearch client", zap.Error(err))
	}

	err = deleteAllDocuments(es)
	ac := client.NewAugurClientImpl(es, client.Async)
	countService := count.NewCountService(ac, logger)
	queryMap := getAllDocumentsQuery()
	querySize := 1000
	logBuckets := []count.Bucket{100}
	spanBuckets := []count.Bucket{100}
	searchCtx, searchCancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer searchCancel()
	resultChannel := ac.SearchAfter(searchCtx, queryMap, []string{"log_index"}, nil, &querySize)
	totalLogs := make([]logModel.LogEntry, 0)
	for result := range resultChannel {
		if result.Error != nil {
			logger.Fatal("Error in search after", zap.Error(*result.Error))
		} else if result.Success == nil {
			logger.Fatal("Result is nil")
		} else {
			logDocs, err := logService.ConvertToLogDocuments(result.Success.Result)
			if err != nil {
				logger.Fatal("Failed to convert search results to log documents", zap.Error(err))
			}
			totalLogs = append(totalLogs, logDocs...)
		}
	}
	for _, log := range totalLogs {
		csCtx, csCancel := context.WithTimeout(context.Background(), 100*time.Second)
		err = countService.CountAndUpdateOccurrences(csCtx, log.ClusterId, count.TimeInfo{
			LogInfo: &count.LogInfo{
				Timestamp: log.Timestamp,
			},
		}, logBuckets)
		if err != nil {
			csCancel()
			logger.Fatal("Failed to count and update occurrences", zap.Error(err))
		}
		csCancel()
	}

	newResultChannel := ac.SearchAfter(searchCtx, queryMap, []string{"span_index"}, nil, &querySize)
	totalSpans := make([]spanModel.Span, 0)
	for result := range newResultChannel {
		if result.Error != nil {
			logger.Fatal("Error in search after", zap.Error(*result.Error))
		} else if result.Success == nil {
			logger.Fatal("Result is nil")
		} else {
			spanDocs, err := spanService.ConvertToSpanDocuments(result.Success.Result)
			if err != nil {
				logger.Fatal("Failed to convert search results to span documents", zap.Error(err))
			}
			totalSpans = append(totalSpans, spanDocs...)
		}
	}
	for _, span := range totalSpans {
		csCtx, csCancel := context.WithTimeout(context.Background(), 100*time.Second)
		err = countService.CountAndUpdateOccurrences(csCtx, span.ClusterId, count.TimeInfo{
			SpanInfo: &count.SpanInfo{
				FromTime: span.StartTime,
				ToTime:   span.EndTime,
			},
		}, spanBuckets)
		if err != nil {
			csCancel()
			logger.Fatal("Failed to count and update occurrences", zap.Error(err))
		}
		csCancel()
	}
	logger.Info("Successfully counted and updated occurrences")
}
