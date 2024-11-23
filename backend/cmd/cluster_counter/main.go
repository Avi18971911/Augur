package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	count "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logService "github.com/Avi18971911/Augur/pkg/log/service"
	spanService "github.com/Avi18971911/Augur/pkg/trace/service"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"
	"sync"
	"time"
)

type DataType string

const (
	Log     DataType = "log"
	Span    DataType = "span"
	Unknown DataType = "unknown"
)

func getAllDocumentsQuery() map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
}

func detectDataType(data map[string]interface{}) DataType {
	if _, ok := data["start_time"]; ok && data["end_time"] != nil {
		return Span
	}
	if _, ok := data["timestamp"]; ok && data["message"] != nil {
		return Log
	}
	return Unknown
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

func groupDataByClusterId(data []map[string]interface{}) (map[string][]map[string]interface{}, error) {
	groupedData := make(map[string][]map[string]interface{})
	for _, item := range data {
		if clusterId, ok := item["cluster_id"].(string); !ok {
			return nil, fmt.Errorf("failed to convert cluster_id to string %v", item["cluster_id"])
		} else {
			groupedData[clusterId] = append(groupedData[clusterId], item)
		}
	}
	return groupedData, nil
}

func processLog(
	untypedLog map[string]interface{},
	countService *count.CountService,
	buckets []count.Bucket,
	logger *zap.Logger,
) error {
	typedLogs, err := logService.ConvertToLogDocuments([]map[string]interface{}{untypedLog})
	if err != nil {
		logger.Error("Failed to convert log to log documents", zap.Error(err))
		return err
	}
	typedLog := typedLogs[0]
	csCtx, csCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer csCancel()
	err = countService.CountAndUpdateOccurrences(csCtx, typedLog.ClusterId, count.TimeInfo{
		LogInfo: &count.LogInfo{
			Timestamp: typedLog.Timestamp,
		},
	}, buckets)
	if err != nil {
		logger.Error("Failed to count and update occurrences for logs", zap.Error(err))
		return err
	}
	return nil
}

func processSpan(
	untypedSpan map[string]interface{},
	countService *count.CountService,
	buckets []count.Bucket,
	logger *zap.Logger,
) error {
	typedSpans, err := spanService.ConvertToSpanDocuments([]map[string]interface{}{untypedSpan})
	if err != nil {
		logger.Error("Failed to convert span to span documents", zap.Error(err))
		return err
	}
	typedSpan := typedSpans[0]
	csCtx, csCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer csCancel()
	err = countService.CountAndUpdateOccurrences(
		csCtx,
		typedSpan.ClusterId,
		count.TimeInfo{
			SpanInfo: &count.SpanInfo{
				FromTime: typedSpan.StartTime,
				ToTime:   typedSpan.EndTime,
			},
		}, buckets,
	)
	if err != nil {
		logger.Error("Failed to count and update occurrences for spans", zap.Error(err))
		return err
	}
	return nil
}

func processData(
	dataByClusterId map[string][]map[string]interface{},
	countService *count.CountService,
	buckets []count.Bucket,
	logger *zap.Logger,
) {
	const workerCount = 50
	dataChannel := make(chan []map[string]interface{}, len(dataByClusterId))
	for _, data := range dataByClusterId {
		dataChannel <- data
	}
	close(dataChannel)

	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for untypedData := range dataChannel {
				for _, untypedItem := range untypedData {
					dataType := detectDataType(untypedItem)
					switch dataType {
					case Log:
						err := processLog(untypedItem, countService, buckets, logger)
						if err != nil {
							logger.Error("Failed to process log", zap.Error(err))
						}
					case Span:
						err := processSpan(untypedItem, countService, buckets, logger)
						if err != nil {
							logger.Error("Failed to process span", zap.Error(err))
						}
					case Unknown:
						logger.Error("Unknown data type", zap.Any("data", untypedItem))
					}
				}
			}
		}()
	}
	wg.Wait()
}

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		logger.Error("Failed to create elasticsearch client", zap.Error(err))
	}

	err = deleteAllDocuments(es)
	ac := client.NewAugurClientImpl(es, client.Wait)
	countService := count.NewCountService(ac, logger)
	queryMap := getAllDocumentsQuery()
	querySize := 1000
	buckets := []count.Bucket{100}
	searchCtx, searchCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer searchCancel()
	resultChannel := ac.SearchAfter(
		searchCtx,
		queryMap,
		[]string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName},
		nil,
		&querySize,
	)
	for result := range resultChannel {
		if result.Error != nil {
			logger.Fatal("Error in search after", zap.Error(*result.Error))
		} else if result.Success == nil {
			logger.Fatal("Result is nil")
		} else {
			dataByClusterId, err := groupDataByClusterId(result.Success.Result)
			if err != nil {
				logger.Fatal("Failed to group data by cluster id", zap.Error(err))
			}
			processData(dataByClusterId, countService, buckets, logger)
		}
	}

	logger.Info("Successfully counted and updated occurrences")
}
