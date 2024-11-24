package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
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
	countService *countService.CountService,
	buckets []countService.Bucket,
	logger *zap.Logger,
) (*countModel.GetCountAndUpdateOccurrencesQueryConstituentsResult, error) {
	typedLogs, err := logService.ConvertToLogDocuments([]map[string]interface{}{untypedLog})
	if err != nil {
		logger.Error("Failed to convert log to log documents", zap.Error(err))
		return nil, err
	}
	typedLog := typedLogs[0]
	csCtx, csCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer csCancel()
	result, err := countService.GetCountAndUpdateOccurrencesQueryConstituents(
		csCtx,
		typedLog.ClusterId,
		countModel.TimeInfo{
			LogInfo: &countModel.LogInfo{
				Timestamp: typedLog.Timestamp,
			},
		},
		buckets,
	)
	if err != nil {
		logger.Error("Failed to count and update occurrences for logs", zap.Error(err))
		return nil, err
	}
	return result, nil
}

func processSpan(
	untypedSpan map[string]interface{},
	countService *countService.CountService,
	buckets []countService.Bucket,
	logger *zap.Logger,
) (*countModel.GetCountAndUpdateOccurrencesQueryConstituentsResult, error) {
	typedSpans, err := spanService.ConvertToSpanDocuments([]map[string]interface{}{untypedSpan})
	if err != nil {
		logger.Error("Failed to convert span to span documents", zap.Error(err))
		return nil, err
	}
	typedSpan := typedSpans[0]
	csCtx, csCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer csCancel()
	result, err := countService.GetCountAndUpdateOccurrencesQueryConstituents(
		csCtx,
		typedSpan.ClusterId,
		countModel.TimeInfo{
			SpanInfo: &countModel.SpanInfo{
				FromTime: typedSpan.StartTime,
				ToTime:   typedSpan.EndTime,
			},
		},
		buckets,
	)
	if err != nil {
		logger.Error("Failed to count and update occurrences for spans", zap.Error(err))
		return nil, err
	}
	return result, nil
}

func processData(
	ac client.AugurClient,
	dataByClusterId map[string][]map[string]interface{},
	countService *countService.CountService,
	buckets []countService.Bucket,
	logger *zap.Logger,
) {
	const workerCount = 50
	inputChannel := make(chan []map[string]interface{}, len(dataByClusterId))
	for _, data := range dataByClusterId {
		inputChannel <- data
	}
	close(inputChannel)

	var wg sync.WaitGroup
	wg.Add(workerCount)

	resultChannel := make(chan *countModel.GetCountAndUpdateOccurrencesQueryConstituentsResult)
	clusterIdList := make([]string, 0, len(dataByClusterId))
	metaMapList := make([]client.MetaMap, 0, len(dataByClusterId))
	documentMapList := make([]client.DocumentMap, 0, len(dataByClusterId))

	for i := 0; i < workerCount; i++ {
		go func() {
			logger.Info("Worker started processing data", zap.Int("workerId", i))
			defer wg.Done()
			for untypedData := range inputChannel {
				for _, untypedItem := range untypedData {
					logger.Debug("Processing data", zap.Any("data", untypedItem))
					dataType := detectDataType(untypedItem)
					switch dataType {
					case Log:
						res, err := processLog(untypedItem, countService, buckets, logger)
						if err != nil {
							logger.Error("Failed to process log", zap.Error(err))
						} else {
							resultChannel <- res
						}
					case Span:
						res, err := processSpan(untypedItem, countService, buckets, logger)
						if err != nil {
							logger.Error("Failed to process span", zap.Error(err))
						} else {
							resultChannel <- res
						}
					case Unknown:
						logger.Error("Unknown data type", zap.Any("data", untypedItem))
					}
				}
			}
		}()
	}

	wg.Wait()
	close(resultChannel)
	logger.Info("All workers have finished processing data")
	for result := range resultChannel {
		if result == nil {
			continue
		}
		if result.ClusterIds != nil && len(result.ClusterIds) > 0 {
			clusterIdList = append(clusterIdList, result.ClusterIds...)
		}
		if result.MetaMapList != nil && len(result.MetaMapList) > 0 {
			if result.DocumentMapList == nil || len(result.DocumentMapList) == 0 {
				logger.Fatal("DocumentMapList is nil or empty, despite MetaMapList being non-empty")
			}
			metaMapList = append(metaMapList, result.MetaMapList...)
			documentMapList = append(documentMapList, result.DocumentMapList...)
		}
	}

	logger.Info("Successfully processed data",
		zap.Int("clusterIdList", len(clusterIdList)),
		zap.Int("metaMapList", len(metaMapList)),
		zap.Int("documentMapList", len(documentMapList)),
	)

	updateCtx, updateCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer updateCancel()
	err := ac.BulkIndex(updateCtx, metaMapList, documentMapList, bootstrapper.CountIndexName)
	if err != nil {
		logger.Fatal("Failed to bulk index", zap.Error(err))
	}
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
	cs := countService.NewCountService(ac, logger)
	queryMap := getAllDocumentsQuery()
	querySize := 1000
	buckets := []countService.Bucket{100}
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
			processData(ac, dataByClusterId, cs, buckets, logger)
		}
	}

	logger.Info("Successfully counted and updated occurrences")
}
