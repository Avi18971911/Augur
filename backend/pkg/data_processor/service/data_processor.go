package service

import (
	"context"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logService "github.com/Avi18971911/Augur/pkg/log/service"
	spanService "github.com/Avi18971911/Augur/pkg/trace/service"
	"go.uber.org/zap"
	"sync"
	"time"
)

const timeout = 2 * time.Second

var querySize = 10000
var buckets = []countService.Bucket{100}

type DataProcessorService struct {
	ac           client.AugurClient
	cs           *countService.CountService
	logger       *zap.Logger
	searchParams *client.SearchAfterParams
}

func NewDataProcessorService(
	ac client.AugurClient,
	cs *countService.CountService,
	logger *zap.Logger,
) *DataProcessorService {
	return &DataProcessorService{
		ac:           ac,
		cs:           cs,
		logger:       logger,
		searchParams: nil,
	}
}

func (dps *DataProcessorService) ProcessData(ctx context.Context, indices []string) {
	// TODO: Add the Clustering Code here as well

	dps.processCounts(ctx, indices)
}

func (dps *DataProcessorService) processCounts(
	ctx context.Context,
	indices []string,
) {
	searchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resultChannel := dps.ac.SearchAfter(
		searchCtx,
		getAllDocumentsQuery(),
		indices,
		dps.searchParams,
		&querySize,
	)
	i := 0
	for result := range resultChannel {
		if result.Error != nil {
			dps.logger.Error("Error in search after", zap.Error(result.Error))
		} else if result.Success == nil {
			dps.logger.Error("Result is nil")
		} else {
			dataByClusterId, err := groupDataByClusterId(result.Success.Result)
			if err != nil {
				dps.logger.Error("Failed to group data by cluster id", zap.Error(err))
				continue
			}
			go func() {
				err := dps.increaseCountForOverlapsAndMisses(ctx, dataByClusterId, indices)
				if err != nil {
					dps.logger.Error(
						"Failed to increase count for overlaps and misses for the given parameters",
						zap.Error(err),
						zap.Time("searchParams", dps.searchParams.CreatedAt),
						zap.Int("page", i),
					)
				}
			}()
		}
		i++
	}
}

func (dps *DataProcessorService) increaseCountForOverlapsAndMisses(
	ctx context.Context,
	dataByClusterId map[string][]map[string]interface{},
	indices []string,
) error {
	increaseMissesInput, err := dps.processCountsForOverlaps(ctx, dataByClusterId, indices)
	if err != nil {
		return fmt.Errorf("failed to process clusters: %w", err)
	}

	if len(increaseMissesInput) > 0 {
		err = dps.processIncrementsForMisses(ctx, increaseMissesInput)
		if err != nil {
			return fmt.Errorf("failed to process increments for misses: %w", err)
		}
	}
	return nil
}

func (dps *DataProcessorService) processCountsForOverlaps(
	ctx context.Context,
	dataByClusterId map[string][]map[string]interface{},
	indices []string,
) ([]countModel.IncreaseMissesInput, error) {
	const workerCount = 50
	inputChannel := make(chan []map[string]interface{}, len(dataByClusterId))
	for _, data := range dataByClusterId {
		inputChannel <- data
	}
	close(inputChannel)

	var wg sync.WaitGroup
	wg.Add(workerCount)

	resultChannel := make(chan *countModel.GetCountAndUpdateQueryDetails, len(dataByClusterId))
	increaseMissesList := make([]countModel.IncreaseMissesInput, 0, len(dataByClusterId))
	metaMapList := make([]client.MetaMap, 0, len(dataByClusterId))
	documentMapList := make([]client.DocumentMap, 0, len(dataByClusterId))

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for untypedData := range inputChannel {
				for _, untypedItem := range untypedData {
					dataType := detectDataType(untypedItem)
					switch dataType {
					case model.Log:
						res, err := dps.processLog(ctx, untypedItem, indices)
						if err != nil {
							dps.logger.Error("Failed to process log", zap.Error(err))
						} else {
							resultChannel <- res
						}
					case model.Span:
						res, err := dps.processSpan(ctx, untypedItem, indices)
						if err != nil {
							dps.logger.Error("Failed to process span", zap.Error(err))
						} else {
							resultChannel <- res
						}
					case model.Unknown:
						dps.logger.Error("Unknown data type", zap.Any("data", untypedItem))
					}
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	for result := range resultChannel {
		if result == nil {
			continue
		}
		increaseMissesList = append(increaseMissesList, result.IncreaseIncrementForMissesInput)
		if result.MetaMapList != nil && len(result.MetaMapList) > 0 {
			if result.DocumentMapList == nil || len(result.DocumentMapList) == 0 {
				return nil, fmt.Errorf("DocumentMapList is nil or empty, despite MetaMapList being non-empty")
			}
			metaMapList = append(metaMapList, result.MetaMapList...)
			documentMapList = append(documentMapList, result.DocumentMapList...)
		}
	}

	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	err := dps.ac.BulkIndex(updateCtx, metaMapList, documentMapList, bootstrapper.CountIndexName)
	if err != nil {
		return nil, fmt.Errorf("failed to bulk index count increments: %w", err)
	}
	return increaseMissesList, nil
}

func (dps *DataProcessorService) processLog(
	ctx context.Context,
	untypedLog map[string]interface{},
	indices []string,
) (*countModel.GetCountAndUpdateQueryDetails, error) {
	typedLogs, err := logService.ConvertToLogDocuments([]map[string]interface{}{untypedLog})
	if err != nil {
		dps.logger.Error("Failed to convert log to log documents", zap.Error(err))
		return nil, err
	}
	typedLog := typedLogs[0]
	csCtx, csCancel := context.WithTimeout(ctx, timeout)
	defer csCancel()
	result, err := dps.cs.GetCountAndUpdateOccurrencesQueryConstituents(
		csCtx,
		typedLog.ClusterId,
		countModel.TimeInfo{
			LogInfo: &countModel.LogInfo{
				Timestamp: typedLog.Timestamp,
			},
		},
		indices,
		buckets,
	)
	if err != nil {
		dps.logger.Error("Failed to count and update occurrences for logs", zap.Error(err))
		return nil, err
	}
	return result, nil
}

func (dps *DataProcessorService) processSpan(
	ctx context.Context,
	untypedSpan map[string]interface{},
	indices []string,
) (*countModel.GetCountAndUpdateQueryDetails, error) {
	typedSpans, err := spanService.ConvertToSpanDocuments([]map[string]interface{}{untypedSpan})
	if err != nil {
		dps.logger.Error("Failed to convert span to span documents", zap.Error(err))
		return nil, err
	}
	typedSpan := typedSpans[0]
	csCtx, csCancel := context.WithTimeout(ctx, timeout)
	defer csCancel()
	result, err := dps.cs.GetCountAndUpdateOccurrencesQueryConstituents(
		csCtx,
		typedSpan.ClusterId,
		countModel.TimeInfo{
			SpanInfo: &countModel.SpanInfo{
				FromTime: typedSpan.StartTime,
				ToTime:   typedSpan.EndTime,
			},
		},
		indices,
		buckets,
	)
	if err != nil {
		dps.logger.Error("Failed to count and update occurrences for spans", zap.Error(err))
		return nil, err
	}
	return result, nil
}

func (dps *DataProcessorService) processIncrementsForMisses(
	ctx context.Context,
	increaseMissesInput []countModel.IncreaseMissesInput,
) error {
	const workerCount = 50
	inputChannel := make(chan countModel.IncreaseMissesInput, len(increaseMissesInput))
	for _, input := range increaseMissesInput {
		inputChannel <- input
	}
	close(inputChannel)

	var wg sync.WaitGroup
	wg.Add(workerCount)

	resultChannel := make(
		chan *countModel.GetIncrementMissesQueryDetails,
		len(increaseMissesInput),
	)
	metaMapList := make([]client.MetaMap, 0, len(increaseMissesInput))
	documentMapList := make([]client.DocumentMap, 0, len(increaseMissesInput))

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for input := range inputChannel {
				csCtx, csCancel := context.WithTimeout(context.Background(), 10*time.Second)
				result, err := dps.cs.GetIncrementMissesQueryInfo(csCtx, input)
				if err != nil {
					dps.logger.Error("Failed to increment occurrences for misses", zap.Error(err))
				} else {
					resultChannel <- result
				}
				csCancel()
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultChannel)
	}()

	for result := range resultChannel {
		if result == nil {
			continue
		}
		if result.MetaMapList != nil && len(result.MetaMapList) > 0 {
			if result.DocumentMapList == nil || len(result.DocumentMapList) == 0 {
				return fmt.Errorf("DocumentMapList is nil or empty, despite MetaMapList being non-empty")
			}
			metaMapList = append(metaMapList, result.MetaMapList...)
			documentMapList = append(documentMapList, result.DocumentMapList...)
		}
	}

	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	err := dps.ac.BulkIndex(updateCtx, metaMapList, documentMapList, bootstrapper.CountIndexName)
	if err != nil {
		return fmt.Errorf("failed to bulk index when incrementing misses: %w", err)
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

// TODO: Move to AugurClient
func detectDataType(data map[string]interface{}) model.DataType {
	if _, ok := data["start_time"]; ok && data["end_time"] != nil {
		return model.Span
	}
	if _, ok := data["timestamp"]; ok && data["message"] != nil {
		return model.Log
	}
	return model.Unknown
}
