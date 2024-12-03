package service

import (
	"context"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logService "github.com/Avi18971911/Augur/pkg/log/service"
	spanService "github.com/Avi18971911/Augur/pkg/trace/service"
	"go.uber.org/zap"
	"sync"
	"time"
)

const timeout = 10 * time.Second
const searchAfterTimeout = 60 * time.Second
const workerCount = 50

var querySize = 10000

type DataProcessorService struct {
	ac           client.AugurClient
	cs           *countService.CountService
	lcs          logService.LogClusterService
	scs          spanService.SpanClusterService
	logger       *zap.Logger
	searchParams *client.SearchAfterParams
}

func NewDataProcessorService(
	ac client.AugurClient,
	cs *countService.CountService,
	lcs logService.LogClusterService,
	scs spanService.SpanClusterService,
	logger *zap.Logger,
) *DataProcessorService {
	return &DataProcessorService{
		ac:           ac,
		cs:           cs,
		lcs:          lcs,
		scs:          scs,
		logger:       logger,
		searchParams: nil,
	}
}

func (dps *DataProcessorService) ProcessData(
	ctx context.Context,
	buckets []countModel.Bucket,
	indices []string,
) ([]bool, []error) {
	searchCtx, cancel := context.WithTimeout(ctx, searchAfterTimeout)
	defer cancel()

	resultChannel := dps.ac.SearchAfter(
		searchCtx,
		getAllDocumentsQuery(),
		indices,
		dps.searchParams,
		&querySize,
	)
	successes, errors := make([]bool, 0), make([]error, 0)
	i := 0
	for result := range resultChannel {
		if result.Error != nil {
			dps.logger.Error("Error in search after", zap.Error(result.Error))
			errors = append(errors, fmt.Errorf("error in search after: %w", result.Error))
			successes = append(successes, false)
		} else if result.Success == nil {
			dps.logger.Error("Result of SearchAfter is nil")
			errors = append(errors, fmt.Errorf("result of SearchAfter is nil"))
			successes = append(successes, false)
		} else {
			if len(result.Success.Result) == 0 {
				break
			}
			err := dps.clusterAndIncreaseCount(
				ctx,
				result.Success.Result,
				buckets,
				indices,
			)
			if err != nil {
				dps.logger.Error(
					"Failed to increase count for overlaps and misses for the given parameters",
					zap.Error(err),
					zap.Int("page", i),
				)
				errors = append(errors, fmt.Errorf("failed to increase count for overlaps and misses: %w", err))
				successes = append(successes, false)
			} else {
				successes = append(successes, true)
				errors = append(errors, nil)
			}
			dps.searchParams = &result.Success.ContinueParams
		}
		i++
	}
	return successes, errors
}

func (dps *DataProcessorService) clusterAndIncreaseCount(
	ctx context.Context,
	clusterOrLogData []map[string]interface{},
	buckets []countModel.Bucket,
	indices []string,
) error {
	err := dps.clusterData(ctx, clusterOrLogData)
	if err != nil {
		return fmt.Errorf("failed to cluster data: %w", err)
	}
	err = dps.increaseCountForOverlapsAndMisses(ctx, clusterOrLogData, buckets, indices)
	if err != nil {
		return fmt.Errorf("failed to increase count for overlaps and misses: %w", err)
	}
	return nil
}

func getResultsWithWorkers[
	inputType any,
	outputType any,
](
	ctx context.Context,
	input []inputType,
	inputFunction func(ctx context.Context, input inputType) (outputType, string, error),
	workerCount int,
	logger *zap.Logger,
) chan outputType {
	inputChannel := make(chan inputType, len(input))
	for _, item := range input {
		inputChannel <- item
	}
	close(inputChannel)

	var wg sync.WaitGroup
	wg.Add(workerCount)

	resultChannel := make(
		chan outputType,
		len(input),
	)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for input := range inputChannel {
				csCtx, csCancel := context.WithTimeout(ctx, timeout)
				result, errorMsg, err := inputFunction(csCtx, input)
				if err != nil {
					logger.Error(errorMsg, zap.Error(err))
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

	return resultChannel
}

func splitLogsAndSpans(data []map[string]interface{}) ([]map[string]interface{}, []map[string]interface{}) {
	logs, spans := make([]map[string]interface{}, 0), make([]map[string]interface{}, 0)
	for _, item := range data {
		dataType := detectDataType(item)
		if dataType == model.Log {
			logs = append(logs, item)
		} else if dataType == model.Span {
			spans = append(spans, item)
		}
	}
	return logs, spans
}

func detectDataType(data map[string]interface{}) model.DataType {
	if _, ok := data["start_time"]; ok && data["end_time"] != nil {
		return model.Span
	}
	if _, ok := data["timestamp"]; ok && data["message"] != nil {
		return model.Log
	}
	return model.Unknown
}
