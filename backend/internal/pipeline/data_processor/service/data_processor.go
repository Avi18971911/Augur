package service

import (
	"context"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/pipeline/data_processor/model"
	"go.uber.org/zap"
	"sync"
	"time"
)

const timeout = 10 * time.Second
const searchAfterTimeout = 60 * time.Second
const dpWorkerCount = 5

var querySize = 10000

type DataProcessorService struct {
	ac           client.AugurClient
	logger       *zap.Logger
	searchParams *client.SearchAfterParams
}

func NewDataProcessorService(
	ac client.AugurClient,
	logger *zap.Logger,
) *DataProcessorService {
	return &DataProcessorService{
		ac:           ac,
		logger:       logger,
		searchParams: nil,
	}
}

func (dps *DataProcessorService) ProcessData(
	ctx context.Context,
	indices []string,
) chan model.DataProcessorOutput {
	outputChannel := make(chan model.DataProcessorOutput)
	go func() {
		defer close(outputChannel)
		searchCtx, cancel := context.WithTimeout(ctx, searchAfterTimeout)
		defer cancel()
		resultChannel := dps.ac.SearchAfter(
			searchCtx,
			getAllDocumentsQuery(),
			indices,
			dps.searchParams,
			&querySize,
		)

		for result := range resultChannel {
			if result.Error != nil {
				dps.logger.Error("Error in search after", zap.Error(result.Error))
				outputChannel <- model.DataProcessorOutput{
					Error: result.Error,
				}
			} else if result.Success == nil {
				dps.logger.Error("Result of SearchAfter is nil")
				outputChannel <- model.DataProcessorOutput{
					Error: fmt.Errorf("result of SearchAfter is nil"),
				}
			} else {
				if len(result.Success.Result) == 0 {
					break
				}
				output := model.DataProcessorOutput{
					SpanOrLogData: result.Success.Result,
					Error:         nil,
				}
				outputChannel <- output
				dps.searchParams = &result.Success.ContinueParams
			}
		}
	}()

	return outputChannel
}

func GetResultsWithWorkers[
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

func DetectDataType(data map[string]interface{}) model.DataType {
	if _, ok := data["start_time"]; ok && data["end_time"] != nil {
		return model.Span
	}
	if _, ok := data["timestamp"]; ok && data["message"] != nil {
		return model.Log
	}
	return model.Unknown
}
