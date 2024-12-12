package service

import (
	"context"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/asaskevich/EventBus"
	"go.uber.org/zap"
	"sync"
	"time"
)

const timeout = 10 * time.Second
const searchAfterTimeout = 60 * time.Second

var querySize = 10000

type DataProcessorService struct {
	ac           client.AugurClient
	bus          EventBus.Bus
	outputTopic  string
	logger       *zap.Logger
	searchParams *client.SearchAfterParams
}

func NewDataProcessorService(
	ac client.AugurClient,
	bus EventBus.Bus,
	outputTopic string,
	logger *zap.Logger,
) *DataProcessorService {
	return &DataProcessorService{
		ac:           ac,
		bus:          bus,
		outputTopic:  outputTopic,
		logger:       logger,
		searchParams: nil,
	}
}

func (dps *DataProcessorService) ProcessData(
	ctx context.Context,
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
			dps.bus.Publish(dps.outputTopic, ctx, result.Success.Result)
			dps.searchParams = &result.Success.ContinueParams
			errors = append(errors, nil)
			successes = append(successes, true)
		}
	}
	return successes, errors
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
