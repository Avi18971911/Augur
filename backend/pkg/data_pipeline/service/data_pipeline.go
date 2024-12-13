package service

import (
	"context"
	"fmt"
	clusterModel "github.com/Avi18971911/Augur/pkg/cluster/model"
	clusterService "github.com/Avi18971911/Augur/pkg/cluster/service"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
	service2 "github.com/Avi18971911/Augur/pkg/data_processor/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/event_bus"
	analyticsService "github.com/Avi18971911/Augur/pkg/inference/service"
	"github.com/asaskevich/EventBus"
	"go.uber.org/zap"
	"time"
)

const dataProcessorOutputTopic = "data_processor_output"
const clusterOutputTopic = "cluster_output"
const countOutputTopic = "count_output"

type DataPipeline struct {
	dataProcessorService    *service2.DataProcessorService
	clusterProcessorService *clusterService.ClusterDataProcessor
	countProcessorService   *countService.CountDataProcessorService
	analyticsService        *analyticsService.AnalyticsService
	logger                  *zap.Logger
}

func NewDataPipeline(
	dataProcessorService *service2.DataProcessorService,
	clusterProcessorService *clusterService.ClusterDataProcessor,
	countProcessorService *countService.CountDataProcessorService,
	analyticsService *analyticsService.AnalyticsService,
	logger *zap.Logger,
) *DataPipeline {
	return &DataPipeline{
		dataProcessorService:    dataProcessorService,
		clusterProcessorService: clusterProcessorService,
		countProcessorService:   countProcessorService,
		analyticsService:        analyticsService,
		logger:                  logger,
	}
}

func (dp *DataPipeline) Start(eventBus EventBus.Bus) (func(), error) {
	err := dp.startClusterProcessor(eventBus)
	if err != nil {
		return nil, fmt.Errorf("failed to start cluster processor: %w", err)
	}
	err = dp.startCountProcessor(eventBus)
	if err != nil {
		return nil, fmt.Errorf("failed to start count processor: %w", err)
	}
	err = dp.startAnalyticsService(eventBus)
	if err != nil {
		return nil, fmt.Errorf("failed to start analytics service: %w", err)
	}
	dpAugurBus := event_bus.NewAugurEventBus[any, model.DataProcessorOutput](eventBus, dp.logger)

	ticker := time.NewTicker(15 * time.Second)

	go func() {
		for range ticker.C {
			for dpOutput := range dp.dataProcessorService.ProcessData(
				context.Background(),
				[]string{bootstrapper.SpanIndexName, bootstrapper.LogIndexName},
			) {
				if dpOutput.Error != nil {
					dp.logger.Error("Failed to process data", zap.Error(dpOutput.Error))
				} else {
					err := dpAugurBus.Publish(dataProcessorOutputTopic, dpOutput)
					if err != nil {
						dp.logger.Error("Failed to publish data processor output", zap.Error(err))
					}
				}
			}
		}
	}()

	return ticker.Stop, nil
}

func (dp *DataPipeline) startClusterProcessor(eventBus EventBus.Bus) error {
	augurBus := event_bus.NewAugurEventBus[model.DataProcessorOutput, clusterModel.ClusterProcessorOutput](
		eventBus,
		dp.logger,
	)

	err := augurBus.Subscribe(
		dataProcessorOutputTopic,
		func(input model.DataProcessorOutput) error {
			ctx := context.Background()
			spanOrLogData := input.SpanOrLogData
			clusterDataOutput, err := dp.clusterProcessorService.ClusterData(ctx, spanOrLogData)
			if err != nil {
				return fmt.Errorf("failed to cluster data: %w", err)
			}
			clusterProcessorOutput := clusterModel.ClusterProcessorOutput{
				ClusterOutput: clusterDataOutput,
			}
			err = augurBus.Publish(clusterOutputTopic, clusterProcessorOutput)
			if err != nil {
				return fmt.Errorf("failed to publish cluster processor output: %w", err)
			}
			return nil
		},
		true,
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to input topic for ClusterDataProcessor: %w", err)
	}
	return nil
}

func (dp *DataPipeline) startCountProcessor(eventBus EventBus.Bus) error {
	augurBus := event_bus.NewAugurEventBus[clusterModel.ClusterProcessorOutput, countModel.CountProcessorOutput](
		eventBus,
		dp.logger,
	)

	err := augurBus.Subscribe(
		clusterOutputTopic,
		func(input clusterModel.ClusterProcessorOutput) error {
			ctx := context.Background()
			clusterOutput := input.ClusterOutput
			countDataOutput, err := dp.countProcessorService.IncreaseCountForOverlapsAndMisses(ctx, clusterOutput)
			if err != nil {
				return fmt.Errorf("failed to count data: %w", err)
			}
			countProcessorOutput := countModel.CountProcessorOutput{
				ModifiedClusters: countDataOutput,
			}
			err = augurBus.Publish(countOutputTopic, countProcessorOutput)
			if err != nil {
				return fmt.Errorf("failed to publish count processor output: %w", err)
			}
			return nil
		},
		true,
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to input topic for CountDataProcessor: %w", err)
	}
	return nil
}

func (dp *DataPipeline) startAnalyticsService(eventBus EventBus.Bus) error {
	augurBus := event_bus.NewAugurEventBus[countModel.CountProcessorOutput, any](
		eventBus,
		dp.logger,
	)

	err := augurBus.Subscribe(
		countOutputTopic,
		func(input countModel.CountProcessorOutput) error {
			ctx := context.Background()
			clusterIds := input.ModifiedClusters
			err := dp.analyticsService.UpdateAnalytics(ctx, clusterIds)
			if err != nil {
				return fmt.Errorf("failed to update analytics: %w", err)
			}
			return nil
		},
		true,
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to input topic for AnalyticsService: %w", err)
	}
	return nil
}
