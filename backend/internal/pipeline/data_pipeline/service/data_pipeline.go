package service

import (
	"context"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	analyticsService "github.com/Avi18971911/Augur/internal/pipeline/analytics/service"
	clusterModel "github.com/Avi18971911/Augur/internal/pipeline/cluster/model"
	clusterService "github.com/Avi18971911/Augur/internal/pipeline/cluster/service"
	countModel "github.com/Avi18971911/Augur/internal/pipeline/count/model"
	countService "github.com/Avi18971911/Augur/internal/pipeline/count/service"
	"github.com/Avi18971911/Augur/internal/pipeline/data_processor/model"
	dataProcessorService "github.com/Avi18971911/Augur/internal/pipeline/data_processor/service"
	"github.com/Avi18971911/Augur/internal/pipeline/event_bus"
	"github.com/asaskevich/EventBus"
	"go.uber.org/zap"
	"time"
)

const dataProcessorOutputTopic = "data_processor_output"
const clusterOutputTopic = "cluster_output"
const countOutputTopic = "count_output"

type DataPipeline struct {
	dataProcessorService    *dataProcessorService.DataProcessorService
	dataProcessorBus        event_bus.AugurEventBus[any, model.DataProcessorOutput]
	clusterProcessorService *clusterService.ClusterDataProcessor
	clusterProcessorBus     event_bus.AugurEventBus[model.DataProcessorOutput, clusterModel.ClusterProcessorOutput]
	countProcessorService   *countService.CountDataProcessorService
	countProcessorBus       event_bus.AugurEventBus[clusterModel.ClusterProcessorOutput, countModel.CountProcessorOutput]
	analyticsService        *analyticsService.AnalyticsService
	analyticsBus            event_bus.AugurEventBus[countModel.CountProcessorOutput, any]
	logger                  *zap.Logger
}

func NewDataPipeline(
	dataProcessorService *dataProcessorService.DataProcessorService,
	clusterProcessorService *clusterService.ClusterDataProcessor,
	countProcessorService *countService.CountDataProcessorService,
	analyticsService *analyticsService.AnalyticsService,
	eventBus EventBus.Bus,
	logger *zap.Logger,
) *DataPipeline {
	dpAugurBus := event_bus.NewAugurEventBus[any, model.DataProcessorOutput](
		eventBus,
		logger,
	)
	cpAugurBus := event_bus.NewAugurEventBus[model.DataProcessorOutput, clusterModel.ClusterProcessorOutput](
		eventBus,
		logger,
	)
	countAugurBus := event_bus.NewAugurEventBus[clusterModel.ClusterProcessorOutput, countModel.CountProcessorOutput](
		eventBus,
		logger,
	)
	analyticsAugurBus := event_bus.NewAugurEventBus[countModel.CountProcessorOutput, any](
		eventBus,
		logger,
	)

	return &DataPipeline{
		dataProcessorService:    dataProcessorService,
		dataProcessorBus:        dpAugurBus,
		clusterProcessorService: clusterProcessorService,
		clusterProcessorBus:     cpAugurBus,
		countProcessorService:   countProcessorService,
		countProcessorBus:       countAugurBus,
		analyticsService:        analyticsService,
		analyticsBus:            analyticsAugurBus,
		logger:                  logger,
	}
}

func (dp *DataPipeline) Start(ticker *time.Ticker) error {
	err := dp.startClusterProcessor()
	if err != nil {
		return fmt.Errorf("failed to start cluster processor: %w", err)
	}
	err = dp.startCountProcessor()
	if err != nil {
		return fmt.Errorf("failed to start count processor: %w", err)
	}
	err = dp.startAnalyticsService()
	if err != nil {
		return fmt.Errorf("failed to start analytics service: %w", err)
	}

	go func() {
		for range ticker.C {
			for dpOutput := range dp.dataProcessorService.ProcessData(
				context.Background(),
				[]string{bootstrapper.SpanIndexName, bootstrapper.LogIndexName},
			) {
				if dpOutput.Error != nil {
					dp.logger.Error("Failed to process data", zap.Error(dpOutput.Error))
				} else {
					err := dp.dataProcessorBus.Publish(dataProcessorOutputTopic, dpOutput)
					if err != nil {
						dp.logger.Error("Failed to publish data processor output", zap.Error(err))
					}
				}
			}
		}
	}()
	return nil
}

func (dp *DataPipeline) startClusterProcessor() error {
	err := dp.clusterProcessorBus.Subscribe(
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
			err = dp.clusterProcessorBus.Publish(clusterOutputTopic, clusterProcessorOutput)
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

func (dp *DataPipeline) startCountProcessor() error {
	err := dp.countProcessorBus.Subscribe(
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
			err = dp.countProcessorBus.Publish(countOutputTopic, countProcessorOutput)
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

func (dp *DataPipeline) startAnalyticsService() error {
	err := dp.analyticsBus.Subscribe(
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
