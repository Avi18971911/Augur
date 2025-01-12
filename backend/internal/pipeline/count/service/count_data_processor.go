package service

import (
	"context"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	countModel "github.com/Avi18971911/Augur/internal/pipeline/count/model"
	"github.com/Avi18971911/Augur/internal/pipeline/data_processor/model"
	"github.com/Avi18971911/Augur/internal/pipeline/data_processor/service"
	"go.uber.org/zap"
	"time"
)

const timeout = 10 * time.Second
const workerCount = 50

type CountDataProcessorService struct {
	ac      client.AugurClient
	cs      *CountService
	buckets []countModel.Bucket
	indices []string
	logger  *zap.Logger
}

func NewCountDataProcessorService(
	ac client.AugurClient,
	cs *CountService,
	buckets []countModel.Bucket,
	indices []string,
	logger *zap.Logger,
) *CountDataProcessorService {
	return &CountDataProcessorService{
		ac:      ac,
		cs:      cs,
		buckets: buckets,
		indices: indices,
		logger:  logger,
	}
}

func (cdp *CountDataProcessorService) IncreaseCountForOverlapsAndMisses(
	ctx context.Context,
	clusterOutput []model.ClusterOutput,
) ([]string, error) {
	increaseMissesInput, err := cdp.processCountsForOverlaps(
		ctx,
		clusterOutput,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to process clusters: %w", err)
	}

	if increaseMissesInput != nil && len(increaseMissesInput) > 0 {
		err = cdp.processIncrementsForMisses(ctx, increaseMissesInput)
		if err != nil {
			return nil, fmt.Errorf("failed to process increments for misses: %w", err)
		}
	} else {
		cdp.logger.Info("No misses to increment")
	}
	alteredClusterIds := getAlteredClusterIdsFromIncreaseMissesInput(increaseMissesInput)
	return alteredClusterIds, nil
}

func (cdp *CountDataProcessorService) processCountsForOverlaps(
	ctx context.Context,
	clusterOutput []model.ClusterOutput,
) ([]countModel.IncreaseMissesInput, error) {
	const logErrorMsg = "Failed to process log"
	const spanErrorMsg = "Failed to process span"
	const unknownErrorMsg = "Failed to process unknown data type"

	resultChannel := service.GetResultsWithWorkers[
		model.ClusterOutput,
		*countModel.GetCountAndUpdateQueryDetails,
	](
		ctx,
		clusterOutput,
		func(
			ctx context.Context,
			data model.ClusterOutput,
		) (*countModel.GetCountAndUpdateQueryDetails, string, error) {
			dataType := data.ClusterDataType
			switch dataType {
			case model.LogClusterType:
				res, err := cdp.processLog(ctx, data)
				if err != nil {
					return nil, logErrorMsg, err
				} else {
					return res, "", nil
				}
			case model.SpanClusterType:
				res, err := cdp.processSpan(ctx, data)
				if err != nil {
					return nil, spanErrorMsg, err
				} else {
					return res, "", nil
				}
			default:
				return nil, unknownErrorMsg, nil
			}
		},
		workerCount,
		cdp.logger,
	)

	bulkResult := cdp.unpackCoClusterResults(resultChannel, len(clusterOutput))

	if bulkResult.TotalCountMetaMap == nil || len(bulkResult.TotalCountMetaMap) == 0 {
		cdp.logger.Info("No co-clusters to increment")
		return bulkResult.MissesInput, nil
	}
	totalCountUpdateCtx, totalCountUpdateCancel := context.WithTimeout(ctx, timeout)
	defer totalCountUpdateCancel()
	totalCountIndex := bootstrapper.ClusterTotalCountIndexName
	err := cdp.ac.BulkIndex(
		totalCountUpdateCtx,
		bulkResult.TotalCountMetaMap,
		bulkResult.TotalCountDocumentMap,
		&totalCountIndex,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to bulk index count increments: %w for documentMapList %s",
			err,
			bulkResult.TotalCountDocumentMap,
		)
	}

	if bulkResult.WindowCountMetaMap == nil || len(bulkResult.WindowCountMetaMap) == 0 {
		cdp.logger.Info("No window co-clusters to increment")
		return bulkResult.MissesInput, nil
	}
	windowCountUpdateCtx, windowCountUpdateCancel := context.WithTimeout(ctx, timeout)
	defer windowCountUpdateCancel()
	windowCountIndex := bootstrapper.ClusterWindowCountIndexName
	err = cdp.ac.BulkIndex(
		windowCountUpdateCtx,
		bulkResult.WindowCountMetaMap,
		bulkResult.WindowCountDocumentMap,
		&windowCountIndex,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to bulk index window count increments: %w for documentMapList %s",
			err,
			bulkResult.WindowCountDocumentMap,
		)
	}

	return bulkResult.MissesInput, nil
}

func (cdp *CountDataProcessorService) processLog(
	ctx context.Context,
	logDetails model.ClusterOutput,
) (*countModel.GetCountAndUpdateQueryDetails, error) {
	csCtx, csCancel := context.WithTimeout(ctx, timeout)
	defer csCancel()
	result, err := cdp.cs.GetCountAndUpdateOccurrencesQueryConstituents(
		csCtx,
		logDetails.ClusterId,
		countModel.TimeInfo{
			LogInfo: &countModel.LogInfo{
				Timestamp: logDetails.LogTimeDetails.Timestamp,
			},
		},
		cdp.indices,
		cdp.buckets,
	)
	if err != nil {
		cdp.logger.Error("Failed to count and update occurrences for logs", zap.Error(err))
		return nil, err
	}
	return result, nil
}

func (cdp *CountDataProcessorService) processSpan(
	ctx context.Context,
	spanDetails model.ClusterOutput,
) (*countModel.GetCountAndUpdateQueryDetails, error) {
	csCtx, csCancel := context.WithTimeout(ctx, timeout)
	defer csCancel()
	result, err := cdp.cs.GetCountAndUpdateOccurrencesQueryConstituents(
		csCtx,
		spanDetails.ClusterId,
		countModel.TimeInfo{
			SpanInfo: &countModel.SpanInfo{
				FromTime: spanDetails.SpanTimeDetails.StartTime,
				ToTime:   spanDetails.SpanTimeDetails.EndTime,
			},
		},
		cdp.indices,
		cdp.buckets,
	)
	if err != nil {
		cdp.logger.Error("Failed to count and update occurrences for spans", zap.Error(err))
		return nil, err
	}
	return result, nil
}

func (cdp *CountDataProcessorService) processIncrementsForMisses(
	ctx context.Context,
	increaseMissesInput []countModel.IncreaseMissesInput,
) error {
	const errorMsg = "Failed to process increments for misses"
	resultChannel := service.GetResultsWithWorkers[
		countModel.IncreaseMissesInput,
		*countModel.GetIncrementMissesQueryDetails,
	](
		ctx,
		increaseMissesInput,
		func(
			ctx context.Context,
			input countModel.IncreaseMissesInput,
		) (*countModel.GetIncrementMissesQueryDetails, string, error) {
			csCtx, csCancel := context.WithTimeout(ctx, timeout)
			defer csCancel()
			res, err := cdp.cs.GetIncrementMissesQueryInfo(csCtx, input)
			if err != nil {
				return nil, errorMsg, err
			} else {
				return res, "", nil
			}
		},
		workerCount,
		cdp.logger,
	)

	metaMapList, documentMapList := cdp.unpackMissResults(resultChannel, len(increaseMissesInput))
	if metaMapList == nil || len(metaMapList) == 0 {
		cdp.logger.Info("No misses to increment after searching")
		return nil
	}
	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	index := bootstrapper.ClusterTotalCountIndexName
	err := cdp.ac.BulkIndex(updateCtx, metaMapList, documentMapList, &index)
	if err != nil {
		cdp.logger.Error("Failed to bulk index when incrementing misses", zap.Error(err))
	}
	return nil
}

func (cdp *CountDataProcessorService) unpackCoClusterResults(
	resultChannel chan *countModel.GetCountAndUpdateQueryDetails,
	startingLength int,
) countModel.BulkClusterQueryResult {
	increaseMissesList := make([]countModel.IncreaseMissesInput, 0, startingLength)
	totalCountMetaMapList := make([]client.MetaMap, 0, startingLength)
	totalCountDocumentMapList := make([]client.DocumentMap, 0, startingLength)
	windowCountMetaMapList := make([]client.MetaMap, 0, startingLength)
	windowCountDocumentMapList := make([]client.DocumentMap, 0, startingLength)

	for result := range resultChannel {
		if result == nil {
			continue
		}
		increaseMissesList = append(increaseMissesList, result.IncreaseIncrementForMissesInput)
		if result.TotalCountMetaMapList != nil && len(result.TotalCountMetaMapList) > 0 {
			if result.TotalCountDocumentMapList == nil || len(result.TotalCountDocumentMapList) == 0 {
				cdp.logger.Error(
					"TotalCountDocumentMapList is nil or empty, despite TotalCountMetaMapList being non-empty when co-clustering",
				)
				continue
			}
			totalCountMetaMapList = append(totalCountMetaMapList, result.TotalCountMetaMapList...)
			totalCountDocumentMapList = append(totalCountDocumentMapList, result.TotalCountDocumentMapList...)
			windowCountDocumentMapList = append(windowCountDocumentMapList, result.WindowCountDocumentMapList...)
			windowCountMetaMapList = append(windowCountMetaMapList, result.WindowCountMetaMapList...)
		}
	}
	return countModel.BulkClusterQueryResult{
		MissesInput:            increaseMissesList,
		TotalCountMetaMap:      totalCountMetaMapList,
		TotalCountDocumentMap:  totalCountDocumentMapList,
		WindowCountMetaMap:     windowCountMetaMapList,
		WindowCountDocumentMap: windowCountDocumentMapList,
	}
}

func (cdp *CountDataProcessorService) unpackMissResults(
	resultChannel chan *countModel.GetIncrementMissesQueryDetails,
	startingLength int,
) ([]client.MetaMap, []client.DocumentMap) {
	metaMapList := make([]client.MetaMap, 0, startingLength)
	documentMapList := make([]client.DocumentMap, 0, startingLength)
	for result := range resultChannel {
		if result == nil {
			continue
		}
		if result.MetaMapList != nil && len(result.MetaMapList) > 0 {
			if result.DocumentMapList == nil || len(result.DocumentMapList) == 0 {
				cdp.logger.Error(
					"TotalCountDocumentMapList is nil or empty, despite TotalCountMetaMapList being non-empty " +
						"when incrementing misses",
				)
				continue
			}
			metaMapList = append(metaMapList, result.MetaMapList...)
			documentMapList = append(documentMapList, result.DocumentMapList...)
		}
	}
	return metaMapList, documentMapList
}

func getAlteredClusterIdsFromIncreaseMissesInput(increaseMissesInput []countModel.IncreaseMissesInput) []string {
	alteredClusterIds := make([]string, 0, len(increaseMissesInput))
	for _, input := range increaseMissesInput {
		alteredClusterIds = append(alteredClusterIds, input.ClusterId)
	}
	return alteredClusterIds
}
