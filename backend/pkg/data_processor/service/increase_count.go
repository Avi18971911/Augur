package service

import (
	"context"
	"fmt"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"go.uber.org/zap"
)

func (dps *DataProcessorService) increaseCountForOverlapsAndMisses(
	ctx context.Context,
	clusterOutput []model.ClusterOutput,
	buckets []countModel.Bucket,
	indices []string,
) error {
	increaseMissesInput, err := dps.processCountsForOverlaps(
		ctx,
		clusterOutput,
		buckets,
		indices,
	)
	if err != nil {
		return fmt.Errorf("failed to process clusters: %w", err)
	}

	if increaseMissesInput != nil && len(increaseMissesInput) > 0 {
		err = dps.processIncrementsForMisses(ctx, increaseMissesInput)
		if err != nil {
			return fmt.Errorf("failed to process increments for misses: %w", err)
		}
	} else {
		dps.logger.Info("No misses to increment")
	}
	return nil
}

func (dps *DataProcessorService) processCountsForOverlaps(
	ctx context.Context,
	clusterOutput []model.ClusterOutput,
	buckets []countModel.Bucket,
	indices []string,
) ([]countModel.IncreaseMissesInput, error) {
	const logErrorMsg = "Failed to process log"
	const spanErrorMsg = "Failed to process span"
	const unknownErrorMsg = "Failed to process unknown data type"

	resultChannel := getResultsWithWorkers[
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
				res, err := dps.processLog(ctx, data, buckets, indices)
				if err != nil {
					return nil, logErrorMsg, err
				} else {
					return res, "", nil
				}
			case model.SpanClusterType:
				res, err := dps.processSpan(ctx, data, buckets, indices)
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
		dps.logger,
	)

	increaseMissesList, metaMapList, documentMapList := dps.unpackCoClusterResults(resultChannel, len(clusterOutput))

	if metaMapList == nil || len(metaMapList) == 0 {
		dps.logger.Info("No co-clusters to increment")
		return nil, nil
	}
	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	index := bootstrapper.CountIndexName
	err := dps.ac.BulkIndex(updateCtx, metaMapList, documentMapList, &index)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to bulk index count increments: %w for documentMapList %s", err, documentMapList,
		)
	}
	return increaseMissesList, nil
}

func (dps *DataProcessorService) processLog(
	ctx context.Context,
	logDetails model.ClusterOutput,
	buckets []countModel.Bucket,
	indices []string,
) (*countModel.GetCountAndUpdateQueryDetails, error) {
	csCtx, csCancel := context.WithTimeout(ctx, timeout)
	defer csCancel()
	result, err := dps.cs.GetCountAndUpdateOccurrencesQueryConstituents(
		csCtx,
		logDetails.ClusterId,
		countModel.TimeInfo{
			LogInfo: &countModel.LogInfo{
				Timestamp: logDetails.LogTimeDetails.Timestamp,
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
	spanDetails model.ClusterOutput,
	buckets []countModel.Bucket,
	indices []string,
) (*countModel.GetCountAndUpdateQueryDetails, error) {
	csCtx, csCancel := context.WithTimeout(ctx, timeout)
	defer csCancel()
	result, err := dps.cs.GetCountAndUpdateOccurrencesQueryConstituents(
		csCtx,
		spanDetails.ClusterId,
		countModel.TimeInfo{
			SpanInfo: &countModel.SpanInfo{
				FromTime: spanDetails.SpanTimeDetails.StartTime,
				ToTime:   spanDetails.SpanTimeDetails.EndTime,
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
	const errorMsg = "Failed to process increments for misses"
	resultChannel := getResultsWithWorkers[
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
			res, err := dps.cs.GetIncrementMissesQueryInfo(csCtx, input)
			if err != nil {
				return nil, errorMsg, err
			} else {
				return res, "", nil
			}
		},
		workerCount,
		dps.logger,
	)

	metaMapList, documentMapList := dps.unpackMissResults(resultChannel, len(increaseMissesInput))
	if metaMapList == nil || len(metaMapList) == 0 {
		dps.logger.Info("No misses to increment after searching")
		return nil
	}
	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	index := bootstrapper.CountIndexName
	err := dps.ac.BulkIndex(updateCtx, metaMapList, documentMapList, &index)
	if err != nil {
		dps.logger.Error("Failed to bulk index when incrementing misses", zap.Error(err))
	}
	return nil
}

func (dps *DataProcessorService) unpackCoClusterResults(
	resultChannel chan *countModel.GetCountAndUpdateQueryDetails,
	startingLength int,
) ([]countModel.IncreaseMissesInput, []client.MetaMap, []client.DocumentMap) {
	increaseMissesList := make([]countModel.IncreaseMissesInput, 0, startingLength)
	metaMapList := make([]client.MetaMap, 0, startingLength)
	documentMapList := make([]client.DocumentMap, 0, startingLength)

	for result := range resultChannel {
		if result == nil {
			continue
		}
		increaseMissesList = append(increaseMissesList, result.IncreaseIncrementForMissesInput)
		if result.MetaMapList != nil && len(result.MetaMapList) > 0 {
			if result.DocumentMapList == nil || len(result.DocumentMapList) == 0 {
				dps.logger.Error(
					"DocumentMapList is nil or empty, despite MetaMapList being non-empty when co-clustering",
				)
				continue
			}
			metaMapList = append(metaMapList, result.MetaMapList...)
			documentMapList = append(documentMapList, result.DocumentMapList...)
		}
	}
	return increaseMissesList, metaMapList, documentMapList
}

func (dps *DataProcessorService) unpackMissResults(
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
				dps.logger.Error(
					"DocumentMapList is nil or empty, despite MetaMapList being non-empty " +
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
