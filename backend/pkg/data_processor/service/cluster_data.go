package service

import (
	"context"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
)

func (dps *DataProcessorService) clusterData(
	ctx context.Context,
	clusterOrLogData []map[string]interface{},
) error {
	logsToCluster, spansToCluster := splitLogsAndSpans(clusterOrLogData)
	if len(logsToCluster) > 0 {
		err := dps.clusterLogs(ctx, logsToCluster)
		if err != nil {
			return fmt.Errorf("failed to cluster logs: %w", err)
		}
	}
	if len(spansToCluster) > 0 {
		err := dps.clusterSpans(ctx, spansToCluster)
		if err != nil {
			return fmt.Errorf("failed to cluster spans: %w", err)
		}
	}
	return nil
}

func (dps *DataProcessorService) clusterLogs(
	ctx context.Context,
	logs []logModel.LogData,
) error {
	const clusterErrorMsg = "Failed to cluster log"
	resultChannel := getResultsWithWorkers[
		logModel.LogData,
		*logModel.LogClusterResult,
	](
		ctx,
		logs,
		func(
			ctx context.Context,
			log logModel.LogData,
		) (*logModel.LogClusterResult, string, error) {
			ids, newClusterIds, err := dps.lcs.ClusterLog(ctx, log)
			if err != nil {
				return nil, clusterErrorMsg, err
			}
			return &logModel.LogClusterResult{
				Ids:                   ids,
				NewClusterIdDocuments: newClusterIds,
			}, "", nil
		},
		workerCount,
		dps.logger,
	)

	logIds, logClusterIds := unpackLogClusterResults(resultChannel, len(logs))
	if logIds == nil || len(logIds) == 0 {
		return nil
	}

	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	err := dps.ac.BulkUpdate(updateCtx, logIds, logClusterIds, bootstrapper.LogIndexName)
	if err != nil {
		return fmt.Errorf("failed to bulk update log clusters: %w", err)
	}
	return nil
}

func unpackLogClusterResults(
	resultChannel chan *logModel.LogClusterResult,
	logCount int,
) ([]string, []map[string]interface{}) {
	logIds, logClusterIds := make([]string, logCount), make([]map[string]interface{}, logCount)
	for result := range resultChannel {
		if result != nil {
			for i, id := range result.Ids {
				logIds[i] = id
			}
			for i, clusterId := range result.NewClusterIdDocuments {
				logClusterIds[i] = clusterId
			}
		}
	}
	return logIds, logClusterIds
}

func (dps *DataProcessorService) clusterSpans(
	ctx context.Context,
	spans []spanModel.SpanData,
) error {
	const clusterErrorMsg = "Failed to cluster span"
	resultChannel := getResultsWithWorkers[
		spanModel.SpanData,
		*spanModel.SpanClusterResult,
	](
		ctx,
		spans,
		func(
			ctx context.Context,
			span spanModel.SpanData,
		) (*spanModel.SpanClusterResult, string, error) {
			ids, newClusterIds, err := dps.scs.ClusterSpan(ctx, span)
			if err != nil {
				return nil, clusterErrorMsg, err
			}
			return &spanModel.SpanClusterResult{
				Ids:                   ids,
				NewClusterIdDocuments: newClusterIds,
			}, "", nil
		},
		workerCount,
		dps.logger,
	)

	spanIds, spanClusterIds := unpackSpanClusterResults(resultChannel, len(spans))
	if spanIds == nil || len(spanIds) == 0 {
		return nil
	}

	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	err := dps.ac.BulkUpdate(updateCtx, spanIds, spanClusterIds, bootstrapper.SpanIndexName)
	if err != nil {
		return fmt.Errorf("failed to bulk update span clusters: %w", err)
	}
	return nil
}

func unpackSpanClusterResults(
	resultChannel chan *spanModel.SpanClusterResult,
	spanCount int,
) ([]string, []map[string]interface{}) {
	spanIds, spanClusterIds := make([]string, spanCount), make([]map[string]interface{}, spanCount)
	for result := range resultChannel {
		if result != nil {
			for i, id := range result.Ids {
				spanIds[i] = id
			}
			for i, clusterId := range result.NewClusterIdDocuments {
				spanClusterIds[i] = clusterId
			}
		}
	}
	return spanIds, spanClusterIds
}
