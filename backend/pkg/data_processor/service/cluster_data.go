package service

import (
	"context"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/Avi18971911/Augur/pkg/log/service"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	spanService "github.com/Avi18971911/Augur/pkg/trace/service"
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
	logs []map[string]interface{},
) error {
	typedLogs, err := service.ConvertToLogDocuments(logs)
	if err != nil {
		return fmt.Errorf("failed to convert logs to typed logs: %w", err)
	}
	const clusterErrorMsg = "Failed to cluster log"
	resultChannel := getResultsWithWorkers[
		logModel.LogEntry,
		*logModel.LogClusterResult,
	](
		ctx,
		typedLogs,
		func(
			ctx context.Context,
			log logModel.LogEntry,
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

	logIds, logClusterIds := unpackLogClusterResults(resultChannel)
	if logIds == nil || len(logIds) == 0 {
		return nil
	}

	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	err = dps.ac.BulkUpdate(updateCtx, logIds, logClusterIds, bootstrapper.LogIndexName)
	if err != nil {
		return fmt.Errorf("failed to bulk update log clusters: %w", err)
	}
	return nil
}

func unpackLogClusterResults(
	resultChannel chan *logModel.LogClusterResult,
) ([]string, []map[string]interface{}) {
	logIds, logClusterIds := make([]string, 0), make([]map[string]interface{}, 0)
	for result := range resultChannel {
		if result != nil {
			for _, id := range result.Ids {
				logIds = append(logIds, id)
			}
			for _, clusterId := range result.NewClusterIdDocuments {
				logClusterIds = append(logClusterIds, clusterId)
			}
		}
	}
	return logIds, logClusterIds
}

func (dps *DataProcessorService) clusterSpans(
	ctx context.Context,
	spans []map[string]interface{},
) error {
	typedSpans, err := spanService.ConvertToSpanDocuments(spans)
	if err != nil {
		return fmt.Errorf("failed to convert spans to typed spans: %w", err)
	}
	const clusterErrorMsg = "Failed to cluster span"
	resultChannel := getResultsWithWorkers[
		spanModel.Span,
		*spanModel.SpanClusterResult,
	](
		ctx,
		typedSpans,
		func(
			ctx context.Context,
			span spanModel.Span,
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

	spanIds, spanClusterIds := unpackSpanClusterResults(resultChannel)
	if spanIds == nil || len(spanIds) == 0 {
		return nil
	}

	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	err = dps.ac.BulkUpdate(updateCtx, spanIds, spanClusterIds, bootstrapper.SpanIndexName)
	if err != nil {
		return fmt.Errorf("failed to bulk update span clusters: %w", err)
	}
	return nil
}

func unpackSpanClusterResults(
	resultChannel chan *spanModel.SpanClusterResult,
) ([]string, []map[string]interface{}) {
	spanIds, spanClusterIds := make([]string, 0), make([]map[string]interface{}, 0)
	for result := range resultChannel {
		if result != nil {
			for _, id := range result.Ids {
				spanIds = append(spanIds, id)
			}
			for _, clusterId := range result.NewClusterIdDocuments {
				spanClusterIds = append(spanClusterIds, clusterId)
			}
		}
	}
	return spanIds, spanClusterIds
}
