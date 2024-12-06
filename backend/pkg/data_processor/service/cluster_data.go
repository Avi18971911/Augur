package service

import (
	"context"
	"fmt"
	clusterModel "github.com/Avi18971911/Augur/pkg/cluster/model"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"go.uber.org/zap"
)

func (dps *DataProcessorService) clusterData(
	ctx context.Context,
	clusterOrLogData []map[string]interface{},
) ([]model.ClusterOutput, error) {
	clusterInputList, err := getClusterInput(clusterOrLogData)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster input: %w", err)
	}
	ids, clusterIds, err := dps.clusterDataIntoLikeIds(ctx, clusterInputList)
	if err != nil {
		return nil, fmt.Errorf("failed to cluster data into like ids: %w", err)
	}
	dataProcessorClusterOutput := dps.finalizeOutput(ids, clusterIds, clusterOrLogData)
	return dataProcessorClusterOutput, nil
}

func (dps *DataProcessorService) clusterDataIntoLikeIds(
	ctx context.Context,
	clusterInputList []clusterModel.ClusterInput,
) ([]string, []string, error) {
	const clusterErrorMsg = "Failed to cluster log"
	resultChannel := getResultsWithWorkers[
		clusterModel.ClusterInput,
		[]clusterModel.ClusterOutput,
	](
		ctx,
		clusterInputList,
		func(
			ctx context.Context,
			input clusterModel.ClusterInput,
		) ([]clusterModel.ClusterOutput, string, error) {
			ids, err := dps.cls.ClusterData(ctx, input)
			if err != nil {
				return nil, clusterErrorMsg, err
			}
			return ids, "", nil
		},
		workerCount,
		dps.logger,
	)

	ids, clusterIds := unionFind(resultChannel)
	if ids == nil || len(ids) == 0 {
		return nil, nil, nil
	}

	updateStatements := getUpdateStatementsFromClusterIds(clusterIds)
	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	err := dps.ac.BulkUpdate(updateCtx, ids, updateStatements, bootstrapper.LogIndexName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to bulk update clusters: %w", err)
	}
	return ids, clusterIds, nil
}

func getClusterInput(
	data []map[string]interface{},
) ([]clusterModel.ClusterInput, error) {
	clusterInputList := make([]clusterModel.ClusterInput, len(data))
	for i, item := range data {
		dataType := detectDataType(item)
		if dataType == model.Log {
			clusterInput, err := getLogClusterDetails(item)
			if err != nil {
				return nil, fmt.Errorf("failed to get log cluster details: %w", err)
			}
			clusterInputList[i] = clusterInput
		} else if dataType == model.Span {
			clusterInput, err := getSpanClusterDetails(item)
			if err != nil {
				return nil, fmt.Errorf("failed to get span cluster details: %w", err)
			}
			clusterInputList[i] = clusterInput
		}
	}
	return clusterInputList, nil
}

func getLogClusterDetails(
	log map[string]interface{},
) (clusterModel.ClusterInput, error) {
	message, ok := log["message"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract message from log")
	}
	id, ok := log["id"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract id from log")
	}
	serviceName, ok := log["service"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract service name from log")
	}

	logClusterInput := clusterModel.ClusterInput{
		DataType:    clusterModel.LogClusterInputType,
		TextualData: message,
		ServiceName: serviceName,
		Id:          id,
	}
	return logClusterInput, nil
}

func getSpanClusterDetails(
	span map[string]interface{},
) (clusterModel.ClusterInput, error) {
	clusterEvent, ok := span["cluster_event"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract cluster event from span")
	}
	id, ok := span["id"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract id from span")
	}
	serviceName, ok := span["service_name"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract service name from span")
	}
	spanClusterInput := clusterModel.ClusterInput{
		DataType:    clusterModel.SpanClusterInputType,
		TextualData: clusterEvent,
		ServiceName: serviceName,
		Id:          id,
	}
	return spanClusterInput, nil
}

func unionFind(
	resultChannel chan []clusterModel.ClusterOutput,
) ([]string, []string) {
	unionSet := make(map[string]string)
	idToClusterIdMap := make(map[string]string)
	const defaultId = "notFound"
	for result := range resultChannel {
		if result != nil && len(result) > 0 {
			var idToClusterOn = defaultId
			for _, clusterOutput := range result {
				clusterId := clusterOutput.ClusterId
				id := clusterOutput.ObjectId

				idToClusterIdMap[id] = clusterId
				if _, ok := unionSet[id]; ok && idToClusterOn == defaultId {
					idToClusterOn = unionSet[id]
				}
			}
			if idToClusterOn == defaultId {
				idToClusterOn = result[0].ObjectId
				unionSet[idToClusterOn] = "-1"
			}
			for _, clusterOutput := range result {
				mergeWithIdToClusterOn(unionSet, clusterOutput.ObjectId, idToClusterOn)
			}
		}
	}
	ids, clusterIds := make([]string, 0), make([]string, 0)
	for _, id := range unionSet {
		respectiveClusterId := idToClusterIdMap[getRootId(unionSet, id)]
		ids = append(ids, id)
		clusterIds = append(clusterIds, respectiveClusterId)
	}
	return ids, clusterIds
}

func getRootId(
	unionSet map[string]string,
	id string,
) string {
	nextId := unionSet[id]
	if nextId == "-1" {
		return id
	}
	rootId := getRootId(unionSet, nextId)
	unionSet[id] = rootId
	return rootId
}

func mergeWithIdToClusterOn(
	unionSet map[string]string,
	idToCluster string,
	idToClusterOn string,
) string {
	rootId := getRootId(unionSet, idToCluster)
	unionSet[rootId] = getRootId(unionSet, idToClusterOn)
	getRootId(unionSet, idToCluster)
	return rootId
}

func getUpdateStatementsFromClusterIds(
	clusterIds []string,
) []map[string]interface{} {
	updateStatements := make([]map[string]interface{}, 0, len(clusterIds))
	for _, clusterId := range clusterIds {
		updateStatement := map[string]interface{}{
			"update": map[string]interface{}{
				"cluster_id": clusterId,
			},
		}
		updateStatements = append(updateStatements, updateStatement)
	}
	return updateStatements
}

func (dps *DataProcessorService) finalizeOutput(
	ids []string,
	clusterIds []string,
	data []map[string]interface{},
) []model.ClusterOutput {
	output := make([]model.ClusterOutput, len(ids))
	for i, id := range ids {
		clusterId := clusterIds[i]
		dataType, spanTimeDetails, logTimeDetails, err := getDetails(data[i])
		if err != nil {
			dps.logger.Error("failed to get details from the data", zap.Error(err))
			continue
		}
		output[i] = model.ClusterOutput{
			Id:              id,
			ClusterId:       clusterId,
			ClusterDataType: dataType,
			SpanTimeDetails: spanTimeDetails,
			LogTimeDetails:  logTimeDetails,
		}
	}
	return output
}

func getDetails(
	item map[string]interface{},
) (
	model.ClusterDataType,
	*model.SpanDetails,
	*model.LogDetails,
	error,
) {
	dataType := detectDataType(item)
	if dataType == model.Log {
		logDetails, err := getLogOutput(item)
		if err != nil {
			return model.LogClusterType, nil, nil, fmt.Errorf("failed to get log output: %w", err)
		}
		return model.LogClusterType, nil, logDetails, nil
	} else {
		spanDetails, err := getSpanOutput(item)
		if err != nil {
			return model.SpanClusterType, nil, nil, fmt.Errorf("failed to get span output: %w", err)
		}
		return model.SpanClusterType, spanDetails, nil, nil
	}
}

func getLogOutput(item map[string]interface{}) (
	*model.LogDetails,
	error,
) {
	timeStampString, ok := item["timestamp"].(string)
	if !ok {
		return nil, fmt.Errorf("failed to extract timestamp from log")
	}
	timeStamp, err := client.NormalizeTimestampToNanoseconds(timeStampString)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize timestamp: %w", err)
	}
	logDetails := &model.LogDetails{
		Timestamp: timeStamp,
	}
	return logDetails, nil
}

func getSpanOutput(item map[string]interface{}) (
	*model.SpanDetails,
	error,
) {
	startTimeString, ok := item["start_time"].(string)
	if !ok {
		return nil, fmt.Errorf("failed to extract start time from span")
	}
	startTime, err := client.NormalizeTimestampToNanoseconds(startTimeString)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize start time: %w", err)
	}
	endTimeString, ok := item["end_time"].(string)
	if !ok {
		return nil, fmt.Errorf("failed to extract end time from span")
	}
	endTime, err := client.NormalizeTimestampToNanoseconds(endTimeString)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize end time: %w", err)
	}
	spanDetails := &model.SpanDetails{
		StartTime: startTime,
		EndTime:   endTime,
	}
	return spanDetails, nil
}
