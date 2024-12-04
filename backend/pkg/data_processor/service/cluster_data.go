package service

import (
	"context"
	"fmt"
	clusterModel "github.com/Avi18971911/Augur/pkg/cluster/model"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
)

func (dps *DataProcessorService) clusterData(
	ctx context.Context,
	clusterOrLogData []map[string]interface{},
) error {
	clusterInputList, err := getClusterInput(clusterOrLogData)
	if err != nil {
		return fmt.Errorf("failed to get cluster input: %w", err)
	}
	clusterIds
	return nil
}

func (dps *DataProcessorService) clusterDataIntoLikeIds(
	ctx context.Context,
	clusterInputList []clusterModel.ClusterInput,
) error {
	const clusterErrorMsg = "Failed to cluster log"
	resultChannel := getResultsWithWorkers[
		clusterModel.ClusterInput,
		[]string,
	](
		ctx,
		clusterInputList,
		func(
			ctx context.Context,
			input clusterModel.ClusterInput,
		) ([]string, string, error) {
			ids, err := dps.cls.ClusterData(ctx, input)
			if err != nil {
				return nil, clusterErrorMsg, err
			}
			return ids, "", nil
		},
		workerCount,
		dps.logger,
	)

	logIds, logClusterIds := unionFind(resultChannel)
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

func getClusterInput(data []map[string]interface{}) ([]clusterModel.ClusterInput, error) {
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
	resultChannel chan []string,
) map[string]string {
	unionSet := make(map[string]string)
	for result := range resultChannel {
		if result != nil {
			var foundId string
			for _, id := range result {
				if _, ok := unionSet[id]; ok {
					foundId = unionSet[id]
					break
				}
			}
			if foundId == "" {
				foundId = result[0]
				unionSet[foundId] = "-1"
			}
			for _, id := range result {
				mergeWithId(unionSet, id, foundId)
			}
		}
	}
	return unionSet
}

func mergeWithId(
	unionSet map[string]string,
	id string,
	setId string,
) string {
	nextId := unionSet[id]
	if nextId == "-1" {
		unionSet[setId] = id
		return id
	} else {
		rootId := mergeWithId(unionSet, nextId, setId)
		unionSet[id] = rootId
		return rootId
	}
}
