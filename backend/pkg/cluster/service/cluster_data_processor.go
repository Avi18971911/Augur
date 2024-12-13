package service

import (
	"context"
	"fmt"
	clusterModel "github.com/Avi18971911/Augur/pkg/cluster/model"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
	"github.com/Avi18971911/Augur/pkg/data_processor/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/event_bus"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"time"
)

const workerCount = 50
const timeout = 10 * time.Second

type ClusterDataProcessor struct {
	ac          client.AugurClient
	cls         ClusterService
	bus         event_bus.AugurEventBus[model.DataProcessorOutput, clusterModel.ClusterProcessorOutput]
	inputTopic  string
	outputTopic string
	logger      *zap.Logger
}

func NewClusterDataProcessor(
	ac client.AugurClient,
	cls ClusterService,
	bus event_bus.AugurEventBus[model.DataProcessorOutput, clusterModel.ClusterProcessorOutput],
	inputTopic string,
	outputTopic string,
	logger *zap.Logger,
) *ClusterDataProcessor {
	return &ClusterDataProcessor{
		ac:          ac,
		cls:         cls,
		bus:         bus,
		inputTopic:  inputTopic,
		outputTopic: outputTopic,
		logger:      logger,
	}
}

func (cdp *ClusterDataProcessor) Start() error {
	err := cdp.bus.Subscribe(
		cdp.inputTopic,
		func(input model.DataProcessorOutput) error {
			ctx := context.Background()
			spanOrLogData := input.SpanOrLogData
			clusterDataOutput, err := cdp.clusterData(ctx, spanOrLogData)
			if err != nil {
				return fmt.Errorf("failed to cluster data: %w", err)
			}
			clusterProcessorOutput := clusterModel.ClusterProcessorOutput{
				ClusterOutput: clusterDataOutput,
			}
			err = cdp.bus.Publish(cdp.outputTopic, clusterProcessorOutput)
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

func (cdp *ClusterDataProcessor) clusterData(
	ctx context.Context,
	spanOrLogData []map[string]interface{},
) ([]model.ClusterOutput, error) {
	clusterInputList, err := getClusterInput(spanOrLogData)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster input: %w", err)
	}
	ids, clusterIds, err := cdp.clusterDataIntoLikeIds(ctx, clusterInputList)
	if err != nil {
		return nil, fmt.Errorf("failed to cluster data into like ids: %w", err)
	}
	dataProcessorClusterOutput := cdp.finalizeOutput(ids, clusterIds, spanOrLogData)
	return dataProcessorClusterOutput, nil
}

func (cdp *ClusterDataProcessor) clusterDataIntoLikeIds(
	ctx context.Context,
	clusterInputList []clusterModel.ClusterInput,
) ([]string, []string, error) {
	const clusterErrorMsg = "Failed to cluster log"
	resultChannel := service.GetResultsWithWorkers[
		clusterModel.ClusterInput,
		[]clusterModel.ClusterOutput,
	](
		ctx,
		clusterInputList,
		func(
			ctx context.Context,
			input clusterModel.ClusterInput,
		) ([]clusterModel.ClusterOutput, string, error) {
			outputList, err := cdp.cls.ClusterData(ctx, input)
			if err != nil {
				return nil, clusterErrorMsg, err
			}
			return outputList, "", nil
		},
		workerCount,
		cdp.logger,
	)

	ids, clusterIds, dataTypes := unionFind(resultChannel)
	if ids == nil || len(ids) == 0 {
		return nil, nil, nil
	}

	// TODO: Segregate by index to speed up updates
	updateStatements := getUpdateStatements(clusterIds)
	metaStatements := getMetaStatements(ids, dataTypes)
	updateCtx, updateCancel := context.WithTimeout(ctx, timeout)
	defer updateCancel()
	err := cdp.ac.BulkUpdate(updateCtx, metaStatements, updateStatements, bootstrapper.LogIndexName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to bulk update clusters: %w", err)
	}
	return ids, clusterIds, nil
}

func (cdp *ClusterDataProcessor) finalizeOutput(
	ids []string,
	clusterIds []string,
	data []map[string]interface{},
) []model.ClusterOutput {
	dataMap := getDataMap(data)
	output := make([]model.ClusterOutput, len(dataMap))
	i := 0
	for _, id := range ids {
		// the ids and clusterIds are from the clustering algorithm, whereas we only need to pass on new entries
		if _, ok := dataMap[id]; !ok {
			continue
		}
		clusterId := clusterIds[i]
		dataType, spanTimeDetails, logTimeDetails, err := getDetails(dataMap[id])
		if err != nil {
			cdp.logger.Error("failed to get details from the data", zap.Error(err))
			continue
		}
		output[i] = model.ClusterOutput{
			Id:              id,
			ClusterId:       clusterId,
			ClusterDataType: dataType,
			SpanTimeDetails: spanTimeDetails,
			LogTimeDetails:  logTimeDetails,
		}
		i++
	}
	return output
}

func getClusterInput(
	data []map[string]interface{},
) ([]clusterModel.ClusterInput, error) {
	clusterInputList := make([]clusterModel.ClusterInput, len(data))
	for i, item := range data {
		dataType := service.DetectDataType(item)
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
	id, ok := log["_id"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract id from log")
	}
	serviceName, ok := log["service"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract service name from log")
	}
	clusterId, ok := log["cluster_id"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract cluster id from log")
	}

	logClusterInput := clusterModel.ClusterInput{
		DataType:    clusterModel.LogClusterInputType,
		TextualData: message,
		ServiceName: serviceName,
		Id:          id,
		ClusterId:   clusterId,
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
	id, ok := span["_id"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract id from span")
	}
	serviceName, ok := span["service_name"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract service name from span")
	}
	clusterId, ok := span["cluster_id"].(string)
	if !ok {
		return clusterModel.ClusterInput{}, fmt.Errorf("failed to extract cluster id from span")
	}

	spanClusterInput := clusterModel.ClusterInput{
		DataType:    clusterModel.SpanClusterInputType,
		TextualData: clusterEvent,
		ServiceName: serviceName,
		Id:          id,
		ClusterId:   clusterId,
	}
	return spanClusterInput, nil
}

func unionFind(
	resultChannel chan []clusterModel.ClusterOutput,
) ([]string, []string, []model.ClusterDataType) {
	unionSet := make(map[string]string)
	idToClusterIdMap := make(map[string]string)
	idToDataTypeMap := make(map[string]model.ClusterDataType)
	const defaultId = "notFound"
	for result := range resultChannel {
		if result != nil && len(result) > 0 {
			var idToClusterOn = defaultId
			for _, clusterOutput := range result {
				if clusterOutput.ClusterId == DefaultClusterId {
					clusterOutput.ClusterId = uuid.NewString()
				}
				clusterId := clusterOutput.ClusterId
				id := clusterOutput.ObjectId

				idToClusterIdMap[id] = clusterId
				idToDataTypeMap[id] = mapClusterOutputToDataType(clusterOutput.DataType)
				if _, ok := unionSet[id]; ok {
					idToClusterOn = getRootId(unionSet, id)
				} else {
					unionSet[id] = id
				}
			}
			if idToClusterOn == defaultId {
				idToClusterOn = result[0].ObjectId
				unionSet[idToClusterOn] = idToClusterOn
			}
			for _, clusterOutput := range result {
				mergeWithIdToClusterOn(unionSet, clusterOutput.ObjectId, idToClusterOn)
			}
		}
	}
	ids, clusterIds, dataTypes := make([]string, 0), make([]string, 0), make([]model.ClusterDataType, 0)
	for id, _ := range unionSet {
		respectiveClusterId := idToClusterIdMap[getRootId(unionSet, id)]
		ids = append(ids, id)
		clusterIds = append(clusterIds, respectiveClusterId)
		dataTypes = append(dataTypes, idToDataTypeMap[id])
	}
	return ids, clusterIds, dataTypes
}

func mapClusterOutputToDataType(
	dataType clusterModel.ClusterDataType,
) model.ClusterDataType {
	if dataType == clusterModel.SpanClusterInputType {
		return model.SpanClusterType
	}
	return model.LogClusterType
}

func getRootId(
	unionSet map[string]string,
	id string,
) string {
	nextId := unionSet[id]
	if nextId == id {
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

func getUpdateStatements(
	clusterIds []string,
) []map[string]interface{} {
	updateStatements := make([]map[string]interface{}, 0, len(clusterIds))
	for _, clusterId := range clusterIds {
		updateStatement := map[string]interface{}{
			"cluster_id": clusterId,
		}
		updateStatements = append(updateStatements, updateStatement)
	}
	return updateStatements
}

func getMetaStatements(
	ids []string,
	dataTypes []model.ClusterDataType,
) []map[string]interface{} {
	metaStatements := make([]map[string]interface{}, 0, len(ids))
	for i, id := range ids {
		var index string
		if dataTypes[i] == model.SpanClusterType {
			index = bootstrapper.SpanIndexName
		} else {
			index = bootstrapper.LogIndexName
		}
		metaStatement := map[string]interface{}{
			"_id":    id,
			"_index": index,
		}
		metaStatements = append(metaStatements, metaStatement)
	}
	return metaStatements
}

func getDetails(
	item map[string]interface{},
) (
	model.ClusterDataType,
	model.SpanDetails,
	model.LogDetails,
	error,
) {
	dataType := service.DetectDataType(item)
	if dataType == model.Log {
		logDetails, err := getLogOutput(item)
		if err != nil {
			return model.LogClusterType,
				model.SpanDetails{},
				model.LogDetails{},
				fmt.Errorf("failed to get log output: %w", err)
		}
		return model.LogClusterType, model.SpanDetails{}, logDetails, nil
	} else {
		spanDetails, err := getSpanOutput(item)
		if err != nil {
			return model.SpanClusterType,
				model.SpanDetails{},
				model.LogDetails{},
				fmt.Errorf("failed to get span output: %w", err)
		}
		return model.SpanClusterType, spanDetails, model.LogDetails{}, nil
	}
}

func getLogOutput(item map[string]interface{}) (
	model.LogDetails,
	error,
) {
	timeStampString, ok := item["timestamp"].(string)
	if !ok {
		return model.LogDetails{}, fmt.Errorf("failed to extract timestamp from log")
	}
	timeStamp, err := client.NormalizeTimestampToNanoseconds(timeStampString)
	if err != nil {
		return model.LogDetails{}, fmt.Errorf("failed to normalize timestamp: %w", err)
	}
	logDetails := model.LogDetails{
		Timestamp: timeStamp,
	}
	return logDetails, nil
}

func getSpanOutput(item map[string]interface{}) (
	model.SpanDetails,
	error,
) {
	startTimeString, ok := item["start_time"].(string)
	if !ok {
		return model.SpanDetails{}, fmt.Errorf("failed to extract start time from span")
	}
	startTime, err := client.NormalizeTimestampToNanoseconds(startTimeString)
	if err != nil {
		return model.SpanDetails{}, fmt.Errorf("failed to normalize start time: %w", err)
	}
	endTimeString, ok := item["end_time"].(string)
	if !ok {
		return model.SpanDetails{}, fmt.Errorf("failed to extract end time from span")
	}
	endTime, err := client.NormalizeTimestampToNanoseconds(endTimeString)
	if err != nil {
		return model.SpanDetails{}, fmt.Errorf("failed to normalize end time: %w", err)
	}
	spanDetails := model.SpanDetails{
		StartTime: startTime,
		EndTime:   endTime,
	}
	return spanDetails, nil
}

func getDataMap(
	data []map[string]interface{},
) map[string]map[string]interface{} {
	dataMap := make(map[string]map[string]interface{}, len(data))
	for _, item := range data {
		id, ok := item["_id"].(string)
		if !ok {
			continue
		}
		dataMap[id] = item
	}
	return dataMap
}
