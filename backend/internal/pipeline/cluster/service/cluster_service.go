package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/pipeline/cluster/model"
	"go.uber.org/zap"
	"strings"
	"time"
)

const csTimeOut = 10 * time.Second
const DefaultClusterId = "NOT_ASSIGNED"

type ClusterService interface {
	GetLikeData(ctx context.Context, input model.ClusterInput) ([]model.ClusterOutput, error)
}

type ClusterServiceImpl struct {
	ac     client.AugurClient
	logger *zap.Logger
}

func NewClusterService(ac client.AugurClient, logger *zap.Logger) ClusterService {
	return &ClusterServiceImpl{
		ac:     ac,
		logger: logger,
	}
}

func (cls *ClusterServiceImpl) GetLikeData(
	ctx context.Context,
	input model.ClusterInput,
) ([]model.ClusterOutput, error) {
	var queryBody, searchIndex string
	var err error
	var queryBodyBytes []byte
	if input.DataType == model.SpanClusterInputType {
		queryBodyBytes, err = json.Marshal(equalityOfClusterEventQueryBuilder(input.Id, input.TextualData))
		searchIndex = bootstrapper.SpanIndexName
	} else {
		queryBodyBytes, err = json.Marshal(
			similarityToLogMessageQueryBuilder(
				input.Id,
				input.ServiceName,
				input.TextualData,
			),
		)
		searchIndex = bootstrapper.LogIndexName
	}
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query body for elasticsearch query: %w", err)
	}
	queryBody = string(queryBodyBytes)
	var querySize = 100
	queryCtx, queryCancel := context.WithTimeout(ctx, csTimeOut)
	defer queryCancel()
	res, err := cls.ac.Search(queryCtx, queryBody, []string{searchIndex}, &querySize)
	if err != nil {
		return nil, fmt.Errorf("failed to search for similar data in Elasticsearch: %w", err)
	}
	var filteredResults []map[string]interface{}
	if input.DataType == model.LogClusterInputType {
		filteredResults, err = eliminateLogResultsWithUnequalMessageTokenLengths(res, input)
		if err != nil {
			return nil, fmt.Errorf("failed to filter search results based on token length: %w", err)
		}
	} else {
		filteredResults = res
	}
	var output []model.ClusterOutput
	if res != nil {
		output, err = extractObjectIdAndClusterId(filteredResults, input.DataType)
		if err != nil {
			return nil, fmt.Errorf("failed to convert search results to cluster details: %w", err)
		}
	}

	outputFromInput := model.ClusterOutput{
		ObjectId:  input.Id,
		ClusterId: input.ClusterId,
		DataType:  input.DataType,
	}
	output = append(output, outputFromInput)
	return output, nil
}

func eliminateLogResultsWithUnequalMessageTokenLengths(
	resultData []map[string]interface{},
	inputData model.ClusterInput,
) ([]map[string]interface{}, error) {
	matchingMessageTokensLength := len(strings.Fields(inputData.TextualData))
	var filteredResults []map[string]interface{}
	for _, result := range resultData {
		message, ok := result["message"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to extract message from search result data")
		}
		tokenizedMessage := strings.Fields(message)
		if len(tokenizedMessage) == matchingMessageTokensLength {
			filteredResults = append(filteredResults, result)
		}
	}
	return filteredResults, nil
}

func extractObjectIdAndClusterId(
	data []map[string]interface{},
	dataType model.ClusterDataType,
) ([]model.ClusterOutput, error) {
	output := make([]model.ClusterOutput, len(data))
	for i, hit := range data {
		id, ok := hit["_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to extract id from data")
		}
		clusterId, ok := hit["cluster_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to extract cluster_id from data")
		}
		output[i] = model.ClusterOutput{
			ObjectId:  id,
			ClusterId: clusterId,
			DataType:  dataType,
		}
	}
	return output, nil
}
