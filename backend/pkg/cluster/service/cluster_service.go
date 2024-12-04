package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/cluster/model"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"time"
)

const csTimeOut = 10 * time.Second

type ClusterService interface {
	ClusterData(ctx context.Context, input model.ClusterInput) ([]string, []model.ClusterIdField, string, error)
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

func clusterData(clusterDetails []model.ClusterDetails) []model.ClusterDetails {
	newClusterDetails := make([]model.ClusterDetails, len(clusterDetails))
	var clusterId string
	for _, clusterDetail := range clusterDetails {
		if clusterDetail.ClusterId != "" && clusterDetail.ClusterId != "NOT_ASSIGNED" {
			clusterId = clusterDetail.ClusterId
			break
		}
	}
	if clusterId == "" {
		clusterId = uuid.NewString()
	}
	for i, clusterDetail := range clusterDetails {
		newClusterDetails[i] = model.ClusterDetails{
			Id:        clusterDetail.Id,
			ClusterId: clusterId,
		}
	}
	return newClusterDetails
}

func (cls *ClusterServiceImpl) ClusterData(
	ctx context.Context,
	input model.ClusterInput,
) ([]string, []model.ClusterIdField, string, error) {
	var queryBody, searchIndex string
	var err error
	var queryBodyBytes []byte
	if input.GetType() == model.SpanClusterInputType {
		queryBodyBytes, err = json.Marshal(equalityQueryBuilder(input.GetTextualData()))
		searchIndex = augurElasticsearch.SpanIndexName
	} else {
		queryBodyBytes, err = json.Marshal(moreLikeThisQueryBuilder(input.GetServiceName(), input.GetTextualData()))
		searchIndex = augurElasticsearch.LogIndexName
	}
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to marshal query body for elasticsearch query: %w", err)
	}
	queryBody = string(queryBodyBytes)
	var querySize = 100
	queryCtx, queryCancel := context.WithTimeout(ctx, csTimeOut)
	defer queryCancel()
	res, err := cls.ac.Search(queryCtx, queryBody, []string{searchIndex}, &querySize)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to search for similar data in Elasticsearch: %w", err)
	}
	clusterDetailsList, err := extractClusterDetails(res)
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to convert search results to cluster details: %w", err)
	}
	inputDetails := model.ClusterDetails{
		ClusterId: input.GetClusterId(),
		Id:        input.GetId(),
	}
	clusterDetailsList = append(clusterDetailsList, inputDetails)
	clusteredData := clusterData(clusterDetailsList)

	ids := make([]string, len(clusteredData))
	fieldList := make([]model.ClusterIdField, len(clusteredData))
	for idx, clusteredDatum := range clusteredData {
		ids[idx] = clusteredDatum.Id
		fieldList[idx] = map[string]interface{}{
			"cluster_id": clusteredDatum.ClusterId,
		}
	}
	return ids, fieldList, clusteredData[0].ClusterId, nil
}

func extractClusterDetails(data []map[string]interface{}) ([]model.ClusterDetails, error) {
	clusterDetailsList := make([]model.ClusterDetails, len(data))
	for i, hit := range data {
		clusterId, ok := hit["cluster_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to extract cluster_id from data")
		}
		id, ok := hit["id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to extract id from data")
		}
		clusterDetailsList[i] = model.ClusterDetails{
			ClusterId: clusterId,
			Id:        id,
		}
	}
	return clusterDetailsList, nil
}
