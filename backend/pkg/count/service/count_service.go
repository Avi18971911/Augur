package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/count/model"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"go.uber.org/zap"
	"time"
)

const csTimeOut = 2 * time.Second

var indices = []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName}

type CountService struct {
	ac     client.AugurClient
	logger *zap.Logger
}

func NewCountService(ac client.AugurClient, logger *zap.Logger) *CountService {
	return &CountService{
		ac:     ac,
		logger: logger,
	}
}

func getTimeRange(timestamp time.Time, bucket Bucket) (time.Time, time.Time) {
	fromTime := timestamp.Add(-time.Millisecond * time.Duration(bucket/2))
	toTime := timestamp.Add(time.Millisecond * time.Duration(bucket/2))
	return fromTime, toTime
}

func (cs *CountService) GetCountAndUpdateOccurrencesQueryConstituents(
	ctx context.Context,
	clusterId string,
	timeInfo model.TimeInfo,
	buckets []Bucket,
) (*model.GetCountAndUpdateOccurrencesQueryConstituentsResult, error) {
	countMap, err := cs.countOccurrencesAndCoOccurrencesByCoClusterId(ctx, clusterId, timeInfo, buckets)
	if err != nil {
		return nil, err
	}
	metaMapList, documentMapList := cs.getUpdateCoOccurrencesQueryConstituents(clusterId, countMap)
	increaseForMissesInput := getIncrementOccurrencesForMissesInput(clusterId, countMap)
	result := &model.GetCountAndUpdateOccurrencesQueryConstituentsResult{
		IncreaseIncrementForMissesInput: increaseForMissesInput,
		MetaMapList:                     metaMapList,
		DocumentMapList:                 documentMapList,
	}
	return result, nil
}

func getIncrementOccurrencesForMissesInput(
	clusterId string,
	countMap map[string]struct{},
) model.IncreaseMissesInput {
	clusterIds := make([]string, len(countMap))
	i := 0
	for key, _ := range countMap {
		clusterIds[i] = key
		i++
	}
	return model.IncreaseMissesInput{
		ClusterId:             clusterId,
		CoClusterIdsToExclude: clusterIds,
	}
}

func (cs *CountService) getUpdateCoOccurrencesQueryConstituents(
	clusterId string,
	countMap map[string]struct{},
) ([]client.MetaMap, []client.DocumentMap) {
	metaMap := make([]client.MetaMap, len(countMap))
	updateMap := make([]client.DocumentMap, len(countMap))
	i := 0
	for otherClusterId, _ := range countMap {
		compositeId := getIDFromConstituents(clusterId, otherClusterId)
		meta, update := buildUpdateClusterCountsQuery(compositeId, clusterId, otherClusterId)
		metaMap[i] = meta
		updateMap[i] = update
		i++
	}
	return metaMap, updateMap
}

func (cs *CountService) countOccurrencesAndCoOccurrencesByCoClusterId(
	ctx context.Context,
	clusterId string,
	timeInfo model.TimeInfo,
	buckets []Bucket,
) (map[string]struct{}, error) {
	var coOccurringClustersByClusterId map[string]struct{}
	for _, bucket := range buckets {
		calculatedTimeInfo, err := getTimeRangeForBucket(timeInfo, bucket)
		if err != nil {
			cs.logger.Error(
				"Failed to calculate time range for bucket",
				zap.Any("timeInfo", timeInfo),
				zap.Error(err),
			)
			return nil, fmt.Errorf("error calculating time range for bucket: %w", err)
		}
		fromTime, toTime := calculatedTimeInfo.FromTime, calculatedTimeInfo.ToTime
		coOccurringClusters, err := cs.getCoOccurringCluster(ctx, clusterId, fromTime, toTime)
		if err != nil {
			cs.logger.Error(
				"Failed to get co-occurring clusters",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
			return nil, err
		}
		coOccurringClustersByClusterId = groupCoOccurringClustersByClusterId(coOccurringClusters)
		// TODO: Just return a list of strings after testing
		if len(coOccurringClustersByClusterId) != len(coOccurringClusters) {
			cs.logger.Fatal("Duplicate co-occurring clusters found")
		}
	}
	return coOccurringClustersByClusterId, nil
}

func (cs *CountService) getCoOccurringCluster(
	ctx context.Context,
	clusterId string,
	fromTime time.Time,
	toTime time.Time,
) ([]model.Cluster, error) {
	queryCtx, cancel := context.WithTimeout(ctx, csTimeOut)
	defer cancel()
	queryBody, err := json.Marshal(buildGetCoOccurringClustersQuery(clusterId, fromTime, toTime))
	if err != nil {
		cs.logger.Error(
			"Failed to marshal count co-occurrences query",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error marshaling query: %w", err)
	}
	var querySize = 10000
	res, err := cs.ac.Search(queryCtx, string(queryBody), indices, &querySize)
	if err != nil {
		cs.logger.Error(
			"Failed to search for count co-occurrences",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error searching for count co-Occurrences: %w", err)
	}
	coOccurringClusters, err := convertDocsToClusters(res)
	if err != nil {
		cs.logger.Error(
			"Failed to convert search results to cluster documents",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error converting search results to cluster: %w", err)
	}
	return coOccurringClusters, nil
}

func (cs *CountService) GetIncrementMissesQueryInfo(
	ctx context.Context,
	input model.IncreaseMissesInput,
) (*model.GetMetaAndDocumentInfoForIncrementMissesQueryResult, error) {
	missingCoClusterIds, err := cs.getNonMatchingCoClusterIds(ctx, input.ClusterId, input.CoClusterIdsToExclude)
	if err != nil {
		return nil, err
	}
	if len(missingCoClusterIds) == 0 {
		return nil, nil
	}

	metaMapList, documentMapList := cs.getIncrementMissesDetails(input.ClusterId, missingCoClusterIds)
	result := model.GetMetaAndDocumentInfoForIncrementMissesQueryResult{
		MetaMapList:     metaMapList,
		DocumentMapList: documentMapList,
	}
	return &result, nil

}

func (cs *CountService) getNonMatchingCoClusterIds(
	ctx context.Context,
	clusterId string,
	listOfCoOccurringClusters []string,
) ([]model.Cluster, error) {
	incrementQuery := buildGetNonMatchedCoClusterIdsQuery(
		clusterId,
		listOfCoOccurringClusters,
	)
	queryBody, err := json.Marshal(incrementQuery)
	if err != nil {
		cs.logger.Error(
			"Failed to marshal increment query",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error marshaling increment query: %w", err)
	}
	searchCtx, searchCancel := context.WithTimeout(ctx, csTimeOut)
	defer searchCancel()
	queryIndices := []string{bootstrapper.CountIndexName}
	querySize := 10000
	res, err := cs.ac.Search(searchCtx, string(queryBody), queryIndices, &querySize)
	nonMatchingCoClusters, err := convertDocsToCoClusters(res)
	if err != nil {
		cs.logger.Error(
			"Failed to convert search results to cluster documents",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error converting search results to cluster: %w", err)
	}
	return nonMatchingCoClusters, nil
}

func (cs *CountService) getIncrementMissesDetails(
	clusterId string,
	missingCoClusterIds []model.Cluster,
) ([]client.MetaMap, []client.DocumentMap) {
	metaMap := make([]client.MetaMap, len(missingCoClusterIds))
	updateMap := make([]client.DocumentMap, len(missingCoClusterIds))
	for i, missingCoClusterId := range missingCoClusterIds {
		compositeKey := getIDFromConstituents(clusterId, missingCoClusterId.CoClusterId)
		meta, update := buildIncrementNonMatchedCoClusterIdsQuery(compositeKey)
		metaMap[i] = meta
		updateMap[i] = update
	}
	if len(updateMap) == 0 {
		return nil, nil
	}
	return metaMap, updateMap
}

func groupCoOccurringClustersByClusterId(clusters []model.Cluster) map[string]struct{} {
	coOccurringClustersByClusterId := make(map[string]struct{})
	for _, cluster := range clusters {
		if _, ok := coOccurringClustersByClusterId[cluster.ClusterId]; !ok {
			coOccurringClustersByClusterId[cluster.ClusterId] = struct{}{}
		}
	}
	return coOccurringClustersByClusterId
}

func convertDocsToClusters(res []map[string]interface{}) ([]model.Cluster, error) {
	clusters := make([]model.Cluster, len(res))
	for i, hit := range res {
		doc := model.Cluster{}
		if clusterId, ok := hit["cluster_id"].(string); !ok {
			return nil, fmt.Errorf("error parsing cluster_id")
		} else {
			doc.ClusterId = clusterId
		}
		clusters[i] = doc
	}
	return clusters, nil
}

func convertDocsToCoClusters(res []map[string]interface{}) ([]model.Cluster, error) {
	clusters := make([]model.Cluster, len(res))
	for i, hit := range res {
		doc := model.Cluster{}
		if coClusterId, ok := hit["co_cluster_id"].(string); !ok {
			return nil, fmt.Errorf("error parsing co_cluster_id")
		} else {
			doc.CoClusterId = coClusterId
		}
		clusters[i] = doc
	}
	return clusters, nil
}

func getTimeRangeForBucket(timeInfo model.TimeInfo, bucket Bucket) (model.CalculatedTimeInfo, error) {
	if timeInfo.SpanInfo != nil {
		return model.CalculatedTimeInfo{
			FromTime: timeInfo.SpanInfo.FromTime.Add(-time.Millisecond * time.Duration(bucket/2)),
			ToTime:   timeInfo.SpanInfo.ToTime.Add(time.Millisecond * time.Duration(bucket/2)),
		}, nil
	} else if timeInfo.LogInfo != nil {
		startTime, endTime := getTimeRange(timeInfo.LogInfo.Timestamp, bucket)
		return model.CalculatedTimeInfo{
			FromTime: startTime,
			ToTime:   endTime,
		}, nil
	} else {
		return model.CalculatedTimeInfo{}, fmt.Errorf("timeInfo.spanInfo or timeInfo.logInfo is required")
	}
}

func getIDFromConstituents(clusterId, coClusterId string) string {
	return fmt.Sprintf("%s;%s", clusterId, coClusterId)
}
