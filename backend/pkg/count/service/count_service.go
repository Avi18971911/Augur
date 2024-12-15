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

func getTimeRange(timestamp time.Time, bucket model.Bucket) (time.Time, time.Time) {
	fromTime := timestamp.Add(-time.Millisecond * time.Duration(bucket/2))
	toTime := timestamp.Add(time.Millisecond * time.Duration(bucket/2))
	return fromTime, toTime
}

func (cs *CountService) GetCountAndUpdateOccurrencesQueryConstituents(
	ctx context.Context,
	clusterId string,
	timeInfo model.TimeInfo,
	indices []string,
	buckets []model.Bucket,
) (*model.GetCountAndUpdateQueryDetails, error) {
	coClusterMapCount, err := cs.countOccurrencesAndCoOccurrencesByCoClusterId(
		ctx,
		clusterId,
		timeInfo,
		indices,
		buckets,
	)
	if err != nil {
		return nil, err
	}
	metaMapList, documentMapList := cs.getUpdateCoOccurrencesQueryConstituents(clusterId, coClusterMapCount)
	increaseForMissesInput := getIncrementOccurrencesForMissesInput(clusterId, coClusterMapCount)
	result := &model.GetCountAndUpdateQueryDetails{
		IncreaseIncrementForMissesInput: increaseForMissesInput,
		MetaMapList:                     metaMapList,
		DocumentMapList:                 documentMapList,
	}
	return result, nil
}

func getIncrementOccurrencesForMissesInput(
	clusterId string,
	coClusterToExcludeMapCount map[string]model.CountInfo,
) model.IncreaseMissesInput {
	coClusterIdsToExclude := getCoClusterIdsFromMap(coClusterToExcludeMapCount)
	return model.IncreaseMissesInput{
		ClusterId:                 clusterId,
		CoClusterDetailsToExclude: coClusterIdsToExclude,
	}
}

func (cs *CountService) getUpdateCoOccurrencesQueryConstituents(
	clusterId string,
	coClusterMapCount map[string]model.CountInfo,
) ([]client.MetaMap, []client.DocumentMap) {
	metaMap := make([]client.MetaMap, len(coClusterMapCount))
	updateMap := make([]client.DocumentMap, len(coClusterMapCount))
	i := 0
	for otherClusterId, coClusterDetails := range coClusterMapCount {
		compositeId := GetIDFromConstituents(clusterId, otherClusterId)
		newValue := coClusterDetails.TotalTDOA / float64(coClusterDetails.Occurrences) // Calculate the average of the matching coClusters
		meta, update := buildUpdateClusterCountsQuery(compositeId, clusterId, otherClusterId, newValue)
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
	indices []string,
	buckets []model.Bucket,
) (map[string]model.CountInfo, error) {
	var coClusterInfoMap = make(map[string]model.CountInfo)
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
		coOccurringClusters, err := cs.getCoOccurringCluster(ctx, clusterId, indices, fromTime, toTime)
		if err != nil {
			cs.logger.Error(
				"Failed to get co-occurring clusters",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
			return nil, err
		}
		err = addCoOccurringClustersToCoClusterInfoMap(coClusterInfoMap, coOccurringClusters, timeInfo)
	}
	return coClusterInfoMap, nil
}

func (cs *CountService) getCoOccurringCluster(
	ctx context.Context,
	clusterId string,
	indices []string,
	fromTime time.Time,
	toTime time.Time,
) ([]model.ClusterQueryResult, error) {
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
) (*model.GetIncrementMissesQueryDetails, error) {
	missingCoClusterIds, err := cs.getNonMatchingCoClusterIds(ctx, input)
	if err != nil {
		return nil, err
	}
	if len(missingCoClusterIds) == 0 {
		return nil, nil
	}

	metaMapList, documentMapList := cs.getIncrementMissesDetails(input.ClusterId, missingCoClusterIds)
	result := model.GetIncrementMissesQueryDetails{
		MetaMapList:     metaMapList,
		DocumentMapList: documentMapList,
	}
	return &result, nil
}

func (cs *CountService) getNonMatchingCoClusterIds(
	ctx context.Context,
	input model.IncreaseMissesInput,
) ([]model.ClusterQueryResult, error) {
	clusterId, coClustersToExclude := input.ClusterId, input.CoClusterDetailsToExclude
	if coClustersToExclude == nil {
		coClustersToExclude = []string{}
	}
	incrementQuery := buildGetNonMatchedCoClusterIdsQuery(
		clusterId,
		coClustersToExclude,
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
	querySize := 10000
	res, err := cs.ac.Search(searchCtx, string(queryBody), []string{bootstrapper.CountIndexName}, &querySize)
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
	missingCoClusterIds []model.ClusterQueryResult,
) ([]client.MetaMap, []client.DocumentMap) {
	metaMap := make([]client.MetaMap, len(missingCoClusterIds))
	updateMap := make([]client.DocumentMap, len(missingCoClusterIds))
	for i, missingCoClusterId := range missingCoClusterIds {
		compositeKey := GetIDFromConstituents(clusterId, missingCoClusterId.CoClusterId)
		meta, update := buildIncrementNonMatchedCoClusterIdsQuery(compositeKey)
		metaMap[i] = meta
		updateMap[i] = update
	}
	if len(updateMap) == 0 {
		return nil, nil
	}
	return metaMap, updateMap
}

func addCoOccurringClustersToCoClusterInfoMap(
	coClusterInfoMap map[string]model.CountInfo,
	clusters []model.ClusterQueryResult,
	timeInfo model.TimeInfo,
) error {
	for _, cluster := range clusters {
		TDOA, err := getTDOA(cluster, timeInfo)
		if err != nil {
			return fmt.Errorf("error calculating TDOA: %w", err)
		}
		if _, ok := coClusterInfoMap[cluster.ClusterId]; !ok {
			coClusterInfoMap[cluster.ClusterId] = model.CountInfo{
				Occurrences: 1,
				TotalTDOA:   TDOA,
			}
		} else {
			coClusterInfoMap[cluster.ClusterId] = model.CountInfo{
				Occurrences: coClusterInfoMap[cluster.ClusterId].Occurrences + 1,
				TotalTDOA:   coClusterInfoMap[cluster.ClusterId].TotalTDOA + TDOA,
			}
		}
	}
	return nil
}

func convertDocsToClusters(res []map[string]interface{}) ([]model.ClusterQueryResult, error) {
	clusters := make([]model.ClusterQueryResult, len(res))
	for i, hit := range res {
		doc := model.ClusterQueryResult{}
		if clusterId, ok := hit["cluster_id"].(string); !ok {
			return nil, fmt.Errorf("error parsing cluster_id")
		} else {
			doc.ClusterId = clusterId
		}
		if startTime, ok := hit["start_time"].(string); ok && hit["end_time"] != nil {
			coClusterStartTime, err := client.NormalizeTimestampToNanoseconds(startTime)
			if err != nil {
				return nil, fmt.Errorf("error parsing start_time")
			}
			coClusterEndTime, err := client.NormalizeTimestampToNanoseconds(hit["end_time"].(string))
			if err != nil {
				return nil, fmt.Errorf("error parsing end_time")
			}
			doc.SpanResult = &model.SpanQueryResult{
				StartTime: coClusterStartTime,
				EndTime:   coClusterEndTime,
			}
		} else if timestamp, ok := hit["timestamp"].(string); ok {
			coClusterTimestamp, err := client.NormalizeTimestampToNanoseconds(timestamp)
			if err != nil {
				return nil, fmt.Errorf("error parsing timestamp")
			}
			doc.LogResult = &model.LogQueryResult{
				Timestamp: coClusterTimestamp,
			}
		} else {
			return nil, fmt.Errorf("error parsing start_time or timestamp")
		}
		clusters[i] = doc
	}
	return clusters, nil
}

func convertDocsToCoClusters(res []map[string]interface{}) ([]model.ClusterQueryResult, error) {
	clusters := make([]model.ClusterQueryResult, len(res))
	for i, hit := range res {
		doc := model.ClusterQueryResult{}
		if coClusterId, ok := hit["co_cluster_id"].(string); !ok {
			return nil, fmt.Errorf("error parsing co_cluster_id")
		} else {
			doc.CoClusterId = coClusterId
		}
		clusters[i] = doc
	}
	return clusters, nil
}

func getTimeRangeForBucket(timeInfo model.TimeInfo, bucket model.Bucket) (model.CalculatedTimeInfo, error) {
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

func GetIDFromConstituents(clusterId, coClusterId string) string {
	return fmt.Sprintf("%s;%s", clusterId, coClusterId)
}

func getTDOA(coCluster model.ClusterQueryResult, clusterTimeInfo model.TimeInfo) (float64, error) {
	var coClusterTime time.Time
	if coCluster.SpanResult != nil {
		coClusterTime = coCluster.SpanResult.StartTime
	} else if coCluster.LogResult != nil {
		coClusterTime = coCluster.LogResult.Timestamp
	} else {
		return 0, fmt.Errorf("coCluster.spanResult or coCluster.logResult is required")
	}

	if clusterTimeInfo.SpanInfo != nil {
		return coClusterTime.Sub(clusterTimeInfo.SpanInfo.FromTime).Seconds(), nil
	} else if clusterTimeInfo.LogInfo != nil {
		return coClusterTime.Sub(clusterTimeInfo.LogInfo.Timestamp).Seconds(), nil
	}
	return 0, fmt.Errorf("clusterTimeInfo.spanInfo or clusterTimeInfo.logInfo is required")
}

func getCoClusterIdsFromMap(coClusterMapCount map[string]model.CountInfo) []string {
	coClusterIds := make([]string, len(coClusterMapCount))
	i := 0
	for coClusterId := range coClusterMapCount {
		coClusterIds[i] = coClusterId
		i++
	}
	return coClusterIds
}
