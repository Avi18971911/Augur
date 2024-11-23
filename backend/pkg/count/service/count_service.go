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

type LogInfo struct {
	Timestamp time.Time
}

type SpanInfo struct {
	FromTime time.Time
	ToTime   time.Time
}

type CalculatedTimeInfo struct {
	FromTime time.Time
	ToTime   time.Time
}

type TimeInfo struct {
	SpanInfo *SpanInfo
	LogInfo  *LogInfo
}

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

func (cs *CountService) CountAndUpdateOccurrences(
	ctx context.Context,
	clusterId string,
	timeInfo TimeInfo,
	buckets []Bucket,
) error {
	countMap, err := cs.CountOccurrencesAndCoOccurrencesByCoClusterId(ctx, clusterId, timeInfo, buckets)
	if err != nil {
		return err
	}
	err = cs.updateCoOccurrences(ctx, clusterId, countMap)
	if err != nil {
		cs.logger.Error("Failed to update co-occurrences", zap.Error(err))
		return err
	}
	err = cs.increaseOccurrencesForMisses(ctx, clusterId, countMap)
	if err != nil {
		cs.logger.Error("Failed to increase occurrences for misses", zap.Error(err))
		return err
	}
	return nil
}

func (cs *CountService) updateCoOccurrences(
	ctx context.Context,
	clusterId string,
	countMap map[string]CountInfo,
) error {
	metaMap := make([]map[string]interface{}, len(countMap))
	updateMap := make([]map[string]interface{}, len(countMap))
	i := 0
	for otherClusterId, countInfo := range countMap {
		meta, update := buildUpdateClusterCountsQuery(clusterId, otherClusterId, countInfo)
		metaMap[i] = meta
		updateMap[i] = update
		i++
	}
	if len(updateMap) == 0 {
		return nil
	}
	updateCtx, cancel := context.WithTimeout(ctx, csTimeOut)
	defer cancel()
	return cs.ac.BulkIndex(updateCtx, updateMap, metaMap, bootstrapper.CountIndexName)
}

func (cs *CountService) CountOccurrencesAndCoOccurrencesByCoClusterId(
	ctx context.Context,
	clusterId string,
	timeInfo TimeInfo,
	buckets []Bucket,
) (map[string]CountInfo, error) {
	var countMap = make(map[string]CountInfo)
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
		coOccurringClustersByClusterId := groupCoOccurringClustersByClusterId(coOccurringClusters)
		for coOccurringClusterId, groupedCoOccurringClusters := range coOccurringClustersByClusterId {
			if _, ok := countMap[coOccurringClusterId]; !ok {
				countMap[coOccurringClusterId] = CountInfo{
					CoOccurrences: int64(len(groupedCoOccurringClusters)),
					Occurrences:   int64(len(groupedCoOccurringClusters)),
				}
			} else {
				countMap[coOccurringClusterId] = CountInfo{
					CoOccurrences: int64(len(groupedCoOccurringClusters)) + countMap[coOccurringClusterId].CoOccurrences,
					Occurrences:   int64(len(groupedCoOccurringClusters)) + countMap[coOccurringClusterId].Occurrences,
				}
			}
		}
	}
	return countMap, nil
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

func (cs *CountService) increaseOccurrencesForMisses(
	ctx context.Context,
	clusterId string,
	coOccurringClustersByCount map[string]CountInfo,
) error {
	listOfCoOccurringClusters := getListOfCoOccurringClusters(coOccurringClustersByCount)

	clusterIds, err := cs.getNonMatchingClusterIds(ctx, clusterId, listOfCoOccurringClusters)
	if err != nil {
		return err
	}
	if len(clusterIds) == 0 {
		return nil
	}

	err = cs.updateOccurrencesForMisses(ctx, clusterIds)
	if err != nil {
		return err
	}
	return nil
}

func (cs *CountService) getNonMatchingClusterIds(
	ctx context.Context,
	clusterId string,
	listOfCoOccurringClusters []string,
) ([]model.Cluster, error) {
	incrementQuery := buildGetNonMatchedClusterIdsQuery(
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
	searchIndices := []string{bootstrapper.CountIndexName}
	searchCtx, searchCancel := context.WithTimeout(ctx, csTimeOut)
	defer searchCancel()
	res, err := cs.ac.Search(searchCtx, string(queryBody), searchIndices, nil)
	nonMatchingClusters, err := convertDocsToClusters(res)
	if err != nil {
		cs.logger.Error(
			"Failed to convert search results to cluster documents",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error converting search results to cluster: %w", err)
	}
	return nonMatchingClusters, nil
}

func (cs *CountService) updateOccurrencesForMisses(
	ctx context.Context,
	clusterIds []model.Cluster,
) error {
	metaMap := make([]map[string]interface{}, len(clusterIds))
	updateMap := make([]map[string]interface{}, len(clusterIds))
	for i, clusterId := range clusterIds {
		meta, update := buildUpdateNonMatchedClusterIdsQuery(clusterId.ClusterId)
		metaMap[i] = meta
		updateMap[i] = update
	}
	if len(updateMap) == 0 {
		return nil
	}
	updateCtx, cancel := context.WithTimeout(ctx, csTimeOut)
	defer cancel()
	err := cs.ac.BulkIndex(updateCtx, updateMap, metaMap, bootstrapper.CountIndexName)
	if err != nil {
		cs.logger.Error("Failed to update occurrences for misses", zap.Error(err))
	}
	return err
}

func getListOfCoOccurringClusters(coOccurringClustersByCount map[string]CountInfo) []string {
	listOfCoOccurringClusters := make([]string, len(coOccurringClustersByCount))
	i := 0
	for key, _ := range coOccurringClustersByCount {
		listOfCoOccurringClusters[i] = key
		i++
	}
	return listOfCoOccurringClusters
}

func groupCoOccurringClustersByClusterId(clusters []model.Cluster) map[string][]model.Cluster {
	coOccurringClustersByClusterId := make(map[string][]model.Cluster)
	for _, cluster := range clusters {
		if _, ok := coOccurringClustersByClusterId[cluster.ClusterId]; !ok {
			coOccurringClustersByClusterId[cluster.ClusterId] = []model.Cluster{cluster}
		} else {
			coOccurringClustersByClusterId[cluster.ClusterId] = append(
				coOccurringClustersByClusterId[cluster.ClusterId],
				cluster,
			)
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

func getTimeRangeForBucket(timeInfo TimeInfo, bucket Bucket) (CalculatedTimeInfo, error) {
	if timeInfo.SpanInfo != nil {
		return CalculatedTimeInfo{
			FromTime: timeInfo.SpanInfo.FromTime.Add(-time.Millisecond * time.Duration(bucket/2)),
			ToTime:   timeInfo.SpanInfo.ToTime.Add(time.Millisecond * time.Duration(bucket/2)),
		}, nil
	} else if timeInfo.LogInfo != nil {
		startTime, endTime := getTimeRange(timeInfo.LogInfo.Timestamp, bucket)
		return CalculatedTimeInfo{
			FromTime: startTime,
			ToTime:   endTime,
		}, nil
	} else {
		return CalculatedTimeInfo{}, fmt.Errorf("timeInfo.spanInfo or timeInfo.logInfo is required")
	}
}
