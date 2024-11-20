package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/count/model"
	"github.com/Avi18971911/Augur/pkg/elasticsearch"
	"go.uber.org/zap"
	"time"
)

const csTimeOut = 2 * time.Second

var indices = []string{elasticsearch.LogIndexName, elasticsearch.SpanIndexName}

type LogInfo struct {
	Timestamp time.Time
}

type SpanInfo struct {
	FromTime time.Time
	ToTime   time.Time
}

type CalculatedTimeInfo struct {
	FromTime         time.Time
	ToTime           time.Time
	ExtendedFromTime time.Time
	ExtendedToTime   time.Time
}

type TimeInfo struct {
	SpanInfo *SpanInfo
	LogInfo  *LogInfo
}

type CountService struct {
	ac     elasticsearch.AugurClient
	logger *zap.Logger
}

func NewCountService(ac elasticsearch.AugurClient, logger *zap.Logger) *CountService {
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
	for otherClusterId, countInfo := range countMap {
		err = cs.updateCounts(ctx, clusterId, otherClusterId, countInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *CountService) updateCounts(
	ctx context.Context,
	clusterId string,
	otherClusterId string,
	countInfo CountInfo,
) error {
	updateStatement := map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.occurrences += params.occurrences; ctx._source.co_occurrences += params.co_occurrences",
			"params": map[string]interface{}{
				"occurrences":    countInfo.Occurrences,
				"co_occurrences": countInfo.CoOccurrences,
			},
		},
		"upsert": map[string]interface{}{
			"cluster_id":     clusterId,
			"co_cluster_id":  otherClusterId,
			"occurrences":    countInfo.Occurrences,
			"co_occurrences": countInfo.CoOccurrences,
		},
	}

	upsertCtx, cancel := context.WithTimeout(ctx, csTimeOut)
	defer cancel()
	err := cs.ac.Upsert(
		upsertCtx,
		updateStatement,
		elasticsearch.CountIndexName,
		clusterId,
	)
	if err != nil {
		cs.logger.Error(
			"Failed to upsert occurrences",
			zap.String("clusterId", clusterId),
			zap.String("otherClusterId", otherClusterId),
			zap.Error(err),
		)
		return fmt.Errorf("error upserting occurrences: %w", err)
	}
	return nil
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
			return nil, fmt.Errorf("error calculating time range for bucket: %w", err)
		}
		fromTime, toTime := calculatedTimeInfo.FromTime, calculatedTimeInfo.ToTime
		fromExtendedTime, toExtendedTime := calculatedTimeInfo.ExtendedFromTime, calculatedTimeInfo.ExtendedToTime
		coOccurringClusters, err := cs.getCoOccurringCluster(ctx, clusterId, fromTime, toTime)
		if err != nil {
			return nil, err
		}
		coOccurringClustersByClusterId := groupCoOccurringClustersByClusterId(coOccurringClusters)
		err = cs.increaseOccurrencesForMisses(ctx, clusterId, coOccurringClustersByClusterId)
		if err != nil {
			cs.logger.Error("Failed to increase occurrences for misses", zap.Error(err))
		}
		for coOccurringClusterId, groupedCoOccurringClusters := range coOccurringClustersByClusterId {
			occurrences, err := cs.getOccurrencesOfClusterId(
				ctx,
				coOccurringClusterId,
				fromExtendedTime,
				toExtendedTime,
			)
			if err != nil {
				return nil, err
			}
			if _, ok := countMap[coOccurringClusterId]; !ok {
				countMap[coOccurringClusterId] = CountInfo{
					CoOccurrences: int64(len(groupedCoOccurringClusters)),
					Occurrences:   occurrences,
				}
			} else {
				countMap[coOccurringClusterId] = CountInfo{
					CoOccurrences: int64(len(groupedCoOccurringClusters)) + countMap[coOccurringClusterId].CoOccurrences,
					Occurrences:   occurrences + countMap[coOccurringClusterId].Occurrences,
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
	queryBody, err := json.Marshal(countCoOccurrencesQueryBuilder(clusterId, fromTime, toTime))
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

func (cs *CountService) getOccurrencesOfClusterId(
	ctx context.Context,
	clusterId string,
	fromTime time.Time,
	toTime time.Time,
) (int64, error) {
	// *2 because we want to search for a decent sized window around the timestamp
	queryCtx, cancel := context.WithTimeout(ctx, csTimeOut)
	defer cancel()
	query := countOccurrencesQueryBuilder(clusterId, fromTime, toTime)
	queryBody, err := json.Marshal(query)
	if err != nil {
		cs.logger.Error(
			"Failed to marshal occurrences query for clusterId",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return 0, fmt.Errorf("error marshaling query: %w", err)
	}
	return cs.ac.Count(queryCtx, string(queryBody), indices)
}

func (cs *CountService) increaseOccurrencesForMisses(
	ctx context.Context,
	clusterId string,
	coOccurringClusters map[string][]model.Cluster,
) error {
	listOfCoOccurringClusters := make([]string, len(coOccurringClusters))
	i := 0
	for key, _ := range coOccurringClusters {
		listOfCoOccurringClusters[i] = key
		i++
	}

	incrementQuery := incrementNonMatchedClusterIds(clusterId, listOfCoOccurringClusters)
	queryBody, err := json.Marshal(incrementQuery)
	if err != nil {
		cs.logger.Error(
			"Failed to marshal increment query",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return fmt.Errorf("error marshaling increment query: %w", err)
	}
	return cs.ac.UpdateByQuery(ctx, string(queryBody), indices)
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
			FromTime:         timeInfo.SpanInfo.FromTime.Add(-time.Millisecond * time.Duration(bucket/2)),
			ToTime:           timeInfo.SpanInfo.ToTime.Add(time.Millisecond * time.Duration(bucket/2)),
			ExtendedFromTime: timeInfo.SpanInfo.FromTime.Add(-time.Millisecond * time.Duration(bucket)),
			ExtendedToTime:   timeInfo.SpanInfo.ToTime.Add(time.Millisecond * time.Duration(bucket)),
		}, nil
	} else if timeInfo.LogInfo != nil {
		startTime, endTime := getTimeRange(timeInfo.LogInfo.Timestamp, bucket)
		extendedStartTime, extendedEndTime := getTimeRange(timeInfo.LogInfo.Timestamp, bucket*2)
		return CalculatedTimeInfo{
			FromTime:         startTime,
			ToTime:           endTime,
			ExtendedFromTime: extendedStartTime,
			ExtendedToTime:   extendedEndTime,
		}, nil
	} else {
		return CalculatedTimeInfo{}, fmt.Errorf("timeInfo.spanInfo or timeInfo.logInfo is required")
	}
}
