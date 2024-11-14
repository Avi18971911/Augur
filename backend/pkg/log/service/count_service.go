package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"go.uber.org/zap"
	"time"
)

const querySize = 10000
const csTimeOut = 500 * time.Millisecond

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

func countOccurrencesQueryBuilder(clusterId string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"cluster_id": clusterId,
						},
					},
				},
			},
		},
	}
}

func countCoOccurrencesQueryBuilder(clusterId string, fromTime time.Time, toTime time.Time) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte": fromTime,
								"lte": toTime,
							},
						},
					},
				},
				"must_not": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"cluster_id": clusterId,
						},
					},
				},
			},
		},
	}
}

func getTimeRange(logTimeStamp time.Time, bucket Bucket) (time.Time, time.Time) {
	fromTime := logTimeStamp.Add(-time.Millisecond * time.Duration(bucket/2))
	toTime := logTimeStamp.Add(time.Millisecond * time.Duration(bucket/2))
	return fromTime, toTime
}

func (cs *CountService) CountAndUpdateOccurrences(newLog model.LogEntry, buckets []Bucket, ctx context.Context) error {
	countMap, err := cs.CountOccurrences(newLog, buckets, ctx)
	if err != nil {
		return err
	}
	for otherClusterId, countInfo := range countMap {
		err = cs.updateOccurrences(newLog.ClusterId, otherClusterId, countInfo, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cs *CountService) updateOccurrences(
	clusterId string,
	otherClusterId string,
	countInfo CountInfo,
	ctx context.Context,
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

	updateBody, err := json.Marshal(updateStatement)
	if err != nil {
		cs.logger.Error(
			"Failed to marshal update statement",
			zap.String("clusterId", clusterId),
			zap.String("otherClusterId", otherClusterId),
			zap.Error(err),
		)
		return fmt.Errorf("error marshaling update statement: %w", err)
	}
	upsertCtx, cancel := context.WithTimeout(ctx, csTimeOut)
	defer cancel()
	err = cs.ac.Upsert(
		string(updateBody),
		elasticsearch.LogIndexName,
		clusterId,
		upsertCtx,
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

func (cs *CountService) CountOccurrences(
	newLog model.LogEntry,
	buckets []Bucket,
	ctx context.Context,
) (map[string]CountInfo, error) {
	var countMap = make(map[string]CountInfo)
	for _, bucket := range buckets {
		fromTime, toTime := getTimeRange(newLog.Timestamp, bucket)
		coOccurringLogs, err := cs.getCoOccurringLogs(newLog.ClusterId, fromTime, toTime, ctx)
		if err != nil {
			return nil, err
		}
		coOccurringLogsByClusterId := groupCoOccurringLogsByClusterId(coOccurringLogs)
		for coOccurringClusterId, groupedCoOccurringLogs := range coOccurringLogsByClusterId {
			occurrences, err := cs.getOccurrencesOfClusterId(coOccurringClusterId, ctx)
			if err != nil {
				return nil, err
			}
			if _, ok := countMap[coOccurringClusterId]; !ok {
				countMap[coOccurringClusterId] = CountInfo{
					CoOccurrences: int64(len(groupedCoOccurringLogs)),
					Occurrences:   occurrences,
				}
			} else {
				countMap[coOccurringClusterId] = CountInfo{
					CoOccurrences: int64(len(groupedCoOccurringLogs)) + countMap[coOccurringClusterId].CoOccurrences,
					Occurrences:   occurrences + countMap[coOccurringClusterId].Occurrences,
				}
			}
		}
	}
	return countMap, nil
}

func (cs *CountService) getCoOccurringLogs(
	clusterId string,
	fromTime time.Time,
	toTime time.Time,
	ctx context.Context,
) ([]model.LogEntry, error) {
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
	res, err := cs.ac.Search(string(queryBody), elasticsearch.LogIndexName, querySize, queryCtx)
	if err != nil {
		cs.logger.Error(
			"Failed to search for count co-occurrences",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error searching for count co-Occurrences: %w", err)
	}
	coOccurringLogs, err := elasticsearch.ConvertToLogDocuments(res)
	if err != nil {
		cs.logger.Error(
			"Failed to convert search results to log documents",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error converting search results to log documents: %w", err)
	}
	return coOccurringLogs, nil
}

func (cs *CountService) getInstancesOfClusterId(
	clusterId string,
	ctx context.Context,
) ([]model.LogEntry, error) {
	queryCtx, cancel := context.WithTimeout(ctx, csTimeOut)
	defer cancel()
	queryBody, err := json.Marshal(countOccurrencesQueryBuilder(clusterId))
	if err != nil {
		cs.logger.Error(
			"Failed to marshal occurrences query for clusterId",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error marshaling query: %w", err)
	}
	res, err := cs.ac.Search(string(queryBody), elasticsearch.LogIndexName, querySize, queryCtx)
	if err != nil {
		cs.logger.Error(
			"Failed to search for occurrences",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error searching for count Occurrences: %w", err)
	}
	searchLogs, err := elasticsearch.ConvertToLogDocuments(res)
	if err != nil {
		cs.logger.Error(
			"Failed to convert search results to log documents",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error converting search results to log documents: %w", err)
	}
	return searchLogs, nil
}

func (cs *CountService) getOccurrencesOfClusterId(clusterId string, ctx context.Context) (int64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, csTimeOut)
	defer cancel()
	query := countOccurrencesQueryBuilder(clusterId)
	queryBody, err := json.Marshal(query)
	if err != nil {
		cs.logger.Error(
			"Failed to marshal occurrences query for clusterId",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return 0, fmt.Errorf("error marshaling query: %w", err)
	}
	return cs.ac.Count(string(queryBody), elasticsearch.LogIndexName, queryCtx)
}

func groupCoOccurringLogsByClusterId(logs []model.LogEntry) map[string][]model.LogEntry {
	coOccurringLogsByClusterId := make(map[string][]model.LogEntry)
	for _, log := range logs {
		if _, ok := coOccurringLogsByClusterId[log.ClusterId]; !ok {
			coOccurringLogsByClusterId[log.ClusterId] = []model.LogEntry{log}
		} else {
			coOccurringLogsByClusterId[log.ClusterId] = append(coOccurringLogsByClusterId[log.ClusterId], log)
		}
	}
	return coOccurringLogsByClusterId
}
