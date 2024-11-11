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

func getTimeRange(logTimeStamp time.Time, bucket bucket) (time.Time, time.Time) {
	fromTime := logTimeStamp.Add(-time.Duration(bucket / 2))
	toTime := logTimeStamp.Add(time.Duration(bucket / 2))
	return fromTime, toTime
}

func (cs *CountService) CountOccurrences(clusterId string, buckets []bucket) (map[string]CountInfo, error) {
	ctx := context.Background()
	matchingLogs, err := cs.getOccurrencesOfClusterId(clusterId, ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting matching logs: %w", err)
	}
	var countMap = make(map[string]CountInfo)
	for _, log := range matchingLogs {
		for _, bucket := range buckets {
			fromTime, toTime := getTimeRange(log.Timestamp, bucket)
			coOccurringLogs, err := cs.getCoOccurringLogs(clusterId, fromTime, toTime, ctx)
			if err != nil {
				return nil, err
			}
			coOccurringLogsByClusterId := groupCoOccurringLogsByClusterId(coOccurringLogs)
			for coOccurringClusterId, groupedCoOccurringLogs := range coOccurringLogsByClusterId {
				if _, ok := countMap[coOccurringClusterId]; !ok {
					countMap[coOccurringClusterId] = CountInfo{
						coOccurrences: int64(len(groupedCoOccurringLogs)),
						occurrences:   int64(len(matchingLogs)),
					}
				} else {
					countMap[coOccurringClusterId] = CountInfo{
						coOccurrences: int64(len(groupedCoOccurringLogs)) + countMap[coOccurringClusterId].coOccurrences,
						occurrences:   int64(len(matchingLogs)) + countMap[coOccurringClusterId].occurrences,
					}
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
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
		return nil, fmt.Errorf("error searching for count co-occurrences: %w", err)
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

func (cs *CountService) getOccurrencesOfClusterId(
	clusterId string,
	ctx context.Context,
) ([]model.LogEntry, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
		return nil, fmt.Errorf("error searching for count occurrences: %w", err)
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
