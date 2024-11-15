package service

import (
	"context"
	"encoding/json"
	"fmt"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"time"
)

const lpTimeOut = 1000 * time.Millisecond

type LogProcessorService interface {
	ParseLogWithMessage(service string, log model.LogEntry, ctx context.Context) (model.LogEntry, error)
}

type LogProcessorServiceImpl struct {
	ac     augurElasticsearch.AugurClient
	logger *zap.Logger
}

func NewLogProcessorService(ac augurElasticsearch.AugurClient, logger *zap.Logger) LogProcessorService {
	return &LogProcessorServiceImpl{
		ac:     ac,
		logger: logger,
	}
}

// TODO: Consider making a Log Repository that handles this Elasticsearch logic
func moreLikeThisQueryBuilder(service string, phrase string) map[string]interface{} {
	// more_like_this: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html
	// bool: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"more_like_this": map[string]interface{}{
							"fields":               []string{"message"},
							"like":                 phrase,
							"min_term_freq":        1,
							"min_doc_freq":         1,
							"minimum_should_match": "50%",
						},
					},
					{
						"term": map[string]interface{}{
							"service": service,
						},
					},
				},
			},
		},
	}
}

func getLogsWithClusterId(logs []model.LogEntry) []model.LogEntry {
	newLogs := make([]model.LogEntry, len(logs))
	var clusterId string
	for _, log := range logs {
		if log.ClusterId != "" {
			clusterId = log.ClusterId
			break
		}
	}
	if clusterId == "" {
		clusterId = uuid.NewString()
	}
	for i, log := range logs {
		newLogs[i] = model.LogEntry{
			Id:        log.Id,
			Timestamp: log.Timestamp,
			Severity:  log.Severity,
			Message:   log.Message,
			Service:   log.Service,
			ClusterId: clusterId,
		}
	}
	return newLogs
}

func (lps *LogProcessorServiceImpl) ParseLogWithMessage(
	service string,
	log model.LogEntry,
	ctx context.Context,
) (model.LogEntry, error) {
	queryBody, err := json.Marshal(moreLikeThisQueryBuilder(service, log.Message))
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to marshal query body for elasticsearch query: %w", err)
	}
	queryCtx, queryCancel := context.WithTimeout(ctx, lpTimeOut)
	defer queryCancel()
	res, err := lps.ac.Search(queryCtx, string(queryBody), augurElasticsearch.LogIndexName, -1)
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to search for similar logs in Elasticsearch: %w", err)
	}
	totalLogs, err := augurElasticsearch.ConvertToLogDocuments(res)
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to convert search results to log documents: %w", err)
	}
	totalLogs = append(totalLogs, log)

	parsedLogs := getLogsWithClusterId(totalLogs)
	lps.logger.Info("Parsed logs", zap.Any("parsedLogs", parsedLogs))

	// last log is the new one so don't update it
	ids := make([]string, len(parsedLogs)-1)
	fieldList := make([]map[string]interface{}, len(parsedLogs)-1)
	for idx, log := range parsedLogs[:len(parsedLogs)-1] {
		ids[idx] = log.Id
		fieldList[idx] = map[string]interface{}{
			"cluster_id": log.ClusterId,
		}
	}
	updateCtx, updateCancel := context.WithTimeout(ctx, lpTimeOut)
	defer updateCancel()
	if len(fieldList) != 0 {
		err = lps.ac.BulkUpdate(updateCtx, ids, fieldList, augurElasticsearch.LogIndexName)
		if err != nil {
			return model.LogEntry{}, fmt.Errorf("failed to update similar logs in Elasticsearch: %w", err)
		}
	}

	newLogEntry := parsedLogs[len(parsedLogs)-1]

	/*
		DISABLE THIS FOR NOW: We want to bulk index logs instead of indexing one by one
		err = lps.ac.Index(newLogEntry, nil, augurElasticsearch.LogIndexName)
		if err != nil {
			return model.LogEntry{}, fmt.Errorf("failed to index new log in Elasticsearch: %w", err)
		}
	*/

	return newLogEntry, nil
}
