package service

import (
	"context"
	"encoding/json"
	"fmt"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"time"
)

type clusterIdField map[string]interface{}

const lpTimeOut = 2 * time.Second

type LogClusterService interface {
	ClusterLog(ctx context.Context, service string, log model.LogEntry) ([]string, []clusterIdField, error)
}

type LogClusterServiceImpl struct {
	ac     client.AugurClient
	logger *zap.Logger
}

func NewLogClusterService(ac client.AugurClient, logger *zap.Logger) LogClusterService {
	return &LogClusterServiceImpl{
		ac:     ac,
		logger: logger,
	}
}

func getLogsWithClusterId(logs []model.LogEntry) []model.LogEntry {
	newLogs := make([]model.LogEntry, len(logs))
	var clusterId string
	for _, log := range logs {
		if log.ClusterId != "" && log.ClusterId != "NOT_ASSIGNED" {
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
			CreatedAt: log.CreatedAt,
			Timestamp: log.Timestamp,
			Severity:  log.Severity,
			Message:   log.Message,
			Service:   log.Service,
			ClusterId: clusterId,
		}
	}
	return newLogs
}

func (lps *LogClusterServiceImpl) ClusterLog(
	ctx context.Context,
	service string,
	log model.LogEntry,
) ([]string, []clusterIdField, error) {
	queryBody, err := json.Marshal(moreLikeThisQueryBuilder(service, log.Message))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal query body for elasticsearch query: %w", err)
	}
	queryCtx, queryCancel := context.WithTimeout(ctx, lpTimeOut)
	defer queryCancel()
	res, err := lps.ac.Search(queryCtx, string(queryBody), []string{augurElasticsearch.LogIndexName}, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to search for similar logs in Elasticsearch: %w", err)
	}
	totalLogs, err := ConvertToLogDocuments(res)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert search results to log documents: %w", err)
	}
	totalLogs = append(totalLogs, log)

	parsedLogs := getLogsWithClusterId(totalLogs)

	ids := make([]string, len(parsedLogs))
	fieldList := make([]clusterIdField, len(parsedLogs))
	for idx, log := range parsedLogs {
		ids[idx] = log.Id
		fieldList[idx] = map[string]interface{}{
			"cluster_id": log.ClusterId,
		}
	}

	return ids, fieldList, nil
}

func ConvertToLogDocuments(data []map[string]interface{}) ([]model.LogEntry, error) {
	var docs []model.LogEntry

	for _, item := range data {
		doc := model.LogEntry{}

		timestamp, ok := item["timestamp"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert timestamp to string %v", item["timestamp"])
		}

		timestampParsed, err := client.NormalizeTimestampToNanoseconds(timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to convert timestamp '%s' to time.Time: %v", timestamp, err)
		}

		doc.Timestamp = timestampParsed

		createdAt, ok := item["created_at"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert created_at to string")
		}
		createdAtParsed, err := client.NormalizeTimestampToNanoseconds(createdAt)
		if err != nil {
			return nil, fmt.Errorf("failed to convert created_at '%s' to time.Time: %v", createdAt, err)
		}
		doc.CreatedAt = createdAtParsed

		severity, ok := item["severity"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert severity to string")
		}

		doc.Severity = model.Level(severity)

		message, ok := item["message"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert message to string")
		}
		doc.Message = message

		service, ok := item["service"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert service to string")
		}
		doc.Service = service

		clusterId, ok := item["cluster_id"].(string)
		if ok {
			doc.ClusterId = clusterId
		}

		doc.Id = item["_id"].(string)
		docs = append(docs, doc)
	}

	return docs, nil
}
