package service

import (
	"context"
	"encoding/json"
	"fmt"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"go.uber.org/zap"
	"strings"
)

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

func findLowestCommonDenominator(logs []model.LogEntry) (string, error) {
	if len(logs) <= 1 {
		return "", fmt.Errorf("not enough logs to find lowest common denominator")
	}
	firstLog := logs[0]
	var lcd = strings.Fields(firstLog.Message)
	for i := 1; i < len(logs); i++ {
		currentLog := logs[i]
		for j := 0; j < len(lcd); j++ {
			lcdWord := lcd[j]
			if !strings.Contains(currentLog.Message, lcdWord) {
				lcd = append(lcd[:j], lcd[j+1:]...)
				j--
			}
		}
	}
	return strings.Join(lcd, " "), nil
}

func convertLogMessagesToLCD(logs []model.LogEntry) []model.LogEntry {
	lcd, err := findLowestCommonDenominator(logs)
	if err != nil {
		return logs
	}
	newLogs := make([]model.LogEntry, len(logs))
	for i, log := range logs {
		newLogs[i] = model.LogEntry{
			Id:        log.Id,
			Timestamp: log.Timestamp,
			Severity:  log.Severity,
			Message:   lcd,
			Service:   log.Service,
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

	res, err := lps.ac.Search(string(queryBody), augurElasticsearch.LogIndexName, ctx)
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to search for similar logs in Elasticsearch: %w", err)
	}
	totalLogs := augurElasticsearch.ToTypedSlice[model.LogEntry](res)

	parsedLogs := convertLogMessagesToLCD(totalLogs)
	lps.logger.Info("Parsed logs", zap.Any("parsedLogs", parsedLogs))

	// last log is the new one so don't update it
	fieldList := make([]map[string]interface{}, len(parsedLogs)-1)
	for _, log := range parsedLogs[:len(parsedLogs)-1] {
		fieldList = append(fieldList, map[string]interface{}{
			"message": log.Message,
		})
	}
	err = lps.ac.Update(log.Id, fieldList)
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to update similar logs in Elasticsearch: %w", err)
	}

	newLogEntry := parsedLogs[len(parsedLogs)-1]
	err = lps.ac.Index(newLogEntry, nil, augurElasticsearch.LogIndexName)
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to index new log in Elasticsearch: %w", err)
	}

	return newLogEntry, nil
}
