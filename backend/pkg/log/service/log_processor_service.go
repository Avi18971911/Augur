package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	"strings"
	"time"
)

type LogProcessorService interface {
	ParseLogWithMessage(service string, log model.LogEntry, ctx context.Context) (model.LogEntry, error)
}

type LogProcessorServiceImpl struct {
	es     *elasticsearch.Client
	logger *zap.Logger
}

func NewLogProcessorService(es *elasticsearch.Client, logger *zap.Logger) LogProcessorService {
	return &LogProcessorServiceImpl{
		es:     es,
		logger: logger,
	}
}

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

	esContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res, err := lps.es.Search(
		lps.es.Search.WithContext(esContext),
		lps.es.Search.WithIndex(augurElasticsearch.LogIndexName),
		lps.es.Search.WithBody(strings.NewReader(string(queryBody))),
		lps.es.Search.WithPretty(),
		lps.es.Search.WithSize(10),
	)
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to search for logs in elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return model.LogEntry{}, fmt.Errorf("failed to search for logs in elasticsearch: %s", res.String())
	}

	var totalLogs []model.LogEntry
	if err := json.NewDecoder(res.Body).Decode(&totalLogs); err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to decode response body: %w", err)
	}
	totalLogs = append(totalLogs, log)

	parsedLogs := convertLogMessagesToLCD(totalLogs)
	lps.logger.Info("Parsed logs", zap.Any("parsedLogs", parsedLogs))

	var buf bytes.Buffer
	// last log is the new one so don't update it
	for _, log := range parsedLogs[:len(parsedLogs)-1] {
		meta := map[string]interface{}{
			"update": map[string]interface{}{
				"_id": log.Id,
			},
		}
		metaJSON, _ := json.Marshal(meta)
		buf.Write(metaJSON)
		buf.WriteByte('\n')

		update := map[string]interface{}{
			"doc": map[string]interface{}{
				"message": log.Message,
			},
		}
		updateJSON, _ := json.Marshal(update)
		buf.Write(updateJSON)
		buf.WriteByte('\n')
	}

	insertRes, err := lps.es.Bulk(bytes.NewReader(buf.Bytes()), lps.es.Bulk.WithIndex(augurElasticsearch.LogIndexName))
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to update logs in Elasticsearch: %w", err)
	}
	defer insertRes.Body.Close()

	if insertRes.IsError() {
		return model.LogEntry{}, fmt.Errorf("bulk update error: %s", res.String())
	}

	buf = bytes.Buffer{}
	newLogEntry := parsedLogs[len(parsedLogs)-1]
	meta := map[string]interface{}{"index": map[string]interface{}{}}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("error marshaling meta to insert new log: %w", err)
	}
	buf.Write(metaJSON)
	buf.WriteByte('\n')

	newLogEntryJSON, err := json.Marshal(newLogEntry)
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("error marshaling new log to insert: %w", err)
	}
	buf.Write(newLogEntryJSON)
	buf.WriteByte('\n')

	newInsertRes, err := lps.es.Index(augurElasticsearch.LogIndexName, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return model.LogEntry{}, fmt.Errorf("failed to insert new log in Elasticsearch: %w", err)
	}
	defer newInsertRes.Body.Close()
	if newInsertRes.IsError() {
		return model.LogEntry{}, fmt.Errorf("new log insert error: %s", newInsertRes.String)
	}

	return newLogEntry, nil
}
