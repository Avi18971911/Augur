package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/elastic/go-elasticsearch/v8"
	"strings"
	"time"
)

type LogProcessorRepository interface {
	// FindLogsOfServiceWithPhrase FindLogsOfService Note this function should only ever return a subset of the total logs
	FindLogsOfServiceWithPhrase(service string, phrase string) ([]model.LogEntry, error)
}

type LogProcessorService struct {
	es *elasticsearch.Client
}

func NewLogProcessorService(es *elasticsearch.Client) *LogProcessorService {
	return &LogProcessorService{
		es: es,
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
							"minimum_should_match": "30%",
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

func (lps *LogProcessorService) FindLogsOfServiceWithPhrase(
	service string,
	phrase string,
	ctx context.Context,
) ([]model.LogEntry, error) {
	queryBody, err := json.Marshal(moreLikeThisQueryBuilder(service, phrase))
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query body for elasticsearch query: %w", err)
	}

	esContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	res, err := lps.es.Search(
		lps.es.Search.WithContext(esContext),
		lps.es.Search.WithIndex("logs"), // Replace with your index name
		lps.es.Search.WithBody(strings.NewReader(string(queryBody))),
		lps.es.Search.WithPretty(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to search for logs in elasticsearch: %w", err)
	}
	defer res.Body.Close()
	
	return nil, nil
}
