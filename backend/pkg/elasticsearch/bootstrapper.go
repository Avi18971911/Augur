package elasticsearch

import (
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	"strings"
)

type Bootstrapper struct {
	esClient *elasticsearch.Client
	logger   *zap.Logger
}

func NewBootstrapper(esClient *elasticsearch.Client, logger *zap.Logger) *Bootstrapper {
	return &Bootstrapper{
		esClient: esClient,
		logger:   logger,
	}
}

func (bs *Bootstrapper) BootstrapElasticsearch() error {
	if err := bs.createIndex(LogIndexName, logIndex); err != nil {
		return fmt.Errorf("error creating index log template: %w", err)
	}

	if err := bs.createIndex(SpanIndexName, spanIndex); err != nil {
		return fmt.Errorf("error creating index trace template: %w", err)

	}

	return nil
}

func (bs *Bootstrapper) createIndex(
	indexName string,
	index map[string]interface{},
) error {
	body, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("error marshaling index input during bootstrap: %w", err)
	}

	res, err := bs.esClient.Indices.Create(
		indexName,
		bs.esClient.Indices.Create.WithBody(strings.NewReader(string(body))),
	)
	if err != nil {
		return fmt.Errorf("error creating index during bootstrap %s: %w", indexName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error response for index %s: %s", indexName, res.String())
	}

	bs.logger.Info("Successfully created index", zap.String("template_name", indexName))
	return nil
}
