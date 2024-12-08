package bootstrapper

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
)

const retries = 30
const waitTime = 5

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

	if err := bs.waitForElasticsearch(retries, waitTime*time.Second); err != nil {
		return fmt.Errorf("failed to connect to Elasticsearch: %w", err)
	}

	if err := bs.createPipeline(tokenLengthPipelineName, tokenLengthPipeline); err != nil {
		return fmt.Errorf("error creating pipeline log template: %w", err)
	}

	if err := bs.createIndex(LogIndexName, logIndex); err != nil {
		return fmt.Errorf("error creating index log template: %w", err)
	}

	if err := bs.putSettings(LogIndexName, tokenLengthSettings); err != nil {
		return fmt.Errorf("error putting settings for log index: %w", err)
	}

	if err := bs.createIndex(SpanIndexName, spanIndex); err != nil {
		return fmt.Errorf("error creating index trace template: %w", err)
	}

	if err := bs.createIndex(CountIndexName, countIndex); err != nil {
		return fmt.Errorf("error creating index trace template: %w", err)
	}

	return nil
}

func (bs *Bootstrapper) waitForElasticsearch(maxRetries int, delay time.Duration) error {
	for i := 0; i < maxRetries; i++ {
		res, err := bs.esClient.Info()
		if err == nil && res.StatusCode == 200 {
			bs.logger.Info("Elasticsearch is available")
			return nil
		}
		bs.logger.Warn(fmt.Sprintf("Elasticsearch not available (attempt %d/%d), retrying...", i+1, maxRetries))

		time.Sleep(delay)
	}

	return fmt.Errorf("Elasticsearch is not available after %d attempts", maxRetries)
}

func (bs *Bootstrapper) createIndex(indexName string, index map[string]interface{}) error {
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

func (bs *Bootstrapper) createPipeline(pipelineName string, pipeline map[string]interface{}) error {
	body, err := json.Marshal(pipeline)
	if err != nil {
		return fmt.Errorf("error marshaling pipeline input during bootstrap: %w", err)
	}

	res, err := bs.esClient.Ingest.PutPipeline(
		pipelineName,
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("error creating pipeline during bootstrap %s: %w", pipelineName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error response for pipeline %s: %s", pipelineName, res.String())
	}

	bs.logger.Info("Successfully created pipeline", zap.String("pipeline_name", pipelineName))
	return nil
}

func (bs *Bootstrapper) putSettings(indexName string, settings map[string]interface{}) error {
	body, err := json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("error marshaling settings input during bootstrap: %w", err)
	}

	res, err := bs.esClient.Indices.PutSettings(
		bytes.NewReader(body),
		bs.esClient.Indices.PutSettings.WithIndex(indexName),
	)

	if err != nil {
		return fmt.Errorf("error putting settings during bootstrap %s: %w", indexName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error response for settings %s: %s", indexName, res.String())
	}

	bs.logger.Info("Successfully put settings", zap.String("index_name", indexName))
	return nil
}
