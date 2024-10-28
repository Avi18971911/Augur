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
	if err := bs.createTemplate("log_index", logIndex); err != nil {
		return fmt.Errorf("error creating index log template: %w", err)
	}

	if err := bs.createTemplate("span_index", spanIndex); err != nil {
		return fmt.Errorf("error creating index trace template: %w", err)

	}

	return nil
}

func (bs *Bootstrapper) createTemplate(
	templateName string,
	template map[string]interface{},
) error {
	body, err := json.Marshal(template)
	if err != nil {
		return fmt.Errorf("error marshaling index template during bootstrap: %w", err)
	}

	res, err := bs.esClient.Indices.Create(
		templateName,
		bs.esClient.Indices.Create.WithBody(strings.NewReader(string(body))),
	)
	if err != nil {
		return fmt.Errorf("error creating index template during bootstrap %s: %w", templateName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("error response for index template %s: %s", templateName, res.String())
	}

	bs.logger.Info("Successfully created index template", zap.String("template_name", templateName))
	return nil
}
