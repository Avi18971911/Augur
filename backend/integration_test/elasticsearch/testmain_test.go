package elasticsearch

import (
	"context"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	"log"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	ctx := context.Background()
	elasticSearchURI, cleanup, err := startElasticSearchContainer(ctx, logger)
	defer cleanup()
	if err != nil {
		logger.Fatal("Failed to start container", zap.Error(err))
	}
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{elasticSearchURI},
	})
	if err != nil {
		logger.Fatal("Failed to create elasticsearch client", zap.Error(err))
	}
	code := m.Run()
	os.Exit(code)
}
