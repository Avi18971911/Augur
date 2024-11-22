package main

import (
	count "github.com/Avi18971911/Augur/pkg/count/service"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"
)

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		logger.Error("Failed to create elasticsearch client", zap.Error(err))
	}

	ac := augurElasticsearch.NewAugurClientImpl(es, augurElasticsearch.Wait)
	countService := count.NewCountService(ac, logger)

}
