package main

import (
	"context"
	"encoding/json"
	count "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch/db_model"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"
)

func getAllDocumentsQuery() map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
}

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		logger.Error("Failed to create elasticsearch client", zap.Error(err))
	}

	ac := client.NewAugurClientImpl(es, client.Wait)
	countService := count.NewCountService(ac, logger)
	queryMap := getAllDocumentsQuery()
	queryBody, err := json.Marshal(queryMap)
	if err != nil {
		logger.Panic("Failed to marshal query map", zap.Error(err))
	}
	allLogs, err := ac.Search(context.Background(), string(queryBody), []string{augurElasticsearch.LogIndexName}, nil)

}
