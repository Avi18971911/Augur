package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/data_processor/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	_ "google.golang.org/grpc/encoding/gzip"
	"time"
)

func deleteAllDocuments(es *elasticsearch.Client) error {
	indexes := []string{bootstrapper.CountIndexName}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	queryJSON, _ := json.Marshal(query)
	res, err := es.DeleteByQuery(indexes, bytes.NewReader(queryJSON), es.DeleteByQuery.WithRefresh(true))
	if err != nil {
		return fmt.Errorf("failed to delete documents by query: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to delete documents in index %s", res.String())
	}
	return nil
}

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		logger.Error("Failed to create elasticsearch client", zap.Error(err))
	}

	err = deleteAllDocuments(es)
	ac := client.NewAugurClientImpl(es, client.Wait)
	cs := countService.NewCountService(ac, logger)
	buckets := []countService.Bucket{100}
	indices := []string{bootstrapper.SpanIndexName}

	start := time.Now()
	dataProcessor := service.NewDataProcessorService(ac, cs, logger)
	dataProcessor.ProcessData(context.Background(), buckets, indices)
	end := time.Now()
	logger.Info("Successfully counted and updated occurrences", zap.Duration("duration", end.Sub(start)))
}
