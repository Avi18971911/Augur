package main

import (
	"context"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	analyticsService "github.com/Avi18971911/Augur/pkg/inference/service"
	"github.com/Avi18971911/Augur/pkg/server/router"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	"net/http"
)

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		logger.Error("Failed to create elasticsearch client", zap.Error(err))
	}

	bs := bootstrapper.NewBootstrapper(es, logger)
	err = bs.BootstrapElasticsearch()
	if err != nil {
		logger.Error("Failed to bootstrap elasticsearch", zap.Error(err))
	}

	ac := client.NewAugurClientImpl(es, client.Wait)
	as := analyticsService.CreateNewAnalyticsQueryService(ac, logger)

	r := router.CreateRouter(context.Background(), as, logger)
	logger.Info("Starting server at :8081")
	if err := http.ListenAndServe(":8081", r); err != nil {
		logger.Fatal("Failed to serve: %v", zap.Error(err))
	}
}