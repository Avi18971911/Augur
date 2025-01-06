package main

import (
	"context"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/query_server/router"
	"github.com/Avi18971911/Augur/internal/query_server/service/inference"
	"github.com/Avi18971911/Augur/internal/query_server/service/log_and_span"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
	"net/http"
)

// @title Augur API
// @version 1.0
// @description This is a monitoring and analytics tool for distributed systems.
// termsOfService: http://swagger.io/terms/
// contact:
//   name: API Support
//   url: http://www.swagger.io/support
//   email: support@swagger.io

// license:
//   name: Apache 2.0
//   url: http://www.apache.org/licenses/LICENSE-2.0.html

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
	as := inference.NewInferenceQueryService(ac, logger)
	errorQueryService := log_and_span.NewLogAndSpanService(ac, logger)

	r := router.CreateRouter(context.Background(), as, errorQueryService, logger)
	logger.Info("Starting query server at :8081")
	if err := http.ListenAndServe(":8081", r); err != nil {
		logger.Fatal("Failed to serve: %v", zap.Error(err))
	}
}
