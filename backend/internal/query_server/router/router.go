package router

import (
	"context"
	"github.com/Avi18971911/Augur/internal/query_server/handler"
	inferenceService "github.com/Avi18971911/Augur/internal/query_server/service/inference"
	"go.uber.org/zap"
	"net/http"
)
import "github.com/gorilla/mux"

func CreateRouter(
	ctx context.Context,
	analyticsQueryService inferenceService.AnalyticsQueryService,
	logger *zap.Logger,
) http.Handler {
	r := mux.NewRouter()

	r.Handle(
		"/graph", handler.ChainOfEventsHandler(
			ctx,
			analyticsQueryService,
			logger,
		),
	).Methods("GET")

	return r
}
