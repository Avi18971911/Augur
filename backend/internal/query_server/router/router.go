package router

import (
	"context"
	"github.com/Avi18971911/Augur/internal/query_server/handler"
	inferenceService "github.com/Avi18971911/Augur/internal/query_server/service/inference"
	"github.com/Avi18971911/Augur/internal/query_server/service/log_and_span"
	"go.uber.org/zap"
	"net/http"
)
import "github.com/gorilla/mux"

func CreateRouter(
	ctx context.Context,
	inferenceQueryService inferenceService.InferenceQueryService,
	logAndSpanQueryService log_and_span.LogAndSpanQueryService,
	logger *zap.Logger,
) http.Handler {
	r := mux.NewRouter()

	r.Handle(
		"/graph", handler.ChainOfEventsHandler(
			ctx,
			inferenceQueryService,
			logger,
		),
	).Methods("POST")

	r.Handle(
		"/data", handler.LogAndSpanHandler(
			ctx,
			logAndSpanQueryService,
			logger,
		),
	).Methods("POST")

	return r
}
