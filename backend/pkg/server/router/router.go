package router

import (
	"context"
	analyticsService "github.com/Avi18971911/Augur/pkg/inference/service"
	"github.com/Avi18971911/Augur/pkg/server/handler"
	"go.uber.org/zap"
	"net/http"
)
import "github.com/gorilla/mux"

func CreateRouter(
	ctx context.Context,
	analyticsService analyticsService.AnalyticsService,
	logger *zap.Logger,
) http.Handler {
	r := mux.NewRouter()

	r.Handle(
		"/graph", handler.ChainOfEventsHandler(
			ctx,
			analyticsService,
			logger,
		),
	).Methods("GET")

	return r
}
