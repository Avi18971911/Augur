package handler

import (
	"context"
	"go.uber.org/zap"
	"net/http"
)

func ErrorHandler(
	ctx context.Context,
	logger *zap.Logger,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Info(
			"Login request received with URL and method",
			zap.String("URL Path", r.URL.Path),
			zap.String("Method", r.Method),
		)
	}
}
