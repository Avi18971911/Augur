package handler

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/query_server/service/log_and_span"
	"go.uber.org/zap"
	"io"
	"net/http"
)

func ErrorHandler(
	ctx context.Context,
	ls log_and_span.LogAndSpanQueryService,
	logger *zap.Logger,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req log_and_span.ErrorSearchParams
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			logger.Error("Error encountered when decoding request body", zap.Error(err))
			HttpError(w, "Invalid request payload", http.StatusBadRequest, logger)
			return
		}

		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				logger.Error("Error encountered when closing request body", zap.Error(err))
			}
		}(r.Body)

		logOrSpanData, err := ls.GetAllErrors(ctx, req)
		if err != nil {
			logger.Error("Error encountered when getting chain of events", zap.Error(err))
			HttpError(w, "Internal server error", http.StatusInternalServerError, logger)
			return
		}

		err = json.NewEncoder(w).Encode(logOrSpanData)
		if err != nil {
			logger.Error("Error encountered when encoding response", zap.Error(err))
			HttpError(w, "Internal server error", http.StatusInternalServerError, logger)
			return
		}
	}
}
