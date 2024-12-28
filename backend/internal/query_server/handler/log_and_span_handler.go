package handler

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/query_server/service/log_and_span"
	"go.uber.org/zap"
	"io"
	"net/http"
)

// ErrorHandler creates a handler for getting errors using search parameters.
// @Summary Get spans or logs detailing errors.
// @Tags analytics
// @Accept json
// @Produce json
// @Param error body log_and_span.ErrorSearchParams true "The optional search parameters"
// @Success 200 {object} ErrorResponseDTO "List of logs and spans detailing errors"
// @Failure 500 {object} ErrorMessage "Internal server error"
// @Router /error [get]
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

		errorResult := convertLogAndSpanDataToErrorResult(logOrSpanData)
		err = json.NewEncoder(w).Encode(errorResult)
		if err != nil {
			logger.Error("Error encountered when encoding response", zap.Error(err))
			HttpError(w, "Internal server error", http.StatusInternalServerError, logger)
			return
		}
	}
}
