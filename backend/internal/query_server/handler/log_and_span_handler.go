package handler

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/query_server/service/log_and_span"
	"go.uber.org/zap"
	"io"
	"net/http"
)

// LogAndSpanHandler creates a handler for getting log and span data using search parameters.
// @Summary Get spans or logs with their associated information.
// @Tags analytics
// @Accept json
// @Produce json
// @Param searchParams body log_and_span.SearchParams true "The optional search parameters"
// @Success 200 {object} DataResponseDTO "List of logs and spans with their corresponding details."
// @Failure 500 {object} ErrorMessage "Internal server error"
// @Router /data [post]
func LogAndSpanHandler(
	ctx context.Context,
	ls log_and_span.LogAndSpanQueryService,
	logger *zap.Logger,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req log_and_span.SearchParams
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

		logOrSpanData, err := ls.GetLogsAndSpans(ctx, req)
		if err != nil {
			logger.Error("Error encountered when getting chain of events", zap.Error(err))
			HttpError(w, "Internal server error", http.StatusInternalServerError, logger)
			return
		}

		dataResult := convertLogAndSpanDataToDTO(logOrSpanData)
		err = json.NewEncoder(w).Encode(dataResult)
		if err != nil {
			logger.Error("Error encountered when encoding response", zap.Error(err))
			HttpError(w, "Internal server error", http.StatusInternalServerError, logger)
			return
		}
	}
}
