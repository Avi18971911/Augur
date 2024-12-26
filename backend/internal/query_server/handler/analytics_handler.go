package handler

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/query_server/inference/service"
	"go.uber.org/zap"
	"io"
	"net/http"
)

// ChainOfEventsHandler creates a handler for getting a sub-graph from a log or trace.
// @Summary Get a sub-graph from a log or trace.
// @Tags analytics
// @Accept json
// @Produce json
// @Param logOrSpanData body ChainOfEventsRequestDTO true "The log or span data to get the sub-graph from"
// @Success 200 {object} ChainOfEventsResponseDTO "Sub-graph from the log or trace"
// @Failure 500 {object} ErrorMessage "Internal server error"
// @Router /graph [get]
func ChainOfEventsHandler(
	ctx context.Context,
	s service.AnalyticsQueryService,
	logger *zap.Logger,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Info(
			"Login request received with URL and method",
			zap.String("URL Path", r.URL.Path),
			zap.String("Method", r.Method),
		)
		var req ChainOfEventsRequestDTO
		logger.Info("Decoding request body", zap.Any("Request", r.Body))
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

		err = validateRequest(req)
		if err != nil {
			logger.Error("Error encountered when validating request", zap.Error(err))
			HttpError(w, err.Error(), http.StatusBadRequest, logger)
			return
		}

		logOrSpanData, err := s.GetSpanOrLogData(ctx, req.Id)
		res, err := s.GetChainOfEvents(ctx, logOrSpanData)
		if err != nil {
			logger.Error("Error encountered when getting chain of events", zap.Error(err))
			HttpError(w, "Internal server error", http.StatusInternalServerError, logger)
			return
		}
		logger.Info("Encoding response", zap.Any("Response", res))
		resDTO := mapChainOfEventsResponseToDTO(res)
		err = json.NewEncoder(w).Encode(resDTO)
		if err != nil {
			logger.Error("Error encountered when encoding response", zap.Error(err))
			HttpError(w, "Internal server error", http.StatusInternalServerError, logger)
			return
		}
	}
}

func validateRequest(req ChainOfEventsRequestDTO) error {
	if req.Id == "" {
		return ErrNoId
	}
	return nil
}
