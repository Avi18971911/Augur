package handler

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/pkg/inference/model"
	analyticsService "github.com/Avi18971911/Augur/pkg/inference/service"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
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
// @Success 200 {object} dto.ChainOfEventsResponseDTO "Sub-graph from the log or trace"
// @Failure 500 {object} utils.ErrorMessage "Internal server error"
// @Router /graph [get]
func ChainOfEventsHandler(
	ctx context.Context,
	s analyticsService.AnalyticsService,
	logger *zap.Logger,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Info(
			"Login request received with URL %s and method %s",
			zap.String("URL Path", r.URL.Path),
			zap.String("Method", r.Method),
		)
		var req ChainOfEventsRequestDTO
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			logger.Error("Error encountered when decoding request body %v", zap.Error(err))
			HttpError(w, "Invalid request payload", http.StatusBadRequest, logger)
			return
		}

		defer func(Body io.ReadCloser) {
			err := Body.Close()
			if err != nil {
				logger.Error("Error encountered when closing request body %v", zap.Error(err))
			}
		}(r.Body)

		analyticsRequest := mapChainOfEventsRequestDTOToModel(req)
		res, err := s.GetChainOfEvents(ctx, analyticsRequest)
	}
}

func mapChainOfEventsRequestDTOToModel(dto ChainOfEventsRequestDTO) model.LogOrSpanData {
	return model.LogOrSpanData{
		Id:        dto.Id,
		ClusterId: dto.ClusterId,
		SpanDetails: &spanModel.Span{
			Id:           dto.SpanDetails.Id,
			CreatedAt:    dto.SpanDetails.CreatedAt,
			SpanID:       dto.SpanDetails.SpanID,
			ParentSpanID: dto.SpanDetails.ParentSpanID,
			TraceID:      dto.SpanDetails.TraceID,
			ServiceName:  dto.SpanDetails.ServiceName,
			StartTime:    dto.SpanDetails.StartTime,
			EndTime:      dto.SpanDetails.EndTime,
			ActionName:   dto.SpanDetails.ActionName,
			SpanKind:     dto.SpanDetails.SpanKind,
			ClusterEvent: dto.SpanDetails.ClusterEvent,
			ClusterId:    dto.SpanDetails.ClusterId,
			Attributes:   dto.SpanDetails.Attributes,
			Events:       mapSpanEventDTOToModel(dto.SpanDetails.Events),
		},
		LogDetails: &logModel.LogEntry{
			Id:        dto.LogDetails.Id,
			CreatedAt: dto.LogDetails.CreatedAt,
			Timestamp: dto.LogDetails.Timestamp,
			Severity:  mapSeverityToLevel(dto.LogDetails.Severity),
			Message:   dto.LogDetails.Message,
			Service:   dto.LogDetails.Service,
			TraceId:   dto.LogDetails.TraceId,
			SpanId:    dto.LogDetails.SpanId,
			ClusterId: dto.LogDetails.ClusterId,
		},
	}
}

func mapSpanEventDTOToModel(dto []SpanEventDTO) []spanModel.SpanEvent {
	var events []spanModel.SpanEvent
	for _, dto := range dto {
		events = append(events, spanModel.SpanEvent{
			Name:       dto.Name,
			Attributes: dto.Attributes,
			Timestamp:  dto.Timestamp,
		})
	}
	return events
}

func mapSeverityToLevel(severity string) logModel.Level {
	switch severity {
	case "info":
		return logModel.InfoLevel
	case "error":
		return logModel.ErrorLevel
	case "debug":
		return logModel.DebugLevel
	case "warn":
		return logModel.WarnLevel
	default:
		return logModel.InfoLevel
	}
}
