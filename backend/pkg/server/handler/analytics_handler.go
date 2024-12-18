package handler

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Avi18971911/Augur/pkg/inference/model"
	analyticsService "github.com/Avi18971911/Augur/pkg/inference/service"
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
// @Success 200 {object} ChainOfEventsResponseDTO "Sub-graph from the log or trace"
// @Failure 500 {object} ErrorMessage "Internal server error"
// @Router /graph [get]
func ChainOfEventsHandler(
	ctx context.Context,
	s analyticsService.AnalyticsQueryService,
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

		logger.Info("Request body decoded successfully", zap.Any("Request", req))
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

func mapChainOfEventsResponseToDTO(mleSequence map[string]*model.ClusterNode) ChainOfEventsResponseDTO {
	graph := make(map[string]ChainOfEventsNodeDTO)
	for _, node := range mleSequence {
		successors := make([]string, len(node.Successors))
		for i, successor := range node.Successors {
			successors[i] = successor.LogOrSpanData.Id
		}
		predecessors := make([]string, len(node.Predecessors))
		for i, predecessor := range node.Predecessors {
			predecessors[i] = predecessor.LogOrSpanData.Id
		}
		var spanDTO = SpanDTO{}
		var logDTO = LogDTO{}
		if node.LogOrSpanData.SpanDetails != nil {
			spanDTO = toSpanDTO(spanDTO, node)
		} else {
			logDTO = toLogDTO(logDTO, node)
		}

		graph[node.LogOrSpanData.Id] = ChainOfEventsNodeDTO{
			Id:           node.LogOrSpanData.Id,
			ClusterId:    node.ClusterId,
			Successors:   successors,
			Predecessors: predecessors,
			SpanDTO:      &spanDTO,
			LogDTO:       &logDTO,
		}
	}
	return ChainOfEventsResponseDTO{
		Graph: graph,
	}
}

func toSpanDTO(spanDTO SpanDTO, node *model.ClusterNode) SpanDTO {
	spanDTO = SpanDTO{
		Id:           node.LogOrSpanData.SpanDetails.Id,
		CreatedAt:    node.LogOrSpanData.SpanDetails.CreatedAt,
		SpanID:       node.LogOrSpanData.SpanDetails.SpanID,
		ParentSpanID: node.LogOrSpanData.SpanDetails.ParentSpanID,
		TraceID:      node.LogOrSpanData.SpanDetails.TraceID,
		ServiceName:  node.LogOrSpanData.SpanDetails.ServiceName,
		StartTime:    node.LogOrSpanData.SpanDetails.StartTime,
		EndTime:      node.LogOrSpanData.SpanDetails.EndTime,
		ActionName:   node.LogOrSpanData.SpanDetails.ActionName,
		SpanKind:     node.LogOrSpanData.SpanDetails.SpanKind,
		ClusterEvent: node.LogOrSpanData.SpanDetails.ClusterEvent,
		ClusterId:    node.LogOrSpanData.SpanDetails.ClusterId,
		Attributes:   node.LogOrSpanData.SpanDetails.Attributes,
		Events:       mapModelToSpanEventDTO(node.LogOrSpanData.SpanDetails.Events),
	}
	return spanDTO
}

func toLogDTO(logDTO LogDTO, node *model.ClusterNode) LogDTO {
	logDTO = LogDTO{
		Id:        node.LogOrSpanData.LogDetails.Id,
		CreatedAt: node.LogOrSpanData.LogDetails.CreatedAt,
		Timestamp: node.LogOrSpanData.LogDetails.Timestamp,
		Severity:  string(node.LogOrSpanData.LogDetails.Severity),
		Message:   node.LogOrSpanData.LogDetails.Message,
		Service:   node.LogOrSpanData.LogDetails.Service,
		TraceId:   node.LogOrSpanData.LogDetails.TraceId,
		SpanId:    node.LogOrSpanData.LogDetails.SpanId,
		ClusterId: node.LogOrSpanData.LogDetails.ClusterId,
	}
	return logDTO
}

func mapModelToSpanEventDTO(events []spanModel.SpanEvent) []SpanEventDTO {
	var dto []SpanEventDTO
	for _, event := range events {
		dto = append(dto, SpanEventDTO{
			Name:       event.Name,
			Attributes: event.Attributes,
			Timestamp:  event.Timestamp,
		})
	}
	return dto
}

var (
	ErrNoLogOrSpanData = errors.New("no log or span data provided")
	ErrNoId            = errors.New("no ID provided")
	ErrNoClusterId     = errors.New("no cluster ID provided")
)
