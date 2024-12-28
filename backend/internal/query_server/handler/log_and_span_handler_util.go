package handler

import (
	logModel "github.com/Avi18971911/Augur/internal/otel_server/log/model"
	spanModel "github.com/Avi18971911/Augur/internal/otel_server/trace/model"
	"github.com/Avi18971911/Augur/internal/query_server/service/log_and_span/model"
)

func convertLogAndSpanDataToErrorResult(input []model.LogAndSpan) ErrorResponseDTO {
	logAndSpanData := make([]LogAndSpanDTO, len(input))
	for i, logAndSpan := range input {
		logAndSpanData[i] = mapLogAndSpanToDTO(logAndSpan)
	}
	return ErrorResponseDTO{
		Errors: logAndSpanData,
	}
}

func mapLogAndSpanToDTO(input model.LogAndSpan) LogAndSpanDTO {
	var spanDTO *SpanDTO = nil
	var logDTO *LogDTO = nil
	if input.SpanDetails != nil {
		spanDTO = spanDetailsToSpanDTO(input.SpanDetails)
	} else {
		logDTO = logDetailsToLogDTO(input.LogDetails)
	}
	return LogAndSpanDTO{
		LogDTO:  logDTO,
		SpanDTO: spanDTO,
	}
}

func logDetailsToLogDTO(input *logModel.LogEntry) *LogDTO {
	return &LogDTO{
		Id:        input.Id,
		CreatedAt: input.CreatedAt,
		Timestamp: input.Timestamp,
		Severity:  string(input.Severity),
		Message:   input.Message,
		Service:   input.Service,
		TraceId:   input.TraceId,
		SpanId:    input.SpanId,
		ClusterId: input.ClusterId,
	}
}

func spanDetailsToSpanDTO(input *spanModel.Span) *SpanDTO {
	return &SpanDTO{
		Id:           input.Id,
		CreatedAt:    input.CreatedAt,
		SpanID:       input.SpanID,
		ParentSpanID: input.ParentSpanID,
		TraceID:      input.TraceID,
		Service:      input.Service,
		StartTime:    input.StartTime,
		EndTime:      input.EndTime,
		ActionName:   input.ActionName,
		SpanKind:     input.SpanKind,
		ClusterEvent: input.ClusterEvent,
		Attributes:   input.Attributes,
		Events:       eventsToSpanEventDTO(input.Events),
	}
}

func eventsToSpanEventDTO(input []spanModel.SpanEvent) []SpanEventDTO {
	var events []SpanEventDTO
	for _, event := range input {
		events = append(events, SpanEventDTO{
			Name:       event.Name,
			Attributes: event.Attributes,
			Timestamp:  event.Timestamp,
		})
	}
	return events
}
