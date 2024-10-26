package otel

import (
	"context"
	"encoding/hex"
	"github.com/Avi18971911/Augur/pkg/otel/model"
	otlp "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"time"
)

type TraceServiceServerImpl struct {
	otlp.UnimplementedTraceServiceServer
	logger           *zap.Logger
	writeBehindCache WriteBehindCache
}

func NewTraceServiceServerImpl(
	logger *zap.Logger,
	cache WriteBehindCache,
) TraceServiceServerImpl {
	return TraceServiceServerImpl{
		logger:           logger,
		writeBehindCache: cache,
	}
}

func (tss TraceServiceServerImpl) Export(
	ctx context.Context,
	req *otlp.ExportTraceServiceRequest,
) (*otlp.ExportTraceServiceResponse, error) {
	for _, resourceSpan := range req.ResourceSpans {
		var serviceName = "Never Assigned"
		for _, attr := range resourceSpan.Resource.Attributes {
			if attr.Key == "service.name" {
				serviceName = attr.Value.GetStringValue()
			}
		}

		for _, libSpan := range resourceSpan.ScopeSpans {
			for _, span := range libSpan.Spans {
				typedSpan := getTypedSpan(span, serviceName)
				err := tss.writeBehindCache.Put(typedSpan.SpanID, typedSpan)
				if err != nil {
					tss.logger.Error("Failed to put span in cache", zap.Error(err))
				}
			}
		}
	}

	return &otlp.ExportTraceServiceResponse{}, nil
}

func getTypedSpan(span *v1.Span, serviceName string) model.Span {
	startTime := time.Unix(0, int64(span.StartTimeUnixNano))
	endTime := time.Unix(0, int64(span.EndTimeUnixNano))
	spanId := hex.EncodeToString(span.SpanId)
	parentSpanId := hex.EncodeToString(span.ParentSpanId)
	traceId := hex.EncodeToString(span.TraceId)
	attributes := getAttributes(span)
	events := getEvents(span)

	return model.Span{
		SpanID:       spanId,
		ParentSpanID: parentSpanId,
		TraceID:      traceId,
		ServiceName:  serviceName,
		StartTime:    startTime,
		EndTime:      endTime,
		ActionName:   span.Name,
		Attributes:   attributes,
		Events:       events,
	}
}

func getEvents(span *v1.Span) []model.SpanEvent {
	events := make([]model.SpanEvent, len(span.Events))
	for i, event := range span.Events {
		eventAttributes := make(map[string]string)
		for _, attribute := range event.Attributes {
			eventAttributes[attribute.Key] = attribute.Value.GetStringValue()
		}
		events[i] = model.SpanEvent{
			Name:       event.Name,
			Attributes: eventAttributes,
			Timestamp:  time.Unix(0, int64(event.TimeUnixNano)),
		}
	}
	return events
}

func getAttributes(span *v1.Span) map[string]string {
	attributes := make(map[string]string)
	for _, attribute := range span.Attributes {
		attributes[attribute.Key] = attribute.Value.GetStringValue()
	}
	return attributes
}
