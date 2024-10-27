package server

import (
	"context"
	"encoding/hex"
	"github.com/Avi18971911/Augur/pkg/trace"
	"github.com/Avi18971911/Augur/pkg/trace/model"
	protoTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"time"
)

type TraceServiceServerImpl struct {
	protoTrace.UnimplementedTraceServiceServer
	logger           *zap.Logger
	writeBehindCache trace.WriteBehindCache
}

func NewTraceServiceServerImpl(
	logger *zap.Logger,
	cache trace.WriteBehindCache,
) TraceServiceServerImpl {
	logger.Info("Creating new TraceServiceServerImpl")
	return TraceServiceServerImpl{
		logger:           logger,
		writeBehindCache: cache,
	}
}

func (tss TraceServiceServerImpl) Export(
	ctx context.Context,
	req *protoTrace.ExportTraceServiceRequest,
) (*protoTrace.ExportTraceServiceResponse, error) {
	for _, resourceSpan := range req.ResourceSpans {
		serviceName := getServiceName(resourceSpan)
		tss.logger.Info("Service Name", zap.String("service_name", serviceName))
		if serviceName == "Never Assigned" {
			tss.logger.Warn("Service name not found in resource span")
		}

		typedSpans := getTypedSpans(resourceSpan, serviceName)
		// we need to group because the spans underneath the same resource span may not have the same trace id
		groupedSpans := groupTypedSpansByTraceID(typedSpans)
		for traceID, spans := range groupedSpans {
			err := tss.writeBehindCache.Put(traceID, spans)
			if err != nil {
				tss.logger.Error("Failed to put span in cache", zap.Error(err))
			}
		}
	}

	return &protoTrace.ExportTraceServiceResponse{}, nil
}

func getServiceName(resourceSpan *v1.ResourceSpans) string {
	var serviceName = "Never Assigned"
	for _, attr := range resourceSpan.Resource.Attributes {
		if attr.Key == "service.name" {
			serviceName = attr.Value.GetStringValue()
		}
	}
	return serviceName
}

func getTypedSpans(resourceSpan *v1.ResourceSpans, serviceName string) []model.Span {
	var typedSpans []model.Span
	for _, libSpan := range resourceSpan.ScopeSpans {
		for _, span := range libSpan.Spans {
			typedSpans = append(typedSpans, getTypedSpan(span, serviceName))
		}
	}
	return typedSpans
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

func groupTypedSpansByTraceID(spans []model.Span) map[string][]model.Span {
	groupedSpans := make(map[string][]model.Span)
	for _, span := range spans {
		groupedSpans[span.TraceID] = append(groupedSpans[span.TraceID], span)
	}
	return groupedSpans
}
