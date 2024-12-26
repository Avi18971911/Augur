package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/write_buffer"
	"github.com/Avi18971911/Augur/internal/otel_server/trace/model"
	clusterService "github.com/Avi18971911/Augur/internal/pipeline/cluster/service"
	protoTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"time"
)

type TraceServiceServerImpl struct {
	protoTrace.UnimplementedTraceServiceServer
	writeBuffer write_buffer.DatabaseWriteBuffer[model.Span]
	logger      *zap.Logger
}

func NewTraceServiceServerImpl(
	logger *zap.Logger,
	dbWriteBuffer write_buffer.DatabaseWriteBuffer[model.Span],
) TraceServiceServerImpl {
	logger.Info("Creating new TraceServiceServerImpl")
	return TraceServiceServerImpl{
		logger:      logger,
		writeBuffer: dbWriteBuffer,
	}
}

func (tss TraceServiceServerImpl) Export(
	ctx context.Context,
	req *protoTrace.ExportTraceServiceRequest,
) (*protoTrace.ExportTraceServiceResponse, error) {
	for _, resourceSpan := range req.ResourceSpans {
		serviceName := getServiceName(resourceSpan)
		if serviceName == "Never Assigned" {
			tss.logger.Warn("Service name not found in resource span")
		}

		typedSpans := getTypedSpans(resourceSpan, serviceName)
		tss.writeBuffer.WriteToBuffer(typedSpans)
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
	spanKind := getSpanKind(span)
	clusterEvent := getClusterString(serviceName, span.Name, spanKind, getAttributesString(attributes))
	spanStatus := getStatus(span)

	return model.Span{
		Id:           generateSpanId(startTime, clusterEvent),
		CreatedAt:    time.Now().UTC(),
		SpanID:       spanId,
		ParentSpanID: parentSpanId,
		TraceID:      traceId,
		Service:      serviceName,
		StartTime:    startTime,
		EndTime:      endTime,
		ActionName:   span.Name,
		Attributes:   attributes,
		Events:       events,
		SpanKind:     spanKind,
		ClusterEvent: clusterEvent,
		ClusterId:    clusterService.DefaultClusterId,
		Status:       spanStatus,
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

func getSpanKind(span *v1.Span) string {
	return span.Kind.String()
}

func getAttributesString(attributes map[string]string) string {
	return fmt.Sprintf("%v", attributes)
}

func getClusterString(serviceName string, actionName string, spanKind string, attributes string) string {
	return fmt.Sprintf(
		"service=%s,operation=%s,kind=%s,attributes=%v",
		serviceName,
		actionName,
		spanKind,
		attributes,
	)
}

func getStatus(span *v1.Span) model.Status {
	if span.Status.Code == 0 {
		return model.Status{
			Message: span.Status.Message,
			Code:    model.UNSET,
		}
	}
	if span.Status.Code == 1 {
		return model.Status{
			Message: span.Status.Message,
			Code:    model.OK,
		}
	}
	return model.Status{
		Message: span.Status.Message,
		Code:    model.ERROR,
	}
}

func generateSpanId(timeStamp time.Time, clusterEvent string) string {
	data := fmt.Sprintf("%s:%s", timeStamp.Format(time.StampNano), clusterEvent)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}
