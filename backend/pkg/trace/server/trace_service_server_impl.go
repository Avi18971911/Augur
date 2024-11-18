package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/cache"
	count "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/Avi18971911/Augur/pkg/trace/service"
	protoTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.opentelemetry.io/proto/otlp/trace/v1"
	"go.uber.org/zap"
	"time"
)

type TraceServiceServerImpl struct {
	protoTrace.UnimplementedTraceServiceServer
	logger           *zap.Logger
	writeBehindCache cache.WriteBehindCache[model.Span]
	clusterService   service.SpanClusterService
	countService     *count.CountService
}

func NewTraceServiceServerImpl(
	logger *zap.Logger,
	cache cache.WriteBehindCache[model.Span],
	clusterService service.SpanClusterService,
	countService *count.CountService,
) TraceServiceServerImpl {
	logger.Info("Creating new TraceServiceServerImpl")
	return TraceServiceServerImpl{
		logger:           logger,
		writeBehindCache: cache,
		clusterService:   clusterService,
		countService:     countService,
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
		// we need to group because the spans underneath the same resource span may not have the same trace id
		groupedSpans := groupTypedSpansByTraceID(typedSpans)
		go func(groupedSpans map[string][]model.Span) {
			processCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for traceID, spans := range groupedSpans {
				newSpansWithClusterId := make([]model.Span, len(spans))
				for i, span := range spans {
					newSpan, err := tss.clusterService.ClusterAndUpdateSpans(processCtx, span)
					if err != nil {
						tss.logger.Error("Failed to cluster and update spans", zap.Error(err))
						continue
					}
					var buckets = []count.Bucket{2500}
					err = tss.countService.CountAndUpdateOccurrences(processCtx, newSpan.ClusterId, count.TimeInfo{
						SpanInfo: &count.SpanInfo{
							FromTime: newSpan.StartTime,
							ToTime:   newSpan.EndTime,
						},
					}, buckets)
					if err != nil {
						tss.logger.Error("Failed to count and update occurrences", zap.Error(err))
						continue
					}
					newSpansWithClusterId[i] = newSpan
				}
				err := tss.writeBehindCache.Put(traceID, newSpansWithClusterId)
				if err != nil {
					tss.logger.Error("Failed to put span in cache", zap.Error(err))
				}
			}
		}(groupedSpans)
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
		SpanKind:     spanKind,
		ClusterEvent: clusterEvent,
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

func groupTypedSpansByTraceID(spans []model.Span) map[string][]model.Span {
	groupedSpans := make(map[string][]model.Span)
	for _, span := range spans {
		groupedSpans[span.TraceID] = append(groupedSpans[span.TraceID], span)
	}
	return groupedSpans
}
