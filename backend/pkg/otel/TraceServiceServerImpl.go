package otel

import (
	"context"
	otlp "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"time"
)

type TraceServiceServerImpl struct {
	otlp.UnimplementedTraceServiceServer
	logger *zap.Logger
}

func NewTraceServiceServerImpl(logger *zap.Logger) TraceServiceServerImpl {
	return TraceServiceServerImpl{
		logger: logger,
	}
}

func (tss TraceServiceServerImpl) Export(
	ctx context.Context,
	req *otlp.ExportTraceServiceRequest,
) (*otlp.ExportTraceServiceResponse, error) {
	for _, resourceSpan := range req.ResourceSpans {
		for _, attr := range resourceSpan.Resource.Attributes {
			if attr.Key == "service.name" {
				serviceName := attr.Value.GetStringValue()
				tss.logger.Info("Service Name", zap.String("service_name", serviceName))
			}
		}

		for _, libSpan := range resourceSpan.ScopeSpans {
			tss.logger.Info("Instrumentation Library: %v", zap.Any("scope", libSpan.Scope))

			for _, span := range libSpan.Spans {
				startTime := time.Unix(0, int64(span.StartTimeUnixNano))
				endTime := time.Unix(0, int64(span.EndTimeUnixNano))
				spanId := string(span.SpanId)
				parentSpanId := string(span.ParentSpanId)

				tss.logger.Info("Processing Span",
					zap.String("Span Name", span.Name),
					zap.String("Span ID", spanId),
					zap.String("Parent Span ID", parentSpanId),
					zap.Time("Start Time", startTime),
					zap.Time("End Time", endTime),
				)

				for _, attribute := range span.Attributes {
					tss.logger.Info("Span Attribute",
						zap.String("Key", attribute.Key),
						zap.Any("Value", attribute.Value),
					)
				}
			}
		}
	}
	return &otlp.ExportTraceServiceResponse{}, nil
}
