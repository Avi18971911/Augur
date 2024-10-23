package otel

import (
	"context"
	otlp "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
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
		tss.logger.Info("Processing ResourceSpan:", zap.Any("resource", resourceSpan.Resource))

		for _, libSpan := range resourceSpan.ScopeSpans {
			tss.logger.Info("Instrumentation Library: %v", zap.Any("scope", libSpan.Scope))

			for _, span := range libSpan.Spans {
				tss.logger.Info("Processing Span: %s", zap.Any("span", span.Name))
			}
		}
	}
	return &otlp.ExportTraceServiceResponse{}, nil
}
