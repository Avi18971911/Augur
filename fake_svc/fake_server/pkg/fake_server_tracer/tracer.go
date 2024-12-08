package fake_server_tracer

import (
	"context"
	"errors"
	"go.opentelemetry.io/otel/trace"
)

func GetTracerFromContext(ctx context.Context) (trace.Tracer, error) {
	if tracer, ok := ctx.Value("tracer").(trace.Tracer); ok {
		return tracer, nil
	}
	return nil, errors.New("unable to get tracer from context")
}

func GetSpanFromContext(ctx context.Context) (trace.Span, error) {
	if span, ok := ctx.Value("span").(trace.Span); ok {
		return span, nil
	}
	return nil, errors.New("unable to get span from context")

}

func PutTracerInContext(ctx context.Context, tracer trace.Tracer) context.Context {
	return context.WithValue(ctx, "tracer", tracer)
}

func PutSpanInContext(ctx context.Context, span trace.Span) context.Context {
	return context.WithValue(ctx, "span", span)
}
