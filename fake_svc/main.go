package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"go.uber.org/zap"
)

func initTracer() func() {
	ctx := context.Background()

	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint("http://otel-collector:4318"), otlptracehttp.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to create trace exporter: %v", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("fake-service"),
		),
	)
	if err != nil {
		log.Fatalf("Failed to create resource: %v", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	return func() {
		_ = tp.Shutdown(ctx)
	}
}

func main() {

	shutdownTracer := initTracer()
	defer shutdownTracer()

	tracer := otel.Tracer("fake-service")

	for {
		func() {
			_, span := tracer.Start(context.Background(), "fake-operation")
			defer span.End()

			opID := randomString()
			span.SetAttributes(attribute.String("operation.id", opID))

			logger.Info("Log message with trace ID",
				zap.String("trace_id", span.SpanContext().TraceID().String()),
				zap.String("operation.id", opID),
			)

			time.Sleep(2 * time.Second)
		}()
	}
}

func randomString() string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, 10)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
