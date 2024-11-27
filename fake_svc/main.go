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
	"go.uber.org/zap" // Import zap for structured logging
)

func initTracer() func() {
	ctx := context.Background()

	// Create OTLP trace exporter
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint("http://otel-collector:4318"), otlptracehttp.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to create trace exporter: %v", err)
	}

	// Create Resource
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("fake-service"),
		),
	)
	if err != nil {
		log.Fatalf("Failed to create resource: %v", err)
	}

	// Create Tracer Provider
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

func initLogger() *zap.Logger {
	// Create zap logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	return logger
}

func main() {
	// Initialize tracer and logger
	shutdownTracer := initTracer()
	defer shutdownTracer()

	// Initialize logger
	logger := initLogger()

	// Start tracer and log messages
	tracer := otel.Tracer("fake-service")

	for {
		func() {
			// Start a new span for each iteration
			_, span := tracer.Start(context.Background(), "fake-operation")
			defer span.End()

			// Generate a random string as an operation ID
			opID := randomString()
			span.SetAttributes(attribute.String("operation.id", opID))

			// Log a message with the trace ID and operation ID using zap
			logger.Info("Log message with trace ID",
				zap.String("trace_id", span.SpanContext().TraceID().String()),
				zap.String("operation.id", opID),
			)

			// Simulate work with a sleep
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
