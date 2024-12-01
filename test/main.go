package main

import (
	"context"
	"fmt"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func main() {
	ctx := context.Background()

	res, err := newResource()
	if err != nil {
		panic(err)
	}

	logExporter, err := otlploghttp.New(ctx, otlploghttp.WithEndpoint("localhost:4318"), otlploghttp.WithInsecure())
	if err != nil {
		panic("failed to initialize exporter")
	}

	lp := log.NewLoggerProvider(
		log.WithResource(res),
		log.WithProcessor(
			log.NewBatchProcessor(logExporter),
		),
	)

	defer lp.Shutdown(ctx)

	global.SetLoggerProvider(lp)

	logger := otelslog.NewLogger("fake-service")

	logger.Debug("Debug: Starting application")
	logger.Info("Info: Application is running successfully")
	logger.Warn("Warning: Something might be wrong here")
	logger.Error("Error: An error has occurred while processing the request", "error", fmt.Errorf("failed to connect"))
	logger.Info("Info: Processing data for user ID", "user_id", 12345)
	logger.Debug("Debug: Fetching configuration from server")
	logger.Warn("Warning: Low disk space on server")
	logger.Error("Error: Could not open database connection", "error", fmt.Errorf("timeout"))

	logger.Info("Info: User logged in", "user", "john_doe", "location", "Singapore")
	logger.Debug("Debug: Calling external API", "url", "https://example.com/api", "method", "GET")
	logger.Warn("Warning: API rate limit approaching", "rate_limit", 95)

	logger.Info("Info: Shutting down application gracefully")

	fmt.Println("Logs have been sent to the OTel Collector.")
}

func newResource() (*resource.Resource, error) {
	return resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName("fake-service"),
			semconv.ServiceVersion("0.1.0"),
		))
}
