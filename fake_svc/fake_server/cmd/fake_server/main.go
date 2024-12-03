package main

import (
	"context"
	"fake_svc/fake_server/pkg/repository"
	"fake_svc/fake_server/pkg/server/router"
	"fake_svc/fake_server/pkg/service"
	"fake_svc/fake_server/pkg/transactional"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/bridges/otellogrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

const serviceName = "fake-server"

var initResourcesOnce sync.Once

func initLogger(lp *sdklog.LoggerProvider) *logrus.Logger {
	logger := logrus.New()
	logger.Level = logrus.DebugLevel
	logger.Formatter = &logrus.JSONFormatter{
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
		TimestampFormat: time.RFC3339Nano,
	}
	logger.Out = os.Stdout
	logger.AddHook(otellogrus.NewHook("fake-server", otellogrus.WithLoggerProvider(lp)))
	return logger
}

func initResource() *sdkresource.Resource {
	var resource *sdkresource.Resource
	initResourcesOnce.Do(func() {
		extraResources, _ := sdkresource.New(
			context.Background(),
			sdkresource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
			sdkresource.WithOS(),
			sdkresource.WithProcess(),
			sdkresource.WithContainer(),
			sdkresource.WithHost(),
		)
		resource, _ = sdkresource.Merge(
			sdkresource.Default(),
			extraResources,
		)
	})
	return resource
}

func initTracerProvider(
	ctx context.Context,
	res *sdkresource.Resource,
	otelgRPCUrl string,
) (*sdktrace.TracerProvider, func()) {
	exporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(otelgRPCUrl),
	)
	if err != nil {
		log.Fatalf("new otlp trace gRPC exporter failed: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	cleanup := func() {
		if err := tp.Shutdown(ctx); err != nil {
			log.Fatalf("failed to shutdown tracer provider due to error: %v", err)
		}
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
	)
	return tp, cleanup
}

func initLogProvider(
	ctx context.Context,
	res *sdkresource.Resource,
	otelgRPCUrl string,
) (*sdklog.LoggerProvider, func()) {
	logExporter, err := otlploggrpc.New(
		ctx,
		otlploggrpc.WithEndpoint(otelgRPCUrl),
		otlploggrpc.WithInsecure(),
	)
	if err != nil {
		log.Fatalf("new otlp log gRPC exporter failed: %v", err)
	}

	batchProcessor := sdklog.NewBatchProcessor(logExporter)
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(batchProcessor),
		sdklog.WithResource(res),
	)
	cleanup := func() {
		if err := lp.Shutdown(ctx); err != nil {
			log.Fatalf("failed to shutdown logger provider due to error: %v", err)
		}
	}
	global.SetLoggerProvider(lp)
	return lp, cleanup
}

func main() {
	res := initResource()
	ctx := context.Background()
	otelgRPCUrl := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if otelgRPCUrl == "" {
		otelgRPCUrl = "localhost:4317"
	}

	lp, lpCleanup := initLogProvider(ctx, res, otelgRPCUrl)
	defer lpCleanup()

	logger := initLogger(lp)

	tp, tpCleanup := initTracerProvider(ctx, res, otelgRPCUrl)
	defer tpCleanup()

	tracer := tp.Tracer(serviceName)

	ar := repository.CreateNewFakeAccountRepository(logger)
	tra := transactional.NewFakeTransactional(logger)
	as := service.CreateNewAccountServiceImpl(ar, tra, logger)
	r := router.CreateRouter(as, context.Background(), tracer, logger)

	logger.Infof("Starting webserver")
	logger.Fatalf("Stopped Listening to Webserver! %v", http.ListenAndServe(":8080", r))
}
