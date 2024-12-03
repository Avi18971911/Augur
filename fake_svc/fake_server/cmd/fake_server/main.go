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
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	goLog "log"
	"net/http"
	"os"
	"sync"
	"time"
)

var initResourcesOnce sync.Once

func initLogger() *logrus.Logger {
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
	return logger
}

func initResource() *sdkresource.Resource {
	var resource *sdkresource.Resource
	initResourcesOnce.Do(func() {
		extraResources, _ := sdkresource.New(
			context.Background(),
			sdkresource.WithAttributes(semconv.ServiceNameKey.String("fake-server")),
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

func initTracerProvider() *sdktrace.TracerProvider {
	ctx := context.Background()

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure(), otlptracegrpc.WithEndpoint("otel-collector:4318"))
	if err != nil {
		goLog.Fatalf("new otlp trace grpc exporter failed: %v", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(initResource()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}),
	)
	return tp
}

func main() {
	res := initResource()
	ctx := context.Background()
	logExporter, err := otlploghttp.New(ctx, otlploghttp.WithEndpoint("otel-collector:4318"), otlploghttp.WithInsecure())
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
	logger := initLogger()
	logger.AddHook(otellogrus.NewHook("fake-server", otellogrus.WithLoggerProvider(lp)))

	tp := initTracerProvider()
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			logger.Errorf("Error shutting down tracer provider: %v", err)
		}
	}()
	tracer := tp.Tracer("fake-server")

	ar := repository.CreateNewFakeAccountRepository(logger)
	tra := transactional.NewFakeTransactional(logger)
	as := service.CreateNewAccountServiceImpl(ar, tra, logger)
	r := router.CreateRouter(as, context.Background(), tracer, logger)

	logger.Infof("Starting webserver")
	logger.Fatalf("Stopped Listening to Webserver! %v", http.ListenAndServe(":8080", r))
}
