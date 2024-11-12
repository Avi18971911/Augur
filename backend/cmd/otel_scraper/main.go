package main

import (
	"net"

	"github.com/Avi18971911/Augur/pkg/cache"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	logsServer "github.com/Avi18971911/Augur/pkg/log/server"
	"github.com/Avi18971911/Augur/pkg/log/service"
	traceModel "github.com/Avi18971911/Augur/pkg/trace/model"
	traceServer "github.com/Avi18971911/Augur/pkg/trace/server"
	"github.com/dgraph-io/ristretto"
	"github.com/elastic/go-elasticsearch/v8"
	protoLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	protoTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()

	es, err := elasticsearch.NewDefaultClient()
	if err != nil {
		logger.Error("Failed to create elasticsearch client", zap.Error(err))
	}

	bs := augurElasticsearch.NewBootstrapper(es, logger)
	err = bs.BootstrapElasticsearch()
	if err != nil {
		logger.Error("Failed to bootstrap elasticsearch", zap.Error(err))
	}

	listener, err := net.Listen("tcp", ":4317")
	if err != nil {
		logger.Error("Failed to listen: %v", zap.Error(err))
	}

	ac := augurElasticsearch.NewAugurClientImpl(es)
	logProcessorService := service.NewLogProcessorService(ac, logger)

	ristrettoTraceCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10,
		MaxCost:     1 << 5,
		BufferItems: 2,
	})
	if err != nil {
		logger.Fatal("Failed to create ristretto cache: %v", zap.Error(err))
	}

	ristrettoLogCache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10,
		MaxCost:     1 << 5,
		BufferItems: 2,
	})
	if err != nil {
		logger.Fatal("Failed to create ristretto cache: %v", zap.Error(err))
	}

	writeBehindTraceCache := cache.NewWriteBehindCacheImpl[traceModel.Span](
		ristrettoTraceCache,
		ac,
		augurElasticsearch.SpanIndexName,
		logger,
	)
	writeBehindLogCache := cache.NewWriteBehindCacheImpl[logModel.LogEntry](
		ristrettoLogCache,
		ac,
		augurElasticsearch.LogIndexName,
		logger,
	)

	srv := grpc.NewServer()
	traceServiceServer := traceServer.NewTraceServiceServerImpl(
		logger,
		writeBehindTraceCache,
	)
	logServiceServer := logsServer.NewLogServiceServerImpl(
		logger,
		writeBehindLogCache,
		logProcessorService,
	)

	protoTrace.RegisterTraceServiceServer(srv, traceServiceServer)
	protoLogs.RegisterLogsServiceServer(srv, logServiceServer)
	logger.Info("gRPC service started, listening for OpenTelemetry traces...")

	if err := srv.Serve(listener); err != nil {
		logger.Fatal("Failed to serve: %v", zap.Error(err))
	}
}
