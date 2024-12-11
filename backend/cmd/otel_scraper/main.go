package main

import (
	"context"
	clusterService "github.com/Avi18971911/Augur/pkg/cluster/service"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	count "github.com/Avi18971911/Augur/pkg/count/service"
	dataProcessor "github.com/Avi18971911/Augur/pkg/data_processor/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/write_buffer"
	"github.com/asaskevich/EventBus"
	"net"
	"time"

	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	logsServer "github.com/Avi18971911/Augur/pkg/log/server"
	traceModel "github.com/Avi18971911/Augur/pkg/trace/model"
	traceServer "github.com/Avi18971911/Augur/pkg/trace/server"
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

	bs := bootstrapper.NewBootstrapper(es, logger)
	err = bs.BootstrapElasticsearch()
	if err != nil {
		logger.Error("Failed to bootstrap elasticsearch", zap.Error(err))
	}

	listener, err := net.Listen("tcp", ":4317")
	if err != nil {
		logger.Error("Failed to listen: %v", zap.Error(err))
	}

	ac := client.NewAugurClientImpl(es, client.Wait)
	cls := clusterService.NewClusterService(ac, logger)
	countService := count.NewCountService(ac, logger)
	inputClusterBus := EventBus.New()
	outputClusterBus := EventBus.New()
	inputCountBus := EventBus.New()
	outputCountBus := EventBus.New()
	inputAnalyticsBus := EventBus.New()

	traceDBBuffer := write_buffer.NewDatabaseWriteBufferImpl[traceModel.Span](
		ac,
		bootstrapper.SpanIndexName,
		logger,
	)
	logDBBuffer := write_buffer.NewDatabaseWriteBufferImpl[logModel.LogEntry](
		ac,
		bootstrapper.LogIndexName,
		logger,
	)

	srv := grpc.NewServer()
	traceServiceServer := traceServer.NewTraceServiceServerImpl(
		logger,
		traceDBBuffer,
	)
	logServiceServer := logsServer.NewLogServiceServerImpl(
		logger,
		logDBBuffer,
	)

	protoTrace.RegisterTraceServiceServer(srv, traceServiceServer)
	protoLogs.RegisterLogsServiceServer(srv, logServiceServer)
	logger.Info("gRPC service started, listening for OpenTelemetry traces...")

	dp := dataProcessor.NewDataProcessorService(
		ac,
		countService,
		cls,
		logger,
	)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	go func() {
		for range ticker.C {
			_, errors := dp.ProcessData(
				context.Background(),
				[]countModel.Bucket{2000},
				[]string{bootstrapper.SpanIndexName, bootstrapper.LogIndexName})
			for _, err := range errors {
				if err != nil {
					logger.Error("Failed to process data", zap.Error(err))
				}
			}
		}
	}()

	if err := srv.Serve(listener); err != nil {
		logger.Fatal("Failed to serve: %v", zap.Error(err))
	}
}
