package main

import (
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/db/write_buffer"
	logModel "github.com/Avi18971911/Augur/internal/otel_server/log/model"
	logsServer "github.com/Avi18971911/Augur/internal/otel_server/log/server"
	spanModel "github.com/Avi18971911/Augur/internal/otel_server/trace/model"
	traceServer "github.com/Avi18971911/Augur/internal/otel_server/trace/server"
	analyticsService "github.com/Avi18971911/Augur/internal/pipeline/analytics/service"
	clusterService "github.com/Avi18971911/Augur/internal/pipeline/cluster/service"
	countModel "github.com/Avi18971911/Augur/internal/pipeline/cluster_count/model"
	count "github.com/Avi18971911/Augur/internal/pipeline/cluster_count/service"
	"github.com/Avi18971911/Augur/internal/pipeline/data_pipeline/service"
	dataProcessor "github.com/Avi18971911/Augur/internal/pipeline/data_processor/service"
	"github.com/asaskevich/EventBus"
	"github.com/elastic/go-elasticsearch/v8"
	protoLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	protoTrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"net"
	"time"
)

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()
	const countDataProcessorBucket = countModel.Bucket(2500)
	const windowCountBucket = 50
	const intervalForPipelineSeconds = 60

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
	windowCountService := count.NewClusterWindowCountService(ac, windowCountBucket, logger)
	countService := count.NewClusterTotalCountService(ac, windowCountService, logger)
	eventBus := EventBus.New()

	codp := count.NewCountDataProcessorService(
		ac,
		countService,
		countDataProcessorBucket,
		[]string{bootstrapper.SpanIndexName, bootstrapper.LogIndexName},
		logger,
	)

	cldp := clusterService.NewClusterDataProcessor(
		ac,
		cls,
		logger,
	)

	andp := analyticsService.NewAnalyticsService(
		ac,
		logger,
	)

	dp := dataProcessor.NewDataProcessorService(
		ac,
		logger,
	)

	dataPipeline := service.NewDataPipeline(
		dp,
		cldp,
		codp,
		andp,
		eventBus,
		logger,
	)
	ticker := time.NewTicker(intervalForPipelineSeconds)
	defer ticker.Stop()

	err = dataPipeline.Start(ticker)
	if err != nil {
		logger.Fatal("Failed to start data pipeline: %v", zap.Error(err))
	}

	traceDBBuffer := write_buffer.NewDatabaseWriteBufferImpl[spanModel.Span](
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

	if err := srv.Serve(listener); err != nil {
		logger.Fatal("Failed to serve: %v", zap.Error(err))
	}
}
