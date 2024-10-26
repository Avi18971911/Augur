package main

import (
	"github.com/Avi18971911/Augur/pkg/otel"
	"github.com/Avi18971911/Augur/pkg/otel/server"
	"github.com/dgraph-io/ristretto"
	v1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"net"
)

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()

	listener, err := net.Listen("tcp", ":4317")
	if err != nil {
		logger.Fatal("Failed to listen: %v", zap.Error(err))
	}

	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10,
		MaxCost:     1 << 5,
		BufferItems: 2,
	})
	if err != nil {
		logger.Fatal("Failed to create ristretto cache: %v", zap.Error(err))
	}
	writeBehindCache := otel.NewWriteBehindCacheImpl(cache)

	srv := grpc.NewServer()
	traceServiceServer := server.NewTraceServiceServerImpl(
		logger,
		writeBehindCache,
	)

	v1.RegisterTraceServiceServer(srv, traceServiceServer)
	logger.Info("gRPC service started, listening for OpenTelemetry traces...")

	if err := srv.Serve(listener); err != nil {
		logger.Fatal("Failed to serve: %v", zap.Error(err))
	}
}
