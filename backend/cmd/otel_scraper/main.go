package main

import (
	"github.com/Avi18971911/Augur/pkg/otel"
	v1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
)

func main() {
	logger, err := zap.NewProduction()
	defer logger.Sync()

	listener, err := net.Listen("tcp", ":4317")
	if err != nil {
		logger.Fatal("Failed to listen: %v", zap.Error(err))
	}

	srv := grpc.NewServer()
	traceServiceServer := otel.NewTraceServiceServerImpl(
		logger,
	)

	v1.RegisterTraceServiceServer(srv, traceServiceServer)
	logger.Info("gRPC service started, listening for OpenTelemetry traces...")

	if err := srv.Serve(listener); err != nil {
		logger.Fatal("Failed to serve: %v", zap.Error(err))
	}
}
