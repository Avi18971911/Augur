package server

import (
	"context"
	protoLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"go.uber.org/zap"
)

type LogServiceServerImpl struct {
	protoLogs.UnimplementedLogsServiceServer
	logger *zap.Logger
}

func NewLogServiceServerImpl(logger *zap.Logger) *LogServiceServerImpl {
	logger.Info("Creating new LogServiceServerImpl")
	return &LogServiceServerImpl{
		logger: logger,
	}
}

func (lss *LogServiceServerImpl) Export(
	ctx context.Context,
	req *protoLogs.ExportLogsServiceRequest,
) (*protoLogs.ExportLogsServiceResponse, error) {
	lss.logger.Info("Exporting logs")
	for _, resourceLogs := range req.ResourceLogs {
		lss.logger.Info("Resource Logs", zap.Any("resource_logs", resourceLogs))
		lss.logger.Info("Resource", zap.Any("resource", resourceLogs.Resource))
		lss.logger.Info("Scope Logs", zap.Any("Scope Logs", resourceLogs.ScopeLogs))
	}
	return &protoLogs.ExportLogsServiceResponse{}, nil
}
