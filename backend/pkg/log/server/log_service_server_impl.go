package server

import (
	"context"
	"github.com/Avi18971911/Augur/pkg/log/model"
	protoLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"go.uber.org/zap"
	"time"
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
	for _, resourceLogs := range req.ResourceLogs {
		// Ignore resource logs for now
		for _, scopeLog := range resourceLogs.ScopeLogs {
			serviceName := scopeLog.Scope.Name
			for _, log := range scopeLog.LogRecords {
				typedLog := typeLog(log, serviceName)
			}
		}
	}
	return &protoLogs.ExportLogsServiceResponse{}, nil
}

func typeLog(log *v1.LogRecord, serviceName string) model.LogEntry {
	timestamp := time.Unix(0, int64(log.TimeUnixNano))
	message := log.Body.GetStringValue()
	severity := getSeverity(log.SeverityNumber)
	return model.LogEntry{
		Timestamp: timestamp,
		Severity:  severity,
		Message:   message,
		Service:   serviceName,
	}
}

func getSeverity(severityNumber v1.SeverityNumber) model.Level {
	switch severityNumber {
	case v1.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED:
		return model.InfoLevel
	case v1.SeverityNumber_SEVERITY_NUMBER_TRACE:
		return model.DebugLevel
	case v1.SeverityNumber_SEVERITY_NUMBER_DEBUG:
		return model.DebugLevel
	case v1.SeverityNumber_SEVERITY_NUMBER_INFO:
		return model.InfoLevel
	case v1.SeverityNumber_SEVERITY_NUMBER_WARN:
		return model.WarnLevel
	case v1.SeverityNumber_SEVERITY_NUMBER_ERROR:
		return model.ErrorLevel
	case v1.SeverityNumber_SEVERITY_NUMBER_FATAL:
		return model.ErrorLevel
	default:
		return model.InfoLevel
	}
}
