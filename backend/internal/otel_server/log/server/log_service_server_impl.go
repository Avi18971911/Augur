package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/write_buffer"
	"github.com/Avi18971911/Augur/internal/otel_server/log/model"
	clusterService "github.com/Avi18971911/Augur/internal/pipeline/cluster/service"
	protoLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"go.uber.org/zap"
	"time"
)

type LogServiceServerImpl struct {
	protoLogs.UnimplementedLogsServiceServer
	dbBuffer write_buffer.DatabaseWriteBuffer[model.LogEntry]
	logger   *zap.Logger
}

func NewLogServiceServerImpl(
	logger *zap.Logger,
	cache write_buffer.DatabaseWriteBuffer[model.LogEntry],
) *LogServiceServerImpl {
	logger.Info("Creating new LogServiceServerImpl")
	return &LogServiceServerImpl{
		logger:   logger,
		dbBuffer: cache,
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
			typedLogs := make([]model.LogEntry, len(scopeLog.LogRecords))
			for i, log := range scopeLog.LogRecords {
				typedLogs[i] = typeLog(log, serviceName)
			}
			lss.dbBuffer.WriteToBuffer(typedLogs)
		}
	}
	return &protoLogs.ExportLogsServiceResponse{}, nil
}

func typeLog(log *v1.LogRecord, serviceName string) model.LogEntry {
	timestamp := time.Unix(0, int64(log.TimeUnixNano))
	message := log.Body.GetStringValue()
	severity := getSeverity(log.SeverityNumber)
	traceId := hex.EncodeToString(log.TraceId)
	spanId := hex.EncodeToString(log.SpanId)
	return model.LogEntry{
		Id:        generateLogId(timestamp, message),
		CreatedAt: time.Now().UTC(),
		ClusterId: clusterService.DefaultClusterId,
		Timestamp: timestamp,
		Severity:  severity,
		Message:   message,
		Service:   serviceName,
		TraceId:   traceId,
		SpanId:    spanId,
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

func generateLogId(timeStamp time.Time, message string) string {
	data := fmt.Sprintf("%s:%s", timeStamp.Format(time.StampNano), message)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}
