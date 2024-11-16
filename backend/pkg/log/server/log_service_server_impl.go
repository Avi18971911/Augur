package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/cache"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/Avi18971911/Augur/pkg/log/service"
	protoLogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"go.uber.org/zap"
	"time"
)

var buckets = []service.Bucket{2500}

type LogServiceServerImpl struct {
	protoLogs.UnimplementedLogsServiceServer
	cache        cache.WriteBehindCache[model.LogEntry]
	logger       *zap.Logger
	logProcessor service.LogProcessorService
	countService *service.CountService
}

func NewLogServiceServerImpl(
	logger *zap.Logger,
	cache cache.WriteBehindCache[model.LogEntry],
	logProcessor service.LogProcessorService,
	countService *service.CountService,
) *LogServiceServerImpl {
	logger.Info("Creating new LogServiceServerImpl")
	return &LogServiceServerImpl{
		logger:       logger,
		cache:        cache,
		logProcessor: logProcessor,
		countService: countService,
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

				go func(typedLog model.LogEntry, serviceName string) {
					processCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					logWithClusterId, err := lss.logProcessor.ParseLogWithMessage(processCtx, serviceName, typedLog)
					if err != nil {
						lss.logger.Error("Failed to parse log with message", zap.Error(err))
						return
					}
					err = lss.cache.Put(processCtx, logWithClusterId.ClusterId, []model.LogEntry{logWithClusterId})
					if err != nil {
						lss.logger.Error("Failed to put log in cache", zap.Error(err))
						return
					}
					err = lss.countService.CountAndUpdateOccurrences(processCtx, logWithClusterId, buckets)
					if err != nil {
						lss.logger.Error("Failed to count and update occurrences", zap.Error(err))
					}
				}(typedLog, serviceName)
			}
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
