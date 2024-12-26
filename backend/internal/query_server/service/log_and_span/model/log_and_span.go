package model

import (
	logModel "github.com/Avi18971911/Augur/internal/otel_server/log/model"
	spanModel "github.com/Avi18971911/Augur/internal/otel_server/trace/model"
)

type LogAndSpan struct {
	LogDetails  *logModel.LogEntry
	SpanDetails *spanModel.Span
}
