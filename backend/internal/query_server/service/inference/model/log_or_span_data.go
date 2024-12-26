package model

import (
	logModel "github.com/Avi18971911/Augur/internal/otel_server/log/model"
	spanModel "github.com/Avi18971911/Augur/internal/otel_server/trace/model"
)

type LogOrSpanData struct {
	Id          string `json:"id"`
	ClusterId   string `json:"cluster_id"`
	SpanDetails *spanModel.Span
	LogDetails  *logModel.LogEntry
}
