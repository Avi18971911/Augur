package model

import "time"

// TODO: Include TraceId and SpanId
type LogEntry struct {
	Id        string    `json:"_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	Severity  Level     `json:"severity"`
	Message   string    `json:"message"`
	Service   string    `json:"service"`
}

type Level string

const (
	InfoLevel  Level = "info"
	ErrorLevel Level = "error"
	DebugLevel Level = "debug"
	WarnLevel  Level = "warn"
)
