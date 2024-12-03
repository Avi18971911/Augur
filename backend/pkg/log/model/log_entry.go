package model

import "time"

type LogData map[string]interface{}

type LogEntry struct {
	Id        string    `json:"_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	Timestamp time.Time `json:"timestamp"`
	Severity  Level     `json:"severity"`
	Message   string    `json:"message"`
	Service   string    `json:"service"`
	TraceId   string    `json:"trace_id,omitempty"`
	SpanId    string    `json:"span_id,omitempty"`
	ClusterId string    `json:"cluster_id,omitempty"` // Cluster ID is the union set of the logs originating from the same line of code
}

type Level string

const (
	InfoLevel  Level = "info"
	ErrorLevel Level = "error"
	DebugLevel Level = "debug"
	WarnLevel  Level = "warn"
)
