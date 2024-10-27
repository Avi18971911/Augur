package model

import "time"

type Span struct {
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId"`
	TraceID      string            `json:"traceId"`
	ServiceName  string            `json:"serviceName"`
	StartTime    time.Time         `json:"startTime"`
	EndTime      time.Time         `json:"endTime"`
	ActionName   string            `json:"actionName"`
	Attributes   map[string]string `json:"attributes"` // Metadata like HTTP status, user info
	Events       []SpanEvent       `json:"events"`     // Important events within the span
	StatusCode   string            `json:"statusCode"` // Status (e.g., OK or ERROR)
	IsError      bool              `json:"isError"`    // Quick error check
}

type SpanEvent struct {
	Name       string            `json:"name"` // Event name
	Attributes map[string]string `json:"attributes"`
	Timestamp  time.Time         `json:"timestamp"` // Event time
}
