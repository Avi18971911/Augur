package model

import "time"

type Span struct {
	Id           string            `json:"_id,omitempty"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id"`
	TraceID      string            `json:"trace_id"`
	ServiceName  string            `json:"service_name"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	ActionName   string            `json:"action_name"`
	Attributes   map[string]string `json:"attributes"` // Metadata like HTTP status, user info
	Events       []SpanEvent       `json:"events"`     // Important events within the span
}

type SpanEvent struct {
	Name       string            `json:"name"` // Event name
	Attributes map[string]string `json:"attributes"`
	Timestamp  time.Time         `json:"timestamp"`
}
