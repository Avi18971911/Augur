package model

import "time"

type Span struct {
	Id           string            `json:"_id,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id"`
	TraceID      string            `json:"trace_id"`
	Service      string            `json:"service"`
	StartTime    time.Time         `json:"start_time"`
	EndTime      time.Time         `json:"end_time"`
	ActionName   string            `json:"action_name"`
	SpanKind     string            `json:"span_kind"`
	ClusterEvent string            `json:"cluster_event"`        // Textual data used for clustering
	ClusterId    string            `json:"cluster_id,omitempty"` // ID of the cluster this span belongs to
	Attributes   map[string]string `json:"attributes"`           // Metadata like HTTP status, user info
	Events       []SpanEvent       `json:"events"`               // Important events within the span
	Status       Status            `json:"status"`               // Status of the span
}

type SpanEvent struct {
	Name       string            `json:"name"` // Event name
	Attributes map[string]string `json:"attributes"`
	Timestamp  time.Time         `json:"timestamp"`
}

type Status struct {
	Message string     `json:"message"`
	Code    StatusCode `json:"code"`
}

type StatusCode string

const (
	UNSET StatusCode = "unset"
	OK    StatusCode = "ok"
	ERROR StatusCode = "error"
)
