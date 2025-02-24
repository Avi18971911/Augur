package handler

import (
	"time"
)

// ChainOfEventsRequestDTO represents the login credentials for an account
// @swagger:model ChainOfEventsRequestDTO
type ChainOfEventsRequestDTO struct {
	// The ID of the log or span data
	Id string `json:"id" validate:"required"`
}

// SpanDTO represents the details of a span
// @swagger:model SpanDTO
type SpanDTO struct {
	Id           string            `json:"id" validate:"required"`
	CreatedAt    time.Time         `json:"created_at" validate:"required"`
	SpanID       string            `json:"span_id" validate:"required"`
	ParentSpanID string            `json:"parent_span_id" validate:"required"`
	TraceID      string            `json:"trace_id" validate:"required"`
	Service      string            `json:"service" validate:"required"`
	StartTime    time.Time         `json:"start_time" validate:"required"`
	EndTime      time.Time         `json:"end_time" validate:"required"`
	ActionName   string            `json:"action_name" validate:"required"`
	SpanKind     string            `json:"span_kind" validate:"required"`
	ClusterEvent string            `json:"cluster_event" validate:"required"`
	ClusterId    string            `json:"cluster_id" validate:"required"`
	Attributes   map[string]string `json:"attributes,omitempty"`
	Events       []SpanEventDTO    `json:"events,omitempty"`
	Status       StatusDTO         `json:"status" validate:"required"`
}

type SpanEventDTO struct {
	// The name of the event
	Name string `json:"name" validate:"required"`
	// The attributes of the event, metadata like HTTP status, user info
	Attributes map[string]string `json:"attributes" validate:"required"`
	// The timestamp of the event
	Timestamp time.Time `json:"timestamp" validate:"required"`
}

type StatusDTO struct {
	Message string     `json:"message" validate:"required"`
	Code    StatusCode `json:"code" validate:"required"`
}

type StatusCode string

const (
	UNSET StatusCode = "unset"
	OK    StatusCode = "ok"
	ERROR StatusCode = "error"
)

// LogDTO represents the details of a log
// @swagger:model LogDTO
type LogDTO struct {
	Id        string    `json:"id" validate:"required"`
	CreatedAt time.Time `json:"created_at" validate:"required"`
	Timestamp time.Time `json:"timestamp" validate:"required"`
	Severity  string    `json:"severity" validate:"required"`
	Message   string    `json:"message" validate:"required"`
	Service   string    `json:"service" validate:"required"`
	TraceId   string    `json:"trace_id,omitempty"`
	SpanId    string    `json:"span_id,omitempty"`
	ClusterId string    `json:"cluster_id" validate:"required"`
}

// ChainOfEventsResponseDTO represents the response to a chain of events request in the form of a graph
// @swagger:model ChainOfEventsResponseDTO
type ChainOfEventsResponseDTO struct {
	// The nodes in the chain of events
	Graph map[string]ChainOfEventsNodeDTO `json:"graph" validate:"required"`
}

// ChainOfEventsNodeDTO represents a node in the chain of events
// @swagger:model ChainOfEventsNodeDTO
type ChainOfEventsNodeDTO struct {
	// The ID of the log or span data
	Id string `json:"id" validate:"required"`
	// The Cluster ID of the cluster belonging to the log or span data
	ClusterId string `json:"cluster_id" validate:"required"`
	// The IDs of the successors of the log or span data
	Successors []EdgeDTO `json:"successors" validate:"required"`
	// The IDs of the predecessors of the log or span data
	Predecessors []EdgeDTO `json:"predecessors" validate:"required"`
	// The details of the span data, if the data is a span
	SpanDTO *SpanDTO `json:"span_dto"`
	// The details of the log data, if the data is a log
	LogDTO *LogDTO `json:"log_dto"`
}

// EdgeDTO represents an edge in the chain of events
// @swagger:model EdgeDTO
type EdgeDTO struct {
	// The ID of the associated log or span data
	Id string `json:"id" validate:"required"`
	// The TDOA of the associated log or span data assuming a log or span was successfully inferred
	TDOA float64 `json:"tdoa" validate:"required"`
}
