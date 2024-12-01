package model

import "time"

// ClusterQueryResult is a struct that represents the result of a query to the log or span index
type ClusterQueryResult struct {
	ClusterId   string           `json:"cluster_id"`
	CoClusterId string           `json:"co_cluster_id"`
	SpanResult  *SpanQueryResult `json:"span_result"`
	LogResult   *LogQueryResult  `json:"log_result"`
}

type SpanQueryResult struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
}

type LogQueryResult struct {
	Timestamp time.Time `json:"timestamp"`
}
