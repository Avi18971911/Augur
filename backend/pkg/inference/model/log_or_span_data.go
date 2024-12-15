package model

import "time"

type LogOrSpanData struct {
	Id              string `json:"id"`
	ClusterId       string `json:"cluster_id"`
	SpanTimeDetails *SpanTimeDetails
	LogTimeDetails  *LogTimeDetails
}

type SpanTimeDetails struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
}

type LogTimeDetails struct {
	Timestamp time.Time `json:"timestamp"`
}
