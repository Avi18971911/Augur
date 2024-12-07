package model

import (
	"time"
)

type ClusterDataType string

const (
	LogClusterType  ClusterDataType = "log"
	SpanClusterType ClusterDataType = "span"
)

type ClusterOutput struct {
	Id              string
	ClusterId       string
	ClusterDataType ClusterDataType
	SpanTimeDetails SpanDetails
	LogTimeDetails  LogDetails
}

type SpanDetails struct {
	StartTime time.Time
	EndTime   time.Time
}

type LogDetails struct {
	Timestamp time.Time
}
