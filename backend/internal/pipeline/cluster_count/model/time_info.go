package model

import "time"

type LogInfo struct {
	Timestamp time.Time
}

type SpanInfo struct {
	FromTime time.Time
	ToTime   time.Time
}

type CalculatedTimeInfo struct {
	FromTime time.Time
	ToTime   time.Time
}

type TimeInfo struct {
	SpanInfo *SpanInfo
	LogInfo  *LogInfo
}
