package model

import "time"

type CountInfo struct {
	Occurrences            int64
	ClusterWindowCountInfo map[string]ClusterWindowCountInfo
}

type ClusterWindowCountInfo struct {
	Occurrences int64
	TotalTDOA   float64
	Start       time.Time
	End         time.Time
}
