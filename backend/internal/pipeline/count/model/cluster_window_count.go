package model

import "time"

type ClusterWindowCount struct {
	ClusterId    string
	CoClusterId  string
	Start        time.Time
	End          time.Time
	Occurrences  int64
	MeanTDOA     float64
	VarianceTDOA float64
}
