package model

type ClusterTotalCountInfo struct {
	Occurrences            int64
	ClusterWindowCountInfo map[string]ClusterWindowCountInfo
}

type ClusterWindowCountInfo struct {
	Occurrences int64
	TotalTDOA   float64
	Start       float64
	End         float64
}
