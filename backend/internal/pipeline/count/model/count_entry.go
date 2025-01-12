package model

type ClusterTotalCountEntry struct {
	Id                          string
	CreatedAt                   string
	ClusterId                   string
	CoClusterId                 string
	TotalInstances              int64
	TotalInstancesWithCoCluster int64
}

type ClusterWindowCountEntry struct {
	Id           string
	ClusterId    string
	CoClusterId  string
	Start        float64
	End          float64
	Occurrences  int64
	MeanTDOA     float64
	VarianceTDOA float64
}
