package model

type ClusterTotalCountEntry struct {
	Id                          string `json:"_id,omitempty"`
	CreatedAt                   string `json:"created_at"`
	ClusterId                   string `json:"cluster_id"`
	CoClusterId                 string `json:"co_cluster_id"`
	TotalInstances              int64  `json:"total_instances"`
	TotalInstancesWithCoCluster int64  `json:"total_instances_with_co_cluster"`
}

type ClusterWindowCountEntry struct {
	Id           string  `json:"_id,omitempty"`
	ClusterId    string  `json:"cluster_id"`
	CoClusterId  string  `json:"co_cluster_id"`
	Start        float64 `json:"start"`
	End          float64 `json:"end"`
	Occurrences  int64   `json:"occurrences"`
	MeanTDOA     float64 `json:"mean_TDOA"`
	VarianceTDOA float64 `json:"variance_TDOA"`
}
