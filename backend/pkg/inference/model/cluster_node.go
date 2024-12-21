package model

type ClusterNode struct {
	Successors    []SimpleClusterNode `json:"successors"`
	Predecessors  []SimpleClusterNode `json:"predecessors"`
	LogOrSpanData LogOrSpanData
}

type SimpleClusterNode struct {
	Id        string `json:"id"`
	ClusterId string `json:"cluster_id"`
}
