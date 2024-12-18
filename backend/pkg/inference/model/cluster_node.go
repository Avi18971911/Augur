package model

type ClusterNode struct {
	ClusterId     string         `json:"cluster_id"`
	Successors    []*ClusterNode `json:"successors"`
	Predecessors  []*ClusterNode `json:"predecessors"`
	LogOrSpanData *LogOrSpanData
}
