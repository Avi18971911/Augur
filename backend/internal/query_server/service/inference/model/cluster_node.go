package model

type ClusterNode struct {
	Successors    []SimpleClusterNode
	Predecessors  []SimpleClusterNode
	LogOrSpanData LogOrSpanData
}

type SimpleClusterNode struct {
	Id          string
	ClusterId   string
	TDOA        float64
	Probability float64
}
