package model

type MostLikelyEstimatorPair struct {
	PreviousNode *ClusterNode        `json:"previous_node"`
	NextNodes    []SimpleClusterNode `json:"next_nodes"`
}
