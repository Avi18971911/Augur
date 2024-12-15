package model

type MostLikelyEstimatorPair struct {
	PreviousNode *ClusterNode   `json:"previous_node"`
	NextNodes    []*ClusterNode `json:"next_nodes"`
}
