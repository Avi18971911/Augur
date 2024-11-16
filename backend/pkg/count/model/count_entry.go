package model

type CountEntry struct {
	ClusterId     string `json:"cluster_id"`
	CoClusterId   string `json:"co_cluster_id"`
	Occurrences   int64  `json:"occurrences"`
	CoOccurrences int64  `json:"co_occurrences"`
}
