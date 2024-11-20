package model

type CountEntry struct {
	Id            string `json:"_id,omitempty"`
	ClusterId     string `json:"cluster_id"`
	CoClusterId   string `json:"co_cluster_id"`
	Occurrences   int64  `json:"occurrences"`
	CoOccurrences int64  `json:"co_occurrences"`
}
