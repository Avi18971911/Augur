package model

type CountEntry struct {
	Id            string  `json:"_id,omitempty"`
	CreatedAt     string  `json:"created_at"`
	ClusterId     string  `json:"cluster_id"`
	CoClusterId   string  `json:"co_cluster_id"`
	Occurrences   int64   `json:"occurrences"`
	CoOccurrences int64   `json:"co_occurrences"`
	MeanTDOA      float64 `json:"mean_TDOA"`     // mean Time Difference of Arrival (TDOA) between cluster and co-cluster
	VarianceTDOA  float64 `json:"variance_TDOA"` // variance of Time Difference of Arrival (TDOA) between cluster and co-cluster
}
