package bootstrapper

const ClusterWindowCountIndexName = "cluster_window_count_index"

/**
 * Note that the id will be the composite key cluster_id;co_cluster_id
 */
var clusterWindowCountIndex = map[string]interface{}{
	"settings": map[string]interface{}{
		"number_of_shards":   1,
		"number_of_replicas": 1,
	},
	"mappings": map[string]interface{}{
		"properties": map[string]interface{}{
			"start": map[string]interface{}{
				"type": "date",
			},
			"end": map[string]interface{}{
				"type": "date",
			},
			"occurrences": map[string]interface{}{
				"type": "integer",
			},
			"mean_TDOA": map[string]interface{}{
				"type": "float",
			},
			"variance_TDOA": map[string]interface{}{
				"type": "float",
			},
		},
	},
}
