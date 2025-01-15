package bootstrapper

const ClusterWindowCountIndexName = "cluster_window_count_index"

/**
 * Note that the id will be the composite key cluster_id;co_cluster_id;start
 */
var clusterWindowCountIndex = map[string]interface{}{
	"settings": map[string]interface{}{
		"number_of_shards":   1,
		"number_of_replicas": 1,
	},
	"mappings": map[string]interface{}{
		"properties": map[string]interface{}{
			"cluster_id": map[string]interface{}{
				"type": "keyword",
			},
			"co_cluster_id": map[string]interface{}{
				"type": "keyword",
			},
			"start": map[string]interface{}{
				"type": "double",
			},
			"end": map[string]interface{}{
				"type": "double",
			},
			"occurrences": map[string]interface{}{
				"type": "integer",
			},
			"mean_TDOA": map[string]interface{}{
				"type": "double",
			},
			"variance_TDOA": map[string]interface{}{
				"type": "double",
			},
		},
	},
}
