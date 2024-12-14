package bootstrapper

const ClusterIndexName = "cluster_index"

var clusterIndex = map[string]interface{}{
	"settings": map[string]interface{}{
		"number_of_shards":   1,
		"number_of_replicas": 1,
	},
	"mappings": map[string]interface{}{
		"properties": map[string]interface{}{
			"created_at": map[string]interface{}{
				"type": "date",
			},
			"cluster_id": map[string]interface{}{
				"type": "keyword",
			},
			"co_cluster_id": map[string]interface{}{
				"type": "keyword",
			},
			"occurrences": map[string]interface{}{
				"type": "integer",
			},
			"co_occurrences": map[string]interface{}{
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
