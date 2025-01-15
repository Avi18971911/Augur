package bootstrapper

const ClusterTotalCountIndexName = "cluster_total_count_index"

var clusterTotalCountIndex = map[string]interface{}{
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
			"total_instances": map[string]interface{}{
				"type": "integer",
			},
			"total_instances_with_co_cluster": map[string]interface{}{
				"type": "integer",
			},
		},
	},
}
