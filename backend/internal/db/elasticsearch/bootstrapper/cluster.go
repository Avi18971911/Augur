package bootstrapper

const ClusterIndexName = "cluster_index"

var clusterIndex = map[string]interface{}{
	"settings": map[string]interface{}{
		"number_of_shards":   1,
		"number_of_replicas": 1,
	},
	"mappings": map[string]interface{}{
		"properties": map[string]interface{}{
			"causes_clusters": map[string]interface{}{
				"type": "keyword",
			},
		},
	},
}
