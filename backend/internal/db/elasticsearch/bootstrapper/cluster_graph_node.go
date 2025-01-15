package bootstrapper

const ClusterGraphNodeIndexName = "cluster_graph_node_index"

var clusterGraphNodeIndex = map[string]interface{}{
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
