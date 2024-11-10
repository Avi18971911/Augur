package elasticsearch

const LogIndexName = "log_index"

var logIndex = map[string]interface{}{
	"settings": map[string]interface{}{
		"number_of_shards":   1,
		"number_of_replicas": 1,
	},
	"mappings": map[string]interface{}{
		"properties": map[string]interface{}{
			"timestamp": map[string]interface{}{
				"type": "date",
			},
			"service": map[string]interface{}{
				"type": "keyword",
			},
			"severity": map[string]interface{}{
				"type": "keyword",
			},
			"message": map[string]interface{}{
				"type": "text",
			},
			"trace_id": map[string]interface{}{
				"type": "keyword",
			},
			"span_id": map[string]interface{}{
				"type": "keyword",
			},
			"cluster_id": map[string]interface{}{
				"type": "keyword",
			},
		},
	},
}
