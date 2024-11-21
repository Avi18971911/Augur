package elasticsearch

const SpanIndexName = "span_index"

var spanIndex = map[string]interface{}{
	"settings": map[string]interface{}{
		"number_of_shards":   1,
		"number_of_replicas": 1,
	},
	"mappings": map[string]interface{}{
		"properties": map[string]interface{}{
			"span_id": map[string]string{
				"type": "keyword",
			},
			"parent_span_id": map[string]string{
				"type": "keyword",
			},
			"trace_id": map[string]string{
				"type": "keyword",
			},
			"service_name": map[string]string{
				"type": "keyword",
			},
			"start_time": map[string]string{
				"type": "date",
			},
			"end_time": map[string]string{
				"type": "date",
			},
			"action_name": map[string]string{
				"type": "text",
			},
			"attributes": map[string]string{
				"type":    "object",
				"enabled": "true",
			},
			"span_kind": map[string]string{
				"type": "keyword",
			},
			"cluster_event": map[string]string{
				"type": "keyword",
			},
			"cluster_id": map[string]string{
				"type": "keyword",
			},
			"events": map[string]interface{}{
				"type": "nested",
				"properties": map[string]interface{}{
					"name": map[string]string{
						"type": "text",
					},
					"attributes": map[string]string{
						"type":    "object",
						"enabled": "true",
					},
					"timestamp": map[string]string{
						"type": "date",
					},
				},
			}},
	},
}
