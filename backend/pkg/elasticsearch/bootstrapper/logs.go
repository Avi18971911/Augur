package bootstrapper

const LogIndexName = "log_index"

var logIndex = map[string]interface{}{
	"settings": map[string]interface{}{
		"number_of_shards":   1,
		"number_of_replicas": 1,
		"analysis": map[string]interface{}{
			"analyzer": map[string]interface{}{
				"message_analyzer": map[string]interface{}{
					"type":      "custom",
					"tokenizer": "standard",
					"filter":    []string{"lowercase", "stop"},
				},
			},
		},
	},
	"mappings": map[string]interface{}{
		"properties": map[string]interface{}{
			"created_at": map[string]interface{}{
				"type": "date",
			},
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
				"type":     "text",
				"analyzer": "message_analyzer",
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
			"token_length": map[string]interface{}{
				"type": "integer",
			},
		},
	},
}

const tokenLengthPipelineName = "token_length_pipeline"

var tokenLengthSettings = map[string]interface{}{
	"index": map[string]interface{}{
		"default_pipeline": tokenLengthPipelineName,
	},
}

var tokenLengthPipeline = map[string]interface{}{
	"description": "Pipeline to process token length",
	"processors": []map[string]interface{}{
		{
			"script": map[string]interface{}{
				"source": "def tokens = ctx.message.tokenize();" +
					"ctx.token_length = tokens.size();",
			},
		},
	},
}
