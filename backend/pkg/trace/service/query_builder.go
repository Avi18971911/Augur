package service

func equalityQueryBuilder(phrase string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"cluster_event": map[string]interface{}{
					"value": phrase,
				},
			},
		},
	}
}
