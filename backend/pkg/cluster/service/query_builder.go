package service

func equalityOfClusterEventQueryBuilder(id, phrase string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": map[string]interface{}{
					"term": map[string]interface{}{
						"cluster_event": map[string]interface{}{
							"value": phrase,
						},
					},
				},
				"must_not": map[string]interface{}{
					"term": map[string]interface{}{
						"_id": id,
					},
				},
			},
		},
	}
}

func similarityToLogMessageQueryBuilder(id, service, phrase string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"match": map[string]interface{}{
							"message": map[string]interface{}{
								"query":                phrase,
								"operator":             "and",
								"minimum_should_match": "80%",
							},
						},
					},
					{
						"term": map[string]interface{}{
							"service": service,
						},
					},
				},
				"must_not": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"_id": id,
						},
					},
				},
			},
		},
	}
}
