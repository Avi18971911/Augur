package service

func equalityQueryBuilder(id, phrase string) map[string]interface{} {
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

func moreLikeThisQueryBuilder(id, service, phrase string) map[string]interface{} {
	// more_like_this: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html
	// bool: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"more_like_this": map[string]interface{}{
							"fields":               []string{"message"},
							"like":                 phrase,
							"min_term_freq":        1,
							"min_doc_freq":         1,
							"minimum_should_match": "80%",
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
