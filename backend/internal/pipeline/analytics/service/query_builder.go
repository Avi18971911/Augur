package service

func buildGetRelatedClustersQuery(clusterId string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"co_cluster_id": clusterId,
						},
					},
				},
			},
		},
	}
}

func buildGetRelatedClusterWindowsQuery(clusterId string, querySize int) map[string]interface{} {
	return map[string]interface{}{
		"size": 0,
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"co_cluster_id": clusterId,
			},
		},
		"aggs": map[string]interface{}{
			"top_tags": map[string]interface{}{
				"terms": map[string]interface{}{
					"field": "cluster_id",
					"size":  querySize,
				},
				"aggs": map[string]interface{}{
					"top_tagged_hits": map[string]interface{}{
						"top_hits": map[string]interface{}{
							"size": 1,
							"sort": []map[string]interface{}{
								{
									"occurrences": map[string]interface{}{
										"order": "desc",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
