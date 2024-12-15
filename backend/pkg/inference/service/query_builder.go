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

func getSuceedingClusterIdsQuery(clusterId string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"cluster_id": clusterId,
						},
					},
				},
			},
		},
	}
}

func getPreceedingClusterIdsQuery(clusterId string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"causes_clusters": clusterId,
			},
		},
	}
}
