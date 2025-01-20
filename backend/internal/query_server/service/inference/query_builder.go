package inference

import "time"

func getSucceedingClusterIdsQuery(clusterId string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"_id": clusterId,
						},
					},
				},
			},
		},
	}
}

func getPrecedingClusterIdsQuery(clusterId string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"causes_clusters": clusterId,
			},
		},
	}
}

func getClusterTotalCountDetailsQuery(clusterId string, coClusterId string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"cluster_id": clusterId,
						},
					},
					{
						"term": map[string]interface{}{
							"co_cluster_id": coClusterId,
						},
					},
				},
			},
		},
	}
}

func getClusterWindowCountBestCandidates(clusterId string, coClusterId string) map[string]interface{} {
	return map[string]interface{}{
		"size": 1,
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"cluster_id": clusterId,
						},
					},
					{
						"term": map[string]interface{}{
							"co_cluster_id": coClusterId,
						},
					},
				},
			},
		},
		"sort": []map[string]interface{}{
			{
				"occurrences": map[string]interface{}{
					"order": "desc",
				},
			},
		},
	}
}

func getLogsAndSpansAroundTimeQuery(clusterId string, startTime time.Time, endTime time.Time) map[string]interface{} {

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
				"should": []map[string]interface{}{
					{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte": startTime,
								"lte": endTime,
							},
						},
					},
					{
						"bool": map[string]interface{}{
							"must": []map[string]interface{}{
								{
									"range": map[string]interface{}{
										"start_time": map[string]interface{}{
											"lte": startTime,
										},
									},
								},
								{
									"range": map[string]interface{}{
										"end_time": map[string]interface{}{
											"gte": endTime,
										},
									},
								},
							},
						},
					},
				},
				"minimum_should_match": 1,
			},
		},
	}
}

func getLogOrSpanQuery(id string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"_id": id,
			},
		},
	}
}
