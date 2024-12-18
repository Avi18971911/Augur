package service

import "time"

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

func getCountClusterDetailsQuery(countId string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"_id": countId,
			},
		},
	}
}

func getLogsAndSpansAroundTimeQuery(clusterId string, timeSlice time.Time, window time.Duration) map[string]interface{} {
	fromTime := timeSlice.Add(-window)
	toTime := timeSlice.Add(window)

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
								"gte": fromTime,
								"lte": toTime,
							},
						},
					},
					{
						"bool": map[string]interface{}{
							"must": []map[string]interface{}{
								{
									"range": map[string]interface{}{
										"start_time": map[string]interface{}{
											"lte": toTime,
										},
									},
								},
								{
									"range": map[string]interface{}{
										"end_time": map[string]interface{}{
											"gte": fromTime,
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
