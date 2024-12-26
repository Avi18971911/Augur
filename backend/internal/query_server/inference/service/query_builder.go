package service

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

func getCountClusterDetailsQuery(countId string) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"_id": countId,
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
