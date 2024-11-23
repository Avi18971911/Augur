package service

import "time"

func countCoOccurrencesQueryBuilder(clusterId string, fromTime time.Time, toTime time.Time) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must_not": []map[string]interface{}{
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

func getIncrementNonMatchedClusterIdsQuery(
	coOccurringClusterId string,
	matchedClusterIds []string,
) map[string]interface{} {
	return map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.occurrences += params.increment",
			"params": map[string]interface{}{
				"increment": 1,
			},
		},
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"co_cluster_id": coOccurringClusterId,
						},
					},
				},
				"must_not": []map[string]interface{}{
					{
						"terms": map[string]interface{}{
							"cluster_id": matchedClusterIds,
						},
					},
				},
			},
		},
	}
}
