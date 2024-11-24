package service

import (
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"time"
)

func buildGetCoOccurringClustersQuery(clusterId string, fromTime time.Time, toTime time.Time) map[string]interface{} {
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
		"_source": []string{"cluster_id"}, // Retrieve only the cluster IDs
	}
}

func buildGetNonMatchedClusterIdsQuery(
	coOccurringClusterId string,
	matchedClusterIds []string,
) map[string]interface{} {
	return map[string]interface{}{
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
		"_source": []string{"cluster_id"}, // Retrieve only the cluster IDs
	}
}

func buildUpdateNonMatchedClusterIdsQuery(
	id string,
) (MetaMap, DocumentMap) {
	updateStatement := map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.occurrences += params.increment",
			"params": map[string]interface{}{
				"increment": 1,
			},
		},
	}
	metaInfo := map[string]interface{}{
		"update": map[string]interface{}{
			"_id":               id,
			"_index":            bootstrapper.CountIndexName,
			"retry_on_conflict": 5,
		},
	}
	return metaInfo, updateStatement
}

func buildUpdateClusterCountsQuery(
	id string,
	clusterId string,
	otherClusterId string,
	countInfo CountInfo,
) (MetaMap, DocumentMap) {
	updateStatement := map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.occurrences += params.occurrences; ctx._source.co_occurrences += params.co_occurrences",
			"params": map[string]interface{}{
				"occurrences":    countInfo.Occurrences,
				"co_occurrences": countInfo.CoOccurrences,
			},
		},
		"upsert": map[string]interface{}{
			"created_at":     time.Now().UTC(),
			"cluster_id":     clusterId,
			"co_cluster_id":  otherClusterId,
			"occurrences":    countInfo.Occurrences,
			"co_occurrences": countInfo.CoOccurrences,
		},
	}

	metaInfo := map[string]interface{}{
		"update": map[string]interface{}{
			"_id":               id,
			"_index":            bootstrapper.CountIndexName,
			"retry_on_conflict": 5,
		},
	}
	return metaInfo, updateStatement
}
