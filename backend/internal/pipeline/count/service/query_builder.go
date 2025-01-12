package service

import (
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	clusterService "github.com/Avi18971911/Augur/internal/pipeline/cluster/service"
	"time"
)

func buildGetCoOccurringClustersQuery(clusterId string, fromTime time.Time, toTime time.Time) map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must_not": []map[string]interface{}{
					{
						"terms": map[string]interface{}{
							"cluster_id": []string{clusterId, clusterService.DefaultClusterId},
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
		"_source": []string{"cluster_id", "start_time", "end_time", "timestamp "}, // Retrieve only the cluster IDs
	}
}

func buildGetNonMatchedCoClusterIdsQuery(
	clusterId string,
	matchedCoClusterIds []string,
) map[string]interface{} {
	matchedCoClusterIds = append(matchedCoClusterIds, clusterService.DefaultClusterId)
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
				"must_not": []map[string]interface{}{
					{
						"terms": map[string]interface{}{
							"co_cluster_id": matchedCoClusterIds,
						},
					},
				},
			},
		},
		"_source": []string{"co_cluster_id"}, // Retrieve only the co-cluster IDs
	}
}

func buildIncrementNonMatchedCoClusterIdsQuery(
	id string,
) (client.MetaMap, client.DocumentMap) {
	updateStatement := map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.total_instances += params.increment",
			"params": map[string]interface{}{
				"increment": 1,
			},
		},
	}
	metaInfo := map[string]interface{}{
		"update": map[string]interface{}{
			"_id":               id,
			"_index":            bootstrapper.ClusterTotalCountIndexName,
			"retry_on_conflict": 5,
		},
	}
	return metaInfo, updateStatement
}

func buildUpdateClusterTotalCountsQuery(
	id string,
	clusterId string,
	otherClusterId string,
) (client.MetaMap, client.DocumentMap) {
	updateStatement := map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.total_instances = ctx._source.total_instances + params.increment;" +
				"ctx._source.total_instances_with_co_cluster = ctx._source.total_instances_with_co_cluster + params.increment;",
			"params": map[string]interface{}{
				"increment": 1,
			},
		},
		"upsert": map[string]interface{}{
			"created_at":                      time.Now().UTC(),
			"cluster_id":                      clusterId,
			"co_cluster_id":                   otherClusterId,
			"total_instances":                 1,
			"total_instances_with_co_cluster": 1,
		},
	}

	metaInfo := map[string]interface{}{
		"update": map[string]interface{}{
			"_id":               id,
			"_index":            bootstrapper.ClusterTotalCountIndexName,
			"retry_on_conflict": 5,
		},
	}
	return metaInfo, updateStatement
}

func buildUpdateClusterWindowCountsQuery(
	id string,
	clusterId string,
	coClusterId string,
	newTDOA float64,
	start float64,
	end float64,
) (client.MetaMap, client.DocumentMap) {
	updateStatement := map[string]interface{}{
		"script": map[string]interface{}{
			"source": "ctx._source.occurrences += params.increment;" +
				// Update mean and variance using Welford's online algorithm
				"def delta = params.new_value - ctx._source.mean_TDOA;" +
				"ctx._source.mean_TDOA = ctx._source.mean_TDOA + (delta * 1.0) / ctx._source.co_occurrences;" +
				"def delta2 = params.new_value - ctx._source.mean_TDOA;" +
				"ctx._source.variance_TDOA = ctx._source.variance_TDOA + delta * delta2;",
			"params": map[string]interface{}{
				"increment": 1,
				"new_value": newTDOA,
			},
		},
		"upsert": map[string]interface{}{
			"cluster_id":    clusterId,
			"co_cluster_id": coClusterId,
			"start":         start,
			"end":           end,
			"occurrences":   1,
			"mean_TDOA":     newTDOA,
			"variance_TDOA": 0,
		},
	}

	metaInfo := map[string]interface{}{
		"update": map[string]interface{}{
			"_id":               id,
			"_index":            bootstrapper.ClusterWindowCountIndexName,
			"retry_on_conflict": 5,
		},
	}
	return metaInfo, updateStatement
}

func buildGetClusterWindowsQuery(clusterId string, coClusterIds []string) map[string]interface{} {

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
						"terms": map[string]interface{}{
							"co_cluster_id": coClusterIds,
						},
					},
				},
			},
		},
	}
}
