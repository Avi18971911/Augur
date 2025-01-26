curl -X POST "http://localhost:9201/cluster_window_count_index/_search?pretty" -H "Content-Type: application/json" -d'
{
  "size": 0,
  "query": {
    "term": {
      "co_cluster_id": "77ebf931-28cc-424e-93be-3d4aadbbd62b"
    }
  },
  "aggs": {
    "group_by_cluster": {
      "terms": {
        "field": "cluster_id",
        "size": 1000
      },
      "aggs": {
        "top_hit": {
          "top_hits": {
            "size": 1,
            "sort": [
              {
                "occurrences": {
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  }
}'
