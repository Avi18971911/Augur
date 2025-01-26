curl -X GET "http://localhost:9200/log_index/_search?pretty" -H "Content-Type: application/json" -d'
{
    "size": 0,
  "aggs": {
    "unique_cluster_ids": {
      "cardinality": {
        "field": "cluster_id"
      }
    }
  }
}'
