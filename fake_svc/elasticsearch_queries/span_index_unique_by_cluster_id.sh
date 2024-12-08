curl -X GET "http://localhost:9200/span_index/_search" -H "Content-Type: application/json" -d'
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
