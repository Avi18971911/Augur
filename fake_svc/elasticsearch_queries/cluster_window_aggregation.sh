curl -X POST "http://localhost:9200/cluster_window_count_index/_search?pretty" -H "Content-Type: application/json" -d'
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "cluster_id": "a9bb5eee-e5bb-4f1e-90ae-8d869a3e6bd4" } },
        { "term": { "co_cluster_id": "6466e406-6c05-4785-b76a-c50d4f420326" } }
      ]
    }
  },
  "sort": [
    { "occurrences": { "order": "desc" } }
  ]
}'
