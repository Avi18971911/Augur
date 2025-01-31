curl -X POST "http://localhost:9200/cluster_window_count_index/_search?pretty" -H "Content-Type: application/json" -d'
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "cluster_id": "5dfc0833-ff93-4967-9144-36ef78791748" } },
        { "term": { "co_cluster_id": "c6f025c9-be5b-4196-91b8-c622cb5b4f7a" } }
      ]
    }
  },
  "sort": [
    { "occurrences": { "order": "desc" } }
  ]
}'
