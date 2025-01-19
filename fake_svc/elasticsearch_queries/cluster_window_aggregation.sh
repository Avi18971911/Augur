curl -X POST "http://localhost:9200/cluster_window_count_index/_search?pretty" -H "Content-Type: application/json" -d'
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "cluster_id": "3543ef7c-f1d4-4bfd-a70d-571fb8fbad37" } },
        { "term": { "co_cluster_id": "77f22acf-83a8-4640-a2ab-ae51b04c5892" } }
      ]
    }
  },
  "sort": [
    { "occurrences": { "order": "desc" } }
  ]
}'
