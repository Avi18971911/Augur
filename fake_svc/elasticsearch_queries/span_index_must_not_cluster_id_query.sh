curl -X GET "http://localhost:9200/span_index/_search" -H "Content-Type: application/json" -d'
{
  "query": {
    "bool": {
      "must_not": {
        "term": {
          "cluster_id": "86dedd59-fcb6-47fc-bf7b-1c8be00b4fea"
        }
      }
    }
  }
}'
