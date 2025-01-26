curl -X GET "http://localhost:9200/log_index/_search" -H "Content-Type: application/json" -d'
{
  "query": {
    "bool": {
      "must": [
        {
          "match_phrase": {
            "message": "Login request received with username: Bob"
          }
        }
      ],
      "must_not": [
        {
          "term": {
            "cluster_id": "b67bb5d4-0ad4-4a97-a8bd-e31e84d63188"
          }
        }
      ]
    }
  }
}'
