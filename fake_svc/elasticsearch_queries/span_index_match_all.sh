curl -X GET "http://localhost:9200/span_index/_search" -H "Content-Type: application/json" -d'
{
  "query" : {
    "match_all" : {}
  },
  "sort": [
    {"created_at": "asc"}
  ],
  "search_after": ["0001-01-01T00:00:00Z"]
}'
