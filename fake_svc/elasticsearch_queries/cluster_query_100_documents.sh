curl -X GET "http://localhost:9200/cluster_index/_search?pretty" -H "Content-Type: application/json" -d'
{
  "query": {
    "match_all": {}
  },
  "size": 100
}'
