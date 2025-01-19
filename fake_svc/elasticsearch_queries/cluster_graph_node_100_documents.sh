curl -X GET "http://localhost:9201/cluster_graph_node_index/_search?pretty" -H "Content-Type: application/json" -d'
{
  "query": {
    "match_all": {}
  },
  "size": 100
}'
