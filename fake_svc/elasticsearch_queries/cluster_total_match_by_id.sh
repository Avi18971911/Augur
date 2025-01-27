curl -X GET "http://localhost:9201/cluster_total_count_index/_search" -H "Content-Type: application/json" -d'
{
  "query" : {
    "bool" : {
      "must" : [
        {
          "term" : {
            "co_cluster_id": "c9a262ea-0e44-4056-ad59-0a7445c44fd4"
          }
        },
        {
          "term" : {
            "cluster_id": "d07ad010-acd1-4e5a-a121-e584dc791d38"
          }
        }
      ]
    }
  }
}'
