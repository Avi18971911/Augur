curl -X GET "http://localhost:9201/cluster_window_count_index/_search" -H "Content-Type: application/json" -d'
{
  "query" : {
    "term" : {
      "cluster_id" : {
        "value" : "4f25dbbd-4035-4397-a6e3-a3a94ebe2c2d"
      }
    }
  }
}'
