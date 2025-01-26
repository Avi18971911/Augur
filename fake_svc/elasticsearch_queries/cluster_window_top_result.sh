curl -X POST "http://localhost:9201/cluster_window_count_index/_search?pretty" -H "Content-Type: application/json" -d'
{
  "query" : {
    "bool" : {
      "must" : [ {
        "term" : {
          "cluster_id" : "d07ad010-acd1-4e5a-a121-e584dc791d38"
        }
      }, {
        "term" : {
          "co_cluster_id" : "4f25dbbd-4035-4397-a6e3-a3a94ebe2c2d"
        }
      } ]
    }
  },
  "size" : 1,
  "sort" : [ {
    "occurrences" : {
      "order" : "desc"
    }
  } ]
}'
