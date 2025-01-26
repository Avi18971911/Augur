curl -X POST "http://localhost:9201/cluster_window_count_index/_search?pretty" -H "Content-Type: application/json" -d'
{
  "aggs" : {
    "group_by_cluster" : {
      "aggs" : {
        "top_hit" : {
          "top_hits" : {
            "size" : 1,
            "sort" : [ {
              "occurrences" : {
                "order" : "desc"
              }
            } ]
          }
        }
      },
      "terms" : {
        "field" : "cluster_id",
        "size" : 1000
      }
    }
  },
  "query" : {
    "term" : {
      "co_cluster_id" : "d07ad010-acd1-4e5a-a121-e584dc791d38"
    }
  },
  "size" : 0
}'
