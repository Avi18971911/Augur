curl -X GET "http://localhost:9200/count_index/_search" -H "Content-Type: application/json" -d'
{
    "query" : {
      "bool" : {
        "must" : [ {
          "term" : {
            "cluster_id" : "initialClusterId"
          }
        } ],
        "must_not" : [ {
          "terms" : {
            "co_cluster_id" : [ ]
          }
        } ]
      }
    }
}'
