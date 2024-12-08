curl -X POST "http://localhost:9200/count_index/_update_by_query" -H "Content-Type: application/json" -d'
{
  "query" : {
    "bool" : {
      "must" : [ {
        "term" : {
          "co_cluster_id" : "differentTime"
        }
      } ],
      "must_not" : [ {
        "terms" : {
          "cluster_id" : [ ]
        }
      } ]
    }
  },
  "script" : {
    "params" : {
      "increment" : 1
    },
    "source" : "ctx._source.occurrences += params.increment"
  }
}'
