curl -X GET "http://localhost:9200/span_index/_search" -H "Content-Type: application/json" -d'
{
  "query" : {
    "bool" : {
      "minimum_should_match" : 1,
      "must_not" : [ {
        "term" : {
          "cluster_id" : "dc2d0b04-3ba0-48bb-9766-487cce46def7"
        }
      } ],
      "should" : [ {
        "range" : {
          "timestamp" : {
            "gte" : "2024-11-19T14:22:22.134569167Z",
            "lte" : "2024-11-19T14:22:24.635116917Z"
          }
        }
      }, {
        "bool" : {
          "must" : [ {
            "range" : {
              "start_time" : {
                "lte" : "2024-11-19T14:22:24.635116917Z"
              }
            }
          }, {
            "range" : {
              "end_time" : {
                "gte" : "2024-11-19T14:22:22.134569167Z"
              }
            }
          } ]
        }
      } ]
    }
  }
}'

