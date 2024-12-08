curl -X GET "http://localhost:9200/log_index/_search" -H "Content-Type: application/json" -d'
{
  "query" : {
    "term" : {
      "cluster_id" : {
        "value" : "9837ce87-0b1e-44ca-a39d-02225871dc0c"
      }
    }
  }
}'
