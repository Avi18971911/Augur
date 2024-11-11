curl -X GET "http://localhost:9200/log_index/_search" -H "Content-Type: application/json" -d'
{
  "query": {
    "bool": {
      "must": [
        {
          "more_like_this": {
            "fields": ["message"],
            "like": "[Partition __consumer_offsets-99 broker=2] Log loaded for partition __consumer_offsets-55 with initial high watermark 0",
            "min_term_freq": 1,
            "min_doc_freq": 1,
            "minimum_should_match": "50%"
          }
        },
        {
          "term": {
            "service": "kafka.cluster.Partition"
          }
        }
      ]
    }
  }
}'

