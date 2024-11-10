curl -X GET "http://localhost:9200/log_index/_search" -H "Content-Type: application/json" -d'
{
  "query": {
    "bool": {
      "must": [
        {
          "more_like_this": {
            "fields": ["message"],
            "like": "[LocalLog partition=__cluster_metadata-0, dir=/tmp/kafka-logs] Deleting segment files LogSegment(baseOffset=1534, size=2160, lastModifiedTime=1730214342744, largestRecordTimestamp=1730214342718)",
            "min_term_freq": 1,
            "min_doc_freq": 1,
            "minimum_should_match": "50%"
          }
        },
        {
          "term": {
            "service": "kafka.log.LocalLog$"
          }
        }
      ]
    }
  }
}'

