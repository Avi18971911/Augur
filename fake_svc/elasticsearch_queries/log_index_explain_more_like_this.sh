curl -X GET "http://localhost:9200/log_index/_explain/X6cuoZMBxA7z04LxJ4-t" -H "Content-Type: application/json" -d'
{
    "query": {
    "more_like_this": {
      "fields": ["message"],
      "like": "Login request received with URL /accounts/login and method POST",
      "min_term_freq": 1,
      "min_doc_freq": 1,
      "minimum_should_match": "95%"
    }
  }
}'
