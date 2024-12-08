curl -X POST "http://localhost:9200/log_index/_doc" -H "Content-Type: application/json" -d'
{
  "message": "Test log message",
  "service": "test-service",
  "severity": "info",
  "timestamp": "2024-12-03T00:00:00Z"
}'
