curl -X GET "http://localhost:9200/log_index/_analyze" -H "Content-Type: application/json" -d'
{
  "analyzer": "message_analyzer",
  "text": "Login request successful with username: fake_username and password: fake_password"
}'
