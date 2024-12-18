package helper

import (
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/log/model"
)

func ConvertFromDocuments(documents []map[string]interface{}) ([]model.LogEntry, error) {
	var logs []model.LogEntry

	for _, item := range documents {
		doc := model.LogEntry{}

		timestamp, ok := item["timestamp"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert timestamp to string %v", item["timestamp"])
		}

		timestampParsed, err := client.NormalizeTimestampToNanoseconds(timestamp)
		if err != nil {
			return nil, fmt.Errorf("failed to convert timestamp '%s' to time.Time: %v", timestamp, err)
		}

		doc.Timestamp = timestampParsed

		createdAt, ok := item["created_at"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert created_at to string")
		}
		createdAtParsed, err := client.NormalizeTimestampToNanoseconds(createdAt)
		if err != nil {
			return nil, fmt.Errorf("failed to convert created_at '%s' to time.Time: %v", createdAt, err)
		}
		doc.CreatedAt = createdAtParsed

		severity, ok := item["severity"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert severity to string")
		}

		doc.Severity = model.Level(severity)

		message, ok := item["message"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert message to string")
		}
		doc.Message = message

		service, ok := item["service"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert service to string")
		}
		doc.Service = service

		clusterId, ok := item["cluster_id"].(string)
		if ok {
			doc.ClusterId = clusterId
		}

		doc.Id = item["_id"].(string)
		logs = append(logs, doc)
	}

	return logs, nil
}
