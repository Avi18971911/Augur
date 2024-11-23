package client

import (
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/model"
	"strings"
	"time"
)

func ToMetaAndDataMap[T any](values []T) ([]map[string]interface{}, []map[string]interface{}, error) {
	dataMap := make([]map[string]interface{}, len(values))
	metaMap := make([]map[string]interface{}, len(values))
	for i, v := range values {
		data, err := json.Marshal(v)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal value to JSON: %w", err)
		}
		var mapStruct map[string]interface{}
		if err := json.Unmarshal(data, &mapStruct); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal JSON to map: %w", err)
		}

		if id, ok := mapStruct["_id"]; ok {
			delete(mapStruct, "_id")
			metaMap[i] = map[string]interface{}{"index": map[string]interface{}{"_id": id}}
		} else {
			metaMap[i] = map[string]interface{}{"index": map[string]interface{}{}}
		}
		dataMap[i] = mapStruct
	}
	return metaMap, dataMap, nil
}

func NormalizeTimestampToNanoseconds(timestamp string) (time.Time, error) {
	isUTC := strings.HasSuffix(timestamp, "Z")
	if isUTC {
		timestamp = strings.TrimSuffix(timestamp, "Z")
	}

	parts := strings.SplitN(timestamp, ".", 2)
	if len(parts) == 2 {
		fractionalPart := parts[1]

		// 9 digits (nanosecond)
		if len(fractionalPart) > 9 {
			fractionalPart = fractionalPart[:9]
		} else if len(fractionalPart) < 9 {
			fractionalPart = fractionalPart + strings.Repeat("0", 9-len(fractionalPart))
		}

		timestamp = parts[0] + "." + fractionalPart
	}

	if isUTC {
		timestamp += "Z"
	}

	layout := "2006-01-02T15:04:05.000000000Z"
	return time.Parse(layout, timestamp)
}

func extractJSON(input string) (string, error) {
	start := -1
	for i, char := range input {
		if char == '{' {
			start = i
			break
		}
	}
	if start == -1 {
		return "", fmt.Errorf("no JSON object found in the input string")
	}
	return input[start:], nil
}

func ParseElasticSearchError(err error) (*model.ElasticsearchError, error) {
	var esError *model.ElasticsearchError
	jsonError, err := extractJSON(err.Error())
	if err != nil {
		return nil, fmt.Errorf("failed to extract JSON from error: %w", err)
	}
	if err := json.Unmarshal([]byte(jsonError), &esError); err != nil {
		return nil, fmt.Errorf("failed to unmarshal error to Elasticsearch Error: %w", err)
	}
	return esError, nil
}

func IsErrorConflict(err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	var parsedError *model.ElasticsearchError
	if parsedError, err = ParseElasticSearchError(err); err != nil {
		return false, fmt.Errorf("failed to parse elasticsearch error: %v", err)
	}
	var hasConflict = false
	for _, failure := range parsedError.Failures {
		if failure.Status == 409 {
			hasConflict = true
			break
		}
	}
	return hasConflict, nil
}
