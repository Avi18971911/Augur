package client

import (
	"encoding/json"
	"fmt"
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
