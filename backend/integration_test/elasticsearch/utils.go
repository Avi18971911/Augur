package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/log/model"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/elastic/go-elasticsearch/v8"
	"time"
)

func deleteAllDocuments(es *elasticsearch.Client) error {
	indexes := []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName, bootstrapper.CountIndexName}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	queryJSON, _ := json.Marshal(query)
	res, err := es.DeleteByQuery(indexes, bytes.NewReader(queryJSON), es.DeleteByQuery.WithRefresh(true))
	if err != nil {
		return fmt.Errorf("failed to delete documents by query: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("failed to delete documents in index %s", res.String())
	}
	return nil
}

func loadDataIntoElasticsearch[Data any](ac client.AugurClient, data []Data, index string) error {
	metaMap, dataMap, err := client.ToMetaAndDataMap(data)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = ac.BulkIndex(ctx, metaMap, dataMap, &index)
	if err != nil {
		return err
	}
	return nil
}

func getAllQuery() map[string]interface{} {
	return map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
}

func convertToLogDocuments(data []map[string]interface{}) ([]model.LogEntry, error) {
	var docs []model.LogEntry

	for _, item := range data {
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
		docs = append(docs, doc)
	}

	return docs, nil
}

func convertToSpanDocuments(res []map[string]interface{}) ([]spanModel.Span, error) {
	var spans []spanModel.Span
	for _, hit := range res {
		doc := spanModel.Span{}

		id, ok := hit["_id"].(string)
		if ok {
			doc.Id = id
		}

		spanId, ok := hit["span_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert span_id to string %s", hit["span_id"])
		}
		doc.SpanID = spanId

		parentSpanId, ok := hit["parent_span_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert parent_span_id to string %s", hit["parent_span_id"])
		}
		doc.ParentSpanID = parentSpanId

		traceId, ok := hit["trace_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert trace_id to string %s", hit["trace_id"])
		}
		doc.TraceID = traceId

		serviceName, ok := hit["service_name"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert service_name to string %s", hit["service_name"])
		}
		doc.ServiceName = serviceName

		startTime, ok := hit["start_time"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert start_time to string %s", hit["start_time"])
		}
		startTimeParsed, err := client.NormalizeTimestampToNanoseconds(startTime)
		if err != nil {
			return nil, fmt.Errorf("failed to parse start_time to time.Time")
		}
		doc.StartTime = startTimeParsed

		endTime, ok := hit["end_time"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert end_time to string %s", hit["end_time"])
		}
		endTimeParsed, err := client.NormalizeTimestampToNanoseconds(endTime)
		if err != nil {
			return nil, fmt.Errorf("failed to parse end_time to time.Time")
		}
		doc.EndTime = endTimeParsed

		actionName, ok := hit["action_name"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert action_name to string %s", hit["action_name"])
		}
		doc.ActionName = actionName

		spanKind, ok := hit["span_kind"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert span_kind to string %s", hit["span_kind"])
		}
		doc.SpanKind = spanKind

		clusterEvent, ok := hit["cluster_event"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert cluster_event to string %s", hit["cluster_event"])
		}
		doc.ClusterEvent = clusterEvent

		clusterId, ok := hit["cluster_id"].(string)
		if ok {
			doc.ClusterId = clusterId
		}

		attributes, ok := hit["attributes"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to convert attributes to map[string]interface{} %s", hit["attributes"])
		}
		doc.Attributes = typeAttributes(attributes)

		events, ok := hit["events"].([]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to convert events to []map[string]interface{} %s", hit["events"])
		}
		doc.Events = make([]spanModel.SpanEvent, len(events))
		for i, event := range events {
			doc.Events[i] = typeEvent(event)
		}

		spans = append(spans, doc)
	}
	return spans, nil
}

func typeEvent(event interface{}) spanModel.SpanEvent {
	eventMap := event.(map[string]interface{})
	eventName := eventMap["name"].(string)
	eventAttributes := eventMap["attributes"].(map[string]interface{})
	eventTimestamp := eventMap["timestamp"].(string)
	eventTimestampParsed, err := client.NormalizeTimestampToNanoseconds(eventTimestamp)
	if err != nil {
		return spanModel.SpanEvent{}
	}
	return spanModel.SpanEvent{
		Name:       eventName,
		Attributes: typeAttributes(eventAttributes),
		Timestamp:  eventTimestampParsed,
	}
}

func typeAttributes(attributes map[string]interface{}) map[string]string {
	typedAttributes := make(map[string]string)
	for k, v := range attributes {
		typedAttributes[k] = fmt.Sprintf("%v", v)
	}
	return typedAttributes
}
