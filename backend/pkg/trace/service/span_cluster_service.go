package service

import (
	"context"
	"encoding/json"
	"fmt"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"time"
)

const scTimeOut = 1000 * time.Millisecond

type SpanClusterService interface {
	ClusterAndUpdateSpans(ctx context.Context, span model.Span) (model.Span, error)
}

type SpanClusterServiceImpl struct {
	ac     augurElasticsearch.AugurClient
	logger *zap.Logger
}

func NewSpanClusterService(ac augurElasticsearch.AugurClient, logger *zap.Logger) SpanClusterService {
	return &SpanClusterServiceImpl{
		ac:     ac,
		logger: logger,
	}
}

// TODO: Consider making a Log Repository that handles this Elasticsearch logic
func moreLikeThisQueryBuilder(phrase string) map[string]interface{} {
	// more_like_this: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html
	// bool: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html
	return map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{
					{
						"more_like_this": map[string]interface{}{
							"fields":               []string{"cluster_event"},
							"like":                 phrase,
							"min_term_freq":        1,
							"min_doc_freq":         1,
							"minimum_should_match": "50%",
						},
					},
				},
			},
		},
	}
}

func getSpansWithClusterId(spans []model.Span) []model.Span {
	newSpans := make([]model.Span, len(spans))
	var clusterId string
	for _, span := range spans {
		if span.ClusterId != "" {
			clusterId = span.ClusterId
			break
		}
	}
	if clusterId == "" {
		clusterId = uuid.NewString()
	}
	for i, span := range spans {
		newSpans[i] = model.Span{
			Id:           span.Id,
			SpanID:       span.SpanID,
			ParentSpanID: span.ParentSpanID,
			TraceID:      span.TraceID,
			ServiceName:  span.ServiceName,
			StartTime:    span.StartTime,
			EndTime:      span.EndTime,
			ActionName:   span.ActionName,
			SpanKind:     span.SpanKind,
			ClusterEvent: span.ClusterEvent,
			ClusterId:    clusterId,
			Attributes:   span.Attributes,
			Events:       span.Events,
		}
	}
	return newSpans
}

func (scs *SpanClusterServiceImpl) ClusterAndUpdateSpans(
	ctx context.Context,
	span model.Span,
) (model.Span, error) {
	queryBody, err := json.Marshal(moreLikeThisQueryBuilder(span.ClusterEvent))
	if err != nil {
		return model.Span{}, fmt.Errorf("failed to marshal query body for elasticsearch query: %w", err)
	}
	var querySize = 100
	queryCtx, queryCancel := context.WithTimeout(ctx, scTimeOut)
	defer queryCancel()
	res, err := scs.ac.Search(queryCtx, string(queryBody), augurElasticsearch.SpanIndexName, &querySize)
	if err != nil {
		return model.Span{}, fmt.Errorf("failed to search for similar spans in Elasticsearch: %w", err)
	}
	totalSpans, err := ConvertToSpanDocuments(res)
	if err != nil {
		return model.Span{}, fmt.Errorf("failed to convert search results to span documents: %w", err)
	}
	totalSpans = append(totalSpans, span)

	clusteredSpans := getSpansWithClusterId(totalSpans)

	// last span is the new one so don't update it
	ids := make([]string, len(clusteredSpans)-1)
	fieldList := make([]map[string]interface{}, len(clusteredSpans)-1)
	for idx, span := range clusteredSpans[:len(clusteredSpans)-1] {
		ids[idx] = span.Id
		fieldList[idx] = map[string]interface{}{
			"cluster_id": span.ClusterId,
		}
	}
	updateCtx, updateCancel := context.WithTimeout(ctx, scTimeOut)
	defer updateCancel()
	if len(fieldList) != 0 {
		err = scs.ac.BulkUpdate(updateCtx, ids, fieldList, augurElasticsearch.SpanIndexName)
		if err != nil {
			return model.Span{}, fmt.Errorf("failed to update similar span in Elasticsearch: %w", err)
		}
	}

	newLogEntry := clusteredSpans[len(clusteredSpans)-1]

	return newLogEntry, nil
}

func ConvertToSpanDocuments(res []map[string]interface{}) ([]model.Span, error) {
	var spans []model.Span
	for _, hit := range res {
		doc := model.Span{}

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
		startTimeParsed, err := augurElasticsearch.NormalizeTimestampToNanoseconds(startTime)
		if err != nil {
			return nil, fmt.Errorf("failed to parse start_time to time.Time")
		}
		doc.StartTime = startTimeParsed

		endTime, ok := hit["end_time"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert end_time to string %s", hit["end_time"])
		}
		endTimeParsed, err := augurElasticsearch.NormalizeTimestampToNanoseconds(endTime)
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
		doc.Events = make([]model.SpanEvent, len(events))
		for i, event := range events {
			doc.Events[i] = typeEvent(event)
		}

		spans = append(spans, doc)
	}
	return spans, nil
}

func typeEvent(event interface{}) model.SpanEvent {
	eventMap := event.(map[string]interface{})
	eventName := eventMap["name"].(string)
	eventAttributes := eventMap["attributes"].(map[string]interface{})
	eventTimestamp := eventMap["timestamp"].(string)
	eventTimestampParsed, err := augurElasticsearch.NormalizeTimestampToNanoseconds(eventTimestamp)
	if err != nil {
		return model.SpanEvent{}
	}
	return model.SpanEvent{
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