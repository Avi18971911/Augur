package elasticsearch

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/Avi18971911/Augur/pkg/trace/service"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSpanClusterUpdates(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := elasticsearch.NewAugurClientImpl(es, elasticsearch.Wait)
	spanClusterer := service.NewSpanClusterService(ac, logger)
	t.Run("should be able to process and update logs of the same type", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		err = loadTestDataFromFile(es, elasticsearch.SpanIndexName, "data_dump/span_index_array.json")
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		span := model.Span{
			ClusterEvent: "service=loadgenerator,operation=GET,kind=SPAN_KIND_CLIENT,attributes=map[http.method:GET http.url:http://frontend-proxy:8080/api/cart",
		}
		newSpan, err := spanClusterer.ClusterAndUpdateSpans(ctx, span)
		if err != nil {
			t.Errorf("Failed to parse span with message: %v", err)
		}
		assert.NotEqual(t, "", newSpan.ClusterId)
		spansQuery := getSpansWithClusterIdQuery(newSpan.ClusterId)
		var querySize = 100
		docs, err := ac.Search(ctx, spansQuery, []string{elasticsearch.SpanIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for similar spans in Elasticsearch: %v", err)
		}
		spanDocs, err := service.ConvertToSpanDocuments(docs)
		if err != nil {
			t.Errorf("Failed to convert search results to span documents: %v", err)
		}
		assert.Equal(t, querySize, len(spanDocs))
		for _, doc := range spanDocs {
			assert.Equal(t, newSpan.ClusterId, doc.ClusterId)
		}
	})
}

func getSpansWithClusterIdQuery(clusterId string) string {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"cluster_id": clusterId,
			},
		},
	}
	queryBody, err := json.Marshal(query)
	if err != nil {
		panic(err)
	}
	return string(queryBody)
}
