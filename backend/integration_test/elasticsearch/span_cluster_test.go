package elasticsearch

import (
	"context"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
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
	ac := client.NewAugurClientImpl(es, client.Immediate)
	spanClusterService := service.NewSpanClusterService(ac, logger)
	t.Run("should be able to process and update spans of the same type", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		err = loadTestDataFromFile(es, bootstrapper.SpanIndexName, "data_dump/span_index_array.json")
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		span := model.Span{
			ClusterEvent: "service=loadgenerator,operation=GET,kind=SPAN_KIND_CLIENT,attributes=map[http.method:GET http.url:http://frontend-proxy:8080/api/cart]",
			Id:           "test",
		}
		ids, spanDocs, err := spanClusterService.ClusterSpan(ctx, span)
		if err != nil {
			t.Errorf("Failed to parse span with message: %v", err)
		}
		assert.NotZero(t, len(spanDocs))
		assert.NotEqual(t, 1, len(spanDocs))
		var equalClusterId = spanDocs[0]["cluster_id"]
		var testClusterId string
		if ids[0] == "test" {
			testClusterId = spanDocs[0]["cluster_id"].(string)
		}
		for i, doc := range spanDocs[1:] {
			assert.Equal(t, equalClusterId, doc["cluster_id"])
			if ids[i+1] == "test" {
				testClusterId = doc["cluster_id"].(string)
			}
		}
		assert.NotEqual(t, "", testClusterId)
	})

	t.Run("should not assign spans with different cluster events to the same cluster id", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		err = loadTestDataFromFile(es, bootstrapper.SpanIndexName, "data_dump/span_index_array.json")
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		span := model.Span{
			ClusterEvent: "service=loadgenerator,operation=GET,kind=SPAN_KIND_CLIENT,attributes=map[http.method:GET http.url:http://frontend-proxy:8080/api/cart]",
			Id:           "test",
		}
		clusterOneIds, clusterOneDocs, err := spanClusterService.ClusterSpan(ctx, span)
		if err != nil {
			t.Errorf("Failed to parse span with message: %v", err)
		}
		span2 := model.Span{
			ClusterEvent: "service=flagd,operation=resolveInt,kind=SPAN_KIND_INTERNAL,attributes=map[]",
			Id:           "test",
		}
		clusterTwoIds, clusterTwoDocs, err := spanClusterService.ClusterSpan(ctx, span2)
		if err != nil {
			t.Errorf("Failed to parse span with message: %v", err)
		}
		var clusterIdOne = clusterOneDocs[0]["cluster_id"]
		var clusterIdTwo = clusterTwoDocs[0]["cluster_id"]

		assert.NotZero(t, len(clusterTwoDocs))
		assert.NotZero(t, len(clusterOneDocs))
		assert.NotEqual(t, 1, len(clusterTwoDocs))
		assert.NotEqual(t, 1, len(clusterOneDocs))

		var clusterOneId string
		var clusterTwoId string
		if clusterOneIds[0] == "test" {
			clusterOneId = clusterOneDocs[0]["cluster_id"].(string)
		}
		for i, doc := range clusterOneDocs[1:] {
			assert.Equal(t, clusterIdOne, doc["cluster_id"])
			assert.NotEqual(t, clusterIdTwo, doc["cluster_id"])
			if clusterOneIds[i+1] == "test" {
				clusterOneId = doc["cluster_id"].(string)
			}
		}
		if clusterTwoIds[0] == "test" {
			clusterTwoId = clusterTwoDocs[0]["cluster_id"].(string)
		}
		for i, doc := range clusterTwoDocs[1:] {
			assert.Equal(t, clusterIdTwo, doc["cluster_id"])
			assert.NotEqual(t, clusterIdOne, doc["cluster_id"])
			if clusterTwoIds[i+1] == "test" {
				clusterTwoId = doc["cluster_id"].(string)
			}
		}
		assert.NotEqual(t, "", clusterOneId)
		assert.NotEqual(t, "", clusterTwoId)
	})
}
