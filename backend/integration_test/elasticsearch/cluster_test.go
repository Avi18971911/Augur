package elasticsearch

import (
	"context"
	clusterModel "github.com/Avi18971911/Augur/pkg/cluster/model"
	clusterService "github.com/Avi18971911/Augur/pkg/cluster/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSpanClusterUpdates(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	cls := clusterService.NewClusterService(ac, logger)
	t.Run("should be able to process and update spans of the same type", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		const notAssigned = "NOT_ASSIGNED"
		onlyTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		const clusterEvent = "service=loadgenerator,operation=GET,kind=SPAN_KIND_CLIENT"
		spanBatch := []spanModel.Span{
			{
				ClusterId:    notAssigned,
				StartTime:    onlyTimeStamp,
				EndTime:      onlyTimeStamp,
				CreatedAt:    createdAt.Add(-time.Second * 500),
				ClusterEvent: clusterEvent,
				Events:       make([]spanModel.SpanEvent, 0),
				Attributes:   make(map[string]string),
			},
			{
				ClusterId:    notAssigned,
				StartTime:    onlyTimeStamp,
				EndTime:      onlyTimeStamp,
				CreatedAt:    createdAt.Add(-time.Second * 400),
				ClusterEvent: clusterEvent,
				Events:       make([]spanModel.SpanEvent, 0),
				Attributes:   make(map[string]string),
			},
		}
		err = loadDataIntoElasticsearch(ac, spanBatch, bootstrapper.SpanIndexName)
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		input := clusterModel.ClusterInput{
			DataType:    clusterModel.SpanClusterInputType,
			TextualData: clusterEvent,
			ClusterId:   notAssigned,
			Id:          "test",
		}
		output, err := cls.ClusterData(ctx, input)
		if err != nil {
			t.Errorf("Failed to cluster span with error: %v", err)
		}
		assert.NotZero(t, len(output))
		hasTest := output[0].ObjectId == "test"
		firstClusterId := output[0].ClusterId
		firstId := output[0].ObjectId
		for _, cluster := range output[1:] {
			if cluster.ObjectId == "test" {
				hasTest = true
			}
			assert.Equal(t, firstClusterId, cluster.ClusterId)
			assert.NotEqual(t, firstId, cluster.ObjectId)
		}
		assert.True(t, hasTest)
	})

	t.Run("should not assign spans with different cluster events to the same cluster id", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		const notAssigned = "NOT_ASSIGNED"
		onlyTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		const clusterEvent = "service=loadgenerator,operation=GET,kind=SPAN_KIND_CLIENT"
		spanBatch := []spanModel.Span{
			{
				ClusterId:    notAssigned,
				StartTime:    onlyTimeStamp,
				EndTime:      onlyTimeStamp,
				CreatedAt:    createdAt.Add(-time.Second * 500),
				ClusterEvent: clusterEvent,
				Events:       make([]spanModel.SpanEvent, 0),
				Attributes:   make(map[string]string),
			},
			{
				ClusterId:    notAssigned,
				StartTime:    onlyTimeStamp,
				EndTime:      onlyTimeStamp,
				CreatedAt:    createdAt.Add(-time.Second * 400),
				ClusterEvent: clusterEvent,
				Events:       make([]spanModel.SpanEvent, 0),
				Attributes:   make(map[string]string),
			},
		}
		err = loadDataIntoElasticsearch(ac, spanBatch, bootstrapper.SpanIndexName)
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		input := clusterModel.ClusterInput{
			DataType:    clusterModel.SpanClusterInputType,
			TextualData: "not the same cluster event indeed",
			ClusterId:   notAssigned,
			Id:          "test",
		}
		output, err := cls.ClusterData(ctx, input)
		if err != nil {
			t.Errorf("Failed to cluster span with error: %v", err)
		}
		assert.NotZero(t, len(output))
		assert.Equal(t, 1, len(output))
		assert.Equal(t, "test", output[0].ObjectId)
	})
}
