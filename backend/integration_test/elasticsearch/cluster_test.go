package elasticsearch

import (
	"context"
	clusterModel "github.com/Avi18971911/Augur/pkg/cluster/model"
	clusterService "github.com/Avi18971911/Augur/pkg/cluster/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSpanCluster(t *testing.T) {
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

func TestLogCluster(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	cls := clusterService.NewClusterService(ac, logger)
	t.Run("should be able to process and update logs of the same type", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		const notAssigned = "NOT_ASSIGNED"
		onlyTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		const message = "this is a log message"
		logBatch := []logModel.LogEntry{
			{
				ClusterId: notAssigned,
				CreatedAt: createdAt.Add(-time.Second * 500),
				Timestamp: onlyTimeStamp,
				Message:   message,
			},
			{
				ClusterId: notAssigned,
				CreatedAt: createdAt.Add(-time.Second * 400),
				Timestamp: onlyTimeStamp,
				Message:   message,
			},
		}
		err = loadDataIntoElasticsearch(ac, logBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		input := clusterModel.ClusterInput{
			DataType:    clusterModel.LogClusterInputType,
			TextualData: message,
			ClusterId:   notAssigned,
			Id:          "test",
		}
		output, err := cls.ClusterData(ctx, input)
		if err != nil {
			t.Errorf("Failed to cluster span with error: %v", err)
		}
		assert.NotZero(t, len(output))
		assert.Equal(t, 3, len(output))
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

	t.Run("should be discriminatory for log messages", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		const notAssigned = "NOT_ASSIGNED"
		onlyTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		const firstMessage = "Login request successful with username: fake_username and password: fake_password"
		const nonMatchingMessage = "Login request received with URL /accounts/login and method POST"
		const serviceName = "Service"
		logBatch := []logModel.LogEntry{
			{
				ClusterId: notAssigned,
				CreatedAt: createdAt.Add(-time.Second * 500),
				Timestamp: onlyTimeStamp,
				Message:   firstMessage,
				Service:   serviceName,
			},
		}
		err = loadDataIntoElasticsearch(ac, logBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		input := clusterModel.ClusterInput{
			DataType:    clusterModel.LogClusterInputType,
			TextualData: nonMatchingMessage,
			ClusterId:   notAssigned,
			Id:          "Test",
			ServiceName: serviceName,
		}
		output, err := cls.ClusterData(ctx, input)
		if err != nil {
			t.Errorf("Failed to cluster span with error: %v", err)
		}
		assert.NotZero(t, len(output))
		assert.Equal(t, 1, len(output))
		assert.Equal(t, "Test", output[0].ObjectId)
	})

	t.Run("Should match if the variable part is exactly in order", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		const notAssigned = "NOT_ASSIGNED"
		onlyTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
		const firstMatchingMessage = "Login request successful with username: Bob and password: Barker"
		const secondMatchingMessage = "Login request successful with username: wew and password: lads"
		const nonMatchingMessage = "Login successful with username: Bob and password: Barker"
		const serviceName = "Service"
		logBatch := []logModel.LogEntry{
			{
				ClusterId: notAssigned,
				CreatedAt: createdAt.Add(-time.Second * 500),
				Timestamp: onlyTimeStamp,
				Message:   firstMatchingMessage,
				Service:   serviceName,
			},
			{
				ClusterId: notAssigned,
				CreatedAt: createdAt.Add(-time.Second * 400),
				Timestamp: onlyTimeStamp,
				Message:   nonMatchingMessage,
				Service:   serviceName,
			},
		}
		err = loadDataIntoElasticsearch(ac, logBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		input := clusterModel.ClusterInput{
			DataType:    clusterModel.LogClusterInputType,
			TextualData: secondMatchingMessage,
			ClusterId:   notAssigned,
			Id:          "Test",
			ServiceName: serviceName,
		}
		output, err := cls.ClusterData(ctx, input)
		if err != nil {
			t.Errorf("Failed to cluster span with error: %v", err)
		}
		assert.NotZero(t, len(output))
		assert.Equal(t, 2, len(output))
	})
}
