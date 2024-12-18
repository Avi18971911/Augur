package elasticsearch

import (
	"context"
	clusterService "github.com/Avi18971911/Augur/pkg/cluster/service"
	"github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	analyticsModel "github.com/Avi18971911/Augur/pkg/inference/model"
	"github.com/Avi18971911/Augur/pkg/inference/service"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"sort"
	"testing"
	"time"
)

func TestChainOfEvents(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Immediate)
	logger, err := zap.NewProduction()
	if err != nil {
		t.Errorf("Failed to create logger: %v", err)
	}
	as := service.NewAnalyticsService(
		ac,
		logger,
	)
	buckets := []model.Bucket{1000 * 30}
	indices := []string{bootstrapper.LogIndexName}
	cls := clusterService.NewClusterService(ac, logger)
	cs := countService.NewCountService(ac, logger)
	cdp := clusterService.NewClusterDataProcessor(ac, cls, logger)
	csp := countService.NewCountDataProcessorService(ac, cs, buckets, indices, logger)

	t.Run("should be able to find out a simple A-B-C relation", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		createdAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		timestampOne := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		timestampTwo := time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC)
		timestampThree := time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC)
		messageA := "The message for cluster A"
		messageB := "Der Message Cluster B"
		messageC := "El mensaje para el cluster C"
		clusterIdA := "1"
		clusterIdB := "2"
		clusterIdC := "3"
		const TDOA = time.Millisecond * 20

		logs := []logModel.LogEntry{
			{
				Id:        "1",
				ClusterId: clusterIdA,
				CreatedAt: createdAt,
				Timestamp: timestampOne,
				Severity:  "INFO",
				Message:   messageA,
				Service:   "serviceA",
			},
			{
				Id:        "2",
				ClusterId: clusterIdB,
				CreatedAt: createdAt,
				Timestamp: timestampOne.Add(TDOA),
				Severity:  "INFO",
				Message:   messageB,
				Service:   "serviceA",
			},
			{
				Id:        "3",
				ClusterId: clusterIdC,
				CreatedAt: createdAt,
				Timestamp: timestampOne.Add(TDOA * 2),
				Severity:  "INFO",
				Message:   messageC,
				Service:   "serviceA",
			},
			{
				Id:        "4",
				ClusterId: clusterIdA,
				CreatedAt: createdAt,
				Timestamp: timestampTwo,
				Severity:  "INFO",
				Message:   messageA,
				Service:   "serviceA",
			},
			{
				Id:        "5",
				ClusterId: clusterIdB,
				CreatedAt: createdAt,
				Timestamp: timestampTwo.Add(TDOA),
				Severity:  "INFO",
				Message:   messageB,
				Service:   "serviceA",
			},
			{
				Id:        "6",
				ClusterId: clusterIdC,
				CreatedAt: createdAt,
				Timestamp: timestampTwo.Add(TDOA * 2),
				Severity:  "INFO",
				Message:   messageC,
				Service:   "serviceA",
			},
			{
				Id:        "7",
				ClusterId: clusterIdA,
				CreatedAt: createdAt,
				Timestamp: timestampThree,
				Severity:  "INFO",
				Message:   messageA,
				Service:   "serviceA",
			},
			{
				Id:        "8",
				ClusterId: clusterIdB,
				CreatedAt: createdAt,
				Timestamp: timestampThree.Add(TDOA),
				Severity:  "INFO",
				Message:   messageB,
				Service:   "serviceA",
			},
			{
				Id:        "9",
				ClusterId: clusterIdC,
				CreatedAt: createdAt,
				Timestamp: timestampThree.Add(TDOA * 2),
				Severity:  "INFO",
				Message:   messageC,
				Service:   "serviceA",
			},
		}
		spanOrLogData := convertLogToSpanOrLogData(logs)

		err = loadDataIntoElasticsearch(ac, logs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load data into Elasticsearch: %v", err)
		}
		clusterOutput, err := cdp.ClusterData(context.Background(), spanOrLogData)
		assert.Nil(t, err)

		updatedClusters, err := csp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterOutput)
		assert.Nil(t, err)

		err = as.UpdateAnalytics(context.Background(), updatedClusters)
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}

		spanOrLogDatum := analyticsModel.LogOrSpanData{
			Id:         logs[0].Id,
			ClusterId:  logs[0].ClusterId,
			LogDetails: &logs[0],
		}
		graph, err := as.GetChainOfEvents(context.Background(), spanOrLogDatum)
		assert.Nil(t, err)

		clusterASuccessors := make([]string, 0)
		clusterAPredecessors := make([]string, 0)
		clusterBSuccessors := make([]string, 0)
		clusterBPredecessors := make([]string, 0)
		clusterCSuccessors := make([]string, 0)
		clusterCPredecessors := make([]string, 0)

		for _, node := range graph[clusterIdA].Successors {
			clusterASuccessors = append(clusterASuccessors, node.ClusterId)
		}
		for _, node := range graph[clusterIdA].Predecessors {
			clusterAPredecessors = append(clusterAPredecessors, node.ClusterId)
		}
		for _, node := range graph[clusterIdB].Successors {
			clusterBSuccessors = append(clusterBSuccessors, node.ClusterId)
		}
		for _, node := range graph[clusterIdB].Predecessors {
			clusterBPredecessors = append(clusterBPredecessors, node.ClusterId)
		}
		for _, node := range graph[clusterIdC].Successors {
			clusterCSuccessors = append(clusterCSuccessors, node.ClusterId)
		}
		for _, node := range graph[clusterIdC].Predecessors {
			clusterCPredecessors = append(clusterCPredecessors, node.ClusterId)
		}

		expectedClusterASuccessors := []string{clusterIdB, clusterIdC}
		sort.Strings(expectedClusterASuccessors)
		sort.Strings(clusterASuccessors)
		expectedClusterAPredecessors := []string{}
		expectedClusterBSuccessors := []string{clusterIdC}
		expectedClusterBPredecessors := []string{clusterIdA}
		expectedClusterCSuccessors := []string{}
		expectedClusterCPredecessors := []string{clusterIdA, clusterIdB}
		sort.Strings(expectedClusterCPredecessors)
		sort.Strings(clusterCPredecessors)

		assert.EqualValues(t, expectedClusterASuccessors, clusterASuccessors)
		assert.EqualValues(t, expectedClusterAPredecessors, clusterAPredecessors)
		assert.EqualValues(t, expectedClusterBSuccessors, clusterBSuccessors)
		assert.EqualValues(t, expectedClusterBPredecessors, clusterBPredecessors)
		assert.EqualValues(t, expectedClusterCSuccessors, clusterCSuccessors)
		assert.EqualValues(t, expectedClusterCPredecessors, clusterCPredecessors)

		assert.Equal(t, logs[0], *graph[clusterIdA].LogOrSpanData.LogDetails)
		assert.Equal(t, logs[1], *graph[clusterIdB].LogOrSpanData.LogDetails)
		assert.Equal(t, logs[2], *graph[clusterIdC].LogOrSpanData.LogDetails)
	})

	t.Run("should be able to find out a simple A-B-C relation with a large TDOA", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		createdAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		timestampOne := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		messageA := "The message for cluster A"
		messageB := "Der Message Cluster B"
		messageC := "El mensaje para el cluster C"
		clusterIdA := "1"
		clusterIdB := "2"
		clusterIdC := "3"
		const TDOA = time.Second * 5

		logs := []logModel.LogEntry{
			{
				Id:        "1",
				ClusterId: clusterIdA,
				CreatedAt: createdAt,
				Timestamp: timestampOne,
				Severity:  "INFO",
				Message:   messageA,
				Service:   "serviceA",
			},
			{
				Id:        "2",
				ClusterId: clusterIdB,
				CreatedAt: createdAt,
				Timestamp: timestampOne.Add(TDOA),
				Severity:  "INFO",
				Message:   messageB,
				Service:   "serviceA",
			},
			{
				Id:        "3",
				ClusterId: clusterIdC,
				CreatedAt: createdAt,
				Timestamp: timestampOne.Add(TDOA * 2),
				Severity:  "INFO",
				Message:   messageC,
				Service:   "serviceA",
			},
		}
		spanOrLogData := convertLogToSpanOrLogData(logs)

		err = loadDataIntoElasticsearch(ac, logs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load data into Elasticsearch: %v", err)
		}
		clusterOutput, err := cdp.ClusterData(context.Background(), spanOrLogData)
		assert.Nil(t, err)

		updatedClusters, err := csp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterOutput)
		assert.Nil(t, err)

		err = as.UpdateAnalytics(context.Background(), updatedClusters)
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}

		spanOrLogDatum := analyticsModel.LogOrSpanData{
			Id:         logs[0].Id,
			ClusterId:  logs[0].ClusterId,
			LogDetails: &logs[0],
		}
		graph, err := as.GetChainOfEvents(context.Background(), spanOrLogDatum)
		assert.Nil(t, err)

		assert.Equal(t, logs[0], *graph[clusterIdA].LogOrSpanData.LogDetails)
		assert.Equal(t, logs[1], *graph[clusterIdB].LogOrSpanData.LogDetails)
		assert.Equal(t, logs[2], *graph[clusterIdC].LogOrSpanData.LogDetails)
	})

	t.Run("should be able to find the correct sequence even with decoys", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		createdAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		timestampOne := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		timestampTwo := time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC)
		timestampThree := time.Date(2021, 1, 3, 0, 0, 0, 0, time.UTC)
		messageA := "The message for cluster A"
		messageB := "Der Message Cluster B"
		messageC := "El mensaje para el cluster C"
		clusterIdA := "1"
		clusterIdB := "2"
		clusterIdC := "3"
		const TDOA = time.Millisecond * 20
		const absurdlyLargeTDOA = time.Hour * 2
		const nearToOriginalTDOA = time.Millisecond * 19

		logs := []logModel.LogEntry{
			{
				Id:        "1",
				ClusterId: clusterIdA,
				CreatedAt: createdAt,
				Timestamp: timestampOne,
				Severity:  "INFO",
				Message:   messageA,
				Service:   "serviceA",
			},
			{
				Id:        "2",
				ClusterId: clusterIdB,
				CreatedAt: createdAt,
				Timestamp: timestampOne.Add(TDOA),
				Severity:  "INFO",
				Message:   messageB,
				Service:   "serviceA",
			},
			{
				Id:        "3",
				ClusterId: clusterIdC,
				CreatedAt: createdAt,
				Timestamp: timestampOne.Add(TDOA * 2),
				Severity:  "INFO",
				Message:   messageC,
				Service:   "serviceA",
			},
			{
				Id:        "4",
				ClusterId: clusterIdA,
				CreatedAt: createdAt,
				Timestamp: timestampTwo,
				Severity:  "INFO",
				Message:   messageA,
				Service:   "serviceA",
			},
			{
				Id:        "5",
				ClusterId: clusterIdB,
				CreatedAt: createdAt,
				Timestamp: timestampTwo.Add(TDOA),
				Severity:  "INFO",
				Message:   messageB,
				Service:   "serviceA",
			},
			{
				Id:        "6",
				ClusterId: clusterIdC,
				CreatedAt: createdAt,
				Timestamp: timestampTwo.Add(TDOA * 2),
				Severity:  "INFO",
				Message:   messageC,
				Service:   "serviceA",
			},
			{
				Id:        "7",
				ClusterId: clusterIdA,
				CreatedAt: createdAt,
				Timestamp: timestampThree,
				Severity:  "INFO",
				Message:   messageA,
				Service:   "serviceA",
			},
			{
				Id:        "8",
				ClusterId: clusterIdB,
				CreatedAt: createdAt,
				Timestamp: timestampThree.Add(TDOA),
				Severity:  "INFO",
				Message:   messageB,
				Service:   "serviceA",
			},
			{
				Id:        "9",
				ClusterId: clusterIdC,
				CreatedAt: createdAt,
				Timestamp: timestampThree.Add(TDOA * 2),
				Severity:  "INFO",
				Message:   messageC,
				Service:   "serviceA",
			},
			{
				Id:        "10",
				ClusterId: clusterIdC,
				CreatedAt: createdAt,
				Timestamp: timestampThree.Add(absurdlyLargeTDOA),
				Severity:  "INFO",
				Message:   messageC,
				Service:   "serviceA",
			},
			{
				Id:        "11",
				ClusterId: clusterIdB,
				CreatedAt: createdAt,
				Timestamp: timestampThree.Add(nearToOriginalTDOA * 2),
				Severity:  "INFO",
				Message:   messageC,
				Service:   "serviceA",
			},
		}
		spanOrLogData := convertLogToSpanOrLogData(logs)

		err = loadDataIntoElasticsearch(ac, logs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load data into Elasticsearch: %v", err)
		}
		clusterOutput, err := cdp.ClusterData(context.Background(), spanOrLogData)
		assert.Nil(t, err)

		updatedClusters, err := csp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterOutput)
		assert.Nil(t, err)

		err = as.UpdateAnalytics(context.Background(), updatedClusters)
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}

		spanOrLogDatum := analyticsModel.LogOrSpanData{
			Id:         logs[6].Id,
			ClusterId:  logs[6].ClusterId,
			LogDetails: &logs[6],
		}
		graph, err := as.GetChainOfEvents(context.Background(), spanOrLogDatum)
		assert.Nil(t, err)

		assert.Equal(t, logs[6], *graph[clusterIdA].LogOrSpanData.LogDetails)
		assert.Equal(t, logs[7], *graph[clusterIdB].LogOrSpanData.LogDetails)
		assert.Equal(t, logs[8], *graph[clusterIdC].LogOrSpanData.LogDetails)
	})
}
