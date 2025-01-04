package elasticsearch

import (
	"context"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/internal/otel_server/log/model"
	"github.com/Avi18971911/Augur/internal/pipeline/analytics/service"
	clusterService "github.com/Avi18971911/Augur/internal/pipeline/cluster/service"
	analyticsModel "github.com/Avi18971911/Augur/internal/pipeline/count/model"
	countService "github.com/Avi18971911/Augur/internal/pipeline/count/service"
	inferenceService "github.com/Avi18971911/Augur/internal/query_server/service/inference"
	inferenceModel "github.com/Avi18971911/Augur/internal/query_server/service/inference/model"
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

	an := service.NewAnalyticsService(
		ac,
		logger,
	)

	as := inferenceService.NewInferenceQueryService(
		ac,
		logger,
	)
	buckets := []analyticsModel.Bucket{1000 * 30}
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

		err = an.UpdateAnalytics(context.Background(), updatedClusters)
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}

		spanOrLogDatum := inferenceModel.LogOrSpanData{
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

		err = an.UpdateAnalytics(context.Background(), updatedClusters)
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}

		spanOrLogDatum := inferenceModel.LogOrSpanData{
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

		err = an.UpdateAnalytics(context.Background(), updatedClusters)
		if err != nil {
			t.Errorf("Failed to update analytics: %v", err)
		}

		spanOrLogDatum := inferenceModel.LogOrSpanData{
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

	t.Run("should be able to handle easy real data with the last log in sequence", func(t *testing.T) {
		err := deleteAllDocuments(es)
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.LogIndexName, "data/easy_inference/log_index.json")
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.ClusterIndexName, "data/easy_inference/cluster_index.json")
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.CountIndexName, "data/easy_inference/count_index.json")
		assert.NoError(t, err)
		const maxLogsInChain = 8

		createdAt, err := time.Parse(time.RFC3339Nano, "2024-12-18T14:33:40.872458799Z")
		assert.NoError(t, err)
		timestamp, err := time.Parse(time.RFC3339Nano, "2024-12-18T14:33:33.234410296Z")
		assert.NoError(t, err)

		clusterId := "66553fb7-2b8f-4002-9a15-c96d5ec00781"
		spanOrLogDatum := inferenceModel.LogOrSpanData{
			Id:        "61d5afe39d21cbea98aa5a395ac158a2564e2d0d1d9b0e300aeced2b67a0cc8f",
			ClusterId: clusterId,
			LogDetails: &logModel.LogEntry{
				Id:        "61d5afe39d21cbea98aa5a395ac158a2564e2d0d1d9b0e300aeced2b67a0cc8f",
				ClusterId: "66553fb7-2b8f-4002-9a15-c96d5ec00781",
				CreatedAt: createdAt,
				Timestamp: timestamp,
				Message:   "Invalid credentials for Login request with username: Yolo and password: Swag",
				Severity:  "error",
				Service:   "fake-server",
			},
		}
		graph, err := as.GetChainOfEvents(context.Background(), spanOrLogDatum)
		assert.NoError(t, err)
		assert.Equal(t, maxLogsInChain, len(graph))
		// this error should always be the last in the chain
		assert.Equal(t, 0, len(graph[clusterId].Successors))
		assert.Equal(t, maxLogsInChain-1, len(graph[clusterId].Predecessors))
		for _, node := range graph[clusterId].Predecessors {
			assert.NotNil(t, graph[node.ClusterId].LogOrSpanData.LogDetails)
		}
	})

	t.Run("should be able to handle real data with the last log in sequence", func(t *testing.T) {
		err := deleteAllDocuments(es)
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.LogIndexName, "data/easy_inference/log_index.json")
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.ClusterIndexName, "data/easy_inference/cluster_index.json")
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.CountIndexName, "data/easy_inference/count_index.json")
		assert.NoError(t, err)
		const maxLogsInChain = 8

		createdAt, err := time.Parse(time.RFC3339Nano, "2024-12-18T14:33:40.872440799Z")
		assert.NoError(t, err)
		timestamp, err := time.Parse(time.RFC3339Nano, "2024-12-18T14:33:32.229644879Z")
		assert.NoError(t, err)

		clusterId := "0b064520-f152-40b0-a34e-661fe3e50cae"
		spanOrLogDatum := inferenceModel.LogOrSpanData{
			Id:        "53aa1506e94ead53a771ad073b0e4c1ea29dd5959297c40f123a344e85b493ae",
			ClusterId: clusterId,
			LogDetails: &logModel.LogEntry{
				Id:        "53aa1506e94ead53a771ad073b0e4c1ea29dd5959297c40f123a344e85b493ae",
				ClusterId: clusterId,
				CreatedAt: createdAt,
				Timestamp: timestamp,
				Message:   "Login request received with URL /accounts/login and method POST",
				Severity:  "info",
				Service:   "fake-server",
			},
		}
		graph, err := as.GetChainOfEvents(context.Background(), spanOrLogDatum)
		assert.NoError(t, err)
		assert.Equal(t, maxLogsInChain, len(graph))
		// this error should always be the last in the chain
		assert.Equal(t, 0, len(graph[clusterId].Predecessors))
		assert.Equal(t, maxLogsInChain-1, len(graph[clusterId].Successors))
		for _, node := range graph[clusterId].Successors {
			assert.NotNil(t, graph[node.ClusterId].LogOrSpanData.LogDetails)
		}
	})

	t.Run("Should be able to do inference even in difficult circumstances where many overlaps occur", func(t *testing.T) {
		err := deleteAllDocuments(es)
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.LogIndexName, "data/difficult_inference/log_index.json")
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.ClusterIndexName, "data/difficult_inference/cluster_index.json")
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.CountIndexName, "data/difficult_inference/count_index.json")
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.SpanIndexName, "data/difficult_inference/span_index.json")
		assert.NoError(t, err)
		const maxLogsInChain = 8

		createdAt, err := time.Parse(time.RFC3339Nano, "2024-12-21T10:30:54.838663084Z")
		assert.NoError(t, err)
		timestamp, err := time.Parse(time.RFC3339Nano, "2024-12-21T10:30:44.890232885Z")
		assert.NoError(t, err)

		clusterId := "d07ad010-acd1-4e5a-a121-e584dc791d38"
		id := "89e039d63387543949a4ff035b76ddf3ae4d687c5083f9fe52289e64be253eb8"
		spanOrLogDatum := inferenceModel.LogOrSpanData{
			Id:        id,
			ClusterId: clusterId,
			LogDetails: &logModel.LogEntry{
				Id:        id,
				ClusterId: clusterId,
				CreatedAt: createdAt,
				Timestamp: timestamp,
				Message:   "Login request received with URL /accounts/login and method POST",
				Severity:  "info",
				Service:   "fake-server",
			},
		}
		// TODO: Do some heavy investigation to ensure adequate functionality in this case
		graph, err := as.GetChainOfEvents(context.Background(), spanOrLogDatum)
		assert.NoError(t, err)
		assert.Equal(t, maxLogsInChain, len(graph))
		// this error should always be the last in the chain
		assert.Equal(t, 0, len(graph[clusterId].Predecessors))
		assert.Equal(t, maxLogsInChain-1, len(graph[clusterId].Successors))
		for _, node := range graph[clusterId].Successors {
			assert.NotNil(t, graph[node.ClusterId].LogOrSpanData.LogDetails)
		}
	})
}
