package elasticsearch

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/internal/otel_server/log/model"
	spanModel "github.com/Avi18971911/Augur/internal/otel_server/trace/model"
	countModel "github.com/Avi18971911/Augur/internal/pipeline/cluster_count/model"
	countService "github.com/Avi18971911/Augur/internal/pipeline/cluster_count/service"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var dpInDBIndices = []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName}

func TestCountDataProcessorWithDataInDB(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	wc := countService.NewClusterWindowCountService(ac, 50, logger)
	cs := countService.NewClusterTotalCountService(ac, wc, logger)
	bucket := countModel.Bucket(100)

	t.Run("should increment both co-occurring clusters, and misses", func(t *testing.T) {
		dp := countService.NewCountDataProcessorService(ac, cs, bucket, dpInDBIndices, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		clusterA := "clusterA"
		clusterB := "clusterB"
		firstTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		overlapWithFirstTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 500, time.UTC)

		secondTimeStamp := time.Date(2021, 2, 1, 0, 0, 1, 0, time.UTC)
		overlapWithSecondTimeStamp := time.Date(2021, 2, 1, 0, 0, 1, 500, time.UTC)

		clusterAMiss := time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC)

		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)

		firstCluster := []logModel.LogEntry{
			{
				ClusterId: clusterA,
				Timestamp: firstTimeStamp,
				CreatedAt: createdAt.Add(-time.Second * 500),
			},
			{
				ClusterId: clusterA,
				Timestamp: secondTimeStamp,
				CreatedAt: createdAt.Add(-time.Second * 500),
			},
		}
		firstClusterOutput := createClusterOutputFromLogs(firstCluster)

		err = loadDataIntoElasticsearch(ac, firstCluster, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), firstClusterOutput)
		if err != nil {
			t.Errorf("failed to process clusters: %v", err)
		}

		secondCluster := []logModel.LogEntry{
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithSecondTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: clusterAMiss,
				CreatedAt: createdAt,
			},
		}

		secondClusterOutput := createClusterOutputFromLogs(secondCluster)
		err = loadDataIntoElasticsearch(ac, secondCluster, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}

		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), secondClusterOutput)
		if err != nil {
			t.Errorf("failed to process clusters: %v", err)
		}
		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := countService.ConvertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		assert.Equal(t, int64(2), countEntries[0].TotalInstancesWithCoCluster)
		assert.Equal(t, int64(3), countEntries[0].TotalInstances)
	})

	t.Run("should increment asymmetrically with multiple overlaps on the same period", func(t *testing.T) {
		dp := countService.NewCountDataProcessorService(ac, cs, bucket, dpInDBIndices, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		clusterA := "clusterA"
		clusterB := "clusterB"

		firstTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
		overlapWithFirstTimeStampOne := time.Date(2021, 1, 1, 0, 0, 0, 500, time.UTC)
		overlapWithFirstTimeStampTwo := time.Date(2021, 1, 1, 0, 0, 0, 750, time.UTC)
		overlapWithFirstTimeStampThree := time.Date(2021, 1, 1, 0, 0, 0, 900, time.UTC)

		clusterBMiss := time.Date(2023, 1, 1, 0, 0, 1, 0, time.UTC)

		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)

		firstBatch := []logModel.LogEntry{
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStampOne,
				CreatedAt: createdAt.Add(-time.Second * 500),
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStampTwo,
				CreatedAt: createdAt.Add(-time.Second * 500),
			},
		}
		firstClusterOutput := createClusterOutputFromLogs(firstBatch)

		err = loadDataIntoElasticsearch(ac, firstBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), firstClusterOutput)
		if err != nil {
			t.Errorf("failed to process clusters: %v", err)
		}

		secondBatch := []logModel.LogEntry{
			{
				ClusterId: clusterA,
				Timestamp: firstTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStampThree,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: clusterBMiss,
				CreatedAt: createdAt,
			},
		}
		secondClusterOutput := createClusterOutputFromLogs(secondBatch)

		err = loadDataIntoElasticsearch(ac, secondBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), secondClusterOutput)
		if err != nil {
			t.Errorf("failed to process clusters: %v", err)
		}

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := countService.ConvertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterAEntries := make([]countModel.ClusterTotalCountEntry, 0)
		clusterBEntries := make([]countModel.ClusterTotalCountEntry, 0)

		for _, entry := range countEntries {
			if entry.ClusterId == clusterA {
				clusterAEntries = append(clusterAEntries, entry)
			} else if entry.ClusterId == clusterB {
				clusterBEntries = append(clusterBEntries, entry)
			}
		}

		assert.Equal(t, int64(1), clusterAEntries[0].TotalInstancesWithCoCluster)
		assert.Equal(t, int64(1), clusterBEntries[0].TotalInstancesWithCoCluster)

		assert.Equal(t, int64(1), clusterAEntries[0].TotalInstances)
		assert.Equal(t, int64(2), clusterBEntries[0].TotalInstances)
	})

	t.Run(
		"should be able to process spans completely unrelated to each other without error", func(t *testing.T) {
			dp := countService.NewCountDataProcessorService(ac, cs, countModel.Bucket(10), dpInDBIndices, logger)
			err := deleteAllDocuments(es)
			if err != nil {
				t.Errorf("Failed to delete all documents: %v", err)
			}
			beginTime := time.Date(2021, 1, 1, 0, 0, 0, 32, time.UTC)
			spanSize := 100
			spans, _ := createSpans(beginTime, time.Millisecond*50, spanSize)
			firstBatchSize := spanSize / 2
			firstBatch := spans[:firstBatchSize]
			secondBatch := spans[firstBatchSize:]
			firstBatchClusterOutput := createClusterOutputFromSpans(firstBatch)
			secondBatchClusterOutput := createClusterOutputFromSpans(secondBatch)

			err = loadDataIntoElasticsearch(ac, firstBatch, bootstrapper.SpanIndexName)
			if err != nil {
				t.Errorf("Failed to load data into Elasticsearch: %v", err)
			}
			_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), firstBatchClusterOutput)
			if err != nil {
				t.Errorf("Failed to process clusters: %v", err)
			}

			err = loadDataIntoElasticsearch(ac, secondBatch, bootstrapper.SpanIndexName)
			if err != nil {
				t.Errorf("Failed to load data into Elasticsearch: %v", err)
			}
			_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), secondBatchClusterOutput)
			if err != nil {
				t.Errorf("Failed to process clusters: %v", err)
			}

			stringQuery, err := json.Marshal(getAllQuery())
			if err != nil {
				t.Errorf("failed to marshal query: %v", err)
			}

			searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			count, err := ac.Count(searchCtx, string(stringQuery), []string{bootstrapper.ClusterTotalCountIndexName})
			if err != nil {
				t.Errorf("Failed to count: %v", err)
			}
			assert.Equal(t, int64(0), count)
		},
	)

	t.Run("overlapping spans and logs should be processed correctly", func(t *testing.T) {
		dp := countService.NewCountDataProcessorService(ac, cs, bucket, dpInDBIndices, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		clusterA := "clusterA"
		clusterB := "clusterB"

		firstTimeStamp := time.Date(2021, 1, 1, 0, 0, 0, 500, time.UTC)
		overlapWithFirstTimeStampOne := time.Date(2021, 1, 1, 0, 0, 0, 100, time.UTC)
		overlapWithFirstTimeStampTwo := time.Date(2021, 1, 1, 0, 0, 0, 200, time.UTC)
		overlapWithFirstTimeStampThree := time.Date(2021, 1, 1, 0, 0, 0, 300, time.UTC)

		clusterBMiss := time.Date(2023, 1, 1, 0, 0, 1, 200, time.UTC)

		createdAt := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)

		clusterALogs := []logModel.LogEntry{
			{
				ClusterId: clusterA,
				Timestamp: firstTimeStamp,
				CreatedAt: createdAt,
			},
		}
		clusterAClusterOutput := createClusterOutputFromLogs(clusterALogs)

		clusterBSpans := []spanModel.Span{
			createSpan(clusterB, overlapWithFirstTimeStampOne, overlapWithFirstTimeStampOne.Add(time.Millisecond*50)),
			createSpan(clusterB, overlapWithFirstTimeStampTwo, overlapWithFirstTimeStampTwo.Add(time.Millisecond*50)),
			createSpan(
				clusterB, overlapWithFirstTimeStampThree, overlapWithFirstTimeStampThree.Add(time.Millisecond*50),
			),
			createSpan(clusterB, clusterBMiss, clusterBMiss.Add(time.Millisecond*50)),
		}
		clusterBClusterOutput := createClusterOutputFromSpans(clusterBSpans)

		err = loadDataIntoElasticsearch(ac, clusterALogs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterAClusterOutput)
		if err != nil {
			t.Errorf("failed to process clusters: %v", err)
		}
		err = loadDataIntoElasticsearch(ac, clusterBSpans, bootstrapper.SpanIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, err = dp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterBClusterOutput)
		if err != nil {
			t.Errorf("failed to process clusters: %v", err)
		}

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		clusterBEntries, err := countService.ConvertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		assert.Equal(t, int64(3), clusterBEntries[0].TotalInstancesWithCoCluster)
		assert.Equal(t, int64(4), clusterBEntries[0].TotalInstances)
		assert.Equal(t, clusterB, clusterBEntries[0].ClusterId)
	})
}
