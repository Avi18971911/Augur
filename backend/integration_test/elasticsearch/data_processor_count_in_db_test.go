package elasticsearch

import (
	"context"
	"encoding/json"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	"github.com/Avi18971911/Augur/pkg/data_processor/model"
	"github.com/Avi18971911/Augur/pkg/data_processor/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/event_bus"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/asaskevich/EventBus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var dpInDBIndices = []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName}

func TestDataProcessorWithDataInDB(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	eventBus := EventBus.New()
	eb := event_bus.NewAugurEventBus[any, model.DataProcessorOutput](
		eventBus,
		logger,
	)

	t.Run("should increment both co-occurring clusters, and misses", func(t *testing.T) {
		dp := service.NewDataProcessorService(ac, eb, "test_input", logger)
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
		err = loadDataIntoElasticsearch(ac, firstCluster, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, firstRoundErrors := dp.ProcessData(context.Background(), dpInDBIndices)

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

		err = loadDataIntoElasticsearch(ac, secondCluster, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}

		_, secondRoundErrors := dp.ProcessData(context.Background(), dpInDBIndices)

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		assertAllErrorsAreNil(t, append(firstRoundErrors, secondRoundErrors...))
		assert.Equal(t, int64(2), countEntries[0].CoOccurrences)
		assert.Equal(t, int64(3), countEntries[0].Occurrences)
	})

	t.Run("should increment asymmetrically with multiple overlaps on the same period", func(t *testing.T) {
		dp := service.NewDataProcessorService(ac, eb, "test_output", logger)
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
		err = loadDataIntoElasticsearch(ac, firstBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, firstRoundErrors := dp.ProcessData(context.Background(), dpInDBIndices)

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

		err = loadDataIntoElasticsearch(ac, secondBatch, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, secondRoundErrors := dp.ProcessData(context.Background(), dpInDBIndices)

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		clusterAEntries := make([]countModel.CountEntry, 0)
		clusterBEntries := make([]countModel.CountEntry, 0)

		for _, entry := range countEntries {
			if entry.ClusterId == clusterA {
				clusterAEntries = append(clusterAEntries, entry)
			} else if entry.ClusterId == clusterB {
				clusterBEntries = append(clusterBEntries, entry)
			}
		}

		assertAllErrorsAreNil(t, append(firstRoundErrors, secondRoundErrors...))
		assert.Equal(t, int64(1), clusterAEntries[0].CoOccurrences)
		assert.Equal(t, int64(1), clusterBEntries[0].CoOccurrences)

		assert.Equal(t, int64(1), clusterAEntries[0].Occurrences)
		assert.Equal(t, int64(2), clusterBEntries[0].Occurrences)
	})

	t.Run(
		"should be able to process spans completely unrelated to each other without error", func(t *testing.T) {
			dp := service.NewDataProcessorService(ac, eb, "test_output", logger)
			err := deleteAllDocuments(es)
			if err != nil {
				t.Errorf("Failed to delete all documents: %v", err)
			}
			beginTime := time.Date(2021, 1, 1, 0, 0, 0, 32, time.UTC)
			spanSize := 100
			spans := createSpans(beginTime, time.Millisecond*50, spanSize)
			firstBatchSize := spanSize / 2
			firstBatch := spans[:firstBatchSize]
			secondBatch := spans[firstBatchSize:]
			err = loadDataIntoElasticsearch(ac, firstBatch, bootstrapper.SpanIndexName)
			if err != nil {
				t.Errorf("Failed to load data into Elasticsearch: %v", err)
			}
			_, firstRoundErrors := dp.ProcessData(context.Background(), dpInDBIndices)

			err = loadDataIntoElasticsearch(ac, secondBatch, bootstrapper.SpanIndexName)
			if err != nil {
				t.Errorf("Failed to load data into Elasticsearch: %v", err)
			}
			_, secondRoundErrors := dp.ProcessData(context.Background(), dpInDBIndices)

			stringQuery, err := json.Marshal(getAllQuery())
			if err != nil {
				t.Errorf("failed to marshal query: %v", err)
			}

			searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			count, err := ac.Count(searchCtx, string(stringQuery), []string{bootstrapper.CountIndexName})
			if err != nil {
				t.Errorf("Failed to count: %v", err)
			}
			assertAllErrorsAreNil(t, append(firstRoundErrors, secondRoundErrors...))
			assert.Equal(t, int64(0), count)
		},
	)

	t.Run("overlapping spans and logs should be processed correctly", func(t *testing.T) {
		dp := service.NewDataProcessorService(ac, eb, "test_output", logger)
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

		clusterBSpans := []spanModel.Span{
			createSpan(clusterB, overlapWithFirstTimeStampOne, overlapWithFirstTimeStampOne.Add(time.Millisecond*50)),
			createSpan(clusterB, overlapWithFirstTimeStampTwo, overlapWithFirstTimeStampTwo.Add(time.Millisecond*50)),
			createSpan(
				clusterB, overlapWithFirstTimeStampThree, overlapWithFirstTimeStampThree.Add(time.Millisecond*50),
			),
			createSpan(clusterB, clusterBMiss, clusterBMiss.Add(time.Millisecond*50)),
		}

		err = loadDataIntoElasticsearch(ac, clusterALogs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, clusterABatchErrors := dp.ProcessData(context.Background(), dpInDBIndices)
		err = loadDataIntoElasticsearch(ac, clusterBSpans, bootstrapper.SpanIndexName)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}
		_, clusterBBatchErrors := dp.ProcessData(context.Background(), dpInDBIndices)

		stringQuery, err := json.Marshal(getAllQuery())
		if err != nil {
			t.Errorf("failed to marshal query: %v", err)
		}
		var querySize = 100

		searchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		docs, err := ac.Search(searchCtx, string(stringQuery), []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		clusterBEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		assertAllErrorsAreNil(t, append(clusterABatchErrors, clusterBBatchErrors...))
		assert.Equal(t, int64(3), clusterBEntries[0].CoOccurrences)
		assert.Equal(t, int64(4), clusterBEntries[0].Occurrences)
		assert.Equal(t, clusterB, clusterBEntries[0].ClusterId)
	})
}
