package elasticsearch

import (
	"context"
	"encoding/json"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/data_processor/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var dpIndices = []string{bootstrapper.LogIndexName}

func TestDataProcessor(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	cs := countService.NewCountService(ac, logger)
	dp := service.NewDataProcessorService(ac, cs, logger)

	t.Run("should increment both co-occurring clusters, and misses", func(t *testing.T) {
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

		coOccurringLogs := []logModel.LogEntry{
			{
				ClusterId: clusterA,
				Timestamp: firstTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterA,
				Timestamp: secondTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithSecondTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterA,
				Timestamp: clusterAMiss,
				CreatedAt: createdAt,
			},
		}

		err := loadDataIntoElasticsearch(ac, coOccurringLogs)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}

		buckets := []countService.Bucket{100}
		dp.ProcessData(context.Background(), buckets, dpIndices)

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

		assert.Equal(t, int64(2), clusterAEntries[0].CoOccurrences)
		assert.Equal(t, int64(2), clusterBEntries[0].CoOccurrences)

		assert.Equal(t, int64(3), clusterAEntries[0].Occurrences)
		assert.Equal(t, int64(2), clusterBEntries[0].Occurrences)
	})

	t.Run("should increment asymmetrically with multiple overlaps on the same period", func(t *testing.T) {
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

		coOccurringLogs := []logModel.LogEntry{
			{
				ClusterId: clusterA,
				Timestamp: firstTimeStamp,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStampOne,
				CreatedAt: createdAt,
			},
			{
				ClusterId: clusterB,
				Timestamp: overlapWithFirstTimeStampTwo,
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

		err := loadDataIntoElasticsearch(ac, coOccurringLogs)
		if err != nil {
			t.Errorf("failed to load data into Elasticsearch: %v", err)
		}

		buckets := []countService.Bucket{100}
		dp.ProcessData(context.Background(), buckets, dpIndices)

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

		assert.Equal(t, int64(1), clusterAEntries[0].CoOccurrences)
		assert.Equal(t, int64(3), clusterBEntries[0].CoOccurrences)

		assert.Equal(t, int64(1), clusterAEntries[0].Occurrences)
		assert.Equal(t, int64(4), clusterBEntries[0].Occurrences)
	})
}
