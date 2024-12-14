package elasticsearch

import (
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/pkg/data_processor/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	logModel "github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

var dpIndices = []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName}

func TestDataProcessor(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)

	t.Run("should be able to scroll through a huge list of logs/spans", func(t *testing.T) {
		dp := service.NewDataProcessorService(ac, logger)
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		logSize := 30000
		logs := make([]logModel.LogEntry, logSize)
		overlapSize := 50
		beginTime := time.Date(2021, 1, 1, 0, 0, 0, 32, time.UTC)
		for i := 0; i < logSize; i++ {
			clusterId := "cluster" + strconv.Itoa(i)
			logs[i] = logModel.LogEntry{
				CreatedAt: beginTime.Add(time.Duration(i) * time.Millisecond * time.Duration(overlapSize)),
				Timestamp: beginTime.Add(time.Duration(i) * time.Millisecond * time.Duration(overlapSize)),
				ClusterId: clusterId,
			}
		}
		err = loadDataIntoElasticsearch(ac, logs, bootstrapper.LogIndexName)
		if err != nil {
			t.Errorf("Failed to load data into Elasticsearch: %v", err)
		}
		result := <-dp.ProcessData(context.Background(), dpIndices)
		errors := []error{result.Error}

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
		assertAllErrorsAreNil(t, errors)
		// every log sequentially overlaps with another, creating two co-occurrences per log (previous and next log)
		// except for the first and last log
		assert.Equal(t, int64(logSize*2-2), count)
	})
}
