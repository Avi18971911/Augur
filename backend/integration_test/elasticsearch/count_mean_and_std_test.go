package elasticsearch

import (
	"context"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var meanAndSTDIndices = []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName}

func TestMeandAndSTD(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	var querySize = 100

	ac := client.NewAugurClientImpl(es, client.Immediate)
	cs := countService.NewCountService(ac, logger)

	t.Run("should maintain a running average for the time difference of arrival for logs", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: initialTime,
		}
		logOneSecondAfter := model.LogEntry{
			ClusterId: "otherClusterId",
			Timestamp: initialTime.Add(time.Second),
		}
		logTwoSecondsAfter := model.LogEntry{
			ClusterId: "otherClusterId",
			Timestamp: initialTime.Add(time.Second * 2),
		}
		err = loadDataIntoElasticsearch(
			ac, []model.LogEntry{
				newLog,
				logOneSecondAfter,
				logTwoSecondsAfter,
			}, bootstrapper.LogIndexName,
		)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		buckets := []countModel.Bucket{4500}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: newLog.Timestamp}},
			indices,
			buckets,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		index := bootstrapper.CountIndexName
		err = ac.BulkIndex(ctx, res.MetaMapList, res.DocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}

		searchQueryBody := countQuery(newLog.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		countEntry := countEntries[0]
		expectedMean := (logOneSecondAfter.Timestamp.Sub(newLog.Timestamp).Seconds() + logTwoSecondsAfter.Timestamp.Sub(newLog.Timestamp).Seconds()) / 2
		assert.Equal(t, expectedMean, countEntry.MeanTDOA)
	})
}
