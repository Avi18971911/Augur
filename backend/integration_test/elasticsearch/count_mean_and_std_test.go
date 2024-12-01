package elasticsearch

import (
	"context"
	countModel "github.com/Avi18971911/Augur/pkg/count/model"
	countService "github.com/Avi18971911/Augur/pkg/count/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/Avi18971911/Augur/pkg/log/model"
	spanModel "github.com/Avi18971911/Augur/pkg/trace/model"
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

	t.Run("should maintain a running variance for the time difference of arrival for spans", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		newSpan := spanModel.Span{
			ClusterId: "initialClusterId",
			StartTime: initialTime,
			EndTime:   initialTime.Add(time.Second),
		}
		spanOneSecondAfter := spanModel.Span{
			ClusterId: "otherClusterId",
			StartTime: initialTime.Add(time.Second),
			EndTime:   initialTime.Add(time.Second * 2),
		}
		spanTwoSecondsAfter := spanModel.Span{
			ClusterId: "otherClusterId",
			StartTime: initialTime.Add(time.Second * 2),
			EndTime:   initialTime.Add(time.Second * 3),
		}
		err = loadDataIntoElasticsearch(
			ac, []spanModel.Span{
				newSpan,
				spanOneSecondAfter,
				spanTwoSecondsAfter,
			},
			bootstrapper.SpanIndexName,
		)
		if err != nil {
			t.Error("Failed to load spans into elasticsearch")
		}
		buckets := []countModel.Bucket{4500}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			newSpan.ClusterId,
			countModel.TimeInfo{SpanInfo: &countModel.SpanInfo{FromTime: newSpan.StartTime, ToTime: newSpan.EndTime}},
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

		searchQueryBody := countQuery(newSpan.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.CountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := convertCountDocsToCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		countEntry := countEntries[0]
		expectedMean := (spanOneSecondAfter.StartTime.Sub(newSpan.StartTime).Seconds() + spanTwoSecondsAfter.StartTime.Sub(newSpan.StartTime).Seconds()) / 2
		assert.Equal(t, expectedMean, countEntry.MeanTDOA)
	})

	t.Run(
		"should maintain a running variance for the time difference of arrival for a mix of spans and logs",
		func(t *testing.T) {
			err := deleteAllDocuments(es)
			if err != nil {
				t.Errorf("Failed to delete all documents: %v", err)
			}
			initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
			newSpan := spanModel.Span{
				ClusterId: "initialClusterId",
				StartTime: initialTime,
				EndTime:   initialTime.Add(time.Second),
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
				ac, []spanModel.Span{
					newSpan,
				},
				bootstrapper.SpanIndexName,
			)
			if err != nil {
				t.Error("Failed to load spans into elasticsearch")
			}
			err = loadDataIntoElasticsearch(
				ac, []model.LogEntry{
					logOneSecondAfter,
					logTwoSecondsAfter,
				},
				bootstrapper.LogIndexName,
			)
			if err != nil {
				t.Error("Failed to load logs into elasticsearch")
			}
			buckets := []countModel.Bucket{4500}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
				ctx,
				newSpan.ClusterId,
				countModel.TimeInfo{SpanInfo: &countModel.SpanInfo{FromTime: newSpan.StartTime, ToTime: newSpan.EndTime}},
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

			searchQueryBody := countQuery(newSpan.ClusterId)
			docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.CountIndexName}, &querySize)
			if err != nil {
				t.Errorf("Failed to search for count: %v", err)
			}
			countEntries, err := convertCountDocsToCountEntries(docs)
			if err != nil {
				t.Errorf("Failed to convert count docs to count entries: %v", err)
			}

			countEntry := countEntries[0]
			expectedMean := (logOneSecondAfter.Timestamp.Sub(newSpan.StartTime).Seconds() + logTwoSecondsAfter.Timestamp.Sub(newSpan.StartTime).Seconds()) / 2
			assert.Equal(t, expectedMean, countEntry.MeanTDOA)
		},
	)

	t.Run(
		"should maintain a running variance for the time difference of arrival for a mix of logs and spans",
		func(t *testing.T) {
			err := deleteAllDocuments(es)
			if err != nil {
				t.Errorf("Failed to delete all documents: %v", err)
			}
			initialTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
			newLog := model.LogEntry{
				ClusterId: "initialClusterId",
				Timestamp: initialTime,
			}
			spanOneSecondAfter := spanModel.Span{
				ClusterId: "otherClusterId",
				StartTime: initialTime.Add(time.Second),
				EndTime:   initialTime.Add(time.Second * 2),
			}
			spanTwoSecondsAfter := spanModel.Span{
				ClusterId: "otherClusterId",
				StartTime: initialTime.Add(time.Second * 2),
				EndTime:   initialTime.Add(time.Second * 3),
			}
			err = loadDataIntoElasticsearch(
				ac, []spanModel.Span{
					spanOneSecondAfter,
					spanTwoSecondsAfter,
				},
				bootstrapper.SpanIndexName,
			)
			if err != nil {
				t.Error("Failed to load spans into elasticsearch")
			}
			err = loadDataIntoElasticsearch(
				ac, []model.LogEntry{
					newLog,
				},
				bootstrapper.LogIndexName,
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
			expectedMean := (spanOneSecondAfter.StartTime.Sub(newLog.Timestamp).Seconds() + spanTwoSecondsAfter.StartTime.Sub(newLog.Timestamp).Seconds()) / 2
			assert.Equal(t, expectedMean, countEntry.MeanTDOA)
		},
	)
}
