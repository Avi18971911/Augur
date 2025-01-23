package elasticsearch

import (
	"context"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/otel_server/log/model"
	spanModel "github.com/Avi18971911/Augur/internal/otel_server/trace/model"
	countModel "github.com/Avi18971911/Augur/internal/pipeline/cluster_count/model"
	countService "github.com/Avi18971911/Augur/internal/pipeline/cluster_count/service"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var meanAndSTDIndices = []string{bootstrapper.LogIndexName, bootstrapper.SpanIndexName}

const tolerance = 0.0001

func TestMeanAndSTD(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	var querySize = 100

	ac := client.NewAugurClientImpl(es, client.Immediate)
	wc := countService.NewClusterWindowCountService(ac, 50, logger)
	cs := countService.NewClusterTotalCountService(ac, wc, logger)

	t.Run("should maintain a running average for the time difference of arrival for logs", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		firstTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		secondTime := time.Date(2021, 1, 2, 0, 0, 1, 204, time.UTC)
		firstTimeLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: firstTime,
		}
		secondTimeLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: secondTime,
		}
		firstTimeSecondAfter := model.LogEntry{
			ClusterId: "otherClusterId",
			Timestamp: firstTime.Add(time.Second),
		}
		secondTimeTwoSecondsAfter := model.LogEntry{
			ClusterId: "otherClusterId",
			Timestamp: secondTime.Add(time.Millisecond * 1010),
		}
		err = loadDataIntoElasticsearch(
			ac, []model.LogEntry{
				firstTimeLog,
				secondTimeLog,
				firstTimeSecondAfter,
				secondTimeTwoSecondsAfter,
			}, bootstrapper.LogIndexName,
		)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		bucket := countModel.Bucket(4500)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			firstTimeLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: firstTimeLog.Timestamp}},
			indices,
			bucket,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		index := bootstrapper.ClusterTotalCountIndexName
		err = ac.BulkIndex(ctx, res.TotalCountMetaMapList, res.TotalCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		err = ac.BulkIndex(ctx, res.WindowCountMetaMapList, res.WindowCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}

		secondRes, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			secondTimeLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: secondTimeLog.Timestamp}},
			indices,
			bucket,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		err = ac.BulkIndex(ctx, secondRes.TotalCountMetaMapList, secondRes.TotalCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}
		err = ac.BulkIndex(ctx, secondRes.WindowCountMetaMapList, secondRes.WindowCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}

		searchQueryBody := countQuery(firstTimeLog.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.ClusterWindowCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := countService.ConvertCountDocsToWindowCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		countEntry := countEntries[0]
		expectedMeanFirstTerm := firstTimeSecondAfter.Timestamp.Sub(firstTimeLog.Timestamp).Seconds()
		expectedMeanSecondTerm := secondTimeTwoSecondsAfter.Timestamp.Sub(secondTimeLog.Timestamp).Seconds()
		expectedMean := (expectedMeanFirstTerm + expectedMeanSecondTerm) / 2
		expectedVarianceFirstTerm := (firstTimeSecondAfter.Timestamp.Sub(firstTimeLog.Timestamp).Seconds() - expectedMean) * (firstTimeSecondAfter.Timestamp.Sub(firstTimeLog.Timestamp).Seconds() - expectedMean)
		expectedVarianceSecondTerm := (secondTimeTwoSecondsAfter.Timestamp.Sub(secondTimeLog.Timestamp).Seconds() - expectedMean) * (secondTimeTwoSecondsAfter.Timestamp.Sub(secondTimeLog.Timestamp).Seconds() - expectedMean)
		expectedVariance := (expectedVarianceFirstTerm + expectedVarianceSecondTerm) / 1
		assert.Equal(t, expectedMean, countEntry.MeanTDOA)
		assert.InDelta(t, expectedVariance, countEntry.VarianceTDOA, tolerance)
	})

	t.Run(
		"should maintain a running mean and variance for the time difference of arrival for spans",
		func(t *testing.T) {
			err := deleteAllDocuments(es)
			if err != nil {
				t.Errorf("Failed to delete all documents: %v", err)
			}
			firstTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
			secondTime := time.Date(2021, 1, 2, 0, 0, 1, 204, time.UTC)

			firstTimeSpan := spanModel.Span{
				ClusterId: "initialClusterId",
				StartTime: firstTime,
				EndTime:   firstTime.Add(time.Second),
			}
			firstTimeSpanSecondAfter := spanModel.Span{
				ClusterId: "otherClusterId",
				StartTime: firstTime.Add(time.Millisecond * 15),
				EndTime:   firstTime.Add(time.Second * 2),
			}
			secondTimeSpan := spanModel.Span{
				ClusterId: "initialClusterId",
				StartTime: secondTime,
				EndTime:   secondTime.Add(time.Second),
			}
			secondTimeSpanTwoSecondsAfter := spanModel.Span{
				ClusterId: "otherClusterId",
				StartTime: secondTime.Add(time.Millisecond * 20),
				EndTime:   secondTime.Add(time.Second * 4),
			}
			err = loadDataIntoElasticsearch(
				ac, []spanModel.Span{
					firstTimeSpan,
					firstTimeSpanSecondAfter,
					secondTimeSpan,
					secondTimeSpanTwoSecondsAfter,
				},
				bootstrapper.SpanIndexName,
			)
			if err != nil {
				t.Error("Failed to load spans into elasticsearch")
			}
			bucket := countModel.Bucket(4500)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
				ctx,
				firstTimeSpan.ClusterId,
				countModel.TimeInfo{
					SpanInfo: &countModel.SpanInfo{
						FromTime: firstTimeSpan.StartTime, ToTime: firstTimeSpan.EndTime,
					},
				},
				indices,
				bucket,
			)
			if err != nil {
				t.Errorf("Failed to count occurrences: %v", err)
			}
			index := bootstrapper.ClusterTotalCountIndexName
			err = ac.BulkIndex(ctx, res.WindowCountMetaMapList, res.WindowCountDocumentMapList, &index)
			if err != nil {
				t.Errorf("Failed to insert records: %v", err)
			}

			secondRes, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
				ctx,
				secondTimeSpan.ClusterId,
				countModel.TimeInfo{
					SpanInfo: &countModel.SpanInfo{
						FromTime: secondTimeSpan.StartTime, ToTime: secondTimeSpan.EndTime,
					},
				},
				indices,
				bucket,
			)
			if err != nil {
				t.Errorf("Failed to count occurrences: %v", err)
			}
			err = ac.BulkIndex(ctx, secondRes.WindowCountMetaMapList, secondRes.WindowCountDocumentMapList, &index)
			if err != nil {
				t.Errorf("Failed to insert records: %v", err)
			}

			searchQueryBody := countQuery(firstTimeSpan.ClusterId)
			docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.ClusterWindowCountIndexName}, &querySize)
			if err != nil {
				t.Errorf("Failed to search for count: %v", err)
			}
			countEntries, err := countService.ConvertCountDocsToWindowCountEntries(docs)
			if err != nil {
				t.Errorf("Failed to convert count docs to count entries: %v", err)
			}

			countEntry := countEntries[0]
			expectedMeanFirstTerm := firstTimeSpanSecondAfter.StartTime.Sub(firstTimeSpan.StartTime).Seconds()
			expectedMeanSecondTerm := secondTimeSpanTwoSecondsAfter.StartTime.Sub(secondTimeSpan.StartTime).Seconds()
			expectedMean := (expectedMeanFirstTerm + expectedMeanSecondTerm) / 2
			expectedVarianceFirstTerm := (firstTimeSpanSecondAfter.StartTime.Sub(firstTimeSpan.StartTime).Seconds() - expectedMean) * (firstTimeSpanSecondAfter.StartTime.Sub(firstTimeSpan.StartTime).Seconds() - expectedMean)
			expectedVarianceSecondTerm := (secondTimeSpanTwoSecondsAfter.StartTime.Sub(secondTimeSpan.StartTime).Seconds() - expectedMean) * (secondTimeSpanTwoSecondsAfter.StartTime.Sub(secondTimeSpan.StartTime).Seconds() - expectedMean)
			expectedVariance := (expectedVarianceFirstTerm + expectedVarianceSecondTerm) / 1
			assert.Equal(t, expectedMean, countEntry.MeanTDOA)
			assert.InDelta(t, expectedVariance, countEntry.VarianceTDOA, tolerance)
		},
	)

	t.Run(
		"should maintain a running mean and variance for the time difference of arrival for a mix of spans and logs",
		func(t *testing.T) {
			err := deleteAllDocuments(es)
			if err != nil {
				t.Errorf("Failed to delete all documents: %v", err)
			}
			firstTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
			secondTime := time.Date(2021, 1, 2, 0, 0, 1, 204, time.UTC)
			firstTimeSpan := spanModel.Span{
				ClusterId: "initialClusterId",
				StartTime: firstTime,
				EndTime:   firstTime.Add(time.Second),
			}
			firstTimeLogOneSecondAfter := model.LogEntry{
				ClusterId: "otherClusterId",
				Timestamp: firstTime.Add(time.Millisecond * 23),
			}
			secondTimeSpan := spanModel.Span{
				ClusterId: "initialClusterId",
				StartTime: secondTime,
				EndTime:   secondTime.Add(time.Second),
			}
			secondTimeLogTwoSecondsAfter := model.LogEntry{
				ClusterId: "otherClusterId",
				Timestamp: secondTime.Add(time.Millisecond * 17),
			}
			err = loadDataIntoElasticsearch(
				ac, []spanModel.Span{
					firstTimeSpan,
					secondTimeSpan,
				},
				bootstrapper.SpanIndexName,
			)
			if err != nil {
				t.Error("Failed to load spans into elasticsearch")
			}
			err = loadDataIntoElasticsearch(
				ac, []model.LogEntry{
					firstTimeLogOneSecondAfter,
					secondTimeLogTwoSecondsAfter,
				},
				bootstrapper.LogIndexName,
			)
			if err != nil {
				t.Error("Failed to load logs into elasticsearch")
			}
			bucket := countModel.Bucket(4500)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
				ctx,
				firstTimeSpan.ClusterId,
				countModel.TimeInfo{
					SpanInfo: &countModel.SpanInfo{
						FromTime: firstTimeSpan.StartTime, ToTime: firstTimeSpan.EndTime,
					},
				},
				indices,
				bucket,
			)
			if err != nil {
				t.Errorf("Failed to count occurrences: %v", err)
			}
			index := bootstrapper.ClusterTotalCountIndexName
			err = ac.BulkIndex(ctx, res.WindowCountMetaMapList, res.WindowCountDocumentMapList, &index)
			if err != nil {
				t.Errorf("Failed to insert records: %v", err)
			}

			secondRes, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
				ctx,
				secondTimeSpan.ClusterId,
				countModel.TimeInfo{
					SpanInfo: &countModel.SpanInfo{
						FromTime: secondTimeSpan.StartTime, ToTime: secondTimeSpan.EndTime,
					},
				},
				indices,
				bucket,
			)
			if err != nil {
				t.Errorf("Failed to count occurrences: %v", err)
			}
			err = ac.BulkIndex(ctx, secondRes.WindowCountMetaMapList, secondRes.WindowCountDocumentMapList, &index)
			if err != nil {
				t.Errorf("Failed to insert records: %v", err)
			}

			searchQueryBody := countQuery(firstTimeSpan.ClusterId)
			docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.ClusterWindowCountIndexName}, &querySize)
			if err != nil {
				t.Errorf("Failed to search for count: %v", err)
			}
			countEntries, err := countService.ConvertCountDocsToWindowCountEntries(docs)
			if err != nil {
				t.Errorf("Failed to convert count docs to count entries: %v", err)
			}

			countEntry := countEntries[0]
			firstMeanTerm := firstTimeLogOneSecondAfter.Timestamp.Sub(firstTimeSpan.StartTime).Seconds()
			secondMeanTerm := secondTimeLogTwoSecondsAfter.Timestamp.Sub(secondTimeSpan.StartTime).Seconds()
			expectedMean := (firstMeanTerm + secondMeanTerm) / 2
			assert.Equal(t, expectedMean, countEntry.MeanTDOA)

			firstVarianceTerm := (firstTimeLogOneSecondAfter.Timestamp.Sub(firstTimeSpan.StartTime).Seconds() - expectedMean) * (firstTimeLogOneSecondAfter.Timestamp.Sub(firstTimeSpan.StartTime).Seconds() - expectedMean)
			secondVarianceTerm := (secondTimeLogTwoSecondsAfter.Timestamp.Sub(secondTimeSpan.StartTime).Seconds() - expectedMean) * (secondTimeLogTwoSecondsAfter.Timestamp.Sub(secondTimeSpan.StartTime).Seconds() - expectedMean)
			expectedVariance := (firstVarianceTerm + secondVarianceTerm) / 1
			assert.InDelta(t, expectedVariance, countEntry.VarianceTDOA, tolerance)
		},
	)

	t.Run(
		"should maintain a running mean and variance for the time difference of arrival for a mix of logs and spans",
		func(t *testing.T) {
			err := deleteAllDocuments(es)
			if err != nil {
				t.Errorf("Failed to delete all documents: %v", err)
			}
			firstTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
			secondTime := time.Date(2021, 1, 2, 0, 0, 1, 204, time.UTC)
			logAtFirstTime := model.LogEntry{
				ClusterId: "initialClusterId",
				Timestamp: firstTime,
			}
			logAtSecondTime := model.LogEntry{
				ClusterId: "initialClusterId",
				Timestamp: secondTime,
			}
			spanOneSecondAfterFirstTime := spanModel.Span{
				ClusterId: "otherClusterId",
				StartTime: firstTime.Add(time.Millisecond * 38),
				EndTime:   firstTime.Add(time.Second * 2),
			}
			spanTwoSecondsAfterSecondTime := spanModel.Span{
				ClusterId: "otherClusterId",
				StartTime: secondTime.Add(time.Millisecond * 47),
				EndTime:   secondTime.Add(time.Second * 3),
			}
			err = loadDataIntoElasticsearch(
				ac, []spanModel.Span{
					spanOneSecondAfterFirstTime,
					spanTwoSecondsAfterSecondTime,
				},
				bootstrapper.SpanIndexName,
			)
			if err != nil {
				t.Error("Failed to load spans into elasticsearch")
			}
			err = loadDataIntoElasticsearch(
				ac, []model.LogEntry{
					logAtFirstTime,
					logAtSecondTime,
				},
				bootstrapper.LogIndexName,
			)
			if err != nil {
				t.Error("Failed to load logs into elasticsearch")
			}
			bucket := countModel.Bucket(4500)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
				ctx,
				logAtFirstTime.ClusterId,
				countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: logAtFirstTime.Timestamp}},
				indices,
				bucket,
			)
			if err != nil {
				t.Errorf("Failed to count occurrences: %v", err)
			}
			index := bootstrapper.ClusterTotalCountIndexName
			err = ac.BulkIndex(ctx, res.WindowCountMetaMapList, res.WindowCountDocumentMapList, &index)
			if err != nil {
				t.Errorf("Failed to insert records: %v", err)
			}

			secondRes, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
				ctx,
				logAtSecondTime.ClusterId,
				countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: logAtSecondTime.Timestamp}},
				indices,
				bucket,
			)
			if err != nil {
				t.Errorf("Failed to count occurrences: %v", err)
			}
			err = ac.BulkIndex(ctx, secondRes.WindowCountMetaMapList, secondRes.WindowCountDocumentMapList, &index)
			if err != nil {
				t.Errorf("Failed to insert records: %v", err)
			}

			searchQueryBody := countQuery(logAtFirstTime.ClusterId)
			docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.ClusterWindowCountIndexName}, &querySize)
			if err != nil {
				t.Errorf("Failed to search for count: %v", err)
			}
			countEntries, err := countService.ConvertCountDocsToWindowCountEntries(docs)
			if err != nil {
				t.Errorf("Failed to convert count docs to count entries: %v", err)
			}

			countEntry := countEntries[0]
			expectedMeanFirstTime := spanOneSecondAfterFirstTime.StartTime.Sub(logAtFirstTime.Timestamp).Seconds()
			expectedMeanSecondTime := spanTwoSecondsAfterSecondTime.StartTime.Sub(logAtSecondTime.Timestamp).Seconds()
			expectedMean := (expectedMeanFirstTime + expectedMeanSecondTime) / 2
			assert.Equal(t, expectedMean, countEntry.MeanTDOA)

			expectedVarianceFirstTime := (spanOneSecondAfterFirstTime.StartTime.Sub(logAtFirstTime.Timestamp).Seconds() - expectedMean) * (spanOneSecondAfterFirstTime.StartTime.Sub(logAtFirstTime.Timestamp).Seconds() - expectedMean)
			expectedVarianceSecondTime := (spanTwoSecondsAfterSecondTime.StartTime.Sub(logAtSecondTime.Timestamp).Seconds() - expectedMean) * (spanTwoSecondsAfterSecondTime.StartTime.Sub(logAtSecondTime.Timestamp).Seconds() - expectedMean)
			expectedVariance := (expectedVarianceFirstTime + expectedVarianceSecondTime) / 1
			assert.InDelta(t, expectedVariance, countEntry.VarianceTDOA, tolerance)
		},
	)

	t.Run("should deal with one-to-many relationships", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		firstTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		firstTimeLog := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: firstTime,
		}
		firstTimeOverlapOne := model.LogEntry{
			ClusterId: "otherClusterId",
			Timestamp: firstTime.Add(time.Millisecond * 4),
		}
		firstTimeOverlapTwo := model.LogEntry{
			ClusterId: "otherClusterId",
			Timestamp: firstTime.Add(time.Millisecond * 12),
		}
		firstTimeOverlapThree := model.LogEntry{
			ClusterId: "otherClusterId",
			Timestamp: firstTime.Add(time.Millisecond * 22),
		}

		err = loadDataIntoElasticsearch(
			ac, []model.LogEntry{
				firstTimeLog,
				firstTimeOverlapOne,
				firstTimeOverlapTwo,
				firstTimeOverlapThree,
			}, bootstrapper.LogIndexName,
		)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		bucket := countModel.Bucket(4500)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			firstTimeLog.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: firstTimeLog.Timestamp}},
			indices,
			bucket,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		index := bootstrapper.ClusterTotalCountIndexName
		err = ac.BulkIndex(ctx, res.WindowCountMetaMapList, res.WindowCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}

		searchQueryBody := countQuery(firstTimeLog.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.ClusterWindowCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := countService.ConvertCountDocsToWindowCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		countEntry := countEntries[0]
		expectedMeanFirstTerm := firstTimeOverlapOne.Timestamp.Sub(firstTimeLog.Timestamp).Seconds()
		expectedMeanSecondTerm := firstTimeOverlapTwo.Timestamp.Sub(firstTimeLog.Timestamp).Seconds()
		expectedMeanThirdTerm := firstTimeOverlapThree.Timestamp.Sub(firstTimeLog.Timestamp).Seconds()
		expectedMean := (expectedMeanFirstTerm + expectedMeanSecondTerm + expectedMeanThirdTerm) / 3
		assert.Equal(t, expectedMean, countEntry.MeanTDOA)
		// count service will condense all three in the one-to-many relationship into one mean value before processing
		assert.InDelta(t, float64(0), countEntry.VarianceTDOA, tolerance)
	})

	t.Run("should deal with many-to-one relationships", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		firstTime := time.Date(2021, 1, 1, 0, 0, 0, 204, time.UTC)
		firstTimeLogOne := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: firstTime,
		}
		firstTimeLogTwo := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: firstTime.Add(time.Millisecond * 10),
		}
		firstTimeLogThree := model.LogEntry{
			ClusterId: "initialClusterId",
			Timestamp: firstTime.Add(time.Millisecond * 20),
		}
		firstTimeLogOverlap := model.LogEntry{
			ClusterId: "otherClusterId",
			Timestamp: firstTime.Add(time.Millisecond * 30),
		}

		err = loadDataIntoElasticsearch(
			ac, []model.LogEntry{
				firstTimeLogOne,
				firstTimeLogTwo,
				firstTimeLogThree,
				firstTimeLogOverlap,
			},
			bootstrapper.LogIndexName,
		)
		if err != nil {
			t.Error("Failed to load logs into elasticsearch")
		}
		bucket := countModel.Bucket(4500)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		res, err := cs.GetCountAndUpdateOccurrencesQueryConstituents(
			ctx,
			firstTimeLogOne.ClusterId,
			countModel.TimeInfo{LogInfo: &countModel.LogInfo{Timestamp: firstTimeLogOne.Timestamp}},
			indices,
			bucket,
		)
		if err != nil {
			t.Errorf("Failed to count occurrences: %v", err)
		}
		index := bootstrapper.ClusterTotalCountIndexName
		err = ac.BulkIndex(ctx, res.WindowCountMetaMapList, res.WindowCountDocumentMapList, &index)
		if err != nil {
			t.Errorf("Failed to insert records: %v", err)
		}

		searchQueryBody := countQuery(firstTimeLogOne.ClusterId)
		docs, err := ac.Search(ctx, searchQueryBody, []string{bootstrapper.ClusterWindowCountIndexName}, &querySize)
		if err != nil {
			t.Errorf("Failed to search for count: %v", err)
		}
		countEntries, err := countService.ConvertCountDocsToWindowCountEntries(docs)
		if err != nil {
			t.Errorf("Failed to convert count docs to count entries: %v", err)
		}

		countEntry := countEntries[0]
		expectedMean := firstTimeLogOverlap.Timestamp.Sub(firstTimeLogOne.Timestamp).Seconds()
		assert.Equal(t, expectedMean, countEntry.MeanTDOA)
		assert.InDelta(t, float64(0), countEntry.VarianceTDOA, tolerance)
	})
}
