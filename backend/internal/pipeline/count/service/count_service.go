package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/pipeline/count/model"
	"go.uber.org/zap"
	"time"
)

const csTimeOut = 2 * time.Second
const clusterWindowSizeMs = 50

type CountService struct {
	ac     client.AugurClient
	logger *zap.Logger
}

func NewCountService(ac client.AugurClient, logger *zap.Logger) *CountService {
	return &CountService{
		ac:     ac,
		logger: logger,
	}
}

func getTimeRange(timestamp time.Time, bucket model.Bucket) (time.Time, time.Time) {
	fromTime := timestamp.Add(-time.Millisecond * time.Duration(bucket/2))
	toTime := timestamp.Add(time.Millisecond * time.Duration(bucket/2))
	return fromTime, toTime
}

func (cs *CountService) GetCountAndUpdateOccurrencesQueryConstituents(
	ctx context.Context,
	clusterId string,
	timeInfo model.TimeInfo,
	indices []string,
	buckets []model.Bucket,
) (*model.GetCountAndUpdateQueryDetails, error) {
	coClusterMapCount, err := cs.countOccurrencesAndCoOccurrencesByCoClusterId(
		ctx,
		clusterId,
		timeInfo,
		indices,
		buckets,
	)
	if err != nil {
		return nil, err
	}
	countResult := cs.getUpdateCoOccurrencesQueryConstituents(clusterId, coClusterMapCount)
	increaseForMissesInput := getIncrementOccurrencesForMissesInput(clusterId, coClusterMapCount)
	result := &model.GetCountAndUpdateQueryDetails{
		IncreaseIncrementForMissesInput: increaseForMissesInput,
		TotalCountMetaMapList:           countResult.TotalCountMetaMapList,
		TotalCountDocumentMapList:       countResult.TotalCountDocumentMapList,
		WindowCountMetaMapList:          countResult.WindowCountMetaMapList,
		WindowCountDocumentMapList:      countResult.WindowCountDocumentMapList,
	}
	return result, nil
}

func getIncrementOccurrencesForMissesInput(
	clusterId string,
	coClusterToExcludeMapCount map[string]model.ClusterTotalCountInfo,
) model.IncreaseMissesInput {
	coClusterIdsToExclude := getCoClusterIdsFromMap(coClusterToExcludeMapCount)
	return model.IncreaseMissesInput{
		ClusterId:                 clusterId,
		CoClusterDetailsToExclude: coClusterIdsToExclude,
	}
}

func (cs *CountService) getUpdateCoOccurrencesQueryConstituents(
	clusterId string,
	coClusterMapCount map[string]model.ClusterTotalCountInfo,
) model.GetCountQueryDetails {
	totalCountMetaMap := make([]client.MetaMap, 0)
	totalCountUpdateMap := make([]client.DocumentMap, 0)
	windowCountMetaMap := make([]client.MetaMap, 0)
	windowCountUpdateMap := make([]client.DocumentMap, 0)

	for otherClusterId, coClusterDetails := range coClusterMapCount {
		compositeId := GetTotalCountId(clusterId, otherClusterId)
		totalCountMeta, totalCountUpdate := buildUpdateClusterTotalCountsQuery(compositeId, clusterId, otherClusterId)
		totalCountMetaMap = append(totalCountMetaMap, totalCountMeta)
		totalCountUpdateMap = append(totalCountUpdateMap, totalCountUpdate)
		for windowStart, windowDetails := range coClusterDetails.ClusterWindowCountInfo {
			windowId := getWindowId(compositeId, windowStart)
			avgTDOA := windowDetails.TotalTDOA / float64(windowDetails.Occurrences)
			windowMeta, windowUpdate := buildUpdateClusterWindowCountsQuery(
				windowId,
				clusterId,
				otherClusterId,
				avgTDOA,
				windowDetails.Start,
				windowDetails.End,
			)
			windowCountMetaMap = append(windowCountMetaMap, windowMeta)
			windowCountUpdateMap = append(windowCountUpdateMap, windowUpdate)
		}
	}
	return model.GetCountQueryDetails{
		TotalCountMetaMapList:      totalCountMetaMap,
		TotalCountDocumentMapList:  totalCountUpdateMap,
		WindowCountMetaMapList:     windowCountMetaMap,
		WindowCountDocumentMapList: windowCountUpdateMap,
	}
}

func (cs *CountService) countOccurrencesAndCoOccurrencesByCoClusterId(
	ctx context.Context,
	clusterId string,
	timeInfo model.TimeInfo,
	indices []string,
	buckets []model.Bucket,
) (map[string]model.ClusterTotalCountInfo, error) {
	var coClusterInfoMap = make(map[string]model.ClusterTotalCountInfo)
	for _, bucket := range buckets {
		calculatedTimeInfo, err := getTimeRangeForBucket(timeInfo, bucket)
		if err != nil {
			cs.logger.Error(
				"Failed to calculate time range for bucket",
				zap.Any("timeInfo", timeInfo),
				zap.Error(err),
			)
			return nil, fmt.Errorf("error calculating time range for bucket: %w", err)
		}
		fromTime, toTime := calculatedTimeInfo.FromTime, calculatedTimeInfo.ToTime
		coOccurringClusters, err := cs.getCoOccurringCluster(ctx, clusterId, indices, fromTime, toTime)
		if err != nil {
			cs.logger.Error(
				"Failed to get co-occurring clusters",
				zap.String("clusterId", clusterId),
				zap.Error(err),
			)
			return nil, err
		}
		clusterWindows, err := cs.getClusterWindows(
			ctx,
			clusterId,
			getCoClusterIdsFromClusterQueryResults(coOccurringClusters),
		)
		err = addCoOccurringClustersToCoClusterInfoMap(
			coClusterInfoMap,
			coOccurringClusters,
			clusterWindows,
			timeInfo,
		)
	}
	return coClusterInfoMap, nil
}

func (cs *CountService) getCoOccurringCluster(
	ctx context.Context,
	clusterId string,
	indices []string,
	fromTime time.Time,
	toTime time.Time,
) ([]model.ClusterQueryResult, error) {
	queryCtx, cancel := context.WithTimeout(ctx, csTimeOut)
	defer cancel()
	queryBody, err := json.Marshal(buildGetCoOccurringClustersQuery(clusterId, fromTime, toTime))
	if err != nil {
		cs.logger.Error(
			"Failed to marshal count co-occurrences query",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error marshaling query: %w", err)
	}
	var querySize = 10000
	res, err := cs.ac.Search(queryCtx, string(queryBody), indices, &querySize)
	if err != nil {
		cs.logger.Error(
			"Failed to search for count co-occurrences",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error searching for count co-Occurrences: %w", err)
	}
	coOccurringClusters, err := convertDocsToClusters(res)
	if err != nil {
		cs.logger.Error(
			"Failed to convert search results to cluster documents",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error converting search results to cluster: %w", err)
	}
	return coOccurringClusters, nil
}

func (cs *CountService) GetIncrementMissesQueryInfo(
	ctx context.Context,
	input model.IncreaseMissesInput,
) (*model.GetIncrementMissesQueryDetails, error) {
	missingCoClusterIds, err := cs.getNonMatchingCoClusterIds(ctx, input)
	if err != nil {
		return nil, err
	}
	if len(missingCoClusterIds) == 0 {
		return nil, nil
	}

	metaMapList, documentMapList := cs.getIncrementMissesDetails(input.ClusterId, missingCoClusterIds)
	result := model.GetIncrementMissesQueryDetails{
		MetaMapList:     metaMapList,
		DocumentMapList: documentMapList,
	}
	return &result, nil
}

func (cs *CountService) getNonMatchingCoClusterIds(
	ctx context.Context,
	input model.IncreaseMissesInput,
) ([]model.ClusterQueryResult, error) {
	clusterId, coClustersToExclude := input.ClusterId, input.CoClusterDetailsToExclude
	if coClustersToExclude == nil {
		coClustersToExclude = []string{}
	}
	incrementQuery := buildGetNonMatchedCoClusterIdsQuery(
		clusterId,
		coClustersToExclude,
	)
	queryBody, err := json.Marshal(incrementQuery)
	if err != nil {
		cs.logger.Error(
			"Failed to marshal increment query",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error marshaling increment query: %w", err)
	}
	searchCtx, searchCancel := context.WithTimeout(ctx, csTimeOut)
	defer searchCancel()
	querySize := 10000
	res, err := cs.ac.Search(searchCtx, string(queryBody), []string{bootstrapper.ClusterTotalCountIndexName}, &querySize)
	nonMatchingCoClusters, err := convertDocsToCoClusters(res)
	if err != nil {
		cs.logger.Error(
			"Failed to convert search results to cluster documents",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error converting search results to cluster: %w", err)
	}
	return nonMatchingCoClusters, nil
}

func (cs *CountService) getIncrementMissesDetails(
	clusterId string,
	missingCoClusterIds []model.ClusterQueryResult,
) ([]client.MetaMap, []client.DocumentMap) {
	metaMap := make([]client.MetaMap, len(missingCoClusterIds))
	updateMap := make([]client.DocumentMap, len(missingCoClusterIds))
	for i, missingCoClusterId := range missingCoClusterIds {
		compositeKey := GetTotalCountId(clusterId, missingCoClusterId.CoClusterId)
		meta, update := buildIncrementNonMatchedCoClusterIdsQuery(compositeKey)
		metaMap[i] = meta
		updateMap[i] = update
	}
	if len(updateMap) == 0 {
		return nil, nil
	}
	return metaMap, updateMap
}

func (cs *CountService) getClusterWindows(
	ctx context.Context,
	clusterId string,
	coOccurringClusterIds []string,
) ([]model.ClusterWindowCount, error) {
	clusterWindowQuery := buildGetClusterWindowsQuery(clusterId, coOccurringClusterIds)
	queryBody, err := json.Marshal(clusterWindowQuery)
	if err != nil {
		cs.logger.Error(
			"Failed to marshal cluster window query",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error marshaling cluster window query: %w", err)
	}
	searchCtx, searchCancel := context.WithTimeout(ctx, csTimeOut)
	defer searchCancel()
	querySize := 10000
	res, err := cs.ac.Search(
		searchCtx,
		string(queryBody),
		[]string{bootstrapper.ClusterTotalCountIndexName},
		&querySize,
	)
	if err != nil {
		cs.logger.Error(
			"Failed to search for cluster windows",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error searching for cluster windows: %w", err)
	}
	clusterWindows, err := convertDocsToClusterWindows(res)
	if err != nil {
		cs.logger.Error(
			"Failed to convert search results to cluster windows",
			zap.String("clusterId", clusterId),
			zap.Error(err),
		)
		return nil, fmt.Errorf("error converting search results to cluster windows: %w", err)
	}
	return clusterWindows, nil
}

func addCoOccurringClustersToCoClusterInfoMap(
	coClusterInfoMap map[string]model.ClusterTotalCountInfo,
	clusters []model.ClusterQueryResult,
	clusterWindows []model.ClusterWindowCount,
	timeInfo model.TimeInfo,
) error {
	for _, cluster := range clusters {
		TDOA, err := getTDOA(cluster, timeInfo)
		if err != nil {
			return fmt.Errorf("error calculating TDOA: %w", err)
		}
		matchingWindow := getMatchingClusterWindow(cluster, clusterWindows, TDOA)
		var coClusterToWindowMap map[string]model.ClusterWindowCountInfo
		if _, ok := coClusterInfoMap[cluster.ClusterId]; !ok {
			coClusterInfoMap[cluster.ClusterId] = model.ClusterTotalCountInfo{
				Occurrences:            1,
				ClusterWindowCountInfo: map[string]model.ClusterWindowCountInfo{},
			}
			coClusterToWindowMap = coClusterInfoMap[cluster.ClusterId].ClusterWindowCountInfo
		} else {
			coClusterInfoMap[cluster.ClusterId] = model.ClusterTotalCountInfo{
				Occurrences: coClusterInfoMap[cluster.ClusterId].Occurrences + 1,
			}
			coClusterToWindowMap = coClusterInfoMap[cluster.ClusterId].ClusterWindowCountInfo
		}

		var windowDetails model.ClusterWindowCount
		if matchingWindow != nil {
			windowDetails = *matchingWindow
		} else {
			start, end := getTimeRangeForClusterWindow(TDOA, clusterWindowSizeMs)
			windowDetails = model.ClusterWindowCount{
				CoClusterId:  cluster.CoClusterId,
				ClusterId:    cluster.ClusterId,
				Occurrences:  0,
				MeanTDOA:     0,
				VarianceTDOA: 0,
				Start:        start,
				End:          end,
			}
		}

		stringStart := windowDetails.Start.Format(time.RFC3339)
		if _, ok := coClusterToWindowMap[stringStart]; !ok {
			coClusterInfoMap[cluster.ClusterId].ClusterWindowCountInfo[stringStart] = model.ClusterWindowCountInfo{
				Occurrences: 1,
				TotalTDOA:   TDOA,
				Start:       windowDetails.Start,
				End:         windowDetails.End,
			}
		} else {
			previousOccurrences := coClusterInfoMap[cluster.ClusterId].ClusterWindowCountInfo[stringStart].Occurrences
			previousTotalTDOA := coClusterInfoMap[cluster.ClusterId].ClusterWindowCountInfo[stringStart].TotalTDOA

			coClusterInfoMap[cluster.ClusterId].ClusterWindowCountInfo[stringStart] = model.ClusterWindowCountInfo{
				Occurrences: previousOccurrences + 1,
				TotalTDOA:   previousTotalTDOA + TDOA,
				Start:       windowDetails.Start,
				End:         windowDetails.End,
			}
		}
	}
	return nil
}

func convertDocsToClusters(res []map[string]interface{}) ([]model.ClusterQueryResult, error) {
	clusters := make([]model.ClusterQueryResult, len(res))
	for i, hit := range res {
		doc := model.ClusterQueryResult{}
		if clusterId, ok := hit["cluster_id"].(string); !ok {
			return nil, fmt.Errorf("error parsing cluster_id")
		} else {
			doc.ClusterId = clusterId
		}
		if startTime, ok := hit["start_time"].(string); ok && hit["end_time"] != nil {
			coClusterStartTime, err := client.NormalizeTimestampToNanoseconds(startTime)
			if err != nil {
				return nil, fmt.Errorf("error parsing start_time")
			}
			coClusterEndTime, err := client.NormalizeTimestampToNanoseconds(hit["end_time"].(string))
			if err != nil {
				return nil, fmt.Errorf("error parsing end_time")
			}
			doc.SpanResult = &model.SpanQueryResult{
				StartTime: coClusterStartTime,
				EndTime:   coClusterEndTime,
			}
		} else if timestamp, ok := hit["timestamp"].(string); ok {
			coClusterTimestamp, err := client.NormalizeTimestampToNanoseconds(timestamp)
			if err != nil {
				return nil, fmt.Errorf("error parsing timestamp")
			}
			doc.LogResult = &model.LogQueryResult{
				Timestamp: coClusterTimestamp,
			}
		} else {
			return nil, fmt.Errorf("error parsing start_time or timestamp")
		}
		clusters[i] = doc
	}
	return clusters, nil
}

func convertDocsToCoClusters(res []map[string]interface{}) ([]model.ClusterQueryResult, error) {
	clusters := make([]model.ClusterQueryResult, len(res))
	for i, hit := range res {
		doc := model.ClusterQueryResult{}
		if coClusterId, ok := hit["co_cluster_id"].(string); !ok {
			return nil, fmt.Errorf("error parsing co_cluster_id")
		} else {
			doc.CoClusterId = coClusterId
		}
		clusters[i] = doc
	}
	return clusters, nil
}

func convertDocsToClusterWindows(res []map[string]interface{}) ([]model.ClusterWindowCount, error) {
	clusterWindowCounts := make([]model.ClusterWindowCount, len(res))
	for i, hit := range res {
		doc := model.ClusterWindowCount{}
		if clusterId, ok := hit["cluster_id"].(string); !ok {
			return nil, fmt.Errorf("error parsing cluster_id for cluster window")
		} else {
			doc.ClusterId = clusterId
		}
		if coClusterId, ok := hit["co_cluster_id"].(string); !ok {
			return nil, fmt.Errorf("error parsing co_cluster_id for cluster window")
		} else {
			doc.CoClusterId = coClusterId
		}
		if occurrences, ok := hit["occurrences"].(float64); !ok {
			return nil, fmt.Errorf("error parsing occurrences for cluster window")
		} else {
			doc.Occurrences = int64(occurrences)
		}
		if meanTDOA, ok := hit["mean_TDOA"].(float64); !ok {
			return nil, fmt.Errorf("error parsing mean_TDOA for cluster window")
		} else {
			doc.MeanTDOA = meanTDOA
		}
		if varianceTDOA, ok := hit["variance_TDOA"].(float64); !ok {
			return nil, fmt.Errorf("error parsing variance_TDOA for cluster window")
		} else {
			doc.VarianceTDOA = varianceTDOA
		}
		if start, ok := hit["start"].(string); !ok {
			return nil, fmt.Errorf("error parsing start for cluster window")
		} else {
			startTime, err := client.NormalizeTimestampToNanoseconds(start)
			if err != nil {
				return nil, fmt.Errorf("error parsing start into time.Time for cluster window")
			}
			doc.Start = startTime
		}
		if end, ok := hit["end"].(string); !ok {
			return nil, fmt.Errorf("error parsing end for cluster window")
		} else {
			endTime, err := client.NormalizeTimestampToNanoseconds(end)
			if err != nil {
				return nil, fmt.Errorf("error parsing end into time.Time for cluster window")
			}
			doc.End = endTime
		}
		clusterWindowCounts[i] = doc
	}
	return clusterWindowCounts, nil
}

func getTimeRangeForBucket(timeInfo model.TimeInfo, bucket model.Bucket) (model.CalculatedTimeInfo, error) {
	if timeInfo.SpanInfo != nil {
		return model.CalculatedTimeInfo{
			FromTime: timeInfo.SpanInfo.FromTime.Add(-time.Millisecond * time.Duration(bucket/2)),
			ToTime:   timeInfo.SpanInfo.ToTime.Add(time.Millisecond * time.Duration(bucket/2)),
		}, nil
	} else if timeInfo.LogInfo != nil {
		startTime, endTime := getTimeRange(timeInfo.LogInfo.Timestamp, bucket)
		return model.CalculatedTimeInfo{
			FromTime: startTime,
			ToTime:   endTime,
		}, nil
	} else {
		return model.CalculatedTimeInfo{}, fmt.Errorf("timeInfo.spanInfo or timeInfo.logInfo is required")
	}
}

func GetTotalCountId(clusterId, coClusterId string) string {
	return fmt.Sprintf("%s;%s", clusterId, coClusterId)
}

func getWindowId(compositeId string, windowStart string) string {
	return fmt.Sprintf("%s;%s", compositeId, windowStart)
}

func getTDOA(coCluster model.ClusterQueryResult, clusterTimeInfo model.TimeInfo) (float64, error) {
	var coClusterTime time.Time
	if coCluster.SpanResult != nil {
		coClusterTime = coCluster.SpanResult.StartTime
	} else if coCluster.LogResult != nil {
		coClusterTime = coCluster.LogResult.Timestamp
	} else {
		return 0, fmt.Errorf("coCluster.spanResult or coCluster.logResult is required")
	}

	if clusterTimeInfo.SpanInfo != nil {
		return coClusterTime.Sub(clusterTimeInfo.SpanInfo.FromTime).Seconds(), nil
	} else if clusterTimeInfo.LogInfo != nil {
		return coClusterTime.Sub(clusterTimeInfo.LogInfo.Timestamp).Seconds(), nil
	}
	return 0, fmt.Errorf("clusterTimeInfo.spanInfo or clusterTimeInfo.logInfo is required")
}

func getCoClusterIdsFromMap(coClusterMapCount map[string]model.ClusterTotalCountInfo) []string {
	coClusterIds := make([]string, len(coClusterMapCount))
	i := 0
	for coClusterId := range coClusterMapCount {
		coClusterIds[i] = coClusterId
		i++
	}
	return coClusterIds
}

func getCoClusterIdsFromClusterQueryResults(clusters []model.ClusterQueryResult) []string {
	coClusterIds := make([]string, len(clusters))
	for i, cluster := range clusters {
		coClusterIds[i] = cluster.CoClusterId
	}
	return coClusterIds
}

func getMatchingClusterWindow(
	cluster model.ClusterQueryResult,
	clusterWindows []model.ClusterWindowCount,
	TDOA float64,
) *model.ClusterWindowCount {
	for _, window := range clusterWindows {
		if window.CoClusterId == cluster.CoClusterId &&
			window.ClusterId == cluster.ClusterId &&
			windowOverlapsWithSample(window, TDOA) {
			return &window
		}
	}
	return nil
}

func windowOverlapsWithSample(
	window model.ClusterWindowCount,
	TDOA float64,
) bool {
	start := float64(window.Start.UnixMicro()) / 1e6
	end := float64(window.End.UnixMicro()) / 1e6
	return start <= TDOA && TDOA <= end
}

func getTimeRangeForClusterWindow(TDOA float64, windowSizeMs int) (time.Time, time.Time) {
	increment := float64(windowSizeMs) / 2
	middlePoint := time.Unix(0, int64(TDOA*1e9))
	start := middlePoint.Add(-time.Millisecond * time.Duration(increment))
	end := middlePoint.Add(time.Millisecond * time.Duration(increment))
	return start, end
}
