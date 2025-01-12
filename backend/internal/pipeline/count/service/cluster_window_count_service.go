package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/pipeline/count/model"
	"go.uber.org/zap"
	"strconv"
	"time"
)

const wcTimeOut = 2 * time.Second

type ClusterWindowCountService struct {
	ac                client.AugurClient
	clusterWindowSize int
	logger            *zap.Logger
}

func NewClusterWindowCountService(
	ac client.AugurClient,
	clusterWindowSizeMs int,
	logger *zap.Logger,
) *ClusterWindowCountService {
	return &ClusterWindowCountService{
		ac:                ac,
		clusterWindowSize: clusterWindowSizeMs,
		logger:            logger,
	}
}

func (cs *ClusterWindowCountService) getClusterWindows(
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
	searchCtx, searchCancel := context.WithTimeout(ctx, wcTimeOut)
	defer searchCancel()
	querySize := 10000
	res, err := cs.ac.Search(
		searchCtx,
		string(queryBody),
		[]string{bootstrapper.ClusterWindowCountIndexName},
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

func (cs *ClusterWindowCountService) addWindowDataToCoClusterInfoMap(
	coClusterToWindowMap map[string]model.ClusterWindowCountInfo,
	clusterWindows []model.ClusterWindowCount,
	clusterId string,
	coClusterId string,
	TDOA float64,
) model.ClusterWindowCount {
	matchingWindow := getMatchingClusterWindow(clusterId, coClusterId, clusterWindows, TDOA)
	var windowDetails model.ClusterWindowCount
	if matchingWindow != nil {
		windowDetails = *matchingWindow
	} else {
		start, end := getTimeRangeForClusterWindow(TDOA, cs.clusterWindowSize)
		windowDetails = model.ClusterWindowCount{
			CoClusterId:  coClusterId,
			ClusterId:    clusterId,
			Occurrences:  0,
			MeanTDOA:     0,
			VarianceTDOA: 0,
			Start:        start,
			End:          end,
		}
	}

	clusterWindowIndex := getIndexFromWindowDetails(windowDetails)
	if _, ok := coClusterToWindowMap[clusterWindowIndex]; !ok {
		coClusterToWindowMap[clusterWindowIndex] = model.ClusterWindowCountInfo{
			Occurrences: 1,
			TotalTDOA:   TDOA,
			Start:       windowDetails.Start,
			End:         windowDetails.End,
		}
	} else {
		previousOccurrences := coClusterToWindowMap[clusterWindowIndex].Occurrences
		previousTotalTDOA := coClusterToWindowMap[clusterWindowIndex].TotalTDOA

		coClusterToWindowMap[clusterWindowIndex] = model.ClusterWindowCountInfo{
			Occurrences: previousOccurrences + 1,
			TotalTDOA:   previousTotalTDOA + TDOA,
			Start:       windowDetails.Start,
			End:         windowDetails.End,
		}
	}
	return windowDetails
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
		if start, ok := hit["start"].(float64); !ok {
			return nil, fmt.Errorf("error parsing start for cluster window")
		} else {
			doc.Start = start
		}
		if end, ok := hit["end"].(float64); !ok {
			return nil, fmt.Errorf("error parsing end for cluster window")
		} else {
			doc.End = end
		}
		clusterWindowCounts[i] = doc
	}
	return clusterWindowCounts, nil
}

func getWindowId(compositeId string, windowStart string) string {
	return fmt.Sprintf("%s;%s", compositeId, windowStart)
}

func getMatchingClusterWindow(
	clusterId string,
	coClusterId string,
	clusterWindows []model.ClusterWindowCount,
	TDOA float64,
) *model.ClusterWindowCount {
	for _, window := range clusterWindows {
		if window.CoClusterId == coClusterId &&
			window.ClusterId == clusterId &&
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
	return window.Start <= TDOA && window.End >= TDOA
}

func getTimeRangeForClusterWindow(tdoaInSeconds float64, windowSizeMs int) (float64, float64) {
	start := tdoaInSeconds - float64(windowSizeMs)/2/1000
	end := tdoaInSeconds + float64(windowSizeMs)/2/1000
	return start, end
}

func getIndexFromWindowDetails(window model.ClusterWindowCount) string {
	return strconv.FormatFloat(window.Start, 'f', -1, 64)
}

func ConvertCountDocsToWindowCountEntries(docs []map[string]interface{}) ([]model.ClusterWindowCountEntry, error) {
	var countEntries []model.ClusterWindowCountEntry
	for _, doc := range docs {
		countEntry := model.ClusterWindowCountEntry{}
		coClusterId, ok := doc["co_cluster_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert co_cluster_id to string")
		}
		countEntry.CoClusterId = coClusterId
		clusterId, ok := doc["cluster_id"].(string)
		if !ok {
			return nil, fmt.Errorf("failed to convert cluster_id to string")
		}
		countEntry.ClusterId = clusterId
		start, ok := doc["start"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert start to float64")
		}
		countEntry.Start = start
		end, ok := doc["end"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert end to float64")
		}
		countEntry.End = end
		occurrences, ok := doc["occurrences"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert occurrences to float64")
		}
		countEntry.Occurrences = int64(occurrences)
		meanTDOA, ok := doc["mean_TDOA"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert mean_tdoa to float64")
		}
		countEntry.MeanTDOA = meanTDOA
		varianceTDOA, ok := doc["variance_TDOA"].(float64)
		if !ok {
			return nil, fmt.Errorf("failed to convert variance_tdoa to float64")
		}
		countEntry.VarianceTDOA = varianceTDOA
		countEntries = append(countEntries, countEntry)
	}
	return countEntries, nil
}
