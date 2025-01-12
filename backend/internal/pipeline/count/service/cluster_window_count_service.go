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
const clusterWindowSizeMs = 50

type ClusterWindowCountService struct {
	ac     client.AugurClient
	logger *zap.Logger
}

func NewClusterWindowCountService(
	ac client.AugurClient,
	logger *zap.Logger,
) *ClusterWindowCountService {
	return &ClusterWindowCountService{
		ac:     ac,
		logger: logger,
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
	matchingWindow *model.ClusterWindowCount,
	TDOA float64,
	cluster model.ClusterQueryResult,
) {
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
