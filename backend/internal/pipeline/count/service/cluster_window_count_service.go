package service

import (
	"fmt"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/pipeline/count/model"
	"go.uber.org/zap"
	"math"
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

func (cs *ClusterWindowCountService) addWindowDataToCoClusterInfoMap(
	coClusterToWindowMap map[string]model.ClusterWindowCountInfo,
	TDOA float64,
) {
	start, end := getTimeRangeForClusterWindow(TDOA, cs.clusterWindowSize)
	clusterWindowIndex := getIndexFromWindowDetails(start)
	if _, ok := coClusterToWindowMap[clusterWindowIndex]; !ok {
		coClusterToWindowMap[clusterWindowIndex] = model.ClusterWindowCountInfo{
			Occurrences: 1,
			TotalTDOA:   TDOA,
			Start:       start,
			End:         end,
		}
	} else {
		previousOccurrences := coClusterToWindowMap[clusterWindowIndex].Occurrences
		previousTotalTDOA := coClusterToWindowMap[clusterWindowIndex].TotalTDOA

		coClusterToWindowMap[clusterWindowIndex] = model.ClusterWindowCountInfo{
			Occurrences: previousOccurrences + 1,
			TotalTDOA:   previousTotalTDOA + TDOA,
			Start:       start,
			End:         end,
		}
	}
}

func getWindowId(compositeId string, windowStart string) string {
	return fmt.Sprintf("%s;%s", compositeId, windowStart)
}

func getTimeRangeForClusterWindow(tdoaInSeconds float64, windowSizeMs int) (float64, float64) {
	multiplier := math.Floor(tdoaInSeconds * 1000 / float64(windowSizeMs))
	start := multiplier * float64(windowSizeMs) / 1000
	end := (multiplier + 1) * float64(windowSizeMs) / 1000
	return start, end
}

func getIndexFromWindowDetails(startTime float64) string {
	return strconv.FormatFloat(startTime, 'f', -1, 64)
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
