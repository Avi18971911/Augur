package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
	"github.com/Avi18971911/Augur/internal/otel_server/log/helper"
	"github.com/Avi18971911/Augur/internal/pipeline/analytics/service"
	clusterService "github.com/Avi18971911/Augur/internal/pipeline/cluster/service"
	"github.com/Avi18971911/Augur/internal/pipeline/count/model"
	countService "github.com/Avi18971911/Augur/internal/pipeline/count/service"
	"github.com/Avi18971911/Augur/internal/query_server/handler"
	"github.com/Avi18971911/Augur/internal/query_server/service/inference"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"net/http/httptest"
	"testing"
)

func TestAnalyticsQuery(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}

	ac := client.NewAugurClientImpl(es, client.Immediate)
	logger, err := zap.NewProduction()
	if err != nil {
		t.Errorf("Failed to create logger: %v", err)
	}

	an := service.NewAnalyticsService(
		ac,
		logger,
	)

	as := inference.NewInferenceQueryService(
		ac,
		logger,
	)
	bucket := model.Bucket(1000 * 30)
	indices := []string{bootstrapper.LogIndexName}
	cls := clusterService.NewClusterService(ac, logger)
	wc := countService.NewClusterWindowCountService(ac, 50, logger)
	cs := countService.NewClusterTotalCountService(ac, wc, logger)
	cdp := clusterService.NewClusterDataProcessor(ac, cls, logger)
	csp := countService.NewCountDataProcessorService(ac, cs, bucket, indices, logger)

	t.Run("should be able to handle easy real data with the last log in sequence", func(t *testing.T) {
		err := deleteAllDocuments(es)
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.LogIndexName, "data/easy_inference/log_index.json")
		assert.NoError(t, err)

		stringQuery, err := json.Marshal(getAllQuery())
		assert.NoError(t, err)
		var querySize = 10000
		logDocs, err := ac.Search(
			context.Background(),
			string(stringQuery),
			[]string{bootstrapper.LogIndexName},
			&querySize,
		)
		logs, err := helper.ConvertFromDocuments(logDocs)
		spanOrLogData := convertLogToSpanOrLogData(logs)
		clusterOutput, err := cdp.ClusterData(context.Background(), spanOrLogData)
		assert.NoError(t, err)
		countOutput, err := csp.IncreaseCountForOverlapsAndMisses(context.Background(), clusterOutput)
		assert.NoError(t, err)
		err = an.UpdateAnalytics(context.Background(), countOutput)
		assert.NoError(t, err)

		const numExpectedLogs = 8
		const id = "61d5afe39d21cbea98aa5a395ac158a2564e2d0d1d9b0e300aeced2b67a0cc8f"
		input := handler.ChainOfEventsRequestDTO{
			Id: id,
		}
		body, err := json.Marshal(input)
		assert.NoError(t, err)
		req := httptest.NewRequest("GET", "/graph", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		w := httptest.NewRecorder()
		var response handler.ChainOfEventsResponseDTO
		handler.ChainOfEventsHandler(context.Background(), as, logger)(w, req)

		err = json.NewDecoder(w.Result().Body).Decode(&response)
		assert.NoError(t, err)
		assert.Equal(t, numExpectedLogs, len(response.Graph))
		for _, node := range response.Graph {
			assert.Equal(t, numExpectedLogs-1, len(node.Successors)+len(node.Predecessors))
		}
		assert.Equal(t, numExpectedLogs-1, len(response.Graph[id].Predecessors))
		assert.Equal(t, 0, len(response.Graph[id].Successors))
	})
}
