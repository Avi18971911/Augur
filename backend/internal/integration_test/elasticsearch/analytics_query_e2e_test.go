package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/internal/db/elasticsearch/client"
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

	as := inference.NewInferenceQueryService(
		ac,
		logger,
	)

	t.Run("should be able to handle easy real data with the last log in sequence", func(t *testing.T) {
		err := deleteAllDocuments(es)
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.LogIndexName, "data/easy_inference/log_index.json")
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.ClusterGraphNodeIndexName, "data/easy_inference/cluster_index.json")
		assert.NoError(t, err)
		err = loadTestDataFromFile(es, bootstrapper.ClusterTotalCountIndexName, "data/easy_inference/count_index.json")
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
