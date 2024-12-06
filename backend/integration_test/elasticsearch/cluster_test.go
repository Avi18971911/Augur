package elasticsearch

import (
	"context"
	clusterModel "github.com/Avi18971911/Augur/pkg/cluster/model"
	clusterService "github.com/Avi18971911/Augur/pkg/cluster/service"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/bootstrapper"
	"github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSpanClusterUpdates(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	ac := client.NewAugurClientImpl(es, client.Immediate)
	cls := clusterService.NewClusterService(ac, logger)
	t.Run("should be able to process and update spans of the same type", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		err = loadTestDataFromFile(es, bootstrapper.SpanIndexName, "data_dump/span_index_array.json")
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		input := clusterModel.ClusterInput{
			DataType:    clusterModel.SpanClusterInputType,
			TextualData: "service=loadgenerator,operation=GET,kind=SPAN_KIND_CLIENT,attributes=map[http.method:GET http.url:http://frontend-proxy:8080/api/cart]",
			Id:          "test",
		}
		output, err := cls.ClusterData(ctx, input)
		if err != nil {
			t.Errorf("Failed to parse span with message: %v", err)
		}
		assert.NotZero(t, len(output))
		firstClusterId := output[0].ClusterId
		firstId := output[0].ObjectId
		for _, cluster := range output[1:] {
			assert.Equal(t, firstClusterId, cluster.ClusterId)
			assert.NotEqual(t, firstId, cluster.ObjectId)
		}
	})

	t.Run("should not assign spans with different cluster events to the same cluster id", func(t *testing.T) {
		err := deleteAllDocuments(es)
		if err != nil {
			t.Errorf("Failed to delete all documents: %v", err)
		}
		err = loadTestDataFromFile(es, bootstrapper.SpanIndexName, "data_dump/span_index_array.json")
		if err != nil {
			t.Errorf("Failed to load test data: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		firstInput := clusterModel.ClusterInput{
			DataType:    clusterModel.SpanClusterInputType,
			TextualData: "service=loadgenerator,operation=GET,kind=SPAN_KIND_CLIENT,attributes=map[http.method:GET http.url:http://frontend-proxy:8080/api/cart]",
			Id:          "test",
		}
		firstClusterOutput, err := cls.ClusterData(ctx, firstInput)
		if err != nil {
			t.Errorf("Failed to parse span with message: %v", err)
		}
		secondInput := clusterModel.ClusterInput{
			DataType:    clusterModel.SpanClusterInputType,
			TextualData: "service=flagd,operation=resolveInt,kind=SPAN_KIND_INTERNAL,attributes=map[]",
			Id:          "test",
		}
		secondClusterOutput, err := cls.ClusterData(ctx, secondInput)
		if err != nil {
			t.Errorf("Failed to parse span with message: %v", err)
		}
		assert.NotZero(t, len(firstClusterOutput))
		assert.NotZero(t, len(secondClusterOutput))
		var clusterIdOne = firstClusterOutput[0].ClusterId
		var clusterIdTwo = secondClusterOutput[0].ClusterId

		assert.NotEqual(t, 1, len(firstClusterOutput))
		assert.NotEqual(t, 1, len(secondClusterOutput))

		for _, output := range firstClusterOutput[1:] {
			assert.Equal(t, clusterIdOne, output.ClusterId)
			assert.NotEqual(t, clusterIdTwo, output.ClusterId)
		}
		for _, output := range secondClusterOutput[1:] {
			assert.Equal(t, clusterIdTwo, output.ClusterId)
			assert.NotEqual(t, clusterIdOne, output.ClusterId)
		}
	})
}
