package elasticsearch

import (
	"context"
	"github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/Avi18971911/Augur/pkg/log/model"
	"github.com/Avi18971911/Augur/pkg/log/service"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestUpdates(t *testing.T) {
	if es == nil {
		t.Error("es is uninitialized or otherwise nil")
	}
	err := loadTestDataFromFile(es, "log_index", "data_dump/log_index_array.json")
	if err != nil {
		t.Errorf("Failed to load test data: %v", err)
	}

	logProcessor := newLogProcessor()
	t.Run("should be able to process and update logs of the same type", func(t *testing.T) {
		logService := "kafka.cluster.Partition"
		ctx := context.Background()
		logEntry := model.LogEntry{
			Service: "kafka.cluster.Partition",
			Message: "[Partition __consumer_offsets-99 broker=2] Log loaded for partition " +
				"__consumer_offsets-55 with initial high watermark 0",
		}
		newLog, err := logProcessor.ParseLogWithMessage(logService, logEntry, ctx)
		if err != nil {
			t.Errorf("Failed to parse log with message: %v", err)
		}
		assert.NotEqual(t, "", newLog.ClusterId)
	})
}

func newLogProcessor() service.LogProcessorService {
	ac := elasticsearch.NewAugurClientImpl(es)
	return service.NewLogProcessorService(ac, logger)
}
