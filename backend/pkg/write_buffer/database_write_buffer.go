package write_buffer

import (
	"context"
	"fmt"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch/client"
	"go.uber.org/zap"
	"sync"
	"time"
)

const WriteQueueSize = 30
const flushTimeOut = 10 * time.Second

type DatabaseWriteBuffer[ValueType any] interface {
	WriteToBuffer(value []ValueType)
}

type DatabaseWriteBufferImpl[ValueType interface{}] struct {
	writeQueue  []ValueType
	ac          augurElasticsearch.AugurClient
	esIndexName string
	logger      *zap.Logger
	mu          sync.Mutex
}

func NewDatabaseWriteBufferImpl[ValueType interface{}](
	ac augurElasticsearch.AugurClient,
	esIndexName string,
	logger *zap.Logger,
) *DatabaseWriteBufferImpl[ValueType] {
	return &DatabaseWriteBufferImpl[ValueType]{
		writeQueue:  []ValueType{},
		ac:          ac,
		esIndexName: esIndexName,
		logger:      logger,
	}
}

func (wbc *DatabaseWriteBufferImpl[ValueType]) WriteToBuffer(
	value []ValueType,
) {
	wbc.mu.Lock()
	wbc.writeQueue = append(wbc.writeQueue, value...)
	wbc.mu.Unlock()
	if len(wbc.writeQueue) > WriteQueueSize {
		go func() {
			err := wbc.flushToElasticsearch()
			if err != nil {
				wbc.logger.Error("Failed to flush to Elasticsearch", zap.Error(err))
			}
		}()
	}
}

func (wbc *DatabaseWriteBufferImpl[ValueType]) flushToElasticsearch() error {
	wbc.mu.Lock()
	defer wbc.mu.Unlock()
	ctx := context.Background()
	bulkCtx, cancel := context.WithTimeout(ctx, flushTimeOut)
	defer cancel()
	metaMap, dataMap, err := augurElasticsearch.ToMetaAndDataMap(wbc.writeQueue)
	if err != nil {
		return fmt.Errorf("error converting write queue to meta and data map: %w", err)
	}
	if metaMap == nil || len(metaMap) == 0 {
		wbc.writeQueue = []ValueType{}
		return nil
	}
	err = wbc.ac.BulkIndex(
		bulkCtx,
		metaMap,
		dataMap,
		&wbc.esIndexName,
	)
	wbc.writeQueue = []ValueType{}
	if err != nil {
		return fmt.Errorf("error bulk indexing to Elasticsearch: %w", err)
	}
	return nil
}
