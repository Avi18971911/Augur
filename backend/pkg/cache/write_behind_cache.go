package cache

import (
	"context"
	"errors"
	"fmt"
	augurElasticsearch "github.com/Avi18971911/Augur/pkg/elasticsearch"
	"github.com/dgraph-io/ristretto"
	"go.uber.org/zap"
	"sync"
	"time"
)

const WriteQueueSize = 100
const timeOut = 500 * time.Millisecond

// WriteBehindCache is an interface for a cache batches writes to a backend store or database.
// Eviction is based on LRU and LFU policies.
// TODO: Remove cache and replace with buffered writes to Elasticsearch DB
// TODO: If we want to use cache, use interface to allow for overwrites
type WriteBehindCache[ValueType any] interface {
	Get(key string) ([]ValueType, error)
	Put(key string, value []ValueType) error
}

type WriteBehindCacheImpl[ValueType interface{}] struct {
	cache       *ristretto.Cache
	writeQueue  []ValueType
	ac          augurElasticsearch.AugurClient
	esIndexName string
	logger      *zap.Logger
	mu          sync.Mutex
}

func NewWriteBehindCacheImpl[ValueType interface{}](
	cache *ristretto.Cache,
	ac augurElasticsearch.AugurClient,
	esIndexName string,
	logger *zap.Logger,
) *WriteBehindCacheImpl[ValueType] {
	return &WriteBehindCacheImpl[ValueType]{
		cache:       cache,
		writeQueue:  []ValueType{},
		ac:          ac,
		esIndexName: esIndexName,
		logger:      logger,
	}
}

func (wbc *WriteBehindCacheImpl[ValueType]) Get(key string) ([]ValueType, error) {
	value, found := wbc.cache.Get(key)
	if !found {
		return nil, ErrKeyNotFound
	}
	typedValue, ok := value.([]ValueType)
	if !ok {
		return nil, fmt.Errorf("value not of expected type %T returned from cache when getting", value)
	}

	return typedValue, nil
}

func (wbc *WriteBehindCacheImpl[ValueType]) Put(key string, value []ValueType) error {
	wbc.mu.Lock()
	wbc.writeQueue = append(wbc.writeQueue, value...)
	wbc.mu.Unlock()
	if len(wbc.writeQueue) > WriteQueueSize {
		err := wbc.flushToElasticsearch()
		if err != nil {
			return fmt.Errorf("error flushing to Elasticsearch: %w", err)
		}
	}
	oldValue, found := wbc.cache.Get(key)
	if found {
		typedOldValue, ok := oldValue.([]ValueType)
		if !ok {
			return fmt.Errorf("value not of expected type %T returned from cache when putting", value)
		}
		totalValue := append(typedOldValue, value...)
		set := wbc.cache.Set(key, totalValue, int64(len(totalValue)))
		if !set {
			return ErrSetFailed
		}
	} else {
		set := wbc.cache.Set(key, value, int64(len(value)))
		if !set {
			return ErrSetFailed
		}
	}
	return nil
}

func (wbc *WriteBehindCacheImpl[ValueType]) flushToElasticsearch() error {
	wbc.mu.Lock()
	defer wbc.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()
	err := wbc.ac.BulkIndex(
		augurElasticsearch.ToInterfaceSlice(wbc.writeQueue),
		nil,
		wbc.esIndexName,
		ctx,
	)
	wbc.writeQueue = []ValueType{}
	if err != nil {
		return fmt.Errorf("error bulk indexing to Elasticsearch: %w", err)
	}
	return nil
}

var (
	ErrKeyNotFound = errors.New("key not found within the cache")
	ErrSetFailed   = errors.New("failed to set value in cache")
)
