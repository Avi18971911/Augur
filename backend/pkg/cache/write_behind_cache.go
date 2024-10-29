package cache

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dgraph-io/ristretto"
	"github.com/elastic/go-elasticsearch/v8"
	"go.uber.org/zap"
)

// WriteBehindCache is an interface for a cache batches writes to a backend store or database.
// Eviction is based on LRU and LFU policies.
// TODO: Remove cache and replace with buffered writes to Elasticsearch DB
// TOOD: If we want to use cache, use interface to allow for overwrites
type WriteBehindCache[ValueType interface{}] interface {
	Get(key string) ([]ValueType, error)
	Put(key string, value []ValueType) error
}

type WriteBehindCacheImpl[ValueType interface{}] struct {
	cache       *ristretto.Cache
	writeQueue  map[string][]ValueType
	es          *elasticsearch.Client
	esIndexName string
	logger      *zap.Logger
}

func NewWriteBehindCacheImpl[ValueType interface{}](
	cache *ristretto.Cache,
	es *elasticsearch.Client,
	esIndexName string,
	logger *zap.Logger,
) *WriteBehindCacheImpl[ValueType] {
	return &WriteBehindCacheImpl[ValueType]{
		cache:       cache,
		writeQueue:  make(map[string][]ValueType),
		es:          es,
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
	wbc.writeQueue[key] = append(wbc.writeQueue[key], value...)
	if len(wbc.writeQueue) > 100 {
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
	wbc.logger.Info("Flushing to Elasticsearch")
	wbc.logger.Info("Number of items in write queue", zap.Int("count", len(wbc.writeQueue)))
	for key, docs := range wbc.writeQueue {
		wbc.logger.Info("Number of items in write queue", zap.Int("count", len(docs)))
		for _, doc := range docs {
			wbc.logger.Info("Adding doc to elasticsearch", zap.Any("doc", doc))
			JSON, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("error marshaling doc to flush to elastic search: %w", err)
			}
			res, err := wbc.es.Index(wbc.esIndexName, bytes.NewReader(JSON))
			if err != nil {
				return fmt.Errorf("error flushing to Elasticsearch: %w", err)
			}
			wbc.logger.Info("Successfully flushed to Elasticsearch", zap.Any("response", res))
			res.Body.Close()
		}
		delete(wbc.writeQueue, key)
	}
	wbc.logger.Info("Successfully flushed to Elasticsearch")
	return nil
}

var (
	ErrKeyNotFound = errors.New("key not found within the cache")
	ErrSetFailed   = errors.New("failed to set value in cache")
)
