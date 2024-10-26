package otel

import (
	"errors"
	"github.com/Avi18971911/Augur/pkg/otel/model"
	"github.com/dgraph-io/ristretto"
)

// WriteBehindCache is an interface for a cache batches writes to a backend store or database.
// Eviction is based on LRU and LFU policies.
type WriteBehindCache interface {
	Get(key string) ([]model.Span, error)
	Put(key string, value []model.Span) error
}

type WriteBehindCacheImpl struct {
	cache      *ristretto.Cache
	writeQueue map[string][]model.Span
}

func NewWriteBehindCacheImpl(cache *ristretto.Cache) *WriteBehindCacheImpl {
	return &WriteBehindCacheImpl{
		cache:      cache,
		writeQueue: make(map[string][]model.Span),
	}
}

func (wbc *WriteBehindCacheImpl) Get(key string) ([]model.Span, error) {
	value, found := wbc.cache.Get(key)
	if !found {
		return nil, ErrKeyNotFound
	}
	typedValue, ok := value.([]model.Span)
	if !ok {
		return nil, errors.New("value not of type []model.Span returned from cache when getting")
	}

	return typedValue, nil
}

func (wbc *WriteBehindCacheImpl) Put(key string, value []model.Span) error {
	wbc.writeQueue[key] = append(wbc.writeQueue[key], value...)
	oldValue, found := wbc.cache.Get(key)
	if found {
		typedOldValue, ok := oldValue.([]model.Span)
		if !ok {
			return errors.New("value not of type []model.Span returned from cache when putting")
		}
		totalValue := append(typedOldValue, value...)
		wbc.cache.Set(key, totalValue, int64(len(totalValue)))
	} else {
		wbc.cache.Set(key, value, int64(len(value)))
	}
	return nil
}

var (
	ErrKeyNotFound = errors.New("key not found within the cache")
)
