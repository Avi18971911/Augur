package trace

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/ristretto"
)

// WriteBehindCache is an interface for a cache batches writes to a backend store or database.
// Eviction is based on LRU and LFU policies.
type WriteBehindCache[ValueType interface{}] interface {
	Get(key string) ([]ValueType, error)
	Put(key string, value []ValueType) error
}

type WriteBehindCacheImpl[ValueType interface{}] struct {
	cache      *ristretto.Cache
	writeQueue map[string][]ValueType
}

func NewWriteBehindCacheImpl[ValueType interface{}](cache *ristretto.Cache) *WriteBehindCacheImpl[ValueType] {
	return &WriteBehindCacheImpl[ValueType]{
		cache:      cache,
		writeQueue: make(map[string][]ValueType),
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

var (
	ErrKeyNotFound = errors.New("key not found within the cache")
	ErrSetFailed   = errors.New("failed to set value in cache")
)
