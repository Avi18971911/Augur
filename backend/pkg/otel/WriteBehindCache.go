package otel

import (
	"github.com/Avi18971911/Augur/pkg/otel/model"
	"github.com/dgraph-io/ristretto"
)

// WriteBehindCache is an interface for a cache batches writes to a backend store or database.
// Eviction is based on LRU and LFU policies.
type WriteBehindCache interface {
	Get(key string) (model.Span, error)
	Put(key string, value model.Span) error
}

type WriteBehindCacheImpl struct {
	cache      *ristretto.Cache
	writeQueue map[string][]model.Span
}

func NewWriteBehindCacheImpl() (*WriteBehindCacheImpl, error) {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		return nil, err
	}

	return &WriteBehindCacheImpl{
		cache:      cache,
		writeQueue: make(map[string][]model.Span),
	}, nil
}
