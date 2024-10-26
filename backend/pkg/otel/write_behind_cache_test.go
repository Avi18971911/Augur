package otel

import (
	"github.com/dgraph-io/ristretto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteBehindCacheImpl_Get(t *testing.T) {
	wbc := getNewWriteBehindCacheImpl()

	t.Run("Returns error if key is not found", func(t *testing.T) {
		_, err := wbc.Get("key")
		if err == nil {
			t.Error("Expected error, got nil")
		}
		assert.Equal(t, ErrKeyNotFound, err)
	})
}

func getNewWriteBehindCacheImpl() *WriteBehindCacheImpl {
	cache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10,
		MaxCost:     1 << 5,
		BufferItems: 2,
	})
	return NewWriteBehindCacheImpl(cache)
}
