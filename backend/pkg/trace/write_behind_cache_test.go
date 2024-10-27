package trace

import (
	"github.com/Avi18971911/Augur/pkg/trace/model"
	"github.com/dgraph-io/ristretto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteBehindCacheImpl_Get(t *testing.T) {
	t.Run("Returns error if key is not found", func(t *testing.T) {
		wbc := getNewWriteBehindCacheImpl()
		_, err := wbc.Get("key")
		if err == nil {
			t.Error("Expected error, got nil")
		}
		assert.Equal(t, ErrKeyNotFound, err)
	})

	t.Run("Returns value if key is found", func(t *testing.T) {
		wbc := getNewWriteBehindCacheImpl()
		key := "key"
		value := []model.Span{
			{
				TraceID: "traceId",
				SpanID:  "spanId",
			},
		}
		err := wbc.Put(key, value)
		assert.Nil(t, err)
		wbc.cache.Wait()
		res, err := wbc.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, value, res)
	})
}

func TestWriteBehindCacheImpl_Put(t *testing.T) {
	t.Run("Sets value if key is not found", func(t *testing.T) {
		wbc := getNewWriteBehindCacheImpl()
		key := "key"
		value := []model.Span{
			{
				TraceID: "traceId",
				SpanID:  "spanId",
			},
		}
		err := wbc.Put(key, value)
		assert.Nil(t, err)
		wbc.cache.Wait()
		res, err := wbc.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, value, res)
	})

	t.Run("Appends value if key is found", func(t *testing.T) {
		wbc := getNewWriteBehindCacheImpl()
		key := "key"
		value := []model.Span{
			{
				TraceID: "traceId",
				SpanID:  "spanId",
			},
		}
		err := wbc.Put(key, value)
		assert.Nil(t, err)
		wbc.cache.Wait()
		err = wbc.Put(key, value)
		assert.Nil(t, err)
		wbc.cache.Wait()
		res, err := wbc.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, append(value, value...), res)
	})
}

func getNewWriteBehindCacheImpl() *WriteBehindCacheImpl {
	cache, _ := ristretto.NewCache(&ristretto.Config{
		NumCounters: (1 << 20) * 10,
		MaxCost:     1 << 20,
		BufferItems: 64,
	})
	return NewWriteBehindCacheImpl(cache)
}
