// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metric

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLabelSliceCache_NewLabelSliceCache(t *testing.T) {
	cache := NewLabelSliceCache()
	require.NotNil(t, cache)
	require.NotNil(t, cache.mu.cache)
}

func TestLabelSliceCache_GetEmpty(t *testing.T) {
	cache := NewLabelSliceCache()

	value, ok := cache.Get(LabelSliceCacheKey(123))
	require.False(t, ok)
	require.Nil(t, value)
}

func TestLabelSliceCache_UpsertAndGet(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(456)
	labelValues := []string{"app1", "db1"}

	// Create initial value
	value := &LabelSliceCacheValue{
		LabelValues: labelValues,
	}

	// Upsert should initialize counter to 1
	cache.Upsert(key, value)

	// Get the value back
	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.NotNil(t, retrievedValue)
	require.Equal(t, labelValues, retrievedValue.LabelValues)
	require.Equal(t, int64(1), retrievedValue.Counter.Load())
}

func TestLabelSliceCache_UpsertIncrementsCounter(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(789)
	labelValues := []string{"app2", "db2"}

	// Create initial value
	value := &LabelSliceCacheValue{
		LabelValues: labelValues,
	}

	// First upsert - should set counter to 1
	cache.Upsert(key, value)
	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.Equal(t, int64(1), retrievedValue.Counter.Load())

	// Second upsert with same key - should increment counter to 2
	cache.Upsert(key, value)
	retrievedValue, ok = cache.Get(key)
	require.True(t, ok)
	require.Equal(t, int64(2), retrievedValue.Counter.Load())

	// Third upsert - should increment to 3
	cache.Upsert(key, value)
	retrievedValue, ok = cache.Get(key)
	require.True(t, ok)
	require.Equal(t, int64(3), retrievedValue.Counter.Load())
}

func TestLabelSliceCache_Delete(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(101112)
	labelValues := []string{"app3", "db3"}

	// Add a value
	value := &LabelSliceCacheValue{
		LabelValues: labelValues,
	}
	cache.Upsert(key, value)

	// Verify it exists
	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.NotNil(t, retrievedValue)

	// Delete it
	cache.Delete(key)

	// Verify it's gone
	retrievedValue, ok = cache.Get(key)
	require.False(t, ok)
	require.Nil(t, retrievedValue)
}

func TestLabelSliceCache_DeleteNonExistent(t *testing.T) {
	cache := NewLabelSliceCache()

	// Deleting a non-existent key should not panic
	cache.Delete(LabelSliceCacheKey(999999))

	// Cache should still be usable
	key := LabelSliceCacheKey(111111)
	value := &LabelSliceCacheValue{
		LabelValues: []string{"test"},
	}
	cache.Upsert(key, value)

	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.NotNil(t, retrievedValue)
}

func TestLabelSliceCache_MultipleKeys(t *testing.T) {
	cache := NewLabelSliceCache()

	// Add multiple different keys
	keys := []LabelSliceCacheKey{1, 2, 3, 4, 5}
	values := [][]string{
		{"app1", "db1"},
		{"app2", "db2"},
		{"app3", "db3"},
		{"app4", "db4"},
		{"app5", "db5"},
	}

	// Insert all values
	for i, key := range keys {
		value := &LabelSliceCacheValue{
			LabelValues: values[i],
		}
		cache.Upsert(key, value)
	}

	// Verify all values exist
	for i, key := range keys {
		retrievedValue, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, values[i], retrievedValue.LabelValues)
		require.Equal(t, int64(1), retrievedValue.Counter.Load())
	}

	// Increment one of them
	value := &LabelSliceCacheValue{
		LabelValues: values[2],
	}
	cache.Upsert(keys[2], value)

	// Verify only that one was incremented
	for i, key := range keys {
		retrievedValue, ok := cache.Get(key)
		require.True(t, ok)
		if i == 2 {
			require.Equal(t, int64(2), retrievedValue.Counter.Load())
		} else {
			require.Equal(t, int64(1), retrievedValue.Counter.Load())
		}
	}
}

func TestLabelSliceCache_ConcurrentAccess(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(12345)
	labelValues := []string{"concurrent", "test"}

	const numGoroutines = 100
	const numIterations = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch multiple goroutines to concurrently upsert the same key
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				value := &LabelSliceCacheValue{
					LabelValues: labelValues,
				}
				cache.Upsert(key, value)
			}
		}()
	}

	wg.Wait()

	// Verify the final counter value
	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.NotNil(t, retrievedValue)
	require.Equal(t, labelValues, retrievedValue.LabelValues)
	require.Equal(t, int64(numGoroutines*numIterations), retrievedValue.Counter.Load())
}

func TestLabelSliceCache_ConcurrentReadWrite(t *testing.T) {
	cache := NewLabelSliceCache()
	keys := []LabelSliceCacheKey{1, 2, 3, 4, 5}

	const numReaders = 10
	const numWriters = 5
	const numIterations = 20

	var wg sync.WaitGroup

	// Launch reader goroutines
	wg.Add(numReaders)
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				for _, key := range keys {
					_, _ = cache.Get(key)
				}
			}
		}()
	}

	// Launch writer goroutines
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				for keyIdx, key := range keys {
					value := &LabelSliceCacheValue{
						LabelValues: []string{fmt.Sprintf("db%d", writerID), fmt.Sprintf("app%d", keyIdx)},
					}
					cache.Upsert(key, value)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all keys have some data and correct counter values
	for _, key := range keys {
		retrievedValue, ok := cache.Get(key)
		require.True(t, ok)
		require.NotNil(t, retrievedValue)
		require.Equal(t, int64(numWriters*numIterations), retrievedValue.Counter.Load())
	}
}

func TestLabelSliceCache_EmptyLabelValues(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(999)

	// Test with empty label values
	value := &LabelSliceCacheValue{
		LabelValues: []string{},
	}

	cache.Upsert(key, value)

	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.NotNil(t, retrievedValue)
	require.Equal(t, []string{}, retrievedValue.LabelValues)
	require.Equal(t, int64(1), retrievedValue.Counter.Load())
}

func TestLabelSliceCache_NilLabelValues(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(888)

	// Test with nil label values
	value := &LabelSliceCacheValue{
		LabelValues: nil,
	}

	cache.Upsert(key, value)

	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.NotNil(t, retrievedValue)
	require.Nil(t, retrievedValue.LabelValues)
	require.Equal(t, int64(1), retrievedValue.Counter.Load())
}

func TestLabelSliceCache_DecrementAndDeleteIfZero_NonExistent(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(123)

	// Decrementing a non-existent key should return false
	deleted := cache.DecrementAndDeleteIfZero(key)
	require.False(t, deleted)
}

func TestLabelSliceCache_DecrementAndDeleteIfZero_SingleCounter(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(456)
	labelValues := []string{"app1", "db1"}

	// Add a value with counter 1
	value := &LabelSliceCacheValue{
		LabelValues: labelValues,
	}
	cache.Upsert(key, value)

	// Verify counter is 1
	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.Equal(t, int64(1), retrievedValue.Counter.Load())

	// Decrement should delete the entry
	deleted := cache.DecrementAndDeleteIfZero(key)
	require.True(t, deleted)

	// Verify entry is gone
	retrievedValue, ok = cache.Get(key)
	require.False(t, ok)
	require.Nil(t, retrievedValue)
}

func TestLabelSliceCache_DecrementAndDeleteIfZero_MultipleCounter(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(789)
	labelValues := []string{"app2", "db2"}

	// Add a value multiple times to get counter 3
	value := &LabelSliceCacheValue{
		LabelValues: labelValues,
	}
	cache.Upsert(key, value)
	cache.Upsert(key, value)
	cache.Upsert(key, value)

	// Verify counter is 3
	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.Equal(t, int64(3), retrievedValue.Counter.Load())

	// First decrement should reduce to 2, not delete
	deleted := cache.DecrementAndDeleteIfZero(key)
	require.False(t, deleted)

	// Entry should still exist and counter should be 2
	retrievedValue, ok = cache.Get(key)
	require.True(t, ok)
	require.Equal(t, int64(2), retrievedValue.Counter.Load())

	// Second decrement should reduce to 1, not delete
	deleted = cache.DecrementAndDeleteIfZero(key)
	require.False(t, deleted)

	// Entry should still exist and counter should be 1
	retrievedValue, ok = cache.Get(key)
	require.True(t, ok)
	require.Equal(t, int64(1), retrievedValue.Counter.Load())

	// Third decrement should delete
	deleted = cache.DecrementAndDeleteIfZero(key)
	require.True(t, deleted)

	// Entry should be gone
	retrievedValue, ok = cache.Get(key)
	require.False(t, ok)
	require.Nil(t, retrievedValue)
}

func TestLabelSliceCache_DecrementAndDeleteIfZero_ConcurrentAccess(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(12345)
	labelValues := []string{"concurrent", "test"}

	const numGoroutines = 50
	const numIncrements = 10

	// First, populate the cache with a high counter value
	value := &LabelSliceCacheValue{
		LabelValues: labelValues,
	}
	for i := 0; i < numGoroutines*numIncrements; i++ {
		cache.Upsert(key, value)
	}

	// Verify initial counter value
	retrievedValue, ok := cache.Get(key)
	require.True(t, ok)
	require.Equal(t, int64(numGoroutines*numIncrements), retrievedValue.Counter.Load())

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch multiple goroutines to concurrently decrement
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numIncrements; j++ {
				cache.DecrementAndDeleteIfZero(key)
			}
		}()
	}

	wg.Wait()

	// Final state should be that the entry is deleted
	retrievedValue, ok = cache.Get(key)
	require.False(t, ok)
	require.Nil(t, retrievedValue)
}

func TestLabelSliceCache_DecrementAndDeleteIfZero_NegativeCounter(t *testing.T) {
	cache := NewLabelSliceCache()
	key := LabelSliceCacheKey(999)
	labelValues := []string{"negative", "test"}

	// Create a value with counter starting at 1
	value := &LabelSliceCacheValue{
		LabelValues: labelValues,
	}
	cache.Upsert(key, value)

	// First decrement should delete (counter goes to 0)
	deleted := cache.DecrementAndDeleteIfZero(key)
	require.True(t, deleted)

	// Entry should be gone
	retrievedValue, ok := cache.Get(key)
	require.False(t, ok)
	require.Nil(t, retrievedValue)

	// Further decrements on non-existent key should return false
	deleted = cache.DecrementAndDeleteIfZero(key)
	require.False(t, deleted)
}
