// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestExecutionCachePutGet(t *testing.T) {
	c := NewExecutionCache(10)
	txnID := uuid.MakeV4()
	attrs := ExecutionAttrs{
		User:     "alice",
		Database: "movr",
		Query:    "SELECT 1",
		TxnID:    txnID,
	}
	id := c.Put(attrs)
	require.NotZero(t, id)

	got, ok := c.Get(id)
	require.True(t, ok)
	require.Equal(t, attrs, got)
}

func TestExecutionCacheGetMiss(t *testing.T) {
	c := NewExecutionCache(10)
	_, ok := c.Get(99)
	require.False(t, ok)
}

func TestExecutionCachePutZeroLimit(t *testing.T) {
	c := NewExecutionCache(0)
	id := c.Put(ExecutionAttrs{User: "alice"})
	require.Zero(t, id)
}

func TestExecutionCacheFIFOEviction(t *testing.T) {
	c := NewExecutionCache(3)
	ids := make([]uint64, 0, 5)
	for i := 0; i < 5; i++ {
		ids = append(ids, c.Put(ExecutionAttrs{User: "u"}))
	}
	// First two should have been evicted.
	for _, id := range ids[:2] {
		_, ok := c.Get(id)
		require.False(t, ok, "id %d should have been evicted", id)
	}
	// Last three should remain.
	for _, id := range ids[2:] {
		_, ok := c.Get(id)
		require.True(t, ok, "id %d should still be present", id)
	}
}

func TestExecutionCacheGetMappings(t *testing.T) {
	c := NewExecutionCache(10)
	id1 := c.Put(ExecutionAttrs{User: "alice"})
	id2 := c.Put(ExecutionAttrs{User: "bob"})
	mappings := c.GetMappings([]uint64{id1, id2, 999})
	require.Len(t, mappings, 2)
	require.Equal(t, "alice", mappings[id1].User)
	require.Equal(t, "bob", mappings[id2].User)
}

func TestExecutionCacheNoEvictionAtLimit(t *testing.T) {
	c := NewExecutionCache(3)
	ids := make([]uint64, 0, 3)
	for i := 0; i < 3; i++ {
		ids = append(ids, c.Put(ExecutionAttrs{User: "u"}))
	}
	for _, id := range ids {
		_, ok := c.Get(id)
		require.True(t, ok, "id %d should be present (at-limit, no eviction)", id)
	}
}

func TestExecutionCacheConcurrent(t *testing.T) {
	const goroutines = 8
	const opsPerGoroutine = 200
	c := NewExecutionCache(1000)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				id := c.Put(ExecutionAttrs{User: "u"})
				_, _ = c.Get(id)
				_ = c.GetMappings([]uint64{id, id + 1})
			}
		}()
	}
	wg.Wait()
}
