// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ash

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestExecutionCache_PutGet(t *testing.T) {
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

func TestExecutionCache_GetMissReturnsZeroValue(t *testing.T) {
	c := NewExecutionCache(10)
	_, ok := c.Get(99)
	require.False(t, ok)
}

func TestExecutionCache_PutZeroLimitReturnsZero(t *testing.T) {
	c := NewExecutionCache(0)
	id := c.Put(ExecutionAttrs{User: "alice"})
	require.Zero(t, id)
}

func TestExecutionCache_FIFOEviction(t *testing.T) {
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

func TestExecutionCache_GetMappingsBatch(t *testing.T) {
	c := NewExecutionCache(10)
	id1 := c.Put(ExecutionAttrs{User: "alice"})
	id2 := c.Put(ExecutionAttrs{User: "bob"})
	mappings := c.GetMappings([]uint64{id1, id2, 999})
	require.Len(t, mappings, 2)
	require.Equal(t, "alice", mappings[id1].User)
	require.Equal(t, "bob", mappings[id2].User)
}
