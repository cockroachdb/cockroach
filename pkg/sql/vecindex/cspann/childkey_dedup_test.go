// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"hash/maphash"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestChildKeyDeDupAddPartitionKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var dd ChildKeyDeDup
	dd.Init(10)

	// Add a new PartitionKey.
	added := dd.TryAdd(ChildKey{PartitionKey: 123})
	require.True(t, added)
	require.Equal(t, 1, dd.Count())

	// Try to add the same key again (should be a duplicate).
	added = dd.TryAdd(ChildKey{PartitionKey: 123})
	require.False(t, added)
	require.Equal(t, 1, dd.Count())

	// Add a different PartitionKey.
	added = dd.TryAdd(ChildKey{PartitionKey: 456})
	require.True(t, added)
	require.Equal(t, 2, dd.Count())
}

func TestChildKeyDeDupAddKeyBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var dd ChildKeyDeDup
	dd.Init(10)

	// Add a new KeyBytes.
	key1 := []byte("key1")
	added := dd.TryAdd(ChildKey{KeyBytes: key1})
	require.True(t, added)
	require.Equal(t, 1, dd.Count())

	// Try to add the same key again (should be a duplicate).
	added = dd.TryAdd(ChildKey{KeyBytes: key1})
	require.False(t, added)
	require.Equal(t, 1, dd.Count())

	// Add a different KeyBytes.
	key2 := []byte("key2")
	added = dd.TryAdd(ChildKey{KeyBytes: key2})
	require.True(t, added)
	require.Equal(t, 2, dd.Count())

	// Verify that both keys are properly stored by checking for duplicates.
	added = dd.TryAdd(ChildKey{KeyBytes: key1})
	require.False(t, added, "Key1 should be detected as duplicate.")
	added = dd.TryAdd(ChildKey{KeyBytes: key2})
	require.False(t, added, "Key2 should be detected as duplicate.")
}

func TestChildKeyDeDupClear(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var dd ChildKeyDeDup
	dd.Init(10)

	// Add a mix of keys.
	require.True(t, dd.TryAdd(ChildKey{PartitionKey: 123}))
	require.True(t, dd.TryAdd(ChildKey{KeyBytes: []byte("key1")}))
	require.Equal(t, 2, dd.Count())

	// Clear the deduplicator.
	dd.Clear()

	// Verify keys were cleared.
	require.Equal(t, 0, dd.Count())
	require.True(t, dd.TryAdd(ChildKey{PartitionKey: 123}))
	require.True(t, dd.TryAdd(ChildKey{KeyBytes: []byte("key1")}))
}

func TestChildKeyDeDupRehashing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test simulates a scenario with many keys and random collisions.

	// Create a custom hash function that has a small range, forcing collisions
	// but making it more realistic than the previous test.
	seed := maphash.MakeSeed()
	customHashFunc := func(key KeyBytes) uint64 {
		// Use only 10 possible hash values.
		return maphash.Bytes(seed, key)
	}

	var dd ChildKeyDeDup
	dd.Init(100)
	dd.hashKeyBytes = customHashFunc

	// Create 50 keys, which have guaranteed collisions with only 10 possible
	// hash values.
	const numKeys = 50
	keys := make([]KeyBytes, 0, numKeys)
	for i := 0; i < numKeys; i++ {
		key := []byte("test-key-" + strconv.Itoa(i))
		keys = append(keys, key)
	}

	// Add all keys.
	for _, key := range keys {
		require.True(t, dd.TryAdd(ChildKey{KeyBytes: key}))
	}

	// Verify all keys are findable.
	for _, key := range keys {
		require.False(t, dd.TryAdd(ChildKey{KeyBytes: key}))
	}
}
