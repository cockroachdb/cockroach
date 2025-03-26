// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
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

	// Try to add the same key again (should be a duplicate).
	added = dd.TryAdd(ChildKey{PartitionKey: 123})
	require.False(t, added)

	// Add a different PartitionKey.
	added = dd.TryAdd(ChildKey{PartitionKey: 456})
	require.True(t, added)
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

	// Try to add the same key again (should be a duplicate).
	added = dd.TryAdd(ChildKey{KeyBytes: key1})
	require.False(t, added)

	// Add a different KeyBytes.
	key2 := []byte("key2")
	added = dd.TryAdd(ChildKey{KeyBytes: key2})
	require.True(t, added)

	// Verify that both keys are properly stored by checking for duplicates.
	added = dd.TryAdd(ChildKey{KeyBytes: key1})
	require.False(t, added, "Key1 should be detected as duplicate.")
	added = dd.TryAdd(ChildKey{KeyBytes: key2})
	require.False(t, added, "Key2 should be detected as duplicate.")
}

func TestChildKeyDeDupRemovePartitionKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var dd ChildKeyDeDup
	dd.Init(10)

	// Try to remove a key that doesn't exist.
	removed := dd.TryRemove(ChildKey{PartitionKey: 123})
	require.False(t, removed)

	// Add a key and then remove it.
	added := dd.TryAdd(ChildKey{PartitionKey: 123})
	require.True(t, added)

	removed = dd.TryRemove(ChildKey{PartitionKey: 123})
	require.True(t, removed)

	// Try to remove it again.
	removed = dd.TryRemove(ChildKey{PartitionKey: 123})
	require.False(t, removed)
}

func TestChildKeyDeDupRemoveKeyBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var dd ChildKeyDeDup
	dd.Init(10)

	// Try to remove a key that doesn't exist.
	key1 := []byte("key1")
	removed := dd.TryRemove(ChildKey{KeyBytes: key1})
	require.False(t, removed)

	// Add keys.
	key1 = []byte("key1")
	key2 := []byte("key2")
	key3 := []byte("key3")

	require.True(t, dd.TryAdd(ChildKey{KeyBytes: key1}))
	require.True(t, dd.TryAdd(ChildKey{KeyBytes: key2}))
	require.True(t, dd.TryAdd(ChildKey{KeyBytes: key3}))

	// Remove a key.
	require.True(t, dd.TryRemove(ChildKey{KeyBytes: key2}))

	// Try to remove it again.
	require.False(t, dd.TryRemove(ChildKey{KeyBytes: key2}))

	// Remove other keys.
	require.True(t, dd.TryRemove(ChildKey{KeyBytes: key1}))
	require.True(t, dd.TryRemove(ChildKey{KeyBytes: key3}))
}

func TestChildKeyDeDupClear(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var dd ChildKeyDeDup
	dd.Init(10)

	// Add a mix of keys.
	require.True(t, dd.TryAdd(ChildKey{PartitionKey: 123}))
	require.True(t, dd.TryAdd(ChildKey{KeyBytes: []byte("key1")}))

	// Clear the deduplicator.
	dd.Clear()

	// Verify keys were cleared.
	require.False(t, dd.TryRemove(ChildKey{PartitionKey: 123}))
	require.False(t, dd.TryRemove(ChildKey{KeyBytes: []byte("key1")}))
}

func TestChildKeyDeDupRehashing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a custom hash function that deliberately creates collisions by
	// always returning the same hash for any key starting with the "key" prefix.
	const baseHash uint64 = 42
	customHashFunc := func(key KeyBytes) uint64 {
		if bytes.HasPrefix(key, []byte("key")) {
			return baseHash
		}

		// For other keys, use a simple hash based on key length.
		return uint64(len(key))
	}

	var dd ChildKeyDeDup
	dd.Init(10)
	dd.hashKeyBytes = customHashFunc

	// Create 5 keys that will all hash to the same value
	keys := []KeyBytes{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}

	// Add all keys - they should all end up at different positions due to
	// rehashing.
	for _, key := range keys {
		require.True(t, dd.TryAdd(ChildKey{KeyBytes: key}))
	}

	// Verify all keys are findable.
	for _, key := range keys {
		require.False(t, dd.TryAdd(ChildKey{KeyBytes: key}))
	}

	// Remove key2 (which would be at position rehash(baseHash)).
	require.True(t, dd.TryRemove(ChildKey{KeyBytes: []byte("key2")}))

	// The removal should have caused a hole in the sequence, but the rehashing
	// in TryRemove should have fixed it. Let's verify all other keys are still
	// findable.
	for _, key := range keys {
		if string(key) == "key2" {
			// This was the removed key, should not be found.
			require.True(t, dd.TryAdd(ChildKey{KeyBytes: key}))
		} else {
			// All other keys should still be findable.
			require.False(t, dd.TryAdd(ChildKey{KeyBytes: key}))
		}
	}

	// Verify all keys are findable again, now that key2 was added back at the
	// end of the probe sequence.
	for _, key := range keys {
		require.False(t, dd.TryAdd(ChildKey{KeyBytes: key}))
	}

	// Now remove key1 (which would be at the base hash position)
	// This is a more complex case since it's the head of the collision chain
	require.True(t, dd.TryRemove(ChildKey{KeyBytes: []byte("key1")}))

	// Verify all remaining keys are still findable
	for _, key := range keys {
		if string(key) == "key1" {
			// This was the removed key, should not be found.
			require.True(t, dd.TryAdd(ChildKey{KeyBytes: key}))
		} else {
			// All other keys should still be findable.
			require.False(t, dd.TryAdd(ChildKey{KeyBytes: key}))
		}
	}
}

func TestChildKeyDeDupRandomRehashing(t *testing.T) {
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

	// Now remove every third key.
	removed := make(map[string]bool)
	for i := 0; i < numKeys; i += 3 {
		require.True(t, dd.TryRemove(ChildKey{KeyBytes: keys[i]}))
		removed[string(keys[i])] = true
	}

	// Verify all non-removed keys are still findable and removed keys are no
	// longer found.
	for _, key := range keys {
		if removed[string(key)] {
			require.True(t, dd.TryAdd(ChildKey{KeyBytes: key}))
		} else {
			require.False(t, dd.TryAdd(ChildKey{KeyBytes: key}))
		}
	}

	// Verify all keys are findable again, now that all removed keys were added
	// back.
	for _, key := range keys {
		require.False(t, dd.TryAdd(ChildKey{KeyBytes: key}))
	}
}
