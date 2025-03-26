// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
	"hash/maphash"
)

// hashKeyFunc is a function type for hashing KeyBytes.
type hashKeyFunc func(KeyBytes) uint64

// ChildKeyDeDup provides de-duplication for ChildKey values. It supports both
// PartitionKey and KeyBytes child keys efficiently without making unnecessary
// allocations.
type ChildKeyDeDup struct {
	// initialCapacity is used to initialize the size of the data structures used
	// by the de-duplicator.
	initialCapacity int

	// partitionKeys is used for PartitionKey deduplication.
	partitionKeys map[PartitionKey]struct{}

	// keyBytesMap maps from a KeyBytes hash to the actual KeyBytes for direct
	// lookup and deduplication.
	keyBytesMap map[uint64]KeyBytes

	// seed pseudo-randomizes the hash function used by the de-duplicator.
	seed maphash.Seed

	// hashKeyBytes is the function used to hash KeyBytes. This is primarily used
	// for testing to override the default hash function.
	hashKeyBytes hashKeyFunc
}

// Init initializes the de-duplicator.
func (dd *ChildKeyDeDup) Init(capacity int) {
	dd.initialCapacity = capacity
	dd.seed = maphash.MakeSeed()
	dd.hashKeyBytes = dd.defaultHashKeyBytes
	dd.Clear()
}

// TryAdd attempts to add a child key to the deduplication set. It returns true
// if the key was added (wasn't a duplicate), or false if the key already exists
// (is a duplicate).
func (dd *ChildKeyDeDup) TryAdd(childKey ChildKey) bool {
	// Handle PartitionKey case - simple map lookup.
	if childKey.PartitionKey != 0 {
		// Lazily initialize the partitionKeys map.
		if dd.partitionKeys == nil {
			dd.partitionKeys = make(map[PartitionKey]struct{}, dd.initialCapacity)
		}

		if _, exists := dd.partitionKeys[childKey.PartitionKey]; exists {
			return false
		}
		dd.partitionKeys[childKey.PartitionKey] = struct{}{}
		return true
	}

	// Handle KeyBytes case. Lazily initialize the KeyBytes structures.
	if dd.keyBytesMap == nil {
		dd.keyBytesMap = make(map[uint64]KeyBytes, dd.initialCapacity)
	}

	// Calculate original hash for the key bytes.
	hash := dd.hashKeyBytes(childKey.KeyBytes)

	// Check for the key, possibly having to look at multiple rehashed slots.
	for {
		existingKey, exists := dd.keyBytesMap[hash]
		if !exists {
			// No collision, we can use this hash.
			break
		}

		// Check if this is the same key.
		if bytes.Equal(existingKey, childKey.KeyBytes) {
			return false
		}

		// Hash collision, rehash to find a new slot.
		hash = dd.rehash(hash)
	}

	// Add the key to the map.
	dd.keyBytesMap[hash] = childKey.KeyBytes
	return true
}

// TryRemove attempts to remove a child key from the deduplication set. Returns
// true if the key was found and removed, or false otherwise.
func (dd *ChildKeyDeDup) TryRemove(childKey ChildKey) bool {
	// Handle PartitionKey case.
	if childKey.PartitionKey != 0 {
		if dd.partitionKeys == nil {
			// No map, so key can't exist.
			return false
		}
		if _, exists := dd.partitionKeys[childKey.PartitionKey]; !exists {
			return false
		}

		// Remove the key from the map.
		delete(dd.partitionKeys, childKey.PartitionKey)
		return true
	}

	// Handle KeyBytes case.
	if dd.keyBytesMap == nil {
		// No map or empty map, so key can't exist.
		return false
	}

	// Calculate hash for the key bytes.
	hash := dd.hashKeyBytes(childKey.KeyBytes)

	// Search for the key using the same probing sequence as TryAdd. This ensures
	// we only check positions where the key could have been stored.
	for {
		existingKey, exists := dd.keyBytesMap[hash]
		if !exists {
			// Key not found at this position. Since we follow the same probing
			// sequence as TryAdd, we know the key doesn't exist.
			return false
		}

		// Check if this is the key we want to remove.
		if bytes.Equal(existingKey, childKey.KeyBytes) {
			// Found it.
			break
		}

		// This hash contains a different key, rehash to try the next position.
		hash = dd.rehash(hash)
	}

	// Remove the key. However, this opens up a "hole" in the probing sequence.
	// If other keys have been rehashed, they won't be found because of this
	// hole. Rehash any keys that follow in this probing sequence to avoid this
	// problem.
	delete(dd.keyBytesMap, hash)

	for {
		hash = dd.rehash(hash)
		key, exists := dd.keyBytesMap[hash]
		if !exists {
			// No more keys in this probe sequence, we're done.
			break
		}

		// The key at this position was probably placed here due to a collision.
		// Remove it from its current position and re-add it through TryAdd.
		delete(dd.keyBytesMap, hash)
		dd.TryAdd(ChildKey{KeyBytes: key})
	}

	return true
}

// Clear removes all entries from the deduplication set.
func (dd *ChildKeyDeDup) Clear() {
	// Reset all the data structures.
	clear(dd.partitionKeys)
	clear(dd.keyBytesMap)
}

// defaultHashKeyBytes is the default implementation of hashKeyBytes.
func (dd *ChildKeyDeDup) defaultHashKeyBytes(key KeyBytes) uint64 {
	return maphash.Bytes(dd.seed, key)
}

// rehash creates a new hash from an existing hash to resolve collisions.
func (dd *ChildKeyDeDup) rehash(hash uint64) uint64 {
	// The constant 1099511628211 is a prime.
	return hash*1099511628211 + hash>>32
}
