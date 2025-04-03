// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cspann

import (
	"bytes"
	"hash/maphash"

	"github.com/cockroachdb/errors"
)

// hashKeyFunc is a function type for hashing KeyBytes.
type hashKeyFunc func(KeyBytes) uint64

// childKeyDeDup provides de-duplication for ChildKey values. It supports both
// PartitionKey and KeyBytes child keys efficiently without making unnecessary
// allocations.
type childKeyDeDup struct {
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
func (dd *childKeyDeDup) Init(capacity int) {
	dd.initialCapacity = capacity
	dd.seed = maphash.MakeSeed()
	dd.hashKeyBytes = dd.defaultHashKeyBytes
	dd.Clear()
}

// TryAdd attempts to add a child key to the deduplication set. It returns true
// if the key was added (wasn't a duplicate), or false if the key already exists
// (is a duplicate).
func (dd *childKeyDeDup) TryAdd(childKey ChildKey) bool {
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
	iterations := 0
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

		iterations++
		if iterations >= 100000 {
			// We must have hit a cycle, which should never happen.
			panic(errors.AssertionFailedf("rehash function cycled"))
		}
	}

	// Add the key to the map.
	dd.keyBytesMap[hash] = childKey.KeyBytes
	return true
}

// Clear removes all entries from the deduplication set.
func (dd *childKeyDeDup) Clear() {
	// Reset all the data structures.
	clear(dd.partitionKeys)
	clear(dd.keyBytesMap)
}

// defaultHashKeyBytes is the default implementation of hashKeyBytes.
func (dd *childKeyDeDup) defaultHashKeyBytes(key KeyBytes) uint64 {
	return maphash.Bytes(dd.seed, key)
}

// rehash creates a new hash from an existing hash to resolve collisions.
func (dd *childKeyDeDup) rehash(hash uint64) uint64 {
	// These constants are large 64-bit primes.
	hash ^= 0xc3a5c85c97cb3127
	hash ^= hash >> 33
	hash *= 0xff51afd7ed558ccd
	hash ^= hash >> 33
	hash *= 0xc4ceb9fe1a85ec53
	hash ^= hash >> 33
	return hash
}
