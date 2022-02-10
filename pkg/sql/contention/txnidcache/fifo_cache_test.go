// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestFIFOCache(t *testing.T) {
	pool := &sync.Pool{
		New: func() interface{} {
			return &messageBlock{}
		},
	}
	cache := newFIFOCache(pool, func() int64 { return 10 } /* capacity */)

	input, expected1 := generateInputBlock(pool, 5 /* size */)
	cache.add(input)
	checkExists(t, cache, expected1)

	input, expected2 := generateInputBlock(pool, 4 /* size */)
	cache.add(input)
	checkExists(t, cache, expected1, expected2)

	input, expected3 := generateInputBlock(pool, 10 /* size */)
	cache.add(input)
	checkExists(t, cache, expected3)
	checkNotExist(t, cache, expected1, expected2)

	// This would cause the newly added block to also be evicted.
	input, expected4 := generateInputBlock(pool, 20 /* size */)
	cache.add(input)
	checkNotExist(t, cache, expected1, expected2, expected3, expected4)
}

func checkExists(
	t *testing.T, cache *fifoCache, expectedMaps ...map[uuid.UUID]roachpb.TransactionFingerprintID,
) {
	t.Helper()
	for _, expected := range expectedMaps {
		for expectedKey, expectedValue := range expected {
			actualValue, found := cache.get(expectedKey)
			require.True(t, found, "expected txnID %s to be present in "+
				"the cache, but it was not found")
			require.Equal(t, expectedValue, actualValue, "expected to find "+
				"transaction fingerprint ID %d for txnID %s, but found %d instead",
				expectedValue, expectedKey, actualValue)
		}
	}
}

func checkNotExist(
	t *testing.T, cache *fifoCache, expectedMaps ...map[uuid.UUID]roachpb.TransactionFingerprintID,
) {
	t.Helper()
	for _, expected := range expectedMaps {
		for expectedKey := range expected {
			_, found := cache.get(expectedKey)
			require.False(t, found, "expected txnID %s to be not present in "+
				"the cache, but it was found", expectedKey)
		}
	}
}

func generateInputBlock(
	pool *sync.Pool, size int,
) (input *messageBlock, expected map[uuid.UUID]roachpb.TransactionFingerprintID) {
	if size > messageBlockSize {
		panic(fmt.Sprintf("input block size cannot be greater than %d", messageBlockSize))
	}

	input = pool.Get().(*messageBlock)
	expected = make(map[uuid.UUID]roachpb.TransactionFingerprintID)

	for i := 0; i < size; i++ {
		input[i].TxnID = uuid.FastMakeV4()
		input[i].TxnFingerprintID = roachpb.TransactionFingerprintID(rand.Uint64())

		expected[input[i].TxnID] = input[i].TxnFingerprintID
	}

	return input, expected
}
