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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestFIFOCache(t *testing.T) {
	cache := newFIFOCache(func() int64 { return 2 * blockSize } /* capacity */)

	// Fill the first eviction block in cache to 1/4 capacity.
	input1, expected1 := generateInputBlock(blockSize * 1 / 4 /* size */)
	cache.add(cloneBlock(input1))
	checkExists(t, cache, expected1)
	checkEvictionListShape(t, cache, []int{blockSize * 1 / 4})
	checkEvictionListContent(t, cache, []*block{input1})

	// Fill the first eviction block in cache to 3/4 capacity.
	input2, expected2 := generateInputBlock(blockSize / 2 /* size */)
	cache.add(cloneBlock(input2))
	checkExists(t, cache, expected1, expected2)
	checkEvictionListShape(t, cache, []int{blockSize * 3 / 4})

	checkEvictionListContent(t, cache, []*block{input1, input2})

	// Overflow the first eviction block, the second block should be 1/2 filled.
	input3, expected3 := generateInputBlock(blockSize * 3 / 4)
	cache.add(cloneBlock(input3))
	checkExists(t, cache, expected1, expected2, expected3)
	checkEvictionListShape(t, cache, []int{blockSize, blockSize / 2})
	checkEvictionListContent(t, cache, []*block{input1, input2, input3})

	// Overflow the second block, cause the first block to be evicted. Second
	// block becomes the first block, and it is completely filled. The new second
	// block should be 1/4 filled. Part of the input3 will be evicted.
	input4, expected4 := generateInputBlock(blockSize * 3 / 4)
	cache.add(cloneBlock(input4))
	checkEvictionListShape(t, cache, []int{blockSize, blockSize / 4})

	// First 1/4 of input3 should be evicted and the remaining [2/4:3/4] should
	// still remain.
	remainingInput3 := &block{}
	copy(remainingInput3[:], input3[blockSize*1/4:blockSize*3/4])
	checkEvictionListContent(t, cache, []*block{remainingInput3, input4})

	// Removes the portion that hasn't been evicted.
	evictedExpected3 := removeEntryFromMap(expected3, input3[blockSize*1/4:blockSize*3/4])

	// Removes the portion that has been evicted.
	remainingExpected3 := removeEntryFromMap(expected3, input3[:blockSize*1/4])

	checkNotExist(t, cache, expected1, expected2, evictedExpected3)
	checkExists(t, cache, remainingExpected3, expected4)

	// Partially fill up the second block in the eviction list.
	input5, expected5 := generateInputBlock(blockSize / 2)
	cache.add(cloneBlock(input5))
	checkNotExist(t, cache, expected1, expected2, evictedExpected3)
	checkExists(t, cache, remainingExpected3, expected4, expected5)
	checkEvictionListShape(t, cache, []int{blockSize, blockSize * 3 / 4})
	checkEvictionListContent(t, cache, []*block{remainingInput3, input4, input5})

	// Overflow the second block again to trigger another eviction, part of the
	// input4 would be evicted.
	input6, expected6 := generateInputBlock(blockSize * 3 / 4)
	cache.add(cloneBlock(input6))
	checkEvictionListShape(t, cache, []int{blockSize, blockSize / 2})

	remainingInput4 := &block{}
	copy(remainingInput4[:], input4[blockSize/2:blockSize*3/4])
	checkEvictionListContent(t, cache, []*block{remainingInput4, input5, input6})

	evictedExpected4 := removeEntryFromMap(expected4, input4[blockSize/2:blockSize*3/4])
	remainingExpected4 := removeEntryFromMap(expected4, input4[:blockSize/2])

	checkNotExist(t, cache, expected1, expected2, expected3, evictedExpected4)
	checkExists(t, cache, remainingExpected4, expected5, expected6)
}

func removeEntryFromMap(
	m map[uuid.UUID]roachpb.TransactionFingerprintID, filterList []contentionpb.ResolvedTxnID,
) map[uuid.UUID]roachpb.TransactionFingerprintID {
	newMap := make(map[uuid.UUID]roachpb.TransactionFingerprintID)
	for k, v := range m {
		newMap[k] = v
	}

	for _, val := range filterList {
		delete(newMap, val.TxnID)
	}

	return newMap
}

func checkEvictionListContent(t *testing.T, cache *fifoCache, expectedBlocks []*block) {
	t.Helper()

	cur := cache.mu.eviction.head
	evictionListBlockIdx := 0
	evictionListSize := 0

	for i := range expectedBlocks {
		require.NotNilf(t, cur, "expect a valid eviction list node, but it (size=%d)"+
			"is nil", evictionListSize)

		for blockIdx := 0; blockIdx < blockSize; blockIdx++ {
			if !expectedBlocks[i][blockIdx].Valid() {
				break
			}

			require.Equal(t, expectedBlocks[i][blockIdx], cur.block[evictionListBlockIdx],
				"expected eviction block at index [%d][%d] to be "+
					"%s, but it is %s", i, blockIdx, expectedBlocks[i][blockIdx].TxnID.String(),
				cur.block[evictionListBlockIdx].TxnID.String())

			evictionListBlockIdx++

			isEvictionListIdxStillValid :=
				evictionListBlockIdx < blockSize && cur.block[evictionListBlockIdx].Valid()

			if !isEvictionListIdxStillValid {
				cur = cur.next
				evictionListBlockIdx = 0
				evictionListSize++
			}
		}
	}

	require.Nilf(t, cur, "expect eviction list to be fully iterated, but it was not")
}

func checkEvictionListShape(t *testing.T, cache *fifoCache, expectedBlockSizes []int) {
	t.Helper()
	cur := cache.mu.eviction.head

	for i := range expectedBlockSizes {
		require.NotNilf(t, cur, "expect an eviction list of size %d, but it has "+
			"a size of %d", len(expectedBlockSizes), i-1)

		actualBlockSize := 0
		for blockIdx := 0; blockIdx < blockSize; blockIdx++ {
			if !cur.block[blockIdx].Valid() {
				break
			}
			actualBlockSize++
		}

		require.Equal(t, expectedBlockSizes[i], actualBlockSize,
			"expected eviction list block at index [%d] to have a size "+
				"of %d, but instead it has a size of %d",
			i, expectedBlockSizes[i], actualBlockSize)

		cur = cur.next
	}
}

func checkExists(
	t *testing.T, cache *fifoCache, expectedMaps ...map[uuid.UUID]roachpb.TransactionFingerprintID,
) {
	t.Helper()
	for _, expected := range expectedMaps {
		for expectedKey, expectedValue := range expected {
			actualValue, found := cache.get(expectedKey)
			require.True(t, found, "expected txnID %s to be present in "+
				"the cache, but it was not found", expectedKey)
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

func cloneBlock(b *block) *block {
	newBlock := &block{}
	copy(newBlock[:], b[:])
	return newBlock
}

func generateInputBlock(
	size int,
) (input *block, expected map[uuid.UUID]roachpb.TransactionFingerprintID) {
	if size > blockSize {
		panic(fmt.Sprintf("input block size cannot be greater than %d", blockSize))
	}

	input = &block{}
	expected = make(map[uuid.UUID]roachpb.TransactionFingerprintID)

	for i := 0; i < size; i++ {
		input[i].TxnID = uuid.FastMakeV4()
		input[i].TxnFingerprintID = roachpb.TransactionFingerprintID(rand.Uint64())

		expected[input[i].TxnID] = input[i].TxnFingerprintID
	}

	return input, expected
}
