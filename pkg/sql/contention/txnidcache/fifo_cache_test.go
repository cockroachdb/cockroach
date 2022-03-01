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
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestFIFOCacheDataDriven(t *testing.T) {
	var cache *fifoCache
	inputBlocks := make(map[string]*block)
	expectedMaps := make(map[string]map[uuid.UUID]roachpb.TransactionFingerprintID)

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var capacity int
				d.ScanArgs(t, "capacity", &capacity)
				cache = newFIFOCache(func() int64 { return int64(capacity) } /* capacity */)
				return fmt.Sprintf("cache_size: %d", cache.size())
			case "newInputBlock":
				var name string
				var percentageFilledStr string

				d.ScanArgs(t, "name", &name)
				d.ScanArgs(t, "percentageFilled", &percentageFilledStr)

				percentageFilled, err := strconv.ParseFloat(percentageFilledStr, 64)
				require.NoError(t, err)

				actualSize := int(blockSize * percentageFilled)
				input, expected := generateInputBlock(actualSize)
				inputBlocks[name] = input
				expectedMaps[name] = expected

				return fmt.Sprintf("blockSize: %d", actualSize)
			case "insertBlock":
				var name string
				d.ScanArgs(t, "name", &name)
				input, ok := inputBlocks[name]
				require.True(t, ok, "input %s is not found", name)
				cache.add(cloneBlock(input))
				return fmt.Sprintf("cache_size: %d\n", cache.size())
			case "verifyBlockExistence":
				var names string
				d.ScanArgs(t, "names", &names)
				blockNames := strings.Split(names, ",")
				for _, name := range blockNames {
					expected := expectedMaps[name]
					checkExists(t, cache, expected)
				}
			case "verifyBlockNonExistence":
				var names string
				d.ScanArgs(t, "names", &names)
				blockNames := strings.Split(names, ",")
				for _, name := range blockNames {
					expected := expectedMaps[name]
					checkNotExist(t, cache, expected)
				}
			case "inspectEvictionListShape":
				result := make([]string, 0)
				blockIdx := 0
				for cur := cache.mu.eviction.head; cur != nil; cur = cur.next {
					actualBlockSize := 0
					for blockOffset := 0; blockOffset < blockSize; blockOffset++ {
						if !cur.block[blockOffset].Valid() {
							break
						}
						actualBlockSize++
					}
					result = append(result,
						fmt.Sprintf("blockIdx: %d, blockSize: %d",
							blockIdx, actualBlockSize))
					blockIdx++
				}
				return strings.Join(result, "\n")
			case "ensureEvictionBlockOrdering":
				var names string
				var result []string

				d.ScanArgs(t, "names", &names)
				blockNames := strings.Split(names, ",")

				blockIdx := 0
				cur := cache.mu.eviction.head
				for _, name := range blockNames {
					if cur == nil {
						break
					}

					input := inputBlocks[name]
					if *input == *cur.block {
						result = append(result,
							fmt.Sprintf("blockIdx: %d, blockName: %s", blockIdx, name))
					} else {
						t.Fatalf("expected block at blockIdx=%d to be %s, but it is"+
							" not", blockIdx, name)
					}

					cur = cur.next
					blockIdx++
				}

				if cur != nil {
					t.Fatalf("expected eviction list to have %d blocks, but it has more", len(blockNames))
				}

				return strings.Join(result, "\n")
			}
			return ""
		})
	})
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
