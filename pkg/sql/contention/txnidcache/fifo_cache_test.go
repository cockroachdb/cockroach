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
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestFIFOCacheDataDriven(t *testing.T) {
	var cache *fifoCache
	inputBlocks := make(map[string]*block)
	expectedMaps := make(map[string]map[uuid.UUID]roachpb.TransactionFingerprintID)
	blockToNameMap := make(map[*block]string)

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
				input = cloneBlock(input)
				cache.add(input)
				blockToNameMap[input] = name
			case "show":
				var result []string

				result = append(result, fmt.Sprintf("cacheSize: %d", cache.size()))
				for cur := cache.mu.eviction.head; cur != nil; cur = cur.next {
					blockName := blockToNameMap[cur.block]
					expected := expectedMaps[blockName]

					actualBlockSize := 0
					for blockOffset := 0; blockOffset < blockSize; blockOffset++ {
						if !cur.block[blockOffset].Valid() {
							break
						}
						actualBlockSize++
						txnID := cur.block[blockOffset].TxnID
						expectedTxnFingerprint, ok := expected[txnID]
						require.True(t, ok, "expected to find txn fingerprint ID "+
							"for txnID %s, but it was not found", txnID)
						require.Equal(t, expectedTxnFingerprint, cache.mu.data[txnID],
							"expected the txnID %s to have txn fingerprint ID %d, but "+
								"got %d", txnID, expectedTxnFingerprint, cache.mu.data[txnID])
					}

					result = append(result,
						fmt.Sprintf("blockName: %s, blockSize: %d",
							blockName, actualBlockSize))
				}

				return strings.Join(result, "\n")
			}
			return ""
		})
	})
}

func cloneBlock(b *block) *block {
	newBlock := &block{}
	copy(newBlock[:], b[:])
	return newBlock
}

var deterministicIntSource = uint128.FromInts(1, 1)

func generateInputBlock(
	size int,
) (input *block, expected map[uuid.UUID]roachpb.TransactionFingerprintID) {
	if size > blockSize {
		panic(fmt.Sprintf("input block size cannot be greater than %d", blockSize))
	}

	input = &block{}
	expected = make(map[uuid.UUID]roachpb.TransactionFingerprintID)

	for i := 0; i < size; i++ {
		input[i].TxnID = uuid.FromUint128(deterministicIntSource)
		input[i].TxnFingerprintID =
			roachpb.TransactionFingerprintID(deterministicIntSource.Lo + deterministicIntSource.Hi)

		expected[input[i].TxnID] = input[i].TxnFingerprintID

		deterministicIntSource = deterministicIntSource.Add(1)
	}

	return input, expected
}
