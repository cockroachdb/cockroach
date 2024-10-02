// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnidcache

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestFIFOCacheDataDriven(t *testing.T) {
	var cache *fifoCache
	inputBlocks := make(map[string]*block)
	expectedMaps := make(map[string]map[uuid.UUID]appstatspb.TransactionFingerprintID)
	blockToNameMap := make(map[*block]string)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
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

					actualBlockSize := 0
					for blockOffset := 0; blockOffset < blockSize; blockOffset++ {
						if !cur.block[blockOffset].Valid() {
							break
						}
						actualBlockSize++
					}

					result = append(result,
						fmt.Sprintf("blockName: %s, blockSize: %d",
							blockName, actualBlockSize))
				}

				return strings.Join(result, "\n")
			case "checkCacheContent":
				var presentInputBlockNamesStr string
				var evictedInputBlockNamesStr string

				if d.HasArg("presentBlockNames") {
					d.ScanArgs(t, "presentBlockNames", &presentInputBlockNamesStr)
				}
				if d.HasArg("evictedBlockNames") {
					d.ScanArgs(t, "evictedBlockNames", &evictedInputBlockNamesStr)
				}

				presentInputBlockNames := strings.Split(presentInputBlockNamesStr, ",")
				for _, name := range presentInputBlockNames {
					expected := expectedMaps[name]
					for txnID, expectedTxnFingerprintID := range expected {
						actualTxnFingerprintID, ok := cache.mu.data[txnID]
						require.True(t, ok, "expected to find txn fingerprint ID "+
							"for txnID %s, but it was not found", txnID)
						require.Equal(t, expectedTxnFingerprintID, actualTxnFingerprintID,
							"expected the txnID %s to have txn fingerprint ID %d, but "+
								"got %d", txnID, expectedTxnFingerprintID, actualTxnFingerprintID)
					}
				}

				evictedInputBlockNames := strings.Split(evictedInputBlockNamesStr, ",")
				for _, name := range evictedInputBlockNames {
					expected := expectedMaps[name]
					for txnID := range expected {
						_, ok := cache.mu.data[txnID]
						require.False(t, ok, "expected to not find txn fingerprint ID "+
							"for txnID %s, but it was found", txnID)
					}
				}

				return "ok"
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

func generateInputBlock(
	size int,
) (input *block, expected map[uuid.UUID]appstatspb.TransactionFingerprintID) {
	if size > blockSize {
		panic(fmt.Sprintf("input block size cannot be greater than %d", blockSize))
	}

	input = &block{}
	expected = make(map[uuid.UUID]appstatspb.TransactionFingerprintID)

	for i := 0; i < size; i++ {
		input[i].TxnID = uuid.MakeV4()
		input[i].TxnFingerprintID =
			appstatspb.TransactionFingerprintID(rand.Uint64())

		expected[input[i].TxnID] = input[i].TxnFingerprintID
	}

	return input, expected
}
