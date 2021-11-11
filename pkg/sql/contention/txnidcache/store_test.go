// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestShardedStore(t *testing.T) {
	store := newShardedCache(func() storage {
		return newShard(func() int64 {
			return int64(1024 * 1024)
		}, defaultRingSize, metric.NewCounter(metric.Metadata{}))
	})

	// Generate a random block count from the half open interval [1, 10).
	testBlockCount := rand.Intn(9) + 1
	inputs, expectedMaps := generateMultipleBlocks(testBlockCount)
	for i := range inputs {
		t.Run(fmt.Sprintf("block=%d", i), func(t *testing.T) {
			store.push(inputs[i])
			validateReader(t, store, expectedMaps[i])
		})
	}
}

func generateMultipleBlocks(
	count int,
) (blocks []messageBlock, expectedMaps []map[uuid.UUID]roachpb.TransactionFingerprintID) {
	blocks = make([]messageBlock, 0, count)
	expectedMaps = make([]map[uuid.UUID]roachpb.TransactionFingerprintID, 0, count)

	for i := 0; i < count; i++ {
		block, expected := generateRandomData()
		blocks = append(blocks, block)
		expectedMaps = append(expectedMaps, expected)
	}

	return blocks, expectedMaps
}

func generateRandomData() (
	block messageBlock,
	expected map[uuid.UUID]roachpb.TransactionFingerprintID,
) {
	expected = make(map[uuid.UUID]roachpb.TransactionFingerprintID)
	for i := int64(0); i < messageBlockSize; i++ {
		txnID := uuid.FastMakeV4()
		txnFingerprintID := roachpb.TransactionFingerprintID(rand.Int63())
		expected[txnID] = txnFingerprintID
		block[i] = ResolvedTxnID{
			TxnID:            txnID,
			TxnFingerprintID: txnFingerprintID,
		}
	}

	return block, expected
}
