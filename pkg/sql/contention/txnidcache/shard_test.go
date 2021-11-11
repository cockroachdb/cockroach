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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestShard(t *testing.T) {
	t.Run("single-strip", func(t *testing.T) {
		shard := newShard(func() int64 {
			return 4
		}, /* capacity */
			1, /* ringSize */
			metric.NewCounter(metric.Metadata{}))

		expected := make(map[uuid.UUID]roachpb.TransactionFingerprintID)
		block1 := messageBlock{
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(1),
			},
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(2),
			},
		}

		expected = buildExpectedLookupMapFromMessageBlock(block1, expected)
		shard.push(block1)
		validateReader(t, shard, expected)
	})

	t.Run("multi-strips", func(t *testing.T) {
		shard := newShard(func() int64 {
			return 4
		}, /* capacity */
			2, /* ringSize */
			metric.NewCounter(metric.Metadata{}))

		// Looking up an empty shard shouldn't cause any problem.
		_, found := shard.Lookup(uuid.FastMakeV4())
		require.False(t, found)

		expected := make(map[uuid.UUID]roachpb.TransactionFingerprintID)
		block1 := messageBlock{
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(1),
			},
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(2),
			},
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(3),
			},
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(4),
			},
		}
		expected = buildExpectedLookupMapFromMessageBlock(block1, expected)

		t.Run("normal_insert", func(t *testing.T) {
			shard.push(block1)
			validateReader(t, shard, expected)
		})

		// We are inserting more than the capacity of the shard. This means
		// the first two ResolvedTxnIDs will be evicted from their corresponding strip.
		block2 := messageBlock{
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(5),
			},
		}

		t.Run("overflow_insert1", func(t *testing.T) {
			expected = buildExpectedLookupMapFromMessageBlock(block2, expected)
			delete(expected, block1[0].TxnID)
			delete(expected, block1[1].TxnID)

			shard.push(block2)
			validateReader(t, shard, expected)
			ensureNotPresentInReader(t, shard, block1[0].TxnID)
			ensureNotPresentInReader(t, shard, block1[1].TxnID)
		})

		// This would result in another eviction of the strip, and this block will
		// end up being stored across two strips.
		block3 := messageBlock{
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(6),
			},
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(7),
			},
		}

		t.Run("overflow_insert2", func(t *testing.T) {
			expected = buildExpectedLookupMapFromMessageBlock(block3, expected)
			delete(expected, block1[2].TxnID)
			delete(expected, block1[3].TxnID)

			shard.push(block3)
			validateReader(t, shard, expected)
			ensureNotPresentInReader(t, shard, block1[2].TxnID)
			ensureNotPresentInReader(t, shard, block1[3].TxnID)
		})

		t.Run("full_block", func(t *testing.T) {
			blocks, expectedMaps := generateMultipleBlocks(4)

			shard := newShard(func() int64 {
				return 4096
			}, /* capacity */
				4, /* ringSize */
				metric.NewCounter(metric.Metadata{}))

			for _, block := range blocks {
				shard.push(block)
			}

			for _, expected := range expectedMaps {
				validateReader(t, shard, expected)
			}
		})
	})
}

func ensureNotPresentInReader(t *testing.T, reader reader, txnID uuid.UUID) {
	_, found := reader.Lookup(txnID)
	require.False(t, found,
		"expected %s to be not found, but it was found", txnID)
}
