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
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestStrip(t *testing.T) {
	block := messageBlock{
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

	t.Run("partial_block_fully_ingested", func(t *testing.T) {
		// Create a strip of size 4, and insert 5 entries into the strip. This
		// should result in the last entry being discarded.
		strip := newStrip(func() int64 {
			return 4
		} /* capacity */)

		expected := make(map[uuid.UUID]roachpb.TransactionFingerprintID)
		expected = buildExpectedLookupMapFromMessageBlock(block, expected)
		endingOffset, more := strip.tryInsertBlock(block, 0 /* blockStartingOffset */)
		validateReader(t, strip, expected)
		require.Equal(t, 4 /* expected */, endingOffset)
		require.Equal(t, false /* expected */, more)

		extraBlock := messageBlock{
			ResolvedTxnID{
				TxnID:            uuid.FastMakeV4(),
				TxnFingerprintID: roachpb.TransactionFingerprintID(5),
			},
		}
		endingOffset, more = strip.tryInsertBlock(extraBlock, 0 /* blockStartingOffset */)
		validateReader(t, strip, expected)
		require.Equal(t, 0 /* expected */, endingOffset,
			"expected no entry being inserted into strip, but %d entries "+
				"were inserted", endingOffset)
		require.Equal(t, true /* expected */, more)

		strip.clear()
		require.True(t, len(strip.data) == 0)
	})

	t.Run("partial_block_partially_ingested", func(t *testing.T) {
		strip := newStrip(func() int64 { return 2 } /* capacity */)
		expected := make(map[uuid.UUID]roachpb.TransactionFingerprintID)
		expected = buildExpectedLookupMapFromMessageBlock(block, expected)
		// Removing the first entry since we are inserting a partial block.
		delete(expected, block[0].TxnID)
		// Removing the last entry since it won't fit into the strip.
		delete(expected, block[3].TxnID)

		// Inserting a partial block into the strip.
		endingOffset, more := strip.tryInsertBlock(block, 1 /* blockStartingOffset */)
		validateReader(t, strip, expected)
		require.Equal(t, 3 /* expected */, endingOffset,
			"expected to have endingOffset 3, but got %d", endingOffset)
		require.Equal(t, true /* expected */, more)
	})

	t.Run("full_block_fully_ingested", func(t *testing.T) {
		strip := newStrip(func() int64 { return messageBlockSize } /* capacity */)
		randomData, expected := generateRandomData()
		endingOffset, more :=
			strip.tryInsertBlock(randomData, 0 /* blockStartingOffset */)
		require.False(t, more, "expect block to be fully ingested, but it was not")
		require.Equal(t, messageBlockSize, endingOffset,
			"expect block to be fully ingested, but it was not")
		validateReader(t, strip, expected)
	})
}

func validateReader(
	t *testing.T, s reader, expectedMap map[uuid.UUID]roachpb.TransactionFingerprintID,
) {
	for k, expected := range expectedMap {
		actual, found := s.Lookup(k)
		require.True(t, found,
			"expected to find %s, but didn't", k.String())
		require.Equal(t, expected, actual,
			"expected to find %d for %s, but found %d", expected, k, actual)
	}
}

func buildExpectedLookupMapFromMessageBlock(
	block messageBlock, existingExpectedLookupMap map[uuid.UUID]roachpb.TransactionFingerprintID,
) map[uuid.UUID]roachpb.TransactionFingerprintID {
	for i := range block {
		if !block[i].valid() {
			break
		}
		existingExpectedLookupMap[block[i].TxnID] = block[i].TxnFingerprintID
	}
	return existingExpectedLookupMap
}
