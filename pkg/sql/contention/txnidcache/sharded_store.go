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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const shardCount = 256

type shardedStore [shardCount]storage

var _ storage = shardedStore{}

func newShardedCache(fn func() storage) shardedStore {
	s := shardedStore{}
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		s[shardIdx] = fn()
	}
	return s
}

// Record implements the storage interface.
func (s shardedStore) push(block messageBlock) {
	var shardedBlock [shardCount]messageBlock
	var shardedIndex [shardCount]int

	// We do a full pass of the block to group each ResolvedTxnID in the block
	// to its corresponding shard.
	for blockIdx := 0; blockIdx < messageBlockSize && block[blockIdx].valid(); blockIdx++ {
		shardIdx := hashTxnID(block[blockIdx].TxnID)
		shardedBlock[shardIdx][shardedIndex[shardIdx]] = block[blockIdx]
		shardedIndex[shardIdx]++
	}

	// In the second pass we push each block shard into their own underlying
	// storage.
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		s[shardIdx].push(shardedBlock[shardIdx])
	}
}

// Lookup implements the reader interface.
func (s shardedStore) Lookup(
	txnID uuid.UUID,
) (result roachpb.TransactionFingerprintID, found bool) {
	shardIdx := hashTxnID(txnID)
	return s[shardIdx].Lookup(txnID)
}
