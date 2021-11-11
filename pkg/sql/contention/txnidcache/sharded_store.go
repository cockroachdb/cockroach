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

type shardedCache [shardCount]storage

var _ storage = shardedCache{}

func newShardedCache(fn func() storage) shardedCache {
	s := shardedCache{}
	for i := 0; i < shardCount; i++ {
		s[i] = fn()
	}
	return s
}

// Record implements the writer interface.
func (s shardedCache) Record(resolvedTxnID ResolvedTxnID) {
	shard := s[hashTxnID(resolvedTxnID.TxnID)]
	shard.Record(resolvedTxnID)
}

// Flush implements the writer interface.
func (s shardedCache) Flush() {
	for _, shard := range s {
		shard.Flush()
	}
}

// Lookup implements the reader interface.
func (s shardedCache) Lookup(
	txnID uuid.UUID,
) (result roachpb.TransactionFingerprintID, found bool) {
	shardIdx := hashTxnID(txnID)

	return s[shardIdx].Lookup(txnID)
}
