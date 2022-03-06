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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// There is no strong reason why shardCount is 16 beyond that Java's
// ConcurrentHashMap also uses 16 shards and has reasonably good performance.
const shardCount = 16

type writer struct {
	shards [shardCount]*concurrentWriteBuffer

	sink      blockSink
	blockPool *sync.Pool
}

var _ Writer = &writer{}

func newWriter(sink blockSink, blockPool *sync.Pool) *writer {
	w := &writer{
		sink:      sink,
		blockPool: blockPool,
	}

	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		w.shards[shardIdx] = newConcurrentWriteBuffer(sink, blockPool)
	}

	return w
}

// Record implements the Writer interface.
func (w *writer) Record(resolvedTxnID ResolvedTxnID) {
	shardIdx := hashTxnID(resolvedTxnID.TxnID)
	buffer := w.shards[shardIdx]
	buffer.Record(resolvedTxnID)
}

// Flush implements the Writer interface.
func (w *writer) Flush() {
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		w.shards[shardIdx].Flush()
	}
}

func hashTxnID(txnID uuid.UUID) int {
	b := txnID.GetBytes()
	_, val, err := encoding.DecodeUint64Descending(b)
	if err != nil {
		panic(err)
	}
	return int(val % shardCount)
}
