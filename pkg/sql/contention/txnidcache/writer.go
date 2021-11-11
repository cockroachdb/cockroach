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

const shardCount = 128

type writer struct {
	shards [shardCount]*concurrentWriteBuffer

	sink         messageSink
	msgBlockPool *sync.Pool
}

var _ Writer = &writer{}

func newWriter(sink messageSink, msgBlockPool *sync.Pool) *writer {
	w := &writer{
		sink:         sink,
		msgBlockPool: msgBlockPool,
	}

	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		w.shards[shardIdx] = newConcurrentWriteBuffer(sink, msgBlockPool)
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
	b, val, err := encoding.DecodeUint64Descending(b)
	if err != nil {
		panic(err)
	}
	return int(val % shardCount)
}
