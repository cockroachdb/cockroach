// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnidcache

import (
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// There is no strong reason why shardCount is 16 beyond that Java's
// ConcurrentHashMap also uses 16 shards and has reasonably good performance.
const shardCount = 16

type writer struct {
	st *cluster.Settings

	shards [shardCount]*concurrentWriteBuffer

	sink blockSink
}

var _ Writer = &writer{}

func newWriter(st *cluster.Settings, sink blockSink) *writer {
	w := &writer{
		st:   st,
		sink: sink,
	}

	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		w.shards[shardIdx] = newConcurrentWriteBuffer(sink)
	}

	return w
}

// Record implements the Writer interface.
func (w *writer) Record(resolvedTxnID contentionpb.ResolvedTxnID) {
	if MaxSize.Get(&w.st.SV) == 0 {
		return
	}

	// There are edge cases where the txnID in the resolvedTxnID will be nil,
	// (e.g. when the connExecutor closes while a transaction is still active).
	// This causes that, occasionally, connExecutor will emit resolvedTxnIDs with
	// invalid txnID but valid txnFingerprintID. Writing invalid txnID into the
	// writer can potentially cause data loss. (Since the TxnID cache stops
	// processing the input batch when it encounters the first invalid txnID).
	if resolvedTxnID.TxnID.Equal(uuid.Nil) {
		return
	}
	shardIdx := hashTxnID(resolvedTxnID.TxnID)
	buffer := w.shards[shardIdx]
	buffer.Record(resolvedTxnID)
}

// DrainWriteBuffer implements the Writer interface.
func (w *writer) DrainWriteBuffer() {
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		w.shards[shardIdx].DrainWriteBuffer()
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
