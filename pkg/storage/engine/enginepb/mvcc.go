// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package enginepb

import (
	"math"
	"sort"
)

// TxnEpoch is a zero-indexed epoch for a transaction. When a transaction
// retries, it increments its epoch, invalidating all of its previous writes.
type TxnEpoch int32

// TxnSeq is a zero-indexed sequence number asssigned to a a request performed
// by a transaction. Writes within a transaction have unique sequences and start
// at sequence number 1. Reads within a transaction have non-unique sequences
// and start at sequence number 0.
//
// Writes within a transaction logically take place in sequence number order.
// Reads within a transaction observe only writes performed by the transaction
// at equal or lower sequence numbers.
type TxnSeq int32

// TxnPriority defines the priority that a transaction operates at. Transactions
// with high priorities are preferred over transaction with low priorities when
// resolving conflicts between themselves. For example, transaction priorities
// are used to determine which transaction to abort when resolving transaction
// deadlocks.
type TxnPriority int32

const (
	// MinTxnPriority is the minimum allowed txn priority.
	MinTxnPriority TxnPriority = 0
	// MaxTxnPriority is the maximum allowed txn priority.
	MaxTxnPriority TxnPriority = math.MaxInt32
)

// Short returns a prefix of the transaction's ID.
func (t TxnMeta) Short() string {
	return t.ID.Short()
}

// Total returns the range size as the sum of the key and value
// bytes. This includes all non-live keys and all versioned values.
func (ms MVCCStats) Total() int64 {
	return ms.KeyBytes + ms.ValBytes
}

// GCBytes is a convenience function which returns the number of gc bytes,
// that is the key and value bytes excluding the live bytes.
func (ms MVCCStats) GCBytes() int64 {
	return ms.KeyBytes + ms.ValBytes - ms.LiveBytes
}

// AvgIntentAge returns the average age of outstanding intents,
// based on current wall time specified via nowNanos.
func (ms MVCCStats) AvgIntentAge(nowNanos int64) float64 {
	if ms.IntentCount == 0 {
		return 0
	}
	// Advance age by any elapsed time since last computed. Note that
	// we operate on a copy.
	ms.AgeTo(nowNanos)
	return float64(ms.IntentAge) / float64(ms.IntentCount)
}

// GCByteAge returns the total age of outstanding gc'able
// bytes, based on current wall time specified via nowNanos.
// nowNanos is ignored if it's a past timestamp as seen by
// rs.LastUpdateNanos.
func (ms MVCCStats) GCByteAge(nowNanos int64) int64 {
	ms.AgeTo(nowNanos) // we operate on a copy
	return ms.GCBytesAge
}

// Forward is like AgeTo, but if nowNanos is not ahead of ms.LastUpdateNanos,
// this method is a noop.
func (ms *MVCCStats) Forward(nowNanos int64) {
	if ms.LastUpdateNanos >= nowNanos {
		return
	}
	ms.AgeTo(nowNanos)
}

// AgeTo encapsulates the complexity of computing the increment in age
// quantities contained in MVCCStats. Two MVCCStats structs only add and
// subtract meaningfully if their LastUpdateNanos matches, so aging them to
// the max of their LastUpdateNanos is a prerequisite, though Add() takes
// care of this internally.
func (ms *MVCCStats) AgeTo(nowNanos int64) {
	// Seconds are counted every time each individual nanosecond timestamp
	// crosses a whole second boundary (i.e. is zero mod 1E9). Thus it would
	// be a mistake to use the (nonequivalent) expression (a-b)/1E9.
	diffSeconds := nowNanos/1e9 - ms.LastUpdateNanos/1e9

	ms.GCBytesAge += ms.GCBytes() * diffSeconds
	ms.IntentAge += ms.IntentCount * diffSeconds
	ms.LastUpdateNanos = nowNanos
}

// Add adds values from oms to ms. The ages will be moved forward to the
// larger of the LastUpdateNano timestamps involved.
func (ms *MVCCStats) Add(oms MVCCStats) {
	// Enforce the max LastUpdateNanos for both ages based on their
	// pre-addition state.
	ms.Forward(oms.LastUpdateNanos)
	oms.Forward(ms.LastUpdateNanos) // on local copy

	ms.ContainsEstimates += oms.ContainsEstimates

	// Now that we've done that, we may just add them.
	ms.IntentAge += oms.IntentAge
	ms.GCBytesAge += oms.GCBytesAge
	ms.LiveBytes += oms.LiveBytes
	ms.KeyBytes += oms.KeyBytes
	ms.ValBytes += oms.ValBytes
	ms.IntentBytes += oms.IntentBytes
	ms.LiveCount += oms.LiveCount
	ms.KeyCount += oms.KeyCount
	ms.ValCount += oms.ValCount
	ms.IntentCount += oms.IntentCount
	ms.SysBytes += oms.SysBytes
	ms.SysCount += oms.SysCount
}

// Subtract removes oms from ms. The ages will be moved forward to the larger of
// the LastUpdateNano timestamps involved.
func (ms *MVCCStats) Subtract(oms MVCCStats) {
	// Enforce the max LastUpdateNanos for both ages based on their
	// pre-subtraction state.
	ms.Forward(oms.LastUpdateNanos)
	oms.Forward(ms.LastUpdateNanos)

	ms.ContainsEstimates -= oms.ContainsEstimates

	// Now that we've done that, we may subtract.
	ms.IntentAge -= oms.IntentAge
	ms.GCBytesAge -= oms.GCBytesAge
	ms.LiveBytes -= oms.LiveBytes
	ms.KeyBytes -= oms.KeyBytes
	ms.ValBytes -= oms.ValBytes
	ms.IntentBytes -= oms.IntentBytes
	ms.LiveCount -= oms.LiveCount
	ms.KeyCount -= oms.KeyCount
	ms.ValCount -= oms.ValCount
	ms.IntentCount -= oms.IntentCount
	ms.SysBytes -= oms.SysBytes
	ms.SysCount -= oms.SysCount
}

// IsInline returns true if the value is inlined in the metadata.
func (meta MVCCMetadata) IsInline() bool {
	return meta.RawBytes != nil
}

// AddToIntentHistory adds the sequence and value to the intent history.
func (meta *MVCCMetadata) AddToIntentHistory(seq TxnSeq, val []byte) {
	meta.IntentHistory = append(meta.IntentHistory,
		MVCCMetadata_SequencedIntent{Sequence: seq, Value: val})
}

// GetPrevIntentSeq goes through the intent history and finds the previous
// intent's sequence number given the current sequence.
func (meta *MVCCMetadata) GetPrevIntentSeq(seq TxnSeq) (TxnSeq, bool) {
	index := sort.Search(len(meta.IntentHistory), func(i int) bool {
		return meta.IntentHistory[i].Sequence >= seq
	})
	if index > 0 && index < len(meta.IntentHistory) {
		return meta.IntentHistory[index-1].Sequence, true
	}
	return 0, false
}

// GetIntentValue goes through the intent history and finds the value
// written at the sequence number.
func (meta *MVCCMetadata) GetIntentValue(seq TxnSeq) ([]byte, bool) {
	index := sort.Search(len(meta.IntentHistory), func(i int) bool {
		return meta.IntentHistory[i].Sequence >= seq
	})
	if index < len(meta.IntentHistory) && meta.IntentHistory[index].Sequence == seq {
		return meta.IntentHistory[index].Value, true
	}
	return nil, false
}
