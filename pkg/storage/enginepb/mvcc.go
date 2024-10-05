// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb

import (
	"math"
	"sort"

	"github.com/cockroachdb/redact"
)

// TxnEpoch is a zero-indexed epoch for a transaction. When a transaction
// retries, it increments its epoch, invalidating all of its previous writes.
type TxnEpoch int32

// SafeValue implements the redact.SafeValue interface.
func (TxnEpoch) SafeValue() {}

// TxnSeq is a zero-indexed sequence number assigned to a request performed by a
// transaction. Writes within a transaction have unique sequences and start at
// sequence number 1. Reads within a transaction have non-unique sequences and
// start at sequence number 0.
//
// Writes within a transaction logically take place in sequence number order.
// Reads within a transaction observe only writes performed by the transaction
// at equal or lower sequence numbers.
type TxnSeq int32

// SafeValue implements the redact.SafeValue interface.
func (TxnSeq) SafeValue() {}

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

// TxnSeqIsIgnored returns true iff the supplied sequence number overlaps with
// any range in the ignored array. The caller should ensure that the ignored
// array is non-overlapping, non-contiguous, and sorted in (increasing) sequence
// number order.
func TxnSeqIsIgnored(seq TxnSeq, ignored []IgnoredSeqNumRange) bool {
	i := sort.Search(len(ignored), func(i int) bool {
		return seq <= ignored[i].End
	})
	// Did we find the smallest index i, such that seq <= ignored[i].End?
	return i != len(ignored) &&
		// AND does seq lie within with the range [start, end] at index i?
		ignored[i].Start <= seq
}

// Short returns a prefix of the transaction's ID.
func (t TxnMeta) Short() redact.SafeString {
	return redact.SafeString(t.ID.Short())
}

// Total returns the range size as the sum of the key and value
// bytes. This includes all non-live keys and all versioned values,
// both for point and range keys.
func (ms MVCCStats) Total() int64 {
	return ms.KeyBytes + ms.ValBytes + ms.RangeKeyBytes + ms.RangeValBytes
}

// GCBytes is a convenience function which returns the number of gc bytes,
// that is the key and value bytes excluding the live bytes, both for
// point keys and range keys.
func (ms MVCCStats) GCBytes() int64 {
	return ms.Total() - ms.LiveBytes
}

// HasNoUserData returns true if there is no user data in the range.
// User data includes RangeKeyCount, KeyCount and IntentCount as those keys
// are user writable. ContainsEstimates must also be zero to avoid false
// positives where range actually has data.
func (ms MVCCStats) HasNoUserData() bool {
	return ms.ContainsEstimates == 0 && ms.RangeKeyCount == 0 && ms.KeyCount == 0 && ms.IntentCount == 0
}

// AvgLockAge returns the average age of outstanding locks,
// based on current wall time specified via nowNanos.
func (ms MVCCStats) AvgLockAge(nowNanos int64) float64 {
	if ms.LockCount == 0 {
		return 0
	}
	// Advance age by any elapsed time since last computed. Note that
	// we operate on a copy.
	ms.AgeTo(nowNanos)
	return float64(ms.LockAge) / float64(ms.LockCount)
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
	ms.LockAge += ms.LockCount * diffSeconds
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
	ms.LockAge += oms.LockAge
	ms.GCBytesAge += oms.GCBytesAge
	ms.LiveBytes += oms.LiveBytes
	ms.KeyBytes += oms.KeyBytes
	ms.ValBytes += oms.ValBytes
	ms.IntentBytes += oms.IntentBytes
	ms.LiveCount += oms.LiveCount
	ms.KeyCount += oms.KeyCount
	ms.ValCount += oms.ValCount
	ms.IntentCount += oms.IntentCount
	ms.LockBytes += oms.LockBytes
	ms.LockCount += oms.LockCount
	ms.RangeKeyCount += oms.RangeKeyCount
	ms.RangeKeyBytes += oms.RangeKeyBytes
	ms.RangeValCount += oms.RangeValCount
	ms.RangeValBytes += oms.RangeValBytes
	ms.SysBytes += oms.SysBytes
	ms.SysCount += oms.SysCount
	ms.AbortSpanBytes += oms.AbortSpanBytes
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
	ms.LockAge -= oms.LockAge
	ms.GCBytesAge -= oms.GCBytesAge
	ms.LiveBytes -= oms.LiveBytes
	ms.KeyBytes -= oms.KeyBytes
	ms.ValBytes -= oms.ValBytes
	ms.IntentBytes -= oms.IntentBytes
	ms.LiveCount -= oms.LiveCount
	ms.KeyCount -= oms.KeyCount
	ms.ValCount -= oms.ValCount
	ms.IntentCount -= oms.IntentCount
	ms.LockBytes -= oms.LockBytes
	ms.LockCount -= oms.LockCount
	ms.RangeKeyCount -= oms.RangeKeyCount
	ms.RangeKeyBytes -= oms.RangeKeyBytes
	ms.RangeValCount -= oms.RangeValCount
	ms.RangeValBytes -= oms.RangeValBytes
	ms.SysBytes -= oms.SysBytes
	ms.SysCount -= oms.SysCount
	ms.AbortSpanBytes -= oms.AbortSpanBytes
}

// IsInline returns true if the value is inlined in the metadata.
func (meta MVCCMetadata) IsInline() bool {
	return meta.RawBytes != nil
}

// GetPrevIntentSeq goes through the intent history and finds the previous
// intent's sequence number given the current sequence.
func (meta *MVCCMetadata) GetPrevIntentSeq(
	seq TxnSeq, ignored []IgnoredSeqNumRange,
) (MVCCMetadata_SequencedIntent, bool) {
	end := len(meta.IntentHistory)
	found := 0
	for {
		index := sort.Search(end, func(i int) bool {
			return meta.IntentHistory[i].Sequence >= seq
		})
		if index == 0 {
			// It is possible that no intent exists such that the sequence is less
			// than the read sequence. In this case, we cannot read a value from the
			// intent history.
			return MVCCMetadata_SequencedIntent{}, false
		}
		candidate := index - 1
		if TxnSeqIsIgnored(meta.IntentHistory[candidate].Sequence, ignored) {
			// This entry was part of an ignored range. Skip it and
			// try the search again, using the current position as new
			// upper bound.
			end = candidate
			continue
		}
		// This history entry has not been ignored, so we're going to keep it.
		found = candidate
		break
	}
	return meta.IntentHistory[found], true
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

// String implements the fmt.Stringer interface.
func (m MVCCMetadata_SequencedIntent) String() string {
	return redact.StringWithoutMarkers(m)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (m MVCCMetadata_SequencedIntent) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf(
		"{%d %s}",
		m.Sequence,
		FormatBytesAsValue(m.Value))
}

// String implements the fmt.Stringer interface.
func (meta *MVCCMetadata) String() string {
	return redact.StringWithoutMarkers(meta)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (meta *MVCCMetadata) SafeFormat(w redact.SafePrinter, _ rune) {
	expand := w.Flag('+')

	w.Printf("txn={%s} ts=%s del=%t klen=%d vlen=%d",
		meta.Txn,
		meta.Timestamp,
		meta.Deleted,
		meta.KeyBytes,
		meta.ValBytes,
	)

	if len(meta.RawBytes) > 0 {
		if expand {
			w.Printf(" raw=%s", FormatBytesAsValue(meta.RawBytes))
		} else {
			w.Printf(" rawlen=%d", len(meta.RawBytes))
		}
	}
	if nih := len(meta.IntentHistory); nih > 0 {
		if expand {
			w.Printf(" ih={")
			for i := range meta.IntentHistory {
				w.Print(meta.IntentHistory[i])
			}
			w.Printf("}")
		} else {
			w.Printf(" nih=%d", nih)
		}
	}

	var txnDidNotUpdateMeta bool
	if meta.TxnDidNotUpdateMeta != nil {
		txnDidNotUpdateMeta = *meta.TxnDidNotUpdateMeta
	}
	w.Printf(" mergeTs=%s txnDidNotUpdateMeta=%t", meta.MergeTimestamp, txnDidNotUpdateMeta)
}

func (meta *MVCCMetadataSubsetForMergeSerialization) String() string {
	var m MVCCMetadata
	m.RawBytes = meta.RawBytes
	m.MergeTimestamp = meta.MergeTimestamp
	return m.String()
}

// String implements the fmt.Stringer interface.
// We implement by value as the object may not reside on the heap.
func (t TxnMeta) String() string {
	return redact.StringWithoutMarkers(t)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (t TxnMeta) SafeFormat(w redact.SafePrinter, _ rune) {
	// Compute priority as a floating point number from 0-100 for readability.
	floatPri := 100 * float64(t.Priority) / float64(math.MaxInt32)
	w.Printf(
		"id=%s key=%s iso=%s pri=%.8f epo=%d ts=%s min=%s seq=%d",
		t.Short(),
		FormatBytesAsKey(t.Key),
		t.IsoLevel,
		floatPri,
		t.Epoch,
		t.WriteTimestamp,
		t.MinTimestamp,
		t.Sequence)
}

// FormatBytesAsKey is injected by module roachpb as dependency upon initialization.
var FormatBytesAsKey = func(k []byte) redact.RedactableString {
	return redact.Sprint(string(k))
}

// FormatBytesAsValue is injected by module roachpb as dependency upon initialization.
var FormatBytesAsValue = func(v []byte) redact.RedactableString {
	return redact.Sprint(string(v))
}
