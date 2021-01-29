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
	"fmt"
	"io"
	"math"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// TxnEpoch is a zero-indexed epoch for a transaction. When a transaction
// retries, it increments its epoch, invalidating all of its previous writes.
type TxnEpoch int32

// TxnSeq is a zero-indexed sequence number assigned to a request performed by a
// transaction. Writes within a transaction have unique sequences and start at
// sequence number 1. Reads within a transaction have non-unique sequences and
// start at sequence number 0.
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

// TxnSeqIsIgnored returns true iff the sequence number overlaps with
// any range in the ignored array.
func TxnSeqIsIgnored(seq TxnSeq, ignored []IgnoredSeqNumRange) bool {
	// The ignored seqnum ranges are guaranteed to be
	// non-overlapping, non-contiguous, and guaranteed to be
	// sorted in seqnum order. We're going to look from the end to
	// see if the current intent seqnum is ignored.
	for i := len(ignored) - 1; i >= 0; i-- {
		if seq < ignored[i].Start {
			// The history entry's sequence number is lower/older than
			// the current ignored range. Go to the previous range
			// and try again.
			continue
		}

		// Here we have a range where the start seqnum is lower than the current
		// intent seqnum. Does it include it?
		if seq > ignored[i].End {
			// Here we have a range where the current history entry's seqnum
			// is higher than the range's end seqnum. Given that the
			// ranges are sorted, we're guaranteed that there won't
			// be any further overlapping range at a lower value of i.
			return false
		}
		// Yes, it's included. We're going to skip over this
		// intent seqnum and retry the search above.
		return true
	}

	// Exhausted the ignore list. Not ignored.
	return false
}

// Short returns a prefix of the transaction's ID.
func (t TxnMeta) Short() redact.SafeString {
	return redact.SafeString(t.ID.Short())
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
	ms.SeparatedIntentCount += oms.SeparatedIntentCount
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
	ms.SeparatedIntentCount -= oms.SeparatedIntentCount
	ms.SysBytes -= oms.SysBytes
	ms.SysCount -= oms.SysCount
	ms.AbortSpanBytes -= oms.AbortSpanBytes
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
func (m *MVCCMetadata_SequencedIntent) String() string {
	var buf strings.Builder
	m.FormatW(&buf, false /* expand */)
	return buf.String()
}

// Format implements the fmt.Formatter interface.
func (m *MVCCMetadata_SequencedIntent) Format(f fmt.State, r rune) {
	m.FormatW(f, f.Flag('+'))
}

// FormatW enables grouping formatters around a single buffer while
// avoiding copies.
func (m *MVCCMetadata_SequencedIntent) FormatW(buf io.Writer, expand bool) {
	fmt.Fprintf(buf,
		"{%d %s}",
		m.Sequence,
		FormatBytesAsValue(m.Value))
}

// String implements the fmt.Stringer interface.
func (meta *MVCCMetadata) String() string {
	var buf strings.Builder
	meta.FormatW(&buf, false /* expand */)
	return buf.String()
}

// Format implements the fmt.Formatter interface.
func (meta *MVCCMetadata) Format(f fmt.State, r rune) {
	meta.FormatW(f, f.Flag('+'))
}

// FormatW enables grouping formatters around a single buffer while
// avoiding copies.
func (meta *MVCCMetadata) FormatW(buf io.Writer, expand bool) {
	fmt.Fprintf(buf, "txn={%s} ts=%s del=%t klen=%d vlen=%d",
		meta.Txn,
		meta.Timestamp,
		meta.Deleted,
		meta.KeyBytes,
		meta.ValBytes,
	)
	if len(meta.RawBytes) > 0 {
		if expand {
			fmt.Fprintf(buf, " raw=%s", FormatBytesAsValue(meta.RawBytes))
		} else {
			fmt.Fprintf(buf, " rawlen=%d", len(meta.RawBytes))
		}
	}
	if nih := len(meta.IntentHistory); nih > 0 {
		if expand {
			fmt.Fprint(buf, " ih={")
			for i := range meta.IntentHistory {
				meta.IntentHistory[i].FormatW(buf, expand)
			}
			fmt.Fprint(buf, "}")
		} else {
			fmt.Fprintf(buf, " nih=%d", nih)
		}
	}
}

func (meta *MVCCMetadataSubsetForMergeSerialization) String() string {
	var m MVCCMetadata
	m.RawBytes = meta.RawBytes
	m.MergeTimestamp = meta.MergeTimestamp
	return m.String()
}

// SafeMessage implements the SafeMessager interface.
//
// This method should be kept largely synchronized with String(), except that it
// can't include sensitive info (e.g. the transaction key).
func (meta *MVCCMetadata) SafeMessage() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "{%s} ts=%s del=%t klen=%d vlen=%d",
		meta.Txn.SafeMessage(),
		meta.Timestamp,
		meta.Deleted,
		meta.KeyBytes,
		meta.ValBytes,
	)
	if len(meta.RawBytes) > 0 {
		fmt.Fprintf(&buf, " rawlen=%d", len(meta.RawBytes))
	}
	if nih := len(meta.IntentHistory); nih > 0 {
		fmt.Fprintf(&buf, " nih=%d", nih)
	}
	return buf.String()
}

// String implements the fmt.Stringer interface.
// We implement by value as the object may not reside on the heap.
func (t TxnMeta) String() string {
	var buf strings.Builder
	t.FormatW(&buf)
	return buf.String()
}

// FormatW enables grouping formatters around a single buffer while
// avoiding copies.
// We implement by value as the object may not reside on the heap.
func (t TxnMeta) FormatW(buf io.Writer) {
	// Compute priority as a floating point number from 0-100 for readability.
	floatPri := 100 * float64(t.Priority) / float64(math.MaxInt32)
	fmt.Fprintf(buf,
		"id=%s key=%s pri=%.8f epo=%d ts=%s min=%s seq=%d",
		t.Short(),
		FormatBytesAsKey(t.Key),
		floatPri,
		t.Epoch,
		t.WriteTimestamp,
		t.MinTimestamp,
		t.Sequence)
}

// SafeMessage implements the SafeMessager interface.
//
// This method should be kept largely synchronized with String(), except that it
// can't include sensitive info (e.g. the transaction key).
//
// We implement by value as the object may not reside on the heap.
func (t TxnMeta) SafeMessage() string {
	var buf strings.Builder
	// Compute priority as a floating point number from 0-100 for readability.
	floatPri := 100 * float64(t.Priority) / float64(math.MaxInt32)
	fmt.Fprintf(&buf,
		"id=%s pri=%.8f epo=%d ts=%s min=%s seq=%d",
		t.Short(),
		floatPri,
		t.Epoch,
		t.WriteTimestamp,
		t.MinTimestamp,
		t.Sequence)
	return buf.String()
}

var _ errors.SafeMessager = (*TxnMeta)(nil)

// FormatBytesAsKey is injected by module roachpb as dependency upon initialization.
var FormatBytesAsKey = func(k []byte) string {
	return string(k)
}

// FormatBytesAsValue is injected by module roachpb as dependency upon initialization.
var FormatBytesAsValue = func(v []byte) string {
	return string(v)
}
