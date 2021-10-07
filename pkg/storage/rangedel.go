// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// MVCCRangeTombstone describes a range tombstone marking a range of
// keyspace deleted at a particular MVCC timestamp. These tombstones are
// stored in the range local operations keyspace.
//
// MVCCRangeTombstones may be written and cleared with the
// MVCCRangeTombstoneWriter type.
type MVCCRangeTombstone struct {
	Start     roachpb.RKey
	End       roachpb.RKey
	Timestamp hlc.Timestamp
}

// String returns a string-formatted version of the tombstone.
func (t MVCCRangeTombstone) String() string {
	return fmt.Sprintf("DEL-RANGE(%s-%s@%s)", t.Start, t.End, t.Timestamp)
}

// Format implements the fmt.Formatter interface.
func (t MVCCRangeTombstone) Format(f fmt.State, c rune) {
	fmt.Fprintf(f, "DEL-RANGE(%s-%s@%s)", t.Start, t.End, t.Timestamp)
}

// Key returns the MVCC key under which this tombstone is stored.
func (t MVCCRangeTombstone) Key() MVCCKey {
	// TODO(jackson): Benchmark anchoring on Start vs End keys.
	return MVCCKey{
		Key:       keys.MVCCRangeDeletionKey(t.Start),
		Timestamp: t.Timestamp,
	}
}

// value returns the value of the tombstone.
func (t MVCCRangeTombstone) value() []byte {
	// TODO(jackson): Should the value be a protocol buffer?
	// We might want to include metadata that can be used to inform read-path
	// optimizations, like when to seek over a range tombstone.
	return t.End
}

// MVCCRangeTombstoneWriter wraps a Writer with knowledge of how to write
// MVCC range tombstones into the range operations key space.
type MVCCRangeTombstoneWriter struct {
	Writer
}

// WriteTombstone writes the provided MVCC range tombstone to the underlying
// Writer.
func (w MVCCRangeTombstoneWriter) WriteTombstone(t MVCCRangeTombstone) error {
	return w.PutMVCC(t.Key(), t.value())
}

// ClearTombstone clears the provided MVCC range tombstone on the underlying
// Writer.
func (w MVCCRangeTombstoneWriter) ClearTombstone(t MVCCRangeTombstone) error {
	return w.ClearMVCC(t.Key())
}

// NewRangeTombstoneIterator creates an interator over all MVCC range
// tombstones that begin within the range [startKey, endKey).
func NewRangeTombstoneIterator(r Reader, startKey, endKey roachpb.RKey) *RangeTombstoneIterator {
	return &RangeTombstoneIterator{
		iter: r.NewEngineIterator(IterOptions{
			LowerBound: keys.MVCCRangeDeletionKey(startKey),
			UpperBound: keys.MVCCRangeDeletionKey(endKey),
		}),
	}
}

// RangeTombstoneIterator is an iterator of MVCC Range tombstones.
// MVCC Range Tombstones reside in the range operations keyspace.
type RangeTombstoneIterator struct {
	iter EngineIterator
}

// First seeks to the first range tombstone with the configured range.
// It returns (true, nil) if the iterator is now positioned over a tombstone.
func (i *RangeTombstoneIterator) First() (valid bool, err error) {
	iter := i.iter.GetRawIter()
	ok := iter.First()
	return ok, iter.Error()
}

// Next advances to the next range tombstone within the configured range.
// It returns (true, nil) if the iterator is now positioned over a tombstone.
func (i *RangeTombstoneIterator) Next() (valid bool, err error) {
	return i.iter.NextEngineKey()
}

// Current returns the range tombstone at the iterator's current position.
//
// Current must not be called unless the most recent positioning call
// returned true for valid.
func (i *RangeTombstoneIterator) Current() (MVCCRangeTombstone, error) {
	startKey, timestamp, err := i.unsafeDecodeCurrentKey()
	if err != nil {
		return MVCCRangeTombstone{}, err
	}
	tomb := MVCCRangeTombstone{
		Start:     make([]byte, len(startKey)),
		End:       make([]byte, len(i.iter.UnsafeValue())),
		Timestamp: timestamp,
	}
	copy(tomb.Start, startKey)
	copy(tomb.End, i.iter.UnsafeValue())
	return tomb, nil
}

// UnsafeCurrent returns the range tombstone at the iterator's current position.
// The returned range tombstone contains pointers into buffer internal state
// and is only valid until the next iterator positioning call.
//
// UnsafeCurrent must not be called unless the most recent positioning call
// returned true for valid.
func (i *RangeTombstoneIterator) UnsafeCurrent() (MVCCRangeTombstone, error) {
	startKey, timestamp, err := i.unsafeDecodeCurrentKey()
	if err != nil {
		return MVCCRangeTombstone{}, err
	}
	return MVCCRangeTombstone{
		Start:     startKey,
		End:       i.iter.UnsafeValue(),
		Timestamp: timestamp,
	}, nil
}

func (i *RangeTombstoneIterator) unsafeDecodeCurrentKey() (roachpb.RKey, hlc.Timestamp, error) {
	k, err := DecodeMVCCKey(i.iter.UnsafeRawEngineKey())
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	startKey, err := keys.DecodeMVCCRangeDeletionKey(k.Key)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	return startKey, k.Timestamp, nil
}

// Close releases the iterator's resources.
// The iterator must not be used after Close is called.
func (i *RangeTombstoneIterator) Close() {
	i.iter.Close()
}
