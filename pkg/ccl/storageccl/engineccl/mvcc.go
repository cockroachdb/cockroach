// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

package engineccl

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

// MVCCIncrementalIterator iterates over the diff of the key range
// [startKey,endKey) and time range (startTime,endTime]. If a key was added or
// modified between startTime and endTime, the iterator will position at the
// most recent version (before or at endTime) of that key. If the key was most
// recently deleted, this is signalled with an empty value.
//
// Note: The endTime is inclusive to be consistent with the non-incremental
// iterator, where reads at a given timestamp return writes at that
// timestamp. The startTime is then made exclusive so that iterating time 1 to
// 2 and then 2 to 3 will only return values with time 2 once. An exclusive
// start time would normally make it difficult to scan timestamp 0, but
// CockroachDB uses that as a sentinel for key metadata anyway.
//
// Expected usage:
//    iter := NewMVCCIncrementalIterator(e, startTime, endTime)
//    defer iter.Close()
//    for iter.Seek(startKey); ; iter.Next() {
//        ok, err := iter.Valid()
//        if !ok { ... }
//        [code using iter.Key() and iter.Value()]
//    }
//    if err := iter.Error(); err != nil {
//      ...
//    }
type MVCCIncrementalIterator struct {
	// TODO(dan): Move all this logic into c++ and make this a thin wrapper.

	iter engine.Iterator

	startTime hlc.Timestamp
	endTime   hlc.Timestamp
	err       error
	valid     bool
	nextkey   bool

	// For allocation avoidance.
	meta enginepb.MVCCMetadata
}

var _ engine.SimpleIterator = &MVCCIncrementalIterator{}

// NewMVCCIncrementalIterator creates an MVCCIncrementalIterator with the
// specified engine and time range.
func NewMVCCIncrementalIterator(
	e engine.Reader, startTime, endTime hlc.Timestamp,
) *MVCCIncrementalIterator {
	return &MVCCIncrementalIterator{
		iter:      e.NewTimeBoundIterator(startTime, endTime),
		startTime: startTime,
		endTime:   endTime,
	}
}

// Seek advances the iterator to the first key in the engine which is >= the
// provided key.
func (i *MVCCIncrementalIterator) Seek(startKey engine.MVCCKey) {
	i.iter.Seek(startKey)
	i.err = nil
	i.valid = true
	i.nextkey = false
	i.NextKey()
}

// Close frees up resources held by the iterator.
func (i *MVCCIncrementalIterator) Close() {
	i.iter.Close()
}

// Next advances the iterator to the next key/value in the iteration. After this
// call, Valid() will be true if the iterator was not positioned at the last
// key.
func (i *MVCCIncrementalIterator) Next() {
	// TODO(dan): Implement Next. We'll need this for `RESTORE ... AS OF SYSTEM
	// TIME`.
	panic("unimplemented")
}

// NextKey advances the iterator to the next MVCC key. This operation is
// distinct from Next which advances to the next version of the current key or
// the next key if the iterator is currently located at the last version for a
// key.
func (i *MVCCIncrementalIterator) NextKey() {
	for {
		if !i.valid {
			return
		}
		if ok, err := i.iter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}

		if i.nextkey {
			i.nextkey = false
			i.iter.NextKey()
			continue
		}

		unsafeMetaKey := i.iter.UnsafeKey()
		if unsafeMetaKey.IsValue() {
			i.meta.Reset()
			i.meta.Timestamp = unsafeMetaKey.Timestamp
		} else {
			if i.err = i.iter.ValueProto(&i.meta); i.err != nil {
				i.valid = false
				return
			}
		}
		if i.meta.IsInline() {
			// Inline values are only used in non-user data. They're not needed
			// for backup, so they're not handled by this method. If one shows
			// up, throw an error so it's obvious something is wrong.
			i.valid = false
			i.err = errors.Errorf("inline values are unsupported by MVCCIncrementalIterator: %s",
				unsafeMetaKey.Key)
			return
		}

		if i.meta.Txn != nil {
			if !i.endTime.Less(i.meta.Timestamp) {
				i.err = &roachpb.WriteIntentError{
					Intents: []roachpb.Intent{{
						Span:   roachpb.Span{Key: i.iter.Key().Key},
						Status: roachpb.PENDING,
						Txn:    *i.meta.Txn,
					}},
				}
				i.valid = false
				return
			}
			i.iter.Next()
			continue
		}

		if i.endTime.Less(i.meta.Timestamp) {
			i.iter.Next()
			continue
		}
		if !i.startTime.Less(i.meta.Timestamp) {
			i.iter.NextKey()
			continue
		}

		// Skip tombstone (len=0) records when startTime is zero (non-incremental).
		if (i.startTime == hlc.Timestamp{}) && len(i.iter.UnsafeValue()) == 0 {
			i.iter.NextKey()
			continue
		}

		i.nextkey = true
		break
	}
}

// Valid must be called after any call to Reset(), Next(), or similar methods.
// It returns (true, nil) if the iterator points to a valid key (it is undefined
// to call Key(), Value(), or similar methods unless Valid() has returned (true,
// nil)). It returns (false, nil) if the iterator has moved past the end of the
// valid range, or (false, err) if an error has occurred. Valid() will never
// return true with a non-nil error.
func (i *MVCCIncrementalIterator) Valid() (bool, error) {
	return i.valid, i.err
}

// Key returns the current key.
func (i *MVCCIncrementalIterator) Key() engine.MVCCKey {
	return i.iter.Key()
}

// Value returns the current value as a byte slice.
func (i *MVCCIncrementalIterator) Value() []byte {
	return i.iter.Value()
}

// UnsafeKey returns the same key as Key, but the memory is invalidated on the
// next call to {Next,Reset,Close}.
func (i *MVCCIncrementalIterator) UnsafeKey() engine.MVCCKey {
	return i.iter.UnsafeKey()
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Reset,Close}.
func (i *MVCCIncrementalIterator) UnsafeValue() []byte {
	return i.iter.UnsafeValue()
}

const (
	// The size of the timestamp portion of MVCC version keys (used to update stats).
	mvccVersionTimestampSize int64 = 12
)

// MVCCComputeStats scans the underlying engine from start to end keys and
// computes stats counters based on the values. This method is used after a
// range is split to recompute stats for each subrange. The start key is
// always adjusted to avoid counting local keys in the event stats are being
// recomputed for the first range (i.e. the one with start key == KeyMin).
// The nowNanos arg specifies the wall time in nanoseconds since the
// epoch and is used to compute the total age of all intents.
//
// This implementation must match engine/db.cc:MVCCComputeStatsInternal.
func MVCCComputeStats(
	iter engine.SimpleIterator, start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	var ms enginepb.MVCCStats

	meta := &enginepb.MVCCMetadata{}
	var prevKey []byte
	first := false

	iter.Seek(start)
	for ; ; iter.Next() {
		ok, err := iter.Valid()
		if err != nil {
			return ms, err
		}
		if !ok || !iter.UnsafeKey().Less(end) {
			break
		}

		unsafeKey := iter.UnsafeKey()
		unsafeValue := iter.UnsafeValue()

		// TODO(dan): There is an EncodedSize method on MVCCKey, but it's
		// simpler than this. It's used in all sorts of places I didn't want to
		// audit in this commit, so as a followup, see if we can move this logic
		// there.
		encodedKeyLen := len(unsafeKey.Key)
		if unsafeKey.Timestamp != (hlc.Timestamp{}) {
			const (
				timestampSentinelLen = 1
				walltimeEncodedLen   = 8
				logicalEncodedLen    = 4
			)
			encodedKeyLen += timestampSentinelLen + walltimeEncodedLen
			if unsafeKey.Timestamp.Logical != 0 {
				encodedKeyLen += logicalEncodedLen
			}
		}

		isSys := bytes.Compare(unsafeKey.Key, keys.LocalMax) < 0
		isValue := unsafeKey.IsValue()
		implicitMeta := isValue && !bytes.Equal(unsafeKey.Key, prevKey)
		prevKey = append(prevKey[:0], unsafeKey.Key...)

		if implicitMeta {
			// No MVCCMetadata entry for this series of keys.
			meta.Reset()
			meta.KeyBytes = mvccVersionTimestampSize
			meta.ValBytes = int64(len(unsafeValue))
			meta.Deleted = len(unsafeValue) == 0
			meta.Timestamp.WallTime = nowNanos
		}

		if !isValue || implicitMeta {
			metaKeySize := int64(encodedKeyLen) + 1
			var metaValSize int64
			if !implicitMeta {
				metaValSize = int64(len(unsafeValue))
			}
			totalBytes := metaKeySize + metaValSize
			first = true
			if err := proto.Unmarshal(unsafeValue, meta); err != nil {
				return ms, err
			}
			if isSys {
				ms.SysBytes += totalBytes
				ms.SysCount++
			} else {
				if !meta.Deleted {
					ms.LiveBytes += totalBytes
					ms.LiveCount++
				} else {
					// First value is deleted, so it's GC'able; add meta key & value bytes to age stat.
					ms.GCBytesAge += totalBytes * (nowNanos/1E9 - meta.Timestamp.WallTime/1E9)
				}
				ms.KeyBytes += metaKeySize
				ms.ValBytes += metaValSize
				ms.KeyCount++
				if meta.IsInline() {
					ms.ValCount++
				}
			}
			if !implicitMeta {
				continue
			}
		} else {
			totalBytes := int64(len(unsafeValue)) + mvccVersionTimestampSize
			if isSys {
				ms.SysBytes += totalBytes
			} else {
				if first {
					first = false
					if !meta.Deleted {
						ms.LiveBytes += totalBytes
					} else {
						// First value is deleted, so it's GC'able; add key & value bytes to age stat.
						ms.GCBytesAge += totalBytes * (nowNanos/1E9 - meta.Timestamp.WallTime/1E9)
					}
					if meta.Txn != nil {
						ms.IntentBytes += totalBytes
						ms.IntentCount++
						ms.IntentAge += nowNanos/1E9 - meta.Timestamp.WallTime/1E9
					}
					if meta.KeyBytes != mvccVersionTimestampSize {
						return ms, errors.Errorf("expected mvcc metadata key bytes to equal %d; got %d", mvccVersionTimestampSize, meta.KeyBytes)
					}
					if meta.ValBytes != int64(len(unsafeValue)) {
						return ms, errors.Errorf("expected mvcc metadata val bytes to equal %d; got %d", len(unsafeValue), meta.ValBytes)
					}
				} else {
					// Overwritten value; add value bytes to the GC'able bytes age stat.
					ms.GCBytesAge += totalBytes * (nowNanos/1E9 - unsafeKey.Timestamp.WallTime/1E9)
				}
				ms.KeyBytes += mvccVersionTimestampSize
				ms.ValBytes += int64(len(unsafeValue))
				ms.ValCount++
			}
		}
	}

	ms.LastUpdateNanos = nowNanos
	return ms, nil
}
