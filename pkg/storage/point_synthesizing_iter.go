// Copyright 2022 The Cockroach Authors.
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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// pointSynthesizingIter wraps an MVCCIterator, and synthesizes MVCC point
// tombstones for MVCC range tombstones above/below existing point keys, and at
// the start keys of MVCC range tombstones (truncated to iterator bounds). If
// emitOnSeekGE, it will also synthesize point tombstones around the SeekGE
// key if it does not have an existing point key.
//
// It does not emit MVCC range keys at all, since these would appear to conflict
// with the synthesized point keys.
type pointSynthesizingIter struct {
	iter MVCCIterator

	// emitOnSeekGE will synthesize point tombstones for the SeekGE seek key if it
	// overlaps with a range tombstone even if no point key exists. The primary
	// use-case is to synthesize point tombstones for e.g. an MVCCGet that does
	// not match a point key but overlaps a range key.
	//
	// This is optional, because e.g. pebbleMVCCScanner often uses seeks as an
	// optimization to skip over old versions of a key, and we don't want to keep
	// synthesizing point tombstones every time it skips ahead.
	//
	// TODO(erikgrinaker): This could instead check for prefix iterators, or a
	// separate SeekPrefixGE() method, but we don't currently have APIs for it.
	emitOnSeekGE bool

	// curKey is the current key position of the synthesizing iterator. This may
	// be out of sync with the underlying iterator. If it is empty, the iterator
	// is invalid.
	curKey roachpb.Key

	// rangeKeys contains the range keys overlapping curKey. rangeKeysIdx points
	// to the current/pending range key to synthesize a point tombstone for. See
	// atPoint for the relative positioning with the underlying iterator.
	rangeKeys      []MVCCRangeKey
	rangeKeysIdx   int
	rangeKeysStart roachpb.Key // used to memoize rangeKeys at adjacent points

	// atPoint is true if the synthesizing iterator is positioned on a real point
	// key in the underlying iterator. In that case, rangeKeysIdx points to a
	// range key following the point key, or beyond the slice bounds when there
	// are no further range keys at this key position.
	//
	// When false, the synthesizing iterator is positioned on a synthetic
	// point tombstone given by rangeKeysIdx. In that case, the underlying
	// iterator points to a following key, which can either be a different
	// version of the current key, a different point or range key, or an
	// exhausted iterator.
	//
	// The relative positioning is mirrored in the forward and reverse direction.
	// For example, when atPoint is true and rangeKeys are exhausted, rangeKeysIdx
	// will be len(rangeKeys) in the forward direction and -1 in the reverse
	// direction. Similarly, the underlying iterator's key is always >= curKey
	// in the forward direction and <= in reverse.
	//
	// See also assertInvariants() which asserts positioning invariants and more.
	atPoint bool

	// reverse is true when the current iterator direction is in reverse, i.e.
	// following a SeekLT or Prev call.
	reverse bool
}

var _ MVCCIterator = new(pointSynthesizingIter)

// newPointSynthesizingIter creates a new pointSynthesizingIter.
func newPointSynthesizingIter(iter MVCCIterator, emitOnSeekGE bool) *pointSynthesizingIter {
	return &pointSynthesizingIter{
		iter:         iter,
		emitOnSeekGE: emitOnSeekGE,
	}
}

// updateKeys updates i.curKey and i.rangeKeys based on the underlying iterator.
// i.rangeKeysIdx is always reset to the first/last range key depending on the
// direction.
func (i *pointSynthesizingIter) updateKeys() {
	if ok, err := i.iter.Valid(); !ok || err != nil {
		i.curKey = i.curKey[:0]
		i.rangeKeys = nil
		i.rangeKeysStart = i.rangeKeysStart[:0]
	} else {
		i.curKey = append(i.curKey[:0], i.iter.UnsafeKey().Key...)
		if rangeStart, _ := i.iter.RangeBounds(); !rangeStart.Equal(i.rangeKeysStart) {
			i.rangeKeys = i.iter.RangeKeys()
			i.rangeKeysStart = append(i.rangeKeysStart[:0], rangeStart...)
		}
	}
	if !i.reverse {
		i.rangeKeysIdx = 0
	} else {
		i.rangeKeysIdx = len(i.rangeKeys) - 1 // NB: -1 is correct with no range keys
	}
}

// updateAtPoint updates i.atPoint according to whether the synthesizing
// iterator is positioned on the real point key in the underlying iterator. It
// requires i.curKey and i.rangeKeysIdx to have been positioned first.
//
// TODO(erikgrinaker): This is likely not performant due to the comparisons,
// and should be optimized.
func (i *pointSynthesizingIter) updateAtPoint() {
	if len(i.curKey) == 0 {
		i.atPoint = false
	} else if hasPoint, hasRange := i.iter.HasPointAndRange(); !hasPoint {
		i.atPoint = false
	} else if !hasRange && len(i.rangeKeys) == 0 {
		// Optimization for common case of no range keys.
		i.atPoint = true
	} else if point := i.iter.UnsafeKey(); !point.Key.Equal(i.curKey) {
		i.atPoint = false
	} else if i.reverse {
		i.atPoint = i.rangeKeysIdx < 0 || (point.Timestamp.IsSet() &&
			point.Timestamp.LessEq(i.rangeKeys[i.rangeKeysIdx].Timestamp))
	} else {
		i.atPoint = point.Timestamp.IsEmpty() ||
			i.rangeKeysIdx >= len(i.rangeKeys) ||
			i.rangeKeys[i.rangeKeysIdx].Timestamp.LessEq(point.Timestamp)
	}
}

// updatePosition updates the synthesizing iterator with the position of the
// underlying iterator. This may step the underlying iterator to position it
// correctly relative to bare range keys.
func (i *pointSynthesizingIter) updatePosition() {
	if !i.reverse {
		i.updateKeys()
		// If we're on a bare range key in the forward direction, we populate the
		// range keys but then step iter ahead before updating the point position,
		// since a following point may be at the same key and must be interleaved.
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
			i.iter.Next()
		}
		i.updateAtPoint()

	} else {
		// If we're now on a bare range key in the reverse direction, and we've
		// already seen this key (as evidenced by curKey), then we skip over the
		// bare range key to avoid duplicates.
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
			if i.iter.UnsafeKey().Key.Equal(i.curKey) {
				i.iter.Prev()
			}
		}
		i.updateKeys()
		i.updateAtPoint()
	}
}

// SeekGE implements MVCCIterator.
func (i *pointSynthesizingIter) SeekGE(key MVCCKey) {
	i.reverse = false
	i.iter.SeekGE(key)

	// If we land in the middle of a bare range key and emitOnSeek is disabled,
	// then skip over it to the next point/range key. However, if we're seeking to
	// a specific version and don't find an older point key at the seek key, then
	// we also need to peek backwards for an existing point key above us, which
	// would mandate that we synthesize point keys here anyway.
	//
	// TODO(erikgrinaker): It might be faster to first do an unversioned seek to
	// look for previous points and then a versioned seek.
	var positioned bool
	if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint && !i.emitOnSeekGE {
		if rangeStart, _ := i.iter.RangeBounds(); !rangeStart.Equal(i.iter.UnsafeKey().Key) {
			i.iter.Next()

			if key.Timestamp.IsSet() {
				ok, err := i.iter.Valid()
				if err == nil && (!ok || !key.Key.Equal(i.iter.UnsafeKey().Key)) {
					i.iter.Prev()
					if hasP, _ := i.iter.HasPointAndRange(); hasP && key.Key.Equal(i.iter.UnsafeKey().Key) {
						i.updateKeys()
						positioned = true
					}
					i.iter.Next()
				}
			}
		}
	}

	if !positioned {
		i.updateKeys()

		// If we're now at a bare range key, it must either be at the start of it,
		// or in the middle with emitOnSeekGE enabled. In either case, we want to
		// move the iterator ahead to look for a point key that may be colocated
		// with the start/seek key in order to interleave it.
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
			i.iter.Next()
		}
	}

	// If we're seeking to a specific version, skip newer range tombstones.
	if len(i.rangeKeys) > 0 && key.Timestamp.IsSet() && key.Key.Equal(i.curKey) {
		i.rangeKeysIdx = sort.Search(len(i.rangeKeys), func(idx int) bool {
			return i.rangeKeys[idx].Timestamp.LessEq(key.Timestamp)
		})
	}

	i.updateAtPoint()

	// It's possible that we seeked past all of the range keys. In this case,
	// we have to reposition on the next key (current iter key).
	if !i.atPoint && i.rangeKeysIdx >= len(i.rangeKeys) {
		i.updatePosition()
	}
}

// SeekIntentGE implements MVCCIterator.
func (i *pointSynthesizingIter) SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID) {
	i.reverse = false
	i.iter.SeekIntentGE(key, txnUUID)

	// If we land in the middle of a bare range key and emitOnSeekGE is disabled,
	// then skip over it to the next point/range key.
	if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint && !i.emitOnSeekGE {
		if rangeStart, _ := i.iter.RangeBounds(); !rangeStart.Equal(i.iter.UnsafeKey().Key) {
			i.iter.Next()
		}
	}

	i.updateKeys()

	// If we're now at a bare range key, it must either be at the start key, or in
	// the middle with emitOnSeekGE enabled. In either case, we want to move the
	// iterator ahead to the next key, which may be a point key colocated with the
	// current key which must be interleaved with the range keys.
	if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
		i.iter.Next()
	}

	i.updateAtPoint()
}

// Next implements MVCCIterator.
func (i *pointSynthesizingIter) Next() {
	// When changing direction, flip the relative positioning with iter.
	if i.reverse {
		i.reverse = false
		if len(i.curKey) == 0 { // iterator was exhausted
			i.iter.Next()
			i.updatePosition()
			return
		} else if i.atPoint {
			i.rangeKeysIdx++
		} else {
			i.iter.Next()
		}
	}

	// Step off the current point, either real or synthetic.
	if i.atPoint {
		i.iter.Next()
	} else {
		i.rangeKeysIdx++
	}
	i.updateAtPoint()

	// If we've exhausted the current range keys, update with the underlying iter
	// position (which must now be different from curKey).
	if !i.atPoint && i.rangeKeysIdx >= len(i.rangeKeys) {
		i.updatePosition()
	}
}

// NextKey implements MVCCIterator.
func (i *pointSynthesizingIter) NextKey() {
	// When changing direction, flip the relative positioning with iter.
	if i.reverse {
		i.reverse = false
		if !i.atPoint {
			i.iter.Next()
		}
	}
	// Don't call NextKey() if the underlying iterator is already on the next key.
	if i.curKey.Equal(i.iter.UnsafeKey().Key) {
		i.iter.NextKey()
	}
	i.updatePosition()
}

// SeekLT implements MVCCIterator.
func (i *pointSynthesizingIter) SeekLT(key MVCCKey) {
	i.reverse = true
	i.iter.SeekLT(key)

	// If we did a versioned seek and found a range key that overlaps the seek
	// key, it's possible that we skipped over existing point key versions of the
	// seek key. These would mandate that we synthesize point keys around them, so
	// we have to peek forward to check.
	//
	// TODO(erikgrinaker): It may possibly be faster to do an unversioned seek
	// from the next key first to look for points.
	var positioned bool
	if key.Timestamp.IsSet() {
		if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange {
			if !hasPoint || !i.iter.UnsafeKey().Key.Equal(key.Key) {
				if _, rangeEnd := i.iter.RangeBounds(); rangeEnd.Compare(key.Key) > 0 {
					i.iter.Next()
					if hasP, _ := i.iter.HasPointAndRange(); hasP && i.iter.UnsafeKey().Key.Equal(key.Key) {
						i.updateKeys()
						positioned = true
					}
					i.iter.Prev()
				}
			}
		}
	}

	if !positioned {
		i.updateKeys()
	}

	// If we're seeking to a specific version, skip over older range tombstones.
	if key.Timestamp.IsSet() && key.Key.Equal(i.curKey) {
		i.rangeKeysIdx = sort.Search(len(i.rangeKeys), func(idx int) bool {
			return i.rangeKeys[idx].Timestamp.LessEq(key.Timestamp)
		}) - 1
	}

	i.updateAtPoint()

	// It's possible that we seeked past all of the range keys. In this case, we
	// have to update with the position of the underlying iter.
	if !i.atPoint && i.rangeKeysIdx < 0 {
		i.updatePosition()
	}
}

// Prev implements MVCCIterator.
func (i *pointSynthesizingIter) Prev() {
	// When changing direction, flip the relative positioning with iter.
	if !i.reverse {
		i.reverse = true
		if len(i.curKey) == 0 { // iterator was exhausted
			i.iter.Prev()
			i.updatePosition()
			return
		} else if i.atPoint {
			i.rangeKeysIdx--
		} else {
			i.iter.Prev()
		}
	}

	// Step off the current point key, real or synthetic.
	if i.atPoint {
		i.iter.Prev()
	} else {
		i.rangeKeysIdx--
	}
	i.updateAtPoint()

	// If we've exhausted the current range keys, update with the underlying iter
	// position (which will be different from curKey).
	if !i.atPoint && i.rangeKeysIdx < 0 {
		i.updatePosition()
	}
}

// Valid implements MVCCIterator.
func (i *pointSynthesizingIter) Valid() (bool, error) {
	if util.RaceEnabled {
		if err := i.assertInvariants(); err != nil {
			panic(err)
		}
	}
	if len(i.curKey) > 0 && !i.atPoint && i.rangeKeysIdx >= 0 && i.rangeKeysIdx < len(i.rangeKeys) {
		return true, nil // on synthetic point tombstone
	}
	return i.iter.Valid()
}

// Key implements MVCCIterator.
func (i *pointSynthesizingIter) Key() MVCCKey {
	return i.UnsafeKey().Clone()
}

// UnsafeKey implements MVCCIterator.
func (i *pointSynthesizingIter) UnsafeKey() MVCCKey {
	if i.atPoint {
		return i.iter.UnsafeKey()
	}
	if len(i.curKey) == 0 || i.rangeKeysIdx >= len(i.rangeKeys) || i.rangeKeysIdx < 0 {
		return MVCCKey{}
	}
	return MVCCKey{
		Key:       i.curKey,
		Timestamp: i.rangeKeys[i.rangeKeysIdx].Timestamp,
	}
}

// UnsafeRawKey implements MVCCIterator.
func (i *pointSynthesizingIter) UnsafeRawKey() []byte {
	if i.atPoint {
		return i.iter.UnsafeRawKey()
	}
	return EncodeMVCCKeyPrefix(i.curKey)
}

// UnsafeRawMVCCKey implements MVCCIterator.
func (i *pointSynthesizingIter) UnsafeRawMVCCKey() []byte {
	if i.atPoint {
		return i.iter.UnsafeRawMVCCKey()
	}
	return EncodeMVCCKey(i.UnsafeKey())
}

// Value implements MVCCIterator.
func (i *pointSynthesizingIter) Value() []byte {
	if v := i.UnsafeValue(); v != nil {
		return append([]byte(nil), v...)
	}
	return nil
}

// UnsafeValue implements MVCCIterator.
func (i *pointSynthesizingIter) UnsafeValue() []byte {
	if i.atPoint {
		return i.iter.UnsafeValue()
	}
	return nil
}

// ValueProto implements MVCCIterator.
func (i *pointSynthesizingIter) ValueProto(msg protoutil.Message) error {
	if i.atPoint {
		return i.iter.ValueProto(msg)
	}
	// Tombstones have no value, but ValueProto() also resets the message.
	msg.Reset()
	return nil
}

// HasPointAndRange implements MVCCIterator.
func (i *pointSynthesizingIter) HasPointAndRange() (bool, bool) {
	ok, err := i.Valid()
	return ok && err == nil, false
}

// RangeBounds implements MVCCIterator.
func (i *pointSynthesizingIter) RangeBounds() (roachpb.Key, roachpb.Key) {
	return nil, nil
}

// RangeKeys implements MVCCIterator.
func (i *pointSynthesizingIter) RangeKeys() []MVCCRangeKey {
	return nil
}

// Close implements MVCCIterator.
func (i *pointSynthesizingIter) Close() {
	i.iter.Close()
}

// ComputeStats implements MVCCIterator.
func (i *pointSynthesizingIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return i.iter.ComputeStats(start, end, nowNanos)
}

// FindSplitKey implements MVCCIterator.
func (i *pointSynthesizingIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	return i.iter.FindSplitKey(start, end, minSplitKey, targetSize)
}

// Stats implements MVCCIterator.
func (i *pointSynthesizingIter) Stats() IteratorStats {
	return i.iter.Stats()
}

// SupportsPrev implements MVCCIterator.
func (i *pointSynthesizingIter) SupportsPrev() bool {
	return i.iter.SupportsPrev()
}

// assertInvariants asserts iterator invariants.
func (i *pointSynthesizingIter) assertInvariants() error {
	// rangeKeysIdx is never more than 1 outside of the slice bounds, and the
	// excess depends on the direction: len(rangeKeys) in the forward direction,
	// -1 in the reverse.
	if i.rangeKeysIdx < 0 || i.rangeKeysIdx >= len(i.rangeKeys) {
		if (!i.reverse && i.rangeKeysIdx != len(i.rangeKeys)) || (i.reverse && i.rangeKeysIdx != -1) {
			return errors.AssertionFailedf("invalid rangeKeysIdx %d with length %d and reverse=%t",
				i.rangeKeysIdx, len(i.rangeKeys), i.reverse)
		}
	}

	// If curKey is empty, so is other state. We've checked all we need then.
	if len(i.curKey) == 0 {
		if i.atPoint {
			return errors.AssertionFailedf("atPoint with no curKey")
		}
		if len(i.rangeKeys) > 0 {
			return errors.AssertionFailedf("rangeKeys %v with no curKey", i.rangeKeys)
		}
		if len(i.rangeKeysStart) > 0 {
			return errors.AssertionFailedf("rangeKeysStart %s with no curKey", i.rangeKeysStart)
		}
		return nil
	}

	// Positioning invariants.
	var iterKey MVCCKey
	if ok, err := i.iter.Valid(); ok && err == nil {
		iterKey = i.iter.UnsafeKey()
	}

	if i.atPoint {
		// When we're at a real point key, it matches curKey, and is between the
		// pending and previous range keys.
		if ok, err := i.iter.Valid(); !ok || err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "atPoint with invalid iter: ok=%t", ok)
		}
		if !iterKey.Key.Equal(i.curKey) {
			return errors.AssertionFailedf("atPoint with curKey mismatch (%s != %s)",
				iterKey.Key, i.curKey)
		}

		if iterKey.Timestamp.IsEmpty() {
			// If the iterator is on an intent, rangeKeysIdx must be either 0 (forward)
			// or -1 (reverse) regardless of their values.
			expectIdx := 0
			if i.reverse {
				expectIdx = -1
			}
			if i.rangeKeysIdx != expectIdx {
				return errors.AssertionFailedf("expected rangeKeysIdx %d for intent %s reverse=%t, got %d",
					expectIdx, iterKey, i.rangeKeysIdx, i.reverse)
			}
		} else {
			// If the iterator is on a regular value, its timestamp must be between
			// the previous and pending range keys, if they exist.
			var lesserIdx, greaterIdx int
			if !i.reverse {
				lesserIdx, greaterIdx = i.rangeKeysIdx, i.rangeKeysIdx-1
			} else {
				lesserIdx, greaterIdx = i.rangeKeysIdx+1, i.rangeKeysIdx
			}
			if lesserIdx >= 0 && lesserIdx < len(i.rangeKeys) {
				if ts := i.rangeKeys[lesserIdx].Timestamp; ts.Compare(iterKey.Timestamp) >= 0 {
					return errors.AssertionFailedf("expected range key %s < %s", ts, iterKey)
				}
			}
			if greaterIdx >= 0 && greaterIdx < len(i.rangeKeys) {
				if ts := i.rangeKeys[greaterIdx].Timestamp; ts.Compare(iterKey.Timestamp) <= 0 {
					return errors.AssertionFailedf("expected range key %s > %s", ts, iterKey)
				}
			}
		}

	} else if len(i.rangeKeys) == 0 {
		// If we're not on a real point key, we must be on a synthetic position.
		return errors.AssertionFailedf("empty rangeKeys and not atPoint for position %s", i.curKey)

	} else if len(iterKey.Key) > 0 {
		// When we're on a synthetic point tombstone, the iterator is positioned
		// correctly relative to it.
		curKey := MVCCKey{Key: i.curKey, Timestamp: i.rangeKeys[i.rangeKeysIdx].Timestamp}
		if !i.reverse && iterKey.Compare(curKey) <= 0 {
			return errors.AssertionFailedf("invalid iterator position %s for forward position %s",
				iterKey, curKey)
		} else if i.reverse && iterKey.Compare(curKey) >= 0 {
			return errors.AssertionFailedf("invalid iterator position %s for reverse position %s",
				iterKey, curKey)
		}
	}

	return nil
}
