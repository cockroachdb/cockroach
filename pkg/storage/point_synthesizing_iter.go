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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// pointSynthesizingIterPool reuses pointSynthesizingIters to avoid allocations.
var pointSynthesizingIterPool = sync.Pool{
	New: func() interface{} {
		return &PointSynthesizingIter{}
	},
}

// PointSynthesizingIter wraps an MVCCIterator, and synthesizes MVCC point keys
// for MVCC range keys above existing point keys (not below), and at the start
// of range keys (truncated to iterator bounds). It does not emit MVCC range
// keys at all, since these would appear to conflict with the synthesized point
// keys.
//
// During iteration, any range keys overlapping the current iterator position
// are kept in rangeKeys. When atPoint is true, the iterator is positioned on a
// real point key in the underlying iterator. Otherwise, it is positioned on a
// synthetic point key given by rangeKeysPos and rangeKeys[rangeKeysIdx].
// rangeKeysEnd specifies where to end point synthesis at the current position,
// i.e. the first range key below an existing point key.
//
// The relative positioning of PointSynthesizingIter and the underlying iterator
// is as follows in the forward direction:
//
//   - atPoint=true: rangeKeysIdx points to a range key following the point key,
//     or beyond rangeKeysEnd when there are no further range keys at this
//     key position.
//
//   - atPoint=false: the underlying iterator is on a following key or exhausted.
//     This can either be a different version of the current key or a different
//     point/range key.
//
// This positioning is mirrored in the reverse direction. For example, when
// atPoint=true and rangeKeys are exhausted, rangeKeysIdx will be rangeKeysEnd
// in the forward direction and -1 in the reverse direction. Similarly, the
// underlying iterator is always >= rangeKeysPos in the forward direction and <=
// in reverse.
//
// See also assertInvariants() which asserts positioning invariants.
type PointSynthesizingIter struct {
	// iter is the underlying MVCC iterator.
	iter MVCCIterator

	// prefix indicates that the underlying iterator is a prefix iterator, which
	// can only be on a single key. This allows omitting cloning and comparison.
	prefix bool

	// reverse is true when the current iterator direction is in reverse, i.e.
	// following a SeekLT or Prev call.
	reverse bool

	// rangeKeys contains any range key versions that overlap the current key
	// position, for which points will be synthesized.
	rangeKeys MVCCRangeKeyVersions

	// rangeKeysBuf is a reusable buffer for rangeKeys. Non-prefix iterators use
	// it as a CloneInto() target and place a reference in rangeKeys.
	rangeKeysBuf MVCCRangeKeyVersions

	// rangeKeysPos is the current key position along the rangeKeys span, where
	// points will be synthesized. It is only set if rangeKeys is non-empty, and
	// may differ from the underlying iterator position.
	rangeKeysPos roachpb.Key

	// rangeKeysIdx is the rangeKeys index of the current/pending range key
	// to synthesize a point for. See struct comment for details.
	rangeKeysIdx int

	// rangeKeysStart contains the start key of the current rangeKeys stack. It is
	// stored separately, rather than holding the entire MVCCRangeKeyStack, to
	// avoid cloning the EndKey.
	rangeKeysStart roachpb.Key

	// rangeKeysStartPassed is true if the iterator has moved past the start bound
	// of the current range key. This allows omitting a key comparison every time
	// the iterator moves to a new key, but only in the forward direction.
	rangeKeysStartPassed bool

	// rangeKeysEnd contains the exclusive index at which to stop synthesizing
	// point keys, since points are not synthesized below existing point keys.
	rangeKeysEnd int

	// atPoint is true if the synthesizing iterator is positioned on a real point
	// key in the underlying iterator. See struct comment for details.
	atPoint bool

	// atRangeKeysPos is true if the underlying iterator is at rangeKeysPos.
	atRangeKeysPos bool

	// The following fields memoize the state of the underlying iterator, and are
	// updated every time its position changes.
	iterValid    bool
	iterErr      error
	iterKey      MVCCKey
	iterHasPoint bool
	iterHasRange bool

	// rangeKeyChanged keeps track of changes to the underlying iter's range keys.
	// It is reset to false on every call to updateRangeKeys(), and accumulates
	// changes during intermediate positioning operations.
	rangeKeyChanged bool
}

var _ MVCCIterator = (*PointSynthesizingIter)(nil)

// NewPointSynthesizingIter creates a new pointSynthesizingIter, or gets one
// from the pool.
func NewPointSynthesizingIter(parent MVCCIterator) *PointSynthesizingIter {
	iter := pointSynthesizingIterPool.Get().(*PointSynthesizingIter)
	*iter = PointSynthesizingIter{
		iter:   parent,
		prefix: parent.IsPrefix(),
		// Reuse pooled byte slices.
		rangeKeysBuf:   iter.rangeKeysBuf,
		rangeKeysPos:   iter.rangeKeysPos,
		rangeKeysStart: iter.rangeKeysStart,
	}
	return iter
}

// NewPointSynthesizingIterAtParent creates a new pointSynthesizingIter and
// loads the position from the parent iterator.
func NewPointSynthesizingIterAtParent(parent MVCCIterator) *PointSynthesizingIter {
	iter := NewPointSynthesizingIter(parent)
	iter.rangeKeyChanged = true // force range key detection
	if ok, err := iter.updateIter(); ok && err == nil {
		iter.updateSeekGEPosition(parent.UnsafeKey())
	}
	return iter
}

// Close implements MVCCIterator.
//
// Close will also close the underlying iterator. Use release() to release it
// back to the pool without closing the parent iterator.
func (i *PointSynthesizingIter) Close() {
	i.iter.Close()
	i.release()
}

// release releases the iterator back into the pool.
func (i *PointSynthesizingIter) release() {
	*i = PointSynthesizingIter{
		// Reuse slices.
		rangeKeysBuf:   i.rangeKeysBuf[:0],
		rangeKeysPos:   i.rangeKeysPos[:0],
		rangeKeysStart: i.rangeKeysStart[:0],
	}
	pointSynthesizingIterPool.Put(i)
}

// iterNext is a convenience function that calls iter.Next()
// and returns the value of updateIter().
func (i *PointSynthesizingIter) iterNext() (bool, error) {
	i.iter.Next()
	return i.updateIter()
}

// iterPrev is a convenience function that calls iter.Prev()
// and returns the value of updateIter().
func (i *PointSynthesizingIter) iterPrev() (bool, error) {
	i.iter.Prev()
	return i.updateIter()
}

// updateIter memoizes the iterator fields from the underlying iterator, and
// also keeps track of rangeKeyChanged. It must be called after every iterator
// positioning operation, and returns the iterator validity/error.
func (i *PointSynthesizingIter) updateIter() (bool, error) {
	if i.iterValid, i.iterErr = i.iter.Valid(); i.iterValid {
		i.iterHasPoint, i.iterHasRange = i.iter.HasPointAndRange()
		i.iterKey = i.iter.UnsafeKey()
		i.atRangeKeysPos = (i.prefix && i.iterHasRange) ||
			(!i.prefix && i.iterKey.Key.Equal(i.rangeKeysPos))
	} else {
		i.iterHasPoint, i.iterHasRange = false, false
		i.iterKey = MVCCKey{}
		i.atRangeKeysPos = false
	}
	i.rangeKeyChanged = i.rangeKeyChanged || i.iter.RangeKeyChanged()
	return i.iterValid, i.iterErr
}

// updateRangeKeys updates i.rangeKeys and related fields with the underlying
// iterator state. It must be called every time the pointSynthesizingIter moves
// to a new key position, i.e. after exhausting all point/range keys at the
// current position. rangeKeysIdx and rangeKeysEnd are reset.
func (i *PointSynthesizingIter) updateRangeKeys() {
	if !i.iterHasRange {
		i.clearRangeKeys()
		return
	}

	// Detect and fetch new range keys in the underlying iterator since the
	// last call to updateRangeKeys().
	if i.rangeKeyChanged {
		i.rangeKeyChanged = false
		if i.prefix {
			// A prefix iterator will always be at the start bound of the range key,
			// and never move onto a different range key, so we can omit the cloning.
			i.rangeKeys = i.iter.RangeKeys().Versions
		} else {
			rangeKeys := i.iter.RangeKeys()
			i.rangeKeysStart = append(i.rangeKeysStart[:0], rangeKeys.Bounds.Key...)
			rangeKeys.Versions.CloneInto(&i.rangeKeysBuf)
			i.rangeKeys = i.rangeKeysBuf
			i.rangeKeysStartPassed = false // we'll compare in updateRangeKeysEnd
		}
	}

	// Update the current iterator position.
	i.rangeKeysPos = append(i.rangeKeysPos[:0], i.iterKey.Key...)
	i.atRangeKeysPos = true // by definition

	// Update the rangeKeysEnd with the range keys to synthesize points for at
	// this position. Notably, we synthesize for all range keys at their start
	// bound, but otherwise only the ones above existing point keys.
	//
	// In the forward direction, once we step off rangeKeysStart we won't see a
	// start bound again until we hit a new range key, so we can omit a key
	// comparison for all point keys in between. This isn't true in reverse,
	// unfortunately, where we only encounter the start key at the end.
	i.rangeKeysEnd = len(i.rangeKeys)
}

// updateRangeKeysEnd updates i.rangeKeysEnd for the current iterator position.
// iter must be positioned on a colocated point key (if any) first.
func (i *PointSynthesizingIter) updateRangeKeysEnd() {
	if i.rangeKeysStartPassed && !i.reverse {
		i.rangeKeysEnd = 0
		i.extendRangeKeysEnd()
	} else if i.prefix || ((!i.iterHasPoint || !i.atRangeKeysPos) && i.rangeKeysPos.Equal(i.rangeKeysStart)) {
		i.rangeKeysEnd = len(i.rangeKeys)
		i.rangeKeysStartPassed = false
	} else {
		i.rangeKeysEnd = 0
		i.extendRangeKeysEnd()
		i.rangeKeysStartPassed = !i.reverse && !i.rangeKeysPos.Equal(i.rangeKeysStart)
	}
	if i.reverse {
		i.rangeKeysIdx = i.rangeKeysEnd - 1
	} else {
		i.rangeKeysIdx = 0
	}
}

// extendRangeKeysEnd extends i.rangeKeysEnd below the current point key's
// timestamp in the underlying iterator. It never reduces i.rangeKeysEnd.
func (i *PointSynthesizingIter) extendRangeKeysEnd() {
	if i.iterHasPoint && i.atRangeKeysPos && !i.iterKey.Timestamp.IsEmpty() {
		if l := len(i.rangeKeys); i.rangeKeysEnd < l {
			i.rangeKeysEnd = sort.Search(l-i.rangeKeysEnd, func(idx int) bool {
				return i.rangeKeys[i.rangeKeysEnd+idx].Timestamp.Less(i.iterKey.Timestamp)
			}) + i.rangeKeysEnd
		}
	}
}

// updateAtPoint updates i.atPoint according to whether the synthesizing
// iterator is positioned on the real point key in the underlying iterator.
// Requires i.rangeKeys to have been positioned first.
func (i *PointSynthesizingIter) updateAtPoint() {
	if !i.iterHasPoint {
		i.atPoint = false
	} else if len(i.rangeKeys) == 0 {
		i.atPoint = true
	} else if !i.atRangeKeysPos {
		i.atPoint = false
	} else if !i.reverse {
		i.atPoint = i.rangeKeysIdx >= i.rangeKeysEnd || !i.iterKey.Timestamp.IsSet() ||
			i.rangeKeys[i.rangeKeysIdx].Timestamp.LessEq(i.iterKey.Timestamp)
	} else {
		i.atPoint = i.rangeKeysIdx < 0 || (i.iterKey.Timestamp.IsSet() &&
			i.iterKey.Timestamp.LessEq(i.rangeKeys[i.rangeKeysIdx].Timestamp))
	}
}

// updatePosition updates the synthesizing iterator for the position of the
// underlying iterator. This may step the underlying iterator to position it
// correctly relative to bare range keys.
func (i *PointSynthesizingIter) updatePosition() {
	if !i.iterHasRange {
		// Fast path: no range keys, so just clear range keys and bail out.
		i.atPoint = i.iterHasPoint
		i.clearRangeKeys()

	} else if !i.reverse {
		// If we're on a bare range key in the forward direction, we populate the
		// range keys but then step iter ahead before updating the point position.
		// The next position may be a point key with the same key as the current
		// range key, which must be interleaved with the synthetic points.
		i.updateRangeKeys()
		if i.iterHasRange && !i.iterHasPoint {
			if _, err := i.iterNext(); err != nil {
				return
			}
		}
		i.updateRangeKeysEnd()
		i.updateAtPoint()

	} else {
		// If we're on a bare range key in the reverse direction, and we've already
		// emitted synthetic points for this key (given by atRangeKeysPos), then we
		// skip over the bare range key to avoid duplicates.
		if i.iterHasRange && !i.iterHasPoint && i.atRangeKeysPos {
			if _, err := i.iterPrev(); err != nil {
				return
			}
		}
		i.updateRangeKeys()
		i.updateRangeKeysEnd()
		i.updateAtPoint()
	}
}

// clearRangeKeys resets the iterator by clearing out all range key state.
// gcassert:inline
func (i *PointSynthesizingIter) clearRangeKeys() {
	if len(i.rangeKeys) != 0 {
		i.rangeKeys = i.rangeKeys[:0]
		i.rangeKeysPos = i.rangeKeysPos[:0]
		i.rangeKeysStart = i.rangeKeysStart[:0]
		i.rangeKeysEnd = 0
		i.rangeKeysStartPassed = false
	}
	if !i.reverse {
		i.rangeKeysIdx = 0
	} else {
		i.rangeKeysIdx = -1
	}
}

// SeekGE implements MVCCIterator.
func (i *PointSynthesizingIter) SeekGE(seekKey MVCCKey) {
	i.reverse = false
	i.iter.SeekGE(seekKey)
	if ok, _ := i.updateIter(); !ok {
		i.updatePosition()
		return
	}
	i.updateSeekGEPosition(seekKey)
}

// SeekIntentGE implements MVCCIterator.
func (i *PointSynthesizingIter) SeekIntentGE(seekKey roachpb.Key, txnUUID uuid.UUID) {
	i.reverse = false
	i.iter.SeekIntentGE(seekKey, txnUUID)
	if ok, _ := i.updateIter(); !ok {
		i.updatePosition()
		return
	}
	i.updateSeekGEPosition(MVCCKey{Key: seekKey})
}

// updateSeekGEPosition updates the iterator state following a SeekGE call, or
// to load the parent iterator's position in newPointSynthesizingIterAtParent.
func (i *PointSynthesizingIter) updateSeekGEPosition(seekKey MVCCKey) {

	// Fast path: no range key, so just reset the iterator and bail out.
	if !i.iterHasRange {
		i.atPoint = i.iterHasPoint
		i.clearRangeKeys()
		return
	}

	// If we land in the middle of a bare range key then skip over it to the next
	// point/range key. If prefix is enabled, we must be at its start key, so we
	// can omit the comparison.
	if i.iterHasRange && !i.iterHasPoint && !i.prefix &&
		!i.iter.RangeBounds().Key.Equal(i.iterKey.Key) {
		if ok, _ := i.iterNext(); !ok {
			i.updatePosition()
			return
		}
	}

	i.updateRangeKeys()

	// If we're still at a bare range key, we must be at its start key. Move the
	// iterator ahead to look for a point key at the same key.
	var positioned bool
	if i.iterHasRange && !i.iterHasPoint {
		if !i.prefix && i.iterKey.Timestamp.IsSet() {
			if ok, err := i.iterPrev(); err != nil {
				return
			} else if ok && i.iterHasPoint && i.iterKey.Key.Equal(seekKey.Key) {
				i.updateRangeKeysEnd()
				positioned = true
			}
		}
		if _, err := i.iterNext(); err != nil {
			return
		}
	}

	if !positioned {
		i.updateRangeKeysEnd()
	} else {
		i.extendRangeKeysEnd()
	}

	// If we're seeking to a specific version, skip newer range keys.
	if len(i.rangeKeys) > 0 && seekKey.Timestamp.IsSet() &&
		(i.prefix || seekKey.Key.Equal(i.rangeKeysPos)) {
		i.rangeKeysIdx = sort.Search(i.rangeKeysEnd, func(idx int) bool {
			return i.rangeKeys[idx].Timestamp.LessEq(seekKey.Timestamp)
		})
	}

	i.updateAtPoint()

	// It's possible that we seeked past all of the range key versions. In this
	// case, we have to reposition on the next key (current iter key).
	if !i.atPoint && i.rangeKeysIdx >= i.rangeKeysEnd {
		i.updatePosition()
	}
}

// Next implements MVCCIterator.
func (i *PointSynthesizingIter) Next() {
	// When changing direction, flip the relative positioning with iter.
	if i.reverse {
		i.reverse = false
		if !i.atPoint && len(i.rangeKeys) == 0 { // iterator was exhausted
			if _, err := i.iterNext(); err != nil {
				return
			}
			i.updatePosition()
			return
		} else if i.atPoint {
			i.rangeKeysIdx++
		} else if _, err := i.iterNext(); err != nil {
			return
		}
	}

	// Step off the current point, either real or synthetic.
	if i.atPoint {
		if _, err := i.iterNext(); err != nil {
			return
		}
		i.extendRangeKeysEnd()
	} else {
		i.rangeKeysIdx++
	}
	i.updateAtPoint()

	// If we've exhausted the current range keys, update with the underlying
	// iterator position (which must now be at a later key).
	if !i.atPoint && i.rangeKeysIdx >= i.rangeKeysEnd {
		i.updatePosition()
	}
}

// NextKey implements MVCCIterator.
func (i *PointSynthesizingIter) NextKey() {
	// When changing direction, flip the relative positioning with iter.
	//
	// NB: This isn't really supported by the MVCCIterator interface, but we have
	// best-effort handling in e.g. `pebbleIterator` and it's simple enough to
	// implement, so we may as well.
	if i.reverse {
		i.reverse = false
		if !i.atPoint {
			if _, err := i.iterNext(); err != nil {
				return
			}
		}
	}
	// Don't call NextKey() if the underlying iterator is already on the next key.
	if i.atPoint || i.atRangeKeysPos {
		i.iter.NextKey()
		if _, err := i.updateIter(); err != nil {
			return
		}
	}
	i.updatePosition()
}

// SeekLT implements MVCCIterator.
func (i *PointSynthesizingIter) SeekLT(seekKey MVCCKey) {
	i.reverse = true
	i.iter.SeekLT(seekKey)
	if ok, _ := i.updateIter(); !ok {
		i.updatePosition()
		return
	}

	// Fast path: no range key, so just reset the iterator and bail out.
	if !i.iterHasRange {
		i.atPoint = i.iterHasPoint
		i.clearRangeKeys()
		return
	}

	// If we did a versioned seek and find a range key that overlaps the seek key,
	// we may have skipped over existing point key versions of the seek key. These
	// would mandate that we synthesize point keys for the seek key after all, so
	// we peek ahead to check for them.
	//
	// TODO(erikgrinaker): It might be faster to do an unversioned seek from the
	// next key first to look for points.
	var positioned bool
	if seekKey.Timestamp.IsSet() && i.iterHasRange &&
		seekKey.Key.Compare(i.iter.RangeBounds().EndKey) < 0 {
		if ok, err := i.iterNext(); err != nil {
			return
		} else if ok && i.iterHasPoint && i.iterKey.Key.Equal(seekKey.Key) {
			i.updateRangeKeys()
			i.updateRangeKeysEnd() // must account for following point
			positioned = true
		}
		if ok, _ := i.iterPrev(); !ok {
			i.updatePosition()
			return
		}
	}

	if !positioned {
		i.updateRangeKeys()
		i.updateRangeKeysEnd()
	}

	// If we're seeking to a specific version, skip over older range keys.
	if seekKey.Timestamp.IsSet() && seekKey.Key.Equal(i.rangeKeysPos) {
		i.rangeKeysIdx = sort.Search(i.rangeKeysEnd, func(idx int) bool {
			return i.rangeKeys[idx].Timestamp.LessEq(seekKey.Timestamp)
		}) - 1
	}

	i.updateAtPoint()

	// It's possible that we seeked past all of the range key versions. In this
	// case, we have to reposition on the previous key (current iter key).
	if !i.atPoint && i.rangeKeysIdx < 0 {
		i.updatePosition()
	}
}

// Prev implements MVCCIterator.
func (i *PointSynthesizingIter) Prev() {
	// When changing direction, flip the relative positioning with iter.
	if !i.reverse {
		i.reverse = true
		if !i.atPoint && len(i.rangeKeys) == 0 { // iterator was exhausted
			if _, err := i.iterPrev(); err != nil {
				return
			}
			i.updatePosition()
			return
		} else if i.atPoint {
			i.rangeKeysIdx--
		} else if _, err := i.iterPrev(); err != nil {
			return
		}
	}

	// Step off the current point key (real or synthetic).
	if i.atPoint {
		if _, err := i.iterPrev(); err != nil {
			return
		}
	} else {
		i.rangeKeysIdx--
	}
	i.updateAtPoint()

	// If we've exhausted the current range keys, and we're not positioned on a
	// point key at the current range key position, then update with the
	// underlying iter position (which must be before the current key).
	if i.rangeKeysIdx < 0 && (!i.atPoint || !i.atRangeKeysPos) {
		i.updatePosition()
	}
}

// Valid implements MVCCIterator.
func (i *PointSynthesizingIter) Valid() (bool, error) {
	valid := i.iterValid ||
		// On synthetic point key.
		(i.iterErr == nil && !i.atPoint && i.rangeKeysIdx >= 0 && i.rangeKeysIdx < i.rangeKeysEnd)

	if util.RaceEnabled && valid {
		if err := i.assertInvariants(); err != nil {
			panic(err)
		}
	}

	return valid, i.iterErr
}

// Key implements MVCCIterator.
func (i *PointSynthesizingIter) Key() MVCCKey {
	return i.UnsafeKey().Clone()
}

// UnsafeKey implements MVCCIterator.
func (i *PointSynthesizingIter) UnsafeKey() MVCCKey {
	if i.atPoint {
		return i.iterKey
	}
	if i.rangeKeysIdx >= i.rangeKeysEnd || i.rangeKeysIdx < 0 {
		return MVCCKey{}
	}
	return MVCCKey{
		Key:       i.rangeKeysPos,
		Timestamp: i.rangeKeys[i.rangeKeysIdx].Timestamp,
	}
}

// UnsafeRawKey implements MVCCIterator.
func (i *PointSynthesizingIter) UnsafeRawKey() []byte {
	if i.atPoint {
		return i.iter.UnsafeRawKey()
	}
	return EncodeMVCCKey(i.UnsafeKey())
}

// UnsafeRawMVCCKey implements MVCCIterator.
func (i *PointSynthesizingIter) UnsafeRawMVCCKey() []byte {
	if i.atPoint {
		return i.iter.UnsafeRawMVCCKey()
	}
	return EncodeMVCCKey(i.UnsafeKey())
}

// Value implements MVCCIterator.
func (i *PointSynthesizingIter) Value() []byte {
	if v := i.UnsafeValue(); v != nil {
		return append([]byte{}, v...)
	}
	return nil
}

// UnsafeValue implements MVCCIterator.
func (i *PointSynthesizingIter) UnsafeValue() []byte {
	if i.atPoint {
		return i.iter.UnsafeValue()
	}
	if i.rangeKeysIdx >= len(i.rangeKeys) || i.rangeKeysIdx < 0 {
		return nil
	}
	return i.rangeKeys[i.rangeKeysIdx].Value
}

// MVCCValueLenAndIsTombstone implements the MVCCIterator interface.
func (i *PointSynthesizingIter) MVCCValueLenAndIsTombstone() (int, bool, error) {
	if i.atPoint {
		return i.iter.MVCCValueLenAndIsTombstone()
	}
	if i.rangeKeysIdx >= len(i.rangeKeys) || i.rangeKeysIdx < 0 {
		return 0, false, errors.Errorf("iter is not Valid")
	}
	val := i.rangeKeys[i.rangeKeysIdx].Value
	// All range keys are tombstones
	return len(val), true, nil
}

// ValueLen implements the MVCCIterator interface.
func (i *PointSynthesizingIter) ValueLen() int {
	if i.atPoint {
		return i.iter.ValueLen()
	}
	if i.rangeKeysIdx >= len(i.rangeKeys) || i.rangeKeysIdx < 0 {
		// Caller has violated invariant!
		return 0
	}
	return len(i.rangeKeys[i.rangeKeysIdx].Value)
}

// ValueProto implements MVCCIterator.
func (i *PointSynthesizingIter) ValueProto(msg protoutil.Message) error {
	return protoutil.Unmarshal(i.UnsafeValue(), msg)
}

// HasPointAndRange implements MVCCIterator.
func (i *PointSynthesizingIter) HasPointAndRange() (bool, bool) {
	return true, false
}

// RangeBounds implements MVCCIterator.
func (i *PointSynthesizingIter) RangeBounds() roachpb.Span {
	return roachpb.Span{}
}

// RangeKeys implements MVCCIterator.
func (i *PointSynthesizingIter) RangeKeys() MVCCRangeKeyStack {
	return MVCCRangeKeyStack{}
}

// RangeKeyChanged implements MVCCIterator.
func (i *PointSynthesizingIter) RangeKeyChanged() bool {
	return false
}

// FindSplitKey implements MVCCIterator.
func (i *PointSynthesizingIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	return i.iter.FindSplitKey(start, end, minSplitKey, targetSize)
}

// Stats implements MVCCIterator.
func (i *PointSynthesizingIter) Stats() IteratorStats {
	return i.iter.Stats()
}

// IsPrefix implements the MVCCIterator interface.
func (i *PointSynthesizingIter) IsPrefix() bool {
	return i.prefix
}

// assertInvariants asserts iterator invariants. The iterator must be valid.
func (i *PointSynthesizingIter) assertInvariants() error {
	// Check general MVCCIterator API invariants.
	if err := assertMVCCIteratorInvariants(i); err != nil {
		return err
	}

	// If the underlying iterator has errored, make sure we're not positioned on a
	// synthetic point such that Valid() will surface the error.
	if _, err := i.iter.Valid(); err != nil {
		if !i.atPoint && i.rangeKeysIdx >= 0 && i.rangeKeysIdx < len(i.rangeKeys) {
			return errors.NewAssertionErrorWithWrappedErrf(err, "iterator error with synthetic point %s",
				i.rangeKeysPos)
		}
		return nil
	}

	// Make sure the state of the underlying iterator matches the memoized state.
	if ok, err := i.iter.Valid(); ok {
		if !i.iterValid {
			return errors.AssertionFailedf("iterValid %t != iter.Valid %t", i.iterValid, ok)
		}
		if i.iterErr != nil {
			return errors.NewAssertionErrorWithWrappedErrf(i.iterErr, "valid iter with error")
		}
		if key := i.iter.UnsafeKey(); !i.iterKey.Equal(key) {
			return errors.AssertionFailedf("iterKey %s != iter.UnsafeKey %s", i.iterKey, key)
		}
		if hasP, hasR := i.iter.HasPointAndRange(); hasP != i.iterHasPoint || hasR != i.iterHasRange {
			return errors.AssertionFailedf(
				"iterHasPoint %t, iterHasRange %t != iter.HasPointAndRange %t %t",
				i.iterHasPoint, i.iterHasRange, hasP, hasR)
		}
		if i.atRangeKeysPos && !i.iterKey.Key.Equal(i.rangeKeysPos) {
			return errors.AssertionFailedf("atRangeKeysPos true but iterKey %s != rangeKeysPos %s",
				i.iterKey, i.rangeKeysPos)
		}
	} else {
		if hasErr, iterHasErr := err != nil, i.iterErr != nil; hasErr != iterHasErr {
			return errors.AssertionFailedf("i.iterErr %t != iter.Valid err %t", iterHasErr, hasErr)
		}
		if i.iterValid {
			return errors.AssertionFailedf("invalid iterator with iterValid %t", i.iterValid)
		}
		if i.iterHasPoint || i.iterHasRange {
			return errors.AssertionFailedf("invalid iterator with iterHasPoint %t, iterHasRange %t",
				i.iterHasPoint, i.iterHasRange)
		}
		if i.atRangeKeysPos {
			return errors.AssertionFailedf("invalid iterator with atRangeKeysPos %t", i.atRangeKeysPos)
		}
	}

	// In prefix mode, the iterator must never be used in reverse.
	if i.prefix && i.reverse {
		return errors.AssertionFailedf("prefix iterator used in reverse")
	}

	// When atPoint is true, the underlying iterator must be valid and on a point.
	if i.atPoint {
		if ok, _ := i.iter.Valid(); !ok {
			return errors.AssertionFailedf("atPoint with invalid iter")
		}
		if hasPoint, _ := i.iter.HasPointAndRange(); !hasPoint {
			return errors.AssertionFailedf("atPoint at non-point position %s", i.iter.UnsafeKey())
		}
	}

	// rangeKeysEnd is never negative, and never greater than len(i.rangeKeys).
	if i.rangeKeysEnd < 0 || i.rangeKeysEnd > len(i.rangeKeys) {
		return errors.AssertionFailedf("invalid rangeKeysEnd %d with length %d",
			i.rangeKeysEnd, len(i.rangeKeys))
	}

	// rangeKeysIdx is never more than 1 outside of the permitted slice interval
	// (0 to rangeKeysEnd), and the excess depends on the direction: rangeKeysEnd
	// in the forward direction, -1 in the reverse.
	if i.rangeKeysIdx < 0 || i.rangeKeysIdx >= i.rangeKeysEnd {
		if (!i.reverse && i.rangeKeysIdx != i.rangeKeysEnd) || (i.reverse && i.rangeKeysIdx != -1) {
			return errors.AssertionFailedf("invalid rangeKeysIdx %d with rangeKeysEnd %d and reverse=%t",
				i.rangeKeysIdx, i.rangeKeysEnd, i.reverse)
		}
	}

	// If rangeKeys is empty, atPoint is true unless exhausted and other state is
	// cleared. In this case, there's nothing more to check.
	if len(i.rangeKeys) == 0 {
		if ok, _ := i.iter.Valid(); ok && !i.atPoint {
			return errors.AssertionFailedf("no rangeKeys nor atPoint")
		}
		if len(i.rangeKeysPos) > 0 {
			return errors.AssertionFailedf("no rangeKeys but rangeKeysPos %s", i.rangeKeysPos)
		}
		if len(i.rangeKeysStart) > 0 {
			return errors.AssertionFailedf("no rangeKeys but rangeKeysStart %s", i.rangeKeysStart)
		}
		if i.rangeKeysStartPassed {
			return errors.AssertionFailedf("no rangeKeys but rangeKeysStartPassed")
		}
		return nil
	}

	// rangeKeysPos must be set when range keys are present.
	if len(i.rangeKeysPos) == 0 {
		return errors.AssertionFailedf("rangeKeysPos not set")
	}

	// rangeKeysStart must be set, and rangeKeysPos must be at or after it.
	// prefix iterators do not set rangeKeysStart.
	if !i.prefix {
		if len(i.rangeKeysStart) == 0 {
			return errors.AssertionFailedf("no rangeKeysStart at %s", i.iter.UnsafeKey())
		}
		if i.rangeKeysPos.Compare(i.rangeKeysStart) < 0 {
			return errors.AssertionFailedf("rangeKeysPos %s not after rangeKeysStart %s",
				i.rangeKeysPos, i.rangeKeysStart)
		}
	} else {
		if len(i.rangeKeysStart) != 0 {
			return errors.AssertionFailedf("rangeKeysStart set to %s for prefix iterator", i.rangeKeysStart)
		}
	}

	// rangeKeysStartPassed must match rangeKeysPos and rangeKeysStart. Note that
	// it is not always correctly enabled when switching directions on a point key
	// covered by a range key after rangeKeysStart, since we only update it when
	// stepping onto a different key.
	if i.rangeKeysStartPassed {
		if i.prefix {
			return errors.AssertionFailedf("rangeKeysStartPassed seen for prefix iterator")
		}
		if i.rangeKeysStartPassed && i.rangeKeysPos.Equal(i.rangeKeysStart) {
			return errors.AssertionFailedf("rangeKeysStartPassed %t, but pos %s and start %s ",
				i.rangeKeysStartPassed, i.rangeKeysPos, i.rangeKeysStart)
		}
	}

	// rangeKeysIdx must be valid if we're not on a point.
	if !i.atPoint && (i.rangeKeysIdx < 0 || i.rangeKeysIdx >= i.rangeKeysEnd) {
		return errors.AssertionFailedf("not atPoint with invalid rangeKeysIdx %d at %s",
			i.rangeKeysIdx, i.rangeKeysPos)
	}

	// If the underlying iterator is exhausted, then there's nothing more to
	// check. We must either be on a synthetic point key or exhausted iterator.
	if ok, _ := i.iter.Valid(); !ok {
		return nil
	}

	// We now have range keys and a non-exhausted iterator.
	//
	// prefix iterators must have range key bounds [key, key.Next), and be
	// positioned on key.
	if _, hasRange := i.iter.HasPointAndRange(); i.prefix && hasRange {
		expect := roachpb.Span{Key: i.rangeKeysPos, EndKey: i.rangeKeysPos.Next()}
		if bounds := i.iter.RangeBounds(); !bounds.Equal(expect) {
			return errors.AssertionFailedf("unexpected range bounds %s with prefix, expected %s",
				bounds, expect)

		} else if key := i.iter.UnsafeKey().Key; !key.Equal(bounds.Key) {
			return errors.AssertionFailedf("iter not on prefix position %s, got %s", bounds, key)
		}
	}

	// Check the relative positioning as minimum and maximum iter keys (in MVCC
	// order). We can assume that overlapping range keys and point keys don't have
	// the same timestamp, since this is enforced by MVCC mutations.
	var minKey, maxKey MVCCKey

	// The iterator should never lag behind the range key position.
	if !i.reverse {
		minKey = MVCCKey{Key: i.rangeKeysPos}
	} else {
		maxKey = MVCCKey{Key: i.rangeKeysPos, Timestamp: hlc.MinTimestamp}
	}

	// If we're not at a real point, then the iterator must be ahead of the
	// current synthesized point. If we are on a point, then it must lie between
	// the surrounding range keys (if they exist).
	minIdx, maxIdx := -1, -1
	if !i.atPoint {
		if !i.reverse {
			minIdx = i.rangeKeysIdx
		} else {
			maxIdx = i.rangeKeysIdx
		}
	} else if !i.reverse {
		minIdx = i.rangeKeysIdx - 1
		maxIdx = i.rangeKeysIdx
	} else {
		minIdx = i.rangeKeysIdx
		maxIdx = i.rangeKeysIdx + 1
	}
	if minIdx >= 0 && minIdx < i.rangeKeysEnd {
		minKey = MVCCKey{Key: i.rangeKeysPos, Timestamp: i.rangeKeys[minIdx].Timestamp}
	}
	if maxIdx >= 0 && maxIdx < i.rangeKeysEnd {
		maxKey = MVCCKey{Key: i.rangeKeysPos, Timestamp: i.rangeKeys[maxIdx].Timestamp}
	}

	iterKey := i.iter.Key()
	if minKey.Key != nil && iterKey.Compare(minKey) < 0 {
		return errors.AssertionFailedf("iter %s below minimum key %s", iterKey, minKey)
	}
	if maxKey.Key != nil && iterKey.Compare(maxKey) > 0 {
		return errors.AssertionFailedf("iter %s above maximum key %s", iterKey, maxKey)
	}

	return nil
}
