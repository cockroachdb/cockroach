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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// pointSynthesizingIter wraps an MVCCIterator, and synthesizes point tombstones
// for range tombstones above/below existing point keys. It does not emit range
// keys at all, since these would appear to conflict with the synthesized point
// keys.
type pointSynthesizingIter struct {
	iter           MVCCIterator
	rangeKeys      []MVCCRangeKey
	rangeKeysIdx   int
	rangeKeysStart roachpb.Key
	// emitOnSeek will cause a SeekGE() call to emit synthetic points for the seek
	// key even if it has no existing point keys.
	emitOnSeek bool
	// curKey is the current key position of the synthesizing iterator. This may
	// be out of sync with the point iterator (when there are no further real
	// point versions for the current key).
	curKey roachpb.Key
	// atPoint is true if the synthesizing iterator is positioned on the real
	// point key. In that case, rangeKeysIdx points to the next range key below
	// the point key, or past the end of rangeKeys if there are none.
	//
	// If atPoint is false, then the point iterator will be positioned on the next
	// point key after the current range tombstone, which can either be an older
	// version of the current key or a different key (or invalid if exhausted).
	atPoint bool
}

var _ MVCCIterator = new(pointSynthesizingIter)

func newPointSynthesizingIter(iter MVCCIterator, emitOnSeek bool) *pointSynthesizingIter {
	return &pointSynthesizingIter{
		iter:       iter,
		emitOnSeek: emitOnSeek,
	}
}

// updateWithFirstPointKey scans to the first point key and updates the iterator
// for its position.
func (i *pointSynthesizingIter) updateWithFirstPointKey() {
	for {
		if ok, err := i.iter.Valid(); !ok || err != nil {
			break
		}
		if hasPoint, _ := i.iter.HasPointAndRange(); hasPoint {
			break
		}
		i.iter.Next()
	}
	i.updatePosition()
}

// maybeUpdateWithBareRangeKey updates the iterator with synthetic point keys
// for the current range key if it is a bare range key and there are no point
// keys overlapping the range key for the current key. It will move ahead to
// check that and leave the iterator at the next key position regardless.
func (i *pointSynthesizingIter) maybeUpdateWithBareRangeKey() bool {
	if ok, err := i.iter.Valid(); !ok || err != nil {
		return false
	}
	hasPoint, hasRange := i.iter.HasPointAndRange()
	if hasPoint || !hasRange {
		return false
	}
	i.updatePosition()

	i.iter.Next()
	if ok, err := i.iter.Valid(); !ok || err != nil {
		return true
	}
	if hasPoint, _ := i.iter.HasPointAndRange(); hasPoint {
		if i.iter.UnsafeKey().Key.Equal(i.curKey) {
			// We found a point key for the seek key, update the position again.
			i.updatePosition()
		}
	}
	return true
}

// updatePosition will update the iterator position to the newest version of the
// current point iterator's key.
func (i *pointSynthesizingIter) updatePosition() {
	if ok, err := i.iter.Valid(); !ok || err != nil {
		i.curKey = nil
		i.rangeKeys = nil
		return
	}

	hasPoint, hasRange := i.iter.HasPointAndRange()

	if hasRange {
		if rangeStart, _ := i.iter.RangeBounds(); !rangeStart.Equal(i.rangeKeysStart) {
			i.rangeKeys = i.iter.RangeKeys()
			i.rangeKeysStart = rangeStart.Clone()
		}
	}

	key := i.iter.UnsafeKey()
	i.curKey = key.Key.Clone()
	i.rangeKeysIdx = 0
	i.atPoint = hasPoint && (len(i.rangeKeys) == 0 || key.Timestamp.IsEmpty() ||
		i.rangeKeys[0].Timestamp.LessEq(key.Timestamp))
}

func (i *pointSynthesizingIter) SeekGE(key MVCCKey) {
	i.iter.SeekGE(key)
	if !i.emitOnSeek || !i.maybeUpdateWithBareRangeKey() {
		i.updateWithFirstPointKey()
	}
}

func (i *pointSynthesizingIter) SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID) {
	i.iter.SeekIntentGE(key, txnUUID)
	i.updateWithFirstPointKey()
}

func (i *pointSynthesizingIter) Next() {
	if i.atPoint {
		// Pass by the current point key and onto the next one. This may be a
		// different version of the current key, or a different key entirely.
		i.atPoint = false
		i.iter.Next()
	} else {
		// Move onto the next range key. This may be below the current point key,
		// we'll find out below.
		i.rangeKeysIdx++
	}
	var key MVCCKey
	if ok, _ := i.iter.Valid(); ok {
		if hasPoint, _ := i.iter.HasPointAndRange(); hasPoint {
			key = i.iter.UnsafeKey()
		}
	}
	if len(key.Key) > 0 && key.Key.Equal(i.curKey) && (i.rangeKeysIdx >= len(i.rangeKeys) ||
		key.Timestamp.IsEmpty() || i.rangeKeys[i.rangeKeysIdx].Timestamp.LessEq(key.Timestamp)) {
		// If the iter point key is at the same position as us and newer than the current
		// range key, then position on the point key.
		i.atPoint = true
	} else if i.rangeKeysIdx >= len(i.rangeKeys) {
		// If we've exhausted the range keys then synthesize points for the current point key,
		// which must now be a different key from curKey.
		i.updateWithFirstPointKey()
	}
	// Otherwise, we're now on the correct range key.
}

func (i *pointSynthesizingIter) NextKey() {
	i.iter.NextKey()
	i.updateWithFirstPointKey()
}

func (i *pointSynthesizingIter) SeekLT(key MVCCKey) {
	panic("not implemented")
}

func (i *pointSynthesizingIter) Prev() {
	panic("not implemented")
}

func (i *pointSynthesizingIter) Valid() (bool, error) {
	if !i.atPoint && i.rangeKeysIdx < len(i.rangeKeys) {
		return true, nil
	}
	return i.iter.Valid()
}

func (i *pointSynthesizingIter) HasPointAndRange() (bool, bool) {
	ok, _ := i.Valid()
	return ok, false
}

func (i *pointSynthesizingIter) RangeBounds() (roachpb.Key, roachpb.Key) {
	return nil, nil
}

func (i *pointSynthesizingIter) RangeKeys() []MVCCRangeKey {
	return nil
}

func (i *pointSynthesizingIter) Key() MVCCKey {
	return i.UnsafeKey().Clone()
}

func (i *pointSynthesizingIter) UnsafeKey() MVCCKey {
	if i.atPoint {
		return i.iter.UnsafeKey()
	}
	if len(i.curKey) == 0 || i.rangeKeysIdx >= len(i.rangeKeys) {
		return MVCCKey{}
	}
	return MVCCKey{
		Key:       i.curKey,
		Timestamp: i.rangeKeys[i.rangeKeysIdx].Timestamp,
	}
}

func (i *pointSynthesizingIter) UnsafeRawKey() []byte {
	if i.atPoint {
		return i.iter.UnsafeRawKey()
	}
	return EncodeMVCCKeyPrefix(i.curKey)
}

func (i *pointSynthesizingIter) UnsafeRawMVCCKey() []byte {
	if i.atPoint {
		return i.iter.UnsafeRawMVCCKey()
	}
	return EncodeMVCCKey(i.UnsafeKey())
}

func (i *pointSynthesizingIter) Value() []byte {
	v := i.UnsafeValue()
	if v != nil {
		v = append([]byte{}, v...)
	}
	return v
}

func (i *pointSynthesizingIter) UnsafeValue() []byte {
	if i.atPoint {
		return i.iter.UnsafeValue()
	}
	return nil
}

func (i *pointSynthesizingIter) Close() {
	i.iter.Close()
}

func (i *pointSynthesizingIter) ValueProto(msg protoutil.Message) error {
	panic("not implemented")
}

func (i *pointSynthesizingIter) ComputeStats(start, end roachpb.Key, nowNanos int64) (enginepb.MVCCStats, error) {
	panic("not implemented")
}

func (i *pointSynthesizingIter) FindSplitKey(start, end, minSplitKey roachpb.Key, targetSize int64) (MVCCKey, error) {
	panic("not implemented")
}

func (i *pointSynthesizingIter) SetUpperBound(key roachpb.Key) {
	panic("not implemented")
}

func (i *pointSynthesizingIter) Stats() IteratorStats {
	return i.iter.Stats()
}

func (i *pointSynthesizingIter) SupportsPrev() bool {
	panic("not implemented")
}
