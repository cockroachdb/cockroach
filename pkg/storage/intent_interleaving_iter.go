// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// TODO: add high-level comment

type intentInterleavingIter struct {
	prefix     bool
	iter       Iterator
	intentIter Iterator
	// The decoded key from the lock table. This is an unsafe key
	// in that it is only valid when intentIter has not been
	// repositioned.
	intentKey roachpb.Key
	// - cmp output of (intentKey, current iter key) when both are non-nil.
	// - intentKey==nil, current iter key!=nil, cmp=dir
	// - intentKey!=nil, current iter key==nil, cmp=-dir
	// If both are nil? valid=false
	intentCmp int

	// The current direction. +1 for forward, -1 for reverse.
	dir   int
	valid bool
	err   error
}

func newIntentInterleavingIterator(reader wrappableReader, opts IterOptions) Iterator {
	intentOpts := opts
	if opts.LowerBound != nil {
		intentOpts.LowerBound = keys.MakeLockTableKeyPrefix(opts.LowerBound)
	}
	if opts.UpperBound != nil {
		intentOpts.UpperBound = keys.MakeLockTableKeyPrefix(opts.UpperBound)
	}
	intentIter := reader.newIterator(intentOpts, StorageKeyIterKind)
	iter := reader.newIterator(opts, MVCCKeyIterKind)
	return &intentInterleavingIter{
		prefix:     opts.Prefix,
		iter:       iter,
		intentIter: intentIter,
	}
}

func (i *intentInterleavingIter) SeekGE(key MVCCKey) {
	i.dir = +1
	i.valid = true
	i.err = nil

	var intentSeekKey roachpb.Key
	if key.Timestamp == (hlc.Timestamp{}) {
		// Common case.
		intentSeekKey = keys.LockTableKeyExclusive(key.Key)
	} else if !i.prefix {
		// Seeking to a specific version, so go past the intent.
		intentSeekKey = keys.LockTableKeyExclusive(key.Key.Next())
	} else {
		// Else seeking to a particular version and using prefix iteration,
		// so don't expect to see the intent
		i.iter.SeekGE(key)
		i.intentCmp = i.dir
		return
	}

	i.intentIter.SeekStorageGE(StorageKey{Key: intentSeekKey})
	if err := i.tryDecodeLockKey(); err != nil {
		return
	}
	i.iter.SeekGE(key)
	i.extract()
}

func (i *intentInterleavingIter) SeekStorageGE(key StorageKey) {
	panic("caller should not call SeekGE")
}

func (i *intentInterleavingIter) extract() {
	valid, err := i.iter.Valid()
	if err != nil || (!valid && i.intentKey == nil) {
		i.err = err
		i.valid = false
		return
	}
	// err == nil && (valid || i.intentKey != nil)
	if !valid {
		i.intentCmp = -i.dir
	} else if i.intentKey == nil {
		i.intentCmp = i.dir
	} else {
		// TODO: we can optimize away some comparisons if we use the fact
		// that each intent needs to have a provisional value.
		i.intentCmp = i.intentKey.Compare(i.iter.UnsafeKey().Key)
	}
}

func (i *intentInterleavingIter) tryDecodeLockKey() error {
	valid, err := i.intentIter.Valid()
	if err != nil {
		i.err = err
		i.valid = false
		return err
	}
	if !valid {
		i.intentKey = nil
		return nil
	}
	i.intentKey, _, err = keys.DecodeLockTableKey(i.intentIter.UnsafeStorageKey().Key)
	if err != nil {
		i.err = err
		i.valid = false
		return err
	}
	return nil
}

func (i *intentInterleavingIter) Valid() (bool, error) {
	return i.valid, i.err
}

func (i *intentInterleavingIter) Next() {
	if i.err != nil {
		return
	}
	if i.dir < 0 {
		// Switching from reverse to forward iteration.
		isCurAtIntent := i.isCurAtIntentIter()
		i.dir = +1
		if !i.valid {
			// Both iterators are exhausted, so step both forward.
			i.valid = true
			i.intentIter.Next()
			if err := i.tryDecodeLockKey(); err != nil {
				return
			}
			i.iter.Next()
			i.extract()
			return
		}
		// At least one of the iterators is not exhausted.
		if isCurAtIntent {
			// iter precedes the intent, so must be at the highest version of the preceding
			// key or exhausted. So step it forward. It will now point to a
			// key that is the same as the intent key since an intent always has a
			// corresponding provisional value.
			// TODO: add a debug mode assertion of the above invariant.
			i.iter.Next()
			i.intentCmp = 0
			if _, err := i.iter.Valid(); err != nil {
				i.err = err
				i.valid = false
				return
			}
		} else {
			// The intent precedes the iter. It could be for the same key, iff this key has an intent,
			// or an earlier key. Either way, stepping forward will take it to an intent for a later
			// key.
			// TODO: add a debug mode assertion of the above invariant.
			i.intentIter.Next()
			i.intentCmp = +1
			if err := i.tryDecodeLockKey(); err != nil {
				return
			}
		}
	}
	if !i.valid {
		return
	}
	if i.intentCmp <= 0 {
		// Currently, there is at most 1 intent for a key, so doing Next() is correct.
		i.intentIter.Next()
		if err := i.tryDecodeLockKey(); err != nil {
			return
		}
		i.extract()
	} else {
		i.iter.Next()
		i.extract()
	}
}

func (i *intentInterleavingIter) NextKey() {
	// NextKey is not called to switch directions, i.e., we must already
	// be in the forward direction.
	if i.dir < 0 {
		panic("")
	}
	if !i.valid {
		return
	}
	if i.intentCmp <= 0 {
		// Currently, there is at most 1 intent for a key, so doing Next() is correct.
		i.intentIter.Next()
		if err := i.tryDecodeLockKey(); err != nil {
			return
		}
		// We can do this because i.intentCmp == 0. TODO: enforce invariant.
		i.iter.NextKey()
		i.extract()
	} else {
		i.iter.NextKey()
		i.extract()
	}
}

func (i *intentInterleavingIter) isCurAtIntentIter() bool {
	return (i.dir > 0 && i.intentCmp <= 0) || (i.dir < 0 && i.intentCmp > 0)
}

func (i *intentInterleavingIter) UnsafeKey() MVCCKey {
	// Note that if there is a separated intent there cannot also be an interleaved
	// intent for the same key.
	if i.isCurAtIntentIter() {
		return MVCCKey{Key: i.intentKey}
	} else {
		return i.iter.UnsafeKey()
	}
}

func (i *intentInterleavingIter) UnsafeStorageKey() StorageKey {
	panic("called should not call UnsafeStorageKey")
}

func (i *intentInterleavingIter) UnsafeValue() []byte {
	if i.isCurAtIntentIter() {
		return i.intentIter.UnsafeValue()
	} else {
		return i.iter.UnsafeValue()
	}
}

func (i *intentInterleavingIter) Key() MVCCKey {
	unsafeKey := i.UnsafeKey()
	return MVCCKey{Key: append(roachpb.Key(nil), unsafeKey.Key...), Timestamp: unsafeKey.Timestamp}
}

func (i *intentInterleavingIter) StorageKey() StorageKey {
	panic("caller should not call StorageKey")
}

func (i *intentInterleavingIter) Value() []byte {
	if i.isCurAtIntentIter() {
		return i.intentIter.Value()
	} else {
		return i.iter.Value()
	}
}

func (i *intentInterleavingIter) Close() {
	i.iter.Close()
	i.intentIter.Close()
}

func (i *intentInterleavingIter) SeekLT(key MVCCKey) {
	i.dir = -1
	i.valid = true
	i.err = nil

	var intentSeekKey roachpb.Key
	if key.Timestamp == (hlc.Timestamp{}) {
		// Common case.
		intentSeekKey = keys.LockTableKeyExclusive(key.Key)
	} else if !i.prefix {
		// Seeking to a specific version, so need to see the intent.
		intentSeekKey = keys.LockTableKeyExclusive(key.Key.Next())
	}
	i.intentIter.SeekStorageLT(StorageKey{Key: intentSeekKey})
	if err := i.tryDecodeLockKey(); err != nil {
		return
	}
	i.iter.SeekLT(key)
	i.extract()
}

func (i *intentInterleavingIter) SeekStorageLT(key StorageKey) {
	panic("called should not call SeekLT")
}

func (i *intentInterleavingIter) Prev() {
	if i.err != nil {
		return
	}
	if i.dir > 0 {
		// Switching from forward to reverse iteration.
		isCurAtIntent := i.isCurAtIntentIter()
		i.dir = -1
		if !i.valid {
			// Both iterators are exhausted, so step both backward.
			i.valid = true
			i.intentIter.Prev()
			if err := i.tryDecodeLockKey(); err != nil {
				return
			}
			i.iter.Prev()
			i.extract()
			return
		}
		// At least one of the iterators is not exhausted.
		if isCurAtIntent {
			// iter is after the intent, so must be at the provisional value. So step it backward.
			// It will now point to a key that is before the intent key.
			// TODO: add a debug mode assertion of the above invariant.
			i.iter.Prev()
			i.intentCmp = +1
			if _, err := i.iter.Valid(); err != nil {
				i.err = err
				i.valid = false
				return
			}
		} else {
			// The intent is after the iter. We don't know whether the iter key has an intent.
			i.intentIter.Prev()
			if err := i.tryDecodeLockKey(); err != nil {
				return
			}
			i.extract()
		}
	}
	if !i.valid {
		return
	}
	if i.intentCmp > 0 {
		// The current position is at the intent.
		// Currently, there is at most 1 intent for a key, so doing Prev() is correct.
		i.intentIter.Prev()
		if err := i.tryDecodeLockKey(); err != nil {
			return
		}
		i.extract()
	} else {
		i.iter.Prev()
		i.extract()
	}
}

// UnsafeRawKeyDangerous returns the current raw key (i.e. the encoded MVCC key),
// from the underlying Pebble iterator. Note, this should be carefully
// used since it does not transform the lock table key.
func (i *intentInterleavingIter) UnsafeRawKeyDangerous() []byte {
	if i.isCurAtIntentIter() {
		return i.intentIter.Value()
	} else {
		return i.iter.Value()
	}
}

func (i *intentInterleavingIter) ValueProto(msg protoutil.Message) error {
	value := i.UnsafeValue()
	return protoutil.Unmarshal(value, msg)
}

func (i *intentInterleavingIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsGo(i, start, end, nowNanos)
}

func (i *intentInterleavingIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	return findSplitKeyUsingIterator(i, start, end, minSplitKey, targetSize)
}

func (i *intentInterleavingIter) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	return checkForKeyCollisionsGo(i, sstData, start, end)
}

func (i *intentInterleavingIter) SetUpperBound(key roachpb.Key) {
	i.iter.SetUpperBound(key)
	intentUpperBound := keys.MakeLockTableKeyPrefix(key)
	i.intentIter.SetUpperBound(intentUpperBound)
}

func (i *intentInterleavingIter) Stats() IteratorStats {
	// Only used in tests.
	stats := i.iter.Stats()
	stats2 := i.intentIter.Stats()
	stats.InternalDeleteSkippedCount += stats2.InternalDeleteSkippedCount
	stats.TimeBoundNumSSTs += stats2.TimeBoundNumSSTs
	return stats
}

func (i *intentInterleavingIter) SupportsPrev() bool {
	return true
}

func (i *intentInterleavingIter) IsCurMetaSeparated() bool {
	return i.isCurAtIntentIter()
}
