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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// intentInterleavingIter makes separated intents appear as interleaved. It
// assumes that there can also be intents that are physically interleaved.
// However, for a particular roachpb.Key there will be at most one intent,
// either interleaved or separated. Additionally, an intent will have a
// corresponding provisional value.
// Additionally, the implementation assumes that the only single key locks
// in the lock table key space are intents.
type intentInterleavingIter struct {
	prefix bool
	// iter is for iterating over MVCC keys and interleaved intents.
	iter MVCCIterator
	// intentIter is for iterating over separated intents, so that
	// intentInterleavingIter can make them look as if they were interleaved.
	intentIter EngineIterator
	// The decoded key from the lock table. This is an unsafe key
	// in that it is only valid when intentIter has not been
	// repositioned. It is nil if the intentIter is considered to be
	// exhausted. Note that the intentIter may still be positioned
	// at a valid position in the case of prefix iteration, but the
	// state of the intentKey overrides that state.
	intentKey roachpb.Key
	// - cmp output of (intentKey, current iter key) when both are non-nil.
	// - intentKey==nil, current iter key!=nil, cmp=dir
	//   (i.e., the nil key is akin to infinity in the forward direction
	//   and -infinity in the reverse direction, since that iterator is
	//   exhausted).
	// - intentKey!=nil, current iter key==nil, cmp=-dir.
	// - If both are nil. cmp is undefined and valid=false.
	intentCmp int
	// The current direction. +1 for forward, -1 for reverse.
	dir   int
	valid bool
	err   error

	hasUpperBound bool

	intentKeyBuf []byte
}

var _ MVCCIterator = &intentInterleavingIter{}

func newIntentInterleavingIterator(reader Reader, opts IterOptions) MVCCIterator {
	if !opts.MinTimestampHint.IsEmpty() || !opts.MaxTimestampHint.IsEmpty() {
		panic("intentInterleavingIter must not be used with timestamp hints")
	}
	intentOpts := opts
	var intentKeyBuf []byte
	if opts.LowerBound != nil {
		intentOpts.LowerBound, intentKeyBuf = keys.LockTableSingleKey(opts.LowerBound, nil)
	}
	if opts.UpperBound != nil {
		intentOpts.UpperBound, _ = keys.LockTableSingleKey(opts.UpperBound, nil)
	}
	intentIter := reader.NewEngineIterator(intentOpts)

	// We assume that callers iterating forward will set an upper bound,
	// and callers iterating in reverse will set a lower bound, which
	// will prevent them from accidentally iterating into the lock-table
	// key space. The MVCCIterator implementations require one of the bounds
	// or prefix iteration. We remember whether the upper bound has been
	// set, so if not set, we can set the upper bound when SeekGE is called
	// for prefix iteration.
	iter := reader.NewMVCCIterator(MVCCKeyIterKind, opts)
	return &intentInterleavingIter{
		prefix:        opts.Prefix,
		iter:          iter,
		intentIter:    intentIter,
		hasUpperBound: opts.UpperBound != nil,
		intentKeyBuf:  intentKeyBuf,
	}
}

func (i *intentInterleavingIter) SeekGE(key MVCCKey) {
	i.dir = +1
	i.valid = true
	i.err = nil

	if i.prefix {
		// Caller will use a mix of SeekGE and Next. If key is before the lock table key
		// space, make sure there is an upper bound, if not explicitly set at creation time
		// or using SetUpperBound.
		if !i.hasUpperBound && keys.IsLocal(key.Key) && !keys.IsLocalStoreKey(key.Key) {
			i.iter.SetUpperBound(keys.LocalRangeLockTablePrefix)
		}
	}
	var intentSeekKey roachpb.Key
	if key.Timestamp == (hlc.Timestamp{}) {
		// Common case.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key, i.intentKeyBuf)
	} else if !i.prefix {
		// Seeking to a specific version, so go past the intent.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key.Next(), i.intentKeyBuf)
	} else {
		// Else seeking to a particular version and using prefix iteration,
		// so don't expect to ever see the intent.
		i.intentKey = nil
	}

	if intentSeekKey != nil {
		valid, err := i.intentIter.SeekEngineKeyGE(EngineKey{Key: intentSeekKey})
		if err != nil {
			i.err = err
			i.valid = false
			return
		}
		if err := i.tryDecodeLockKey(valid); err != nil {
			return
		}
	}
	i.iter.SeekGE(key)
	i.computePos()
}

func (i *intentInterleavingIter) computePos() {
	valid, err := i.iter.Valid()
	if err != nil || (!valid && i.intentKey == nil) {
		i.err = err
		i.valid = false
		return
	}
	// INVARIANT: err == nil && (valid || i.intentKey != nil)
	if !valid {
		i.intentCmp = -i.dir
	} else if i.intentKey == nil {
		i.intentCmp = i.dir
	} else {
		i.intentCmp = i.intentKey.Compare(i.iter.UnsafeKey().Key)
	}
}

func (i *intentInterleavingIter) tryDecodeLockKey(valid bool) error {
	if !valid {
		i.intentKey = nil
		return nil
	}
	engineKey, err := i.intentIter.UnsafeEngineKey()
	if err != nil {
		i.err = err
		i.valid = false
		return err
	}
	if i.intentKey, err = keys.DecodeLockTableSingleKey(engineKey.Key); err != nil {
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
			// Both iterators are exhausted, since intentKey is synchronized with
			// intentIter for non-prefix iteration, so step both forward.
			i.valid = true
			valid, err := i.intentIter.NextEngineKey()
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			if err := i.tryDecodeLockKey(valid); err != nil {
				return
			}
			i.iter.Next()
			i.computePos()
			return
		}
		// At least one of the iterators is not exhausted.
		if isCurAtIntent {
			// iter precedes the intentIter, so must be at the lowest version of the
			// preceding key or exhausted. So step it forward. It will now point to
			// a key that is the same as the intent key since an intent always has a
			// corresponding provisional value.
			i.iter.Next()
			i.intentCmp = 0
			if valid, err := i.iter.Valid(); err != nil || !valid {
				if err == nil {
					err = errors.Errorf("intent has no provisional value")
				}
				i.err = err
				i.valid = false
				return
			}
			if util.RaceEnabled {
				cmp := i.intentKey.Compare(i.iter.UnsafeKey().Key)
				if cmp != 0 {
					i.err = errors.Errorf("intent has no provisional value, cmp: %d", cmp)
					i.valid = false
					return
				}
			}
		} else {
			// The intentIter precedes the iter. It could be for the same key, iff
			// this key has an intent, or an earlier key. Either way, stepping
			// forward will take it to an intent for a later key. Note that iter
			// could also be positioned at an intent.
			valid, err := i.intentIter.NextEngineKey()
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			i.intentCmp = +1
			if err := i.tryDecodeLockKey(valid); err != nil {
				return
			}
			if util.RaceEnabled && valid {
				cmp := i.intentKey.Compare(i.iter.UnsafeKey().Key)
				if cmp <= 0 {
					i.err = errors.Errorf("intentIter incorrectly positioned, cmp: %d", cmp)
					i.valid = false
					return
				}
			}
		}
	}
	if !i.valid {
		return
	}
	if i.intentCmp <= 0 {
		// The iterator is positioned at an intent in intentIter. iter must be
		// positioned at the provisional value.
		if i.intentCmp != 0 {
			i.err = errors.Errorf("intentIter at intent, but iter not at provisional value")
			i.valid = false
			return
		}
		valid, err := i.intentIter.NextEngineKey()
		if err != nil {
			i.err = err
			i.valid = false
			return
		}
		if err := i.tryDecodeLockKey(valid); err != nil {
			return
		}
		if valid, err := i.iter.Valid(); err != nil || !valid {
			if err == nil {
				err = errors.Errorf("iter expected to be at provisional value, but is exhausted")
			}
			i.err = err
			i.valid = false
			return
		}
		i.intentCmp = +1
		if util.RaceEnabled && i.intentKey != nil {
			cmp := i.intentKey.Compare(i.iter.UnsafeKey().Key)
			if cmp <= 0 {
				i.err = errors.Errorf("intentIter incorrectly positioned, cmp: %d", cmp)
				i.valid = false
				return
			}
		}
	} else {
		// Common case:
		// The iterator is positioned at iter. It could be a value or an intent,
		// though usually it will be a value.
		i.iter.Next()
		i.computePos()
	}
}

func (i *intentInterleavingIter) NextKey() {
	// NextKey is not called to switch directions, i.e., we must already
	// be in the forward direction.
	if i.dir < 0 {
		i.err = errors.Errorf("NextKey cannot be used to switch iteration direction")
		i.valid = false
		return
	}
	if !i.valid {
		return
	}
	if i.intentCmp <= 0 {
		// The iterator is positioned at an intent in intentIter. iter must be
		// positioned at the provisional value.
		if i.intentCmp != 0 {
			i.err = errors.Errorf("intentIter at intent, but iter not at provisional value")
			i.valid = false
			return
		}
		valid, err := i.intentIter.NextEngineKey()
		if err != nil {
			i.err = err
			i.valid = false
			return
		}
		if err := i.tryDecodeLockKey(valid); err != nil {
			return
		}
		// Step the iter to NextKey(), i.e., past all the versions of this key.
		i.iter.NextKey()
		i.computePos()
	} else {
		// The iterator is positioned at iter. It could be a value or an intent,
		// though usually it will be a value.
		// Step the iter to NextKey(), i.e., past all the versions of this key.
		i.iter.NextKey()
		i.computePos()
	}
}

func (i *intentInterleavingIter) isCurAtIntentIter() bool {
	return (i.dir > 0 && i.intentCmp <= 0) || (i.dir < 0 && i.intentCmp > 0)
}

func (i *intentInterleavingIter) UnsafeKey() MVCCKey {
	// If there is a separated intent there cannot also be an interleaved intent
	// for the same key.
	if i.isCurAtIntentIter() {
		return MVCCKey{Key: i.intentKey}
	} else {
		return i.iter.UnsafeKey()
	}
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
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key, i.intentKeyBuf)
	} else {
		// Seeking to a specific version, so need to see the intent.
		if i.prefix {
			i.err = errors.Errorf("prefix iteration is not permitted with SeekLT")
			i.valid = false
			return
		} else {
			intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key.Next(), i.intentKeyBuf)
		}
	}
	valid, err := i.intentIter.SeekEngineKeyLT(EngineKey{Key: intentSeekKey})
	if err != nil {
		i.err = err
		i.valid = false
		return
	}
	if err := i.tryDecodeLockKey(valid); err != nil {
		return
	}
	i.iter.SeekLT(key)
	i.computePos()
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
			valid, err := i.intentIter.PrevEngineKey()
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			if err := i.tryDecodeLockKey(valid); err != nil {
				return
			}
			i.iter.Prev()
			i.computePos()
			return
		}
		// At least one of the iterators is not exhausted.
		if isCurAtIntent {
			// iter is after the intentIter, so must be at the provisional value.
			// Step it backward. It will now point to a key that is before the
			// intent key.
			if i.intentCmp != 0 {
				i.err = errors.Errorf("iter not at provisional value, cmp: %d", i.intentCmp)
				i.valid = false
				return
			}
			i.iter.Prev()
			i.intentCmp = +1
			valid, err := i.iter.Valid()
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			if util.RaceEnabled && valid {
				cmp := i.intentKey.Compare(i.iter.UnsafeKey().Key)
				if cmp <= 0 {
					i.err = errors.Errorf("intentIter should be after iter, cmp: %d", cmp)
					i.valid = false
					return
				}
			}
		} else {
			// The intentIter is after the iter. We don't know whether the iter key
			// has an intent. Note that the iter could itself be positioned at an
			// intent.
			valid, err := i.intentIter.PrevEngineKey()
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			if err := i.tryDecodeLockKey(valid); err != nil {
				return
			}
			i.computePos()
		}
	}
	if !i.valid {
		return
	}
	if i.intentCmp > 0 {
		// The iterator is positioned at an intent in intentIter. Stepping it backward
		// will ensure that it is less than the position of iter (since iter must have
		// the provisional value of the preceding separated intent).
		valid, err := i.intentIter.PrevEngineKey()
		if err != nil {
			i.err = err
			i.valid = false
			return
		}
		if err := i.tryDecodeLockKey(valid); err != nil {
			return
		}
		if valid, err = i.iter.Valid(); err != nil || !valid {
			i.err = err
			i.valid = false
			return
		}
		i.intentCmp = -1
		if util.RaceEnabled && i.intentKey != nil {
			cmp := i.intentKey.Compare(i.iter.UnsafeKey().Key)
			if cmp > 0 {
				i.err = errors.Errorf("intentIter should be before iter, cmp: %d", cmp)
				i.valid = false
				return
			}
		}
	} else {
		// Common case:
		// The iterator is positioned at iter. It could be a value or an intent,
		// though usually it will be a value.
		i.iter.Prev()
		i.computePos()
	}
}

func (i *intentInterleavingIter) UnsafeRawKey() []byte {
	if i.isCurAtIntentIter() {
		// TODO(sumeer): this is inefficient, but the users of UnsafeRawKey are
		// incorrect, so this method will go away.
		key, err := i.intentIter.UnsafeEngineKey()
		if err != nil {
			// Should be able to parse it again.
			panic(err)
		}
		return key.Encode()
	} else {
		return i.iter.UnsafeRawKey()
	}
}

func (i *intentInterleavingIter) ValueProto(msg protoutil.Message) error {
	value := i.UnsafeValue()
	return protoutil.Unmarshal(value, msg)
}

func (i *intentInterleavingIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	return ComputeStatsForRange(i, start, end, nowNanos)
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
	var intentUpperBound roachpb.Key
	intentUpperBound, i.intentKeyBuf = keys.LockTableSingleKey(key, i.intentKeyBuf)
	i.intentIter.SetUpperBound(intentUpperBound)
	i.hasUpperBound = key != nil
}

func (i *intentInterleavingIter) Stats() IteratorStats {
	// Only used in tests.
	return i.iter.Stats()
}

func (i *intentInterleavingIter) SupportsPrev() bool {
	return true
}
