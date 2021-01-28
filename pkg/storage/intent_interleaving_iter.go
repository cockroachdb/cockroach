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
	"bytes"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// intentInterleavingIter makes separated intents appear as interleaved. It
// relies on the following assumptions:
// - There can also be intents that are physically interleaved.
//   However, for a particular roachpb.Key there will be at most one intent,
//   either interleaved or separated.
// - An intent will have a corresponding provisional value.
// - The only single key locks in the lock table key space are intents.
//
// Semantically, the functionality is equivalent to merging two MVCCIterators:
// - A MVCCIterator on the MVCC key space.
// - A MVCCIterator constructed by wrapping an EngineIterator on the lock table
//   key space where the EngineKey is transformed into the corresponding
//   intent key and appears as MVCCKey{Key: intentKey}.
// The implementation below is specialized to reduce unnecessary comparisons
// and iteration, by utilizing the aforementioned assumptions. The intentIter
// iterates over the lock table key space and iter over the MVCC key space.
// They are kept synchronized in the following way (for forward iteration):
// - At the same MVCCKey.Key: the intentIter is at the intent and iter at the
//   provisional value.
// - At different MVCCKey.Keys: the intentIter is ahead of iter, at the first
//   key after iter's MVCCKey.Key that has a separated intent.
// Note that in both cases the iterators are apart by the minimal possible
// distance. This minimal distance rule applies for reverse iteration too, and
// can be used to construct similar invariants.
// The one exception to the minimal distance rule is a sub-case of prefix
// iteration, when we know that no separated intents need to be seen, and so
// don't bother positioning intentIter.
//
// The implementation of intentInterleavingIter assumes callers iterating
// forward (reverse) are setting an upper (lower) bound. There is protection
// for misbehavior by the callers for the lock table iterator, which prevents
// that iterator from leaving the lock table key space, by adding additional
// bounds. But we do not manufacture bounds to prevent MVCCIterator to iterate
// into the lock table key space, for the following reasons:
// - Adding a bound where the caller has not specified one adds a key
//   comparison when iterating. We don't expect much iteration (next/prev
//   calls) over the lock table iterator, because of the rarity of intents,
//   but that is not the case for the MVCC key space.
// - The MVCC key space is split into two spans: local keys preceding the lock
//   table key space, and global keys. Adding a bound where one is not
//   specified by the caller requires us to know which one the caller wants to
//   iterate over -- the bounds may not fully specify the intent of the caller
//   e.g. a caller could use ["", \xFF\xFF) as the bounds, and use the seek
//   key to narrow down iteration over the local key space or the global key
//   space.
// Instead, pebbleIterator, the base implementation of MVCCIterator, cheaply
// checks whether it has iterated into the lock table key space, and if so,
// marks itself as exhausted (Valid() returns false, nil).
//
// A limitation of these MVCCIterator implementations, that follows from the
// fact that the MVCC key space is split into two spans, is that one can't
// typically iterate using next/prev from the local MVCC key space to the
// global one and vice versa. One needs to seek in-between.
type intentInterleavingIter struct {
	prefix bool

	// iter is for iterating over MVCC keys and interleaved intents.
	iter MVCCIterator
	// The valid value from iter.Valid() after the last positioning call.
	iterValid bool
	// When iterValid = true, this contains the result of iter.UnsafeKey(). We
	// store it here to avoid repeatedly calling UnsafeKey() since it repeats
	// key parsing.
	iterKey MVCCKey

	// intentIter is for iterating over separated intents, so that
	// intentInterleavingIter can make them look as if they were interleaved.
	intentIter EngineIterator
	// The decoded key from the lock table. This is an unsafe key
	// in that it is only valid when intentIter has not been
	// repositioned. It is nil if the intentIter is considered to be
	// exhausted. Note that the intentIter may still be positioned
	// at a valid position in the case of prefix iteration, but the
	// state of the intentKey overrides that state.
	intentKey                            roachpb.Key
	intentKeyAsNoTimestampMVCCKey        []byte
	intentKeyAsNoTimestampMVCCKeyBacking []byte

	// - cmp output of (intentKey, current iter key) when both are valid.
	//   This does not take timestamps into consideration. So if intentIter
	//   is at an intent, and iter is at the corresponding provisional value,
	//   cmp will be 0. See the longer struct-level comment for more on the
	//   relative positioning of intentIter and iter.
	// - intentKey==nil, iterValid==true, cmp=dir
	//   (i.e., the nil key is akin to infinity in the forward direction
	//   and -infinity in the reverse direction, since that iterator is
	//   exhausted).
	// - intentKey!=nil, iterValid=false, cmp=-dir.
	// - If both are invalid. cmp is undefined and valid=false.
	intentCmp int
	// The current direction. +1 for forward, -1 for reverse.
	dir   int
	valid bool
	err   error

	hasUpperBound bool

	intentKeyBuf []byte
}

var _ MVCCIterator = &intentInterleavingIter{}

var intentInterleavingIterPool = sync.Pool{
	New: func() interface{} {
		return &intentInterleavingIter{}
	},
}

func isLocal(k roachpb.Key) bool {
	return len(k) == 0 || keys.IsLocal(k)
}

func newIntentInterleavingIterator(reader Reader, opts IterOptions) MVCCIterator {
	if !opts.MinTimestampHint.IsEmpty() || !opts.MaxTimestampHint.IsEmpty() {
		panic("intentInterleavingIter must not be used with timestamp hints")
	}
	if opts.LowerBound != nil && opts.UpperBound != nil {
		lowerIsLocal := isLocal(opts.LowerBound)
		upperIsLocal := isLocal(opts.UpperBound) || bytes.Equal(opts.UpperBound, keys.LocalMax)
		if lowerIsLocal != upperIsLocal {
			panic(fmt.Sprintf(
				"intentInterleavingIter cannot span from lowerIsLocal %t, %s to upperIsLocal %t, %s",
				lowerIsLocal, opts.LowerBound.String(), upperIsLocal, opts.UpperBound.String()))
		}
	}
	intentOpts := opts
	var intentKeyBuf []byte
	if opts.LowerBound != nil {
		intentOpts.LowerBound, intentKeyBuf = keys.LockTableSingleKey(opts.LowerBound, nil)
	} else {
		// Sometimes callers iterate backwards without having a lower bound.
		// Make sure we don't step outside the lock table key space.
		intentOpts.LowerBound = keys.LockTableSingleKeyStart
	}
	if opts.UpperBound != nil {
		intentOpts.UpperBound, _ = keys.LockTableSingleKey(opts.UpperBound, nil)
	} else {
		// Sometimes callers iterate forwards without having an upper bound.
		// Make sure we don't step outside the lock table key space.
		intentOpts.UpperBound = keys.LockTableSingleKeyEnd
	}
	// Note that we can reuse intentKeyBuf after NewEngineIterator returns.
	intentIter := reader.NewEngineIterator(intentOpts)

	// We assume that callers iterating forward will set an upper bound,
	// and callers iterating in reverse will set a lower bound, which
	// will prevent them from accidentally iterating into the lock-table
	// key space. The MVCCIterator implementations require one of the bounds
	// or prefix iteration. We remember whether the upper bound has been
	// set, so if not set, we can set the upper bound when SeekGE is called
	// for prefix iteration.
	//
	// The creation of these iterators can race with concurrent mutations, which
	// may make them inconsistent with each other. So we clone here, to ensure
	// consistency (certain Reader implementations already ensure consistency,
	// and we use that when possible to save allocations).
	var iter MVCCIterator
	if reader.ConsistentIterators() {
		iter = reader.NewMVCCIterator(MVCCKeyIterKind, opts)
	} else {
		iter = newMVCCIteratorByCloningEngineIter(intentIter, opts)
	}
	iiIter := intentInterleavingIterPool.Get().(*intentInterleavingIter)
	*iiIter = intentInterleavingIter{
		prefix:        opts.Prefix,
		iter:          iter,
		intentIter:    intentIter,
		hasUpperBound: opts.UpperBound != nil,
		intentKeyBuf:  intentKeyBuf,
	}
	return iiIter
}

func (i *intentInterleavingIter) SeekGE(key MVCCKey) {
	i.dir = +1
	i.valid = true
	i.err = nil

	if i.prefix {
		// Caller will use a mix of SeekGE and Next. If key is before the lock table key
		// space, make sure there is an upper bound, if not explicitly set at creation time
		// or using SetUpperBound. We do not set hasUpperBound to true since this is
		// an implicit (not set by the user) upper-bound, and we want to change it on
		// a subsequent call to SeekGE.
		if !i.hasUpperBound && keys.IsLocal(key.Key) && !keys.IsLocalStoreKey(key.Key) {
			i.iter.SetUpperBound(keys.LocalRangeLockTablePrefix)
		}
	}
	var intentSeekKey roachpb.Key
	if key.Timestamp.IsEmpty() {
		// Common case.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key, i.intentKeyBuf)
	} else if !i.prefix {
		// Seeking to a specific version, so go past the intent.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key.Next(), i.intentKeyBuf)
	} else {
		// Else seeking to a particular version and using prefix iteration,
		// so don't expect to ever see the intent. NB: intentSeekKey is nil.
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
	var err error
	i.iterValid, err = i.iter.Valid()
	if err != nil || (!i.iterValid && i.intentKey == nil) {
		i.err = err
		i.valid = false
		return
	}
	// INVARIANT: err == nil && (i.iterValid || i.intentKey != nil)
	if !i.iterValid {
		i.intentCmp = -i.dir
		return
	}
	i.iterKey = i.iter.UnsafeKey()
	if i.intentKey == nil {
		i.intentCmp = i.dir
	} else {
		i.intentCmp = i.intentKey.Compare(i.iterKey.Key)
	}
}

func (i *intentInterleavingIter) tryDecodeLockKey(valid bool) error {
	if !valid {
		// NB: this does not set i.valid = false, since this method does not care
		// about the state of i.iter, which may be valid. It is the caller's
		// responsibility to additionally use the state of i.iter to appropriately
		// set i.valid.
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
	// If we were to encode MVCCKey{Key: i.intentKey}, i.e., encode it as an
	// MVCCKey with no timestamp, the encoded bytes would be intentKey + \x00.
	// Such an encoding is needed by callers of UnsafeRawMVCCKey. We would like
	// to avoid copying the bytes in intentKey, if possible, for this encoding.
	// Fortunately, the common case in the above call of
	// DecodeLockTableSingleKey, that decodes intentKey from engineKey.Key, is
	// for intentKey to not need un-escaping, so it will point to the slice that
	// was backing engineKey.Key. engineKey.Key uses an encoding that terminates
	// the intent key using \x00\x01. So the \x00 we need is conveniently there.
	// This optimization also usually works when there is un-escaping, since the
	// slice growth algorithm usually ends up with a cap greater than len. Since
	// these extra bytes in the cap are 0-initialized, the first byte following
	// intentKey is \x00.
	//
	// If this optimization is not possible, we leave
	// intentKeyAsNoTimestampMVCCKey as nil, and lazily initialize it, if
	// needed.
	i.intentKeyAsNoTimestampMVCCKey = nil
	if cap(i.intentKey) > len(i.intentKey) {
		prospectiveKey := i.intentKey[:len(i.intentKey)+1]
		if prospectiveKey[len(i.intentKey)] == 0 {
			i.intentKeyAsNoTimestampMVCCKey = prospectiveKey
		}
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
			// corresponding provisional value, and provisional values must have a
			// higher timestamp than any committed value on a key. Note that the
			// code below does not specifically care if a bug (external to this
			// code) violates the invariant that the iter is pointing to the
			// provisional value, but it does care that iter is pointing to some
			// version of that key.
			i.iter.Next()
			i.intentCmp = 0
			var err error
			if i.iterValid, err = i.iter.Valid(); err != nil || !i.iterValid {
				if err == nil {
					err = errors.Errorf("intent has no provisional value")
				}
				i.err = err
				i.valid = false
				return
			}
			i.iterKey = i.iter.UnsafeKey()
			if util.RaceEnabled {
				cmp := i.intentKey.Compare(i.iterKey.Key)
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
			// could also be positioned at an intent. We are assuming that there
			// isn't a bug (external to this code) that has caused two intents to be
			// present for the same key.
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
				cmp := i.intentKey.Compare(i.iterKey.Key)
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
		// positioned at the provisional value. Note that the code below does not
		// specifically care if a bug (external to this code) violates the
		// invariant that the iter is pointing to the provisional value, but it
		// does care that iter is pointing to some version of that key.
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
		if !i.iterValid {
			i.err = errors.Errorf("iter expected to be at provisional value, but is exhausted")
			i.valid = false
			return
		}
		i.intentCmp = +1
		if util.RaceEnabled && i.intentKey != nil {
			cmp := i.intentKey.Compare(i.iterKey.Key)
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
	// When both iter and intentIter are exhausted, the return value is
	// immaterial since this function won't be called. We examine the remaining
	// cases below.
	//
	// During forward iteration (dir > 0), we have the following cases:
	// - iter is exhausted: intentCmp < 0. This will never happen and callers
	//   check. Returns true.
	// - intentIter is exhausted: intentCmp > 0. Returns false.
	// - Neither is exhausted:
	//   - intentCmp < 0. This will never happen and callers check. Returns true.
	//   - intentCmp = 0. Returns true.
	//   - intentCmp > 0. Returns false.
	//
	// During reverse iteration (dir < 0), we have the following cases:
	// - iter is exhausted: intentCmp > 0. Returns true.
	// - intentIter is exhausted: intentCmp < 0. Returns false.
	// - Neither is exhausted:
	//   - intentCmp <= 0. Returns false.
	//   - intentCmp > 0. Returns true.
	return (i.dir > 0 && i.intentCmp <= 0) || (i.dir < 0 && i.intentCmp > 0)
}

func (i *intentInterleavingIter) UnsafeKey() MVCCKey {
	// If there is a separated intent there cannot also be an interleaved intent
	// for the same key.
	if i.isCurAtIntentIter() {
		return MVCCKey{Key: i.intentKey}
	}
	return i.iterKey
}

func (i *intentInterleavingIter) UnsafeValue() []byte {
	if i.isCurAtIntentIter() {
		return i.intentIter.UnsafeValue()
	}
	return i.iter.UnsafeValue()
}

func (i *intentInterleavingIter) Key() MVCCKey {
	key := i.UnsafeKey()
	keyCopy := make([]byte, len(key.Key))
	copy(keyCopy, key.Key)
	key.Key = keyCopy
	return key
}

func (i *intentInterleavingIter) Value() []byte {
	if i.isCurAtIntentIter() {
		return i.intentIter.Value()
	}
	return i.iter.Value()
}

func (i *intentInterleavingIter) Close() {
	i.iter.Close()
	i.intentIter.Close()
	*i = intentInterleavingIter{}
	intentInterleavingIterPool.Put(i)
}

func (i *intentInterleavingIter) SeekLT(key MVCCKey) {
	i.dir = -1
	i.valid = true
	i.err = nil

	var intentSeekKey roachpb.Key
	if key.Timestamp.IsEmpty() {
		// Common case.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key, i.intentKeyBuf)
	} else {
		// Seeking to a specific version, so need to see the intent.
		if i.prefix {
			i.err = errors.Errorf("prefix iteration is not permitted with SeekLT")
			i.valid = false
			return
		}
		// Since we need to see the intent for key.Key, and we don't have SeekLE, call
		// Next() on the key before doing SeekLT.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key.Next(), i.intentKeyBuf)
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
			// intent key. Note that the code below does not specifically care if a
			// bug (external to this code) violates the invariant that the
			// provisional value is the highest timestamp key, but it does care that
			// there is a timestamped value for this key (which it checks below).
			// The internal invariant of this iterator implementation will ensure
			// that iter is pointing to the highest timestamped key.
			if i.intentCmp != 0 {
				i.err = errors.Errorf("iter not at provisional value, cmp: %d", i.intentCmp)
				i.valid = false
				return
			}
			i.iter.Prev()
			i.intentCmp = +1
			var err error
			i.iterValid, err = i.iter.Valid()
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
			if i.iterValid {
				i.iterKey = i.iter.UnsafeKey()
			}
			if util.RaceEnabled && i.iterValid {
				cmp := i.intentKey.Compare(i.iterKey.Key)
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
			if i.intentKey == nil {
				i.intentCmp = -1
			} else {
				i.intentCmp = i.intentKey.Compare(i.iterKey.Key)
			}
		}
	}
	if !i.valid {
		return
	}
	if i.intentCmp > 0 {
		// The iterator is positioned at an intent in intentIter, and iter is
		// exhausted or positioned at a versioned value of a preceding key.
		// Stepping intentIter backward will ensure that intentKey is <= the key
		// of iter (when neither is exhausted).
		intentIterValid, err := i.intentIter.PrevEngineKey()
		if err != nil {
			i.err = err
			i.valid = false
			return
		}
		if err := i.tryDecodeLockKey(intentIterValid); err != nil {
			return
		}
		if !i.iterValid {
			// It !i.iterValid, the intentIter can no longer be valid either.
			if intentIterValid {
				i.err = errors.Errorf("reverse iteration discovered intent without provisional value")
			}
			i.valid = false
			return
		}
		// iterValid == true. So positioned at iter.
		i.intentCmp = -1
		if i.intentKey != nil {
			i.intentCmp = i.intentKey.Compare(i.iterKey.Key)
			if i.intentCmp > 0 {
				i.err = errors.Errorf("intentIter should not be after iter")
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
		return i.intentIter.UnsafeRawEngineKey()
	}
	return i.iter.UnsafeRawKey()
}

func (i *intentInterleavingIter) UnsafeRawMVCCKey() []byte {
	if i.isCurAtIntentIter() {
		if i.intentKeyAsNoTimestampMVCCKey == nil {
			// Slow-path: tryDecodeLockKey was not able to initialize.
			if cap(i.intentKeyAsNoTimestampMVCCKeyBacking) < len(i.intentKey)+1 {
				i.intentKeyAsNoTimestampMVCCKeyBacking = make([]byte, 0, len(i.intentKey)+1)
			}
			i.intentKeyAsNoTimestampMVCCKeyBacking = append(
				i.intentKeyAsNoTimestampMVCCKeyBacking[:0], i.intentKey...)
			// Append the 0 byte representing the absence of a timestamp.
			i.intentKeyAsNoTimestampMVCCKeyBacking = append(
				i.intentKeyAsNoTimestampMVCCKeyBacking, 0)
			i.intentKeyAsNoTimestampMVCCKey = i.intentKeyAsNoTimestampMVCCKeyBacking
		}
		return i.intentKeyAsNoTimestampMVCCKey
	}
	return i.iter.UnsafeRawKey()
}

func (i *intentInterleavingIter) ValueProto(msg protoutil.Message) error {
	value := i.UnsafeValue()
	return protoutil.Unmarshal(value, msg)
}

func (i *intentInterleavingIter) IsCurIntentSeparated() bool {
	return i.isCurAtIntentIter()
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
	if key != nil {
		intentUpperBound, i.intentKeyBuf = keys.LockTableSingleKey(key, i.intentKeyBuf)
		// Note that we can reuse intentKeyBuf after SetUpperBound returns.
	} else {
		// Sometimes callers iterate forwards without having an upper bound.
		// Make sure we don't step outside the lock table key space.
		intentUpperBound = keys.LockTableSingleKeyEnd
	}
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

// newMVCCIteratorByCloningEngineIter assumes MVCCKeyIterKind and no timestamp
// hints. It uses pebble.Iterator.Clone to ensure that the two iterators see
// the identical engine state.
func newMVCCIteratorByCloningEngineIter(iter EngineIterator, opts IterOptions) MVCCIterator {
	pIter := iter.GetRawIter()
	it := newPebbleIterator(nil, pIter, opts)
	if iter == nil {
		panic("couldn't create a new iterator")
	}
	return it
}
