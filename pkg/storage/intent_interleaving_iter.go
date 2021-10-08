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
	"math/rand"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

type intentInterleavingIterConstraint int8

const (
	notConstrained intentInterleavingIterConstraint = iota
	constrainedToLocal
	constrainedToGlobal
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
// for misbehavior by the callers that don't set such bounds, by manufacturing
// bounds. These manufactured bounds prevent the lock table iterator from
// leaving the lock table key space. We also need to manufacture bounds for
// the MVCCIterator to prevent it from iterating into the lock table. Note
// that any manufactured bounds for both the lock table iterator and
// MVCCIterator must be consistent since the intentInterleavingIter does not
// like to see a lock with no corresponding provisional value (it will
// consider than an error). Manufacturing of bounds is complicated by the fact
// that the MVCC key space is split into two spans: local keys preceding the
// lock table key space, and global keys. To manufacture a bound, we need to
// know whether the caller plans to iterate over local or global keys. Setting
// aside prefix iteration, which doesn't need any of these manufactured
// bounds, the call to newIntentInterleavingIter must have specified at least
// one of the lower or upper bound. We use that to "constrain" the iterator as
// either a local key iterator or global key iterator and panic if a caller
// violates that in a subsequent SeekGE/SeekLT/SetUpperBound call.
type intentInterleavingIter struct {
	prefix     bool
	constraint intentInterleavingIterConstraint

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
	intentIter      EngineIterator
	intentIterState pebble.IterValidityState
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

	// Buffers to reuse memory when constructing lock table keys for bounds and
	// seeks.
	intentKeyBuf      []byte
	intentLimitKeyBuf []byte
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
	var lowerIsLocal, upperIsLocal bool
	var constraint intentInterleavingIterConstraint
	if opts.LowerBound != nil {
		lowerIsLocal = isLocal(opts.LowerBound)
		if lowerIsLocal {
			constraint = constrainedToLocal
		} else {
			constraint = constrainedToGlobal
		}
	}
	if opts.UpperBound != nil {
		upperIsLocal = isLocal(opts.UpperBound) || bytes.Equal(opts.UpperBound, keys.LocalMax)
		if opts.LowerBound != nil && lowerIsLocal != upperIsLocal {
			panic(fmt.Sprintf(
				"intentInterleavingIter cannot span from lowerIsLocal %t, %s to upperIsLocal %t, %s",
				lowerIsLocal, opts.LowerBound.String(), upperIsLocal, opts.UpperBound.String()))
		}
		if upperIsLocal {
			constraint = constrainedToLocal
		} else {
			constraint = constrainedToGlobal
		}
	}
	if !opts.Prefix {
		if opts.LowerBound == nil && opts.UpperBound == nil {
			// This is the same requirement as pebbleIterator.
			panic("iterator must set prefix or upper bound or lower bound")
		}
		// At least one bound is specified, so constraint != notConstrained. But
		// may need to manufacture a bound for the currently unbounded side.
		if opts.LowerBound == nil && constraint == constrainedToGlobal {
			// Iterating over global keys, and need a lower-bound, to prevent the MVCCIterator
			// from iterating into the lock table.
			opts.LowerBound = keys.LocalMax
		}
		if opts.UpperBound == nil && constraint == constrainedToLocal {
			// Iterating over local keys, and need an upper-bound, to prevent the MVCCIterator
			// from iterating into the lock table.
			opts.UpperBound = keys.LocalRangeLockTablePrefix
		}
	}
	// Else prefix iteration, so do not need to manufacture bounds for both
	// iterators since the pebble.Iterator implementation will hide the keys
	// that do not match the prefix. Note that this is not equivalent to
	// constraint==notConstrained -- it is acceptable for a caller to specify a
	// bound for prefix iteration, though since they don't need to, most callers
	// don't.

	iiIter := intentInterleavingIterPool.Get().(*intentInterleavingIter)
	intentOpts := opts
	intentKeyBuf := iiIter.intentKeyBuf
	intentLimitKeyBuf := iiIter.intentLimitKeyBuf
	if opts.LowerBound != nil {
		intentOpts.LowerBound, intentKeyBuf = keys.LockTableSingleKey(opts.LowerBound, intentKeyBuf)
	} else if !opts.Prefix {
		// Make sure we don't step outside the lock table key space. Note that
		// this is the case where the lower bound was not set and
		// constrainedToLocal.
		intentOpts.LowerBound = keys.LockTableSingleKeyStart
	}
	if opts.UpperBound != nil {
		intentOpts.UpperBound, intentLimitKeyBuf =
			keys.LockTableSingleKey(opts.UpperBound, intentLimitKeyBuf)
	} else if !opts.Prefix {
		// Make sure we don't step outside the lock table key space. Note that
		// this is the case where the upper bound was not set and
		// constrainedToGlobal.
		intentOpts.UpperBound = keys.LockTableSingleKeyEnd
	}
	// Note that we can reuse intentKeyBuf, intentLimitKeyBuf after
	// NewEngineIterator returns.
	intentIter := reader.NewEngineIterator(intentOpts)

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

	*iiIter = intentInterleavingIter{
		prefix:                               opts.Prefix,
		constraint:                           constraint,
		iter:                                 iter,
		intentIter:                           intentIter,
		intentKeyAsNoTimestampMVCCKeyBacking: iiIter.intentKeyAsNoTimestampMVCCKeyBacking,
		intentKeyBuf:                         intentKeyBuf,
		intentLimitKeyBuf:                    intentLimitKeyBuf,
	}
	return iiIter
}

// TODO(sumeer): the limits generated below are tight for the current value of
// i.iterKey.Key. And the semantics of the underlying *WithLimit methods in
// pebble.Iterator are best-effort, but the implementation is not. Consider
// strengthening the semantics and using the tightness of these limits to
// avoid comparisons between iterKey and intentKey.

// makeUpperLimitKey uses the current value of i.iterKey.Key (and assumes
// i.iterValid=true), to construct an exclusive upper limit roachpb.Key that
// will include the intent for i.iterKey.Key.
func (i *intentInterleavingIter) makeUpperLimitKey() roachpb.Key {
	key := i.iterKey.Key
	// The +2 is to account for the call to BytesNext and the need to append a
	// '\x00' in the implementation of the *WithLimit function. The rest is the
	// same as in the implementation of LockTableSingleKey. The BytesNext is to
	// construct the exclusive roachpb.Key as mentioned earlier. The
	// implementation of *WithLimit (in pebbleIterator), has to additionally
	// append '\x00' (the sentinel byte) to construct an encoded EngineKey with
	// an empty version.
	keyLen :=
		len(keys.LocalRangeLockTablePrefix) + len(keys.LockTableSingleKeyInfix) + len(key) + 3 + 2
	if cap(i.intentLimitKeyBuf) < keyLen {
		i.intentLimitKeyBuf = make([]byte, 0, keyLen)
	}
	_, i.intentLimitKeyBuf = keys.LockTableSingleKey(key, i.intentLimitKeyBuf)
	// To construct the exclusive limitKey, roachpb.BytesNext gives us a
	// tight limit. Since it appends \x00, this is not decodable, except at
	// the Pebble level, which is all we need here. We don't actually use
	// BytesNext since it tries not to overwrite the slice.
	i.intentLimitKeyBuf = append(i.intentLimitKeyBuf, '\x00')
	return i.intentLimitKeyBuf
}

// makeLowerLimitKey uses the current value of i.iterKey.Key (and assumes
// i.iterValid=true), to construct an inclusive lower limit roachpb.Key that
// will include the intent for i.iterKey.Key.
func (i *intentInterleavingIter) makeLowerLimitKey() roachpb.Key {
	key := i.iterKey.Key
	// The +1 is to account for the need to append a '\x00' in the
	// implementation of the *WithLimit function. The rest is the same as in the
	// implementation of LockTableSingleKey.  The implementation of *WithLimit
	// (in pebbleIterator), has to additionally append '\x00' (the sentinel
	// byte) to construct an encoded EngineKey with an empty version.
	keyLen :=
		len(keys.LocalRangeLockTablePrefix) + len(keys.LockTableSingleKeyInfix) + len(key) + 3 + 1
	if cap(i.intentLimitKeyBuf) < keyLen {
		i.intentLimitKeyBuf = make([]byte, 0, keyLen)
	}
	_, i.intentLimitKeyBuf = keys.LockTableSingleKey(key, i.intentLimitKeyBuf)
	return i.intentLimitKeyBuf
}

func (i *intentInterleavingIter) SeekGE(key MVCCKey) {
	i.dir = +1
	i.valid = true
	i.err = nil

	if i.constraint != notConstrained {
		i.checkConstraint(key.Key, false)
	}
	i.iter.SeekGE(key)
	if err := i.tryDecodeKey(); err != nil {
		return
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
		var limitKey roachpb.Key
		if i.iterValid && !i.prefix {
			limitKey = i.makeUpperLimitKey()
		}
		iterState, err := i.intentIter.SeekEngineKeyGEWithLimit(EngineKey{Key: intentSeekKey}, limitKey)
		if err = i.tryDecodeLockKey(iterState, err); err != nil {
			return
		}
	}
	i.computePos()
}

func (i *intentInterleavingIter) SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID) {
	i.dir = +1
	i.valid = true

	if i.constraint != notConstrained {
		i.checkConstraint(key, false)
	}
	i.iter.SeekGE(MVCCKey{Key: key})
	if err := i.tryDecodeKey(); err != nil {
		return
	}
	var engineKey EngineKey
	engineKey, i.intentKeyBuf = LockTableKey{
		Key:      key,
		Strength: lock.Exclusive,
		TxnUUID:  txnUUID[:],
	}.ToEngineKey(i.intentKeyBuf)
	var limitKey roachpb.Key
	if i.iterValid && !i.prefix {
		limitKey = i.makeUpperLimitKey()
	}
	iterState, err := i.intentIter.SeekEngineKeyGEWithLimit(engineKey, limitKey)
	if err = i.tryDecodeLockKey(iterState, err); err != nil {
		return
	}
	i.computePos()
}

func (i *intentInterleavingIter) checkConstraint(k roachpb.Key, isExclusiveUpper bool) {
	kConstraint := constrainedToGlobal
	if isLocal(k) {
		if bytes.Compare(k, keys.LocalRangeLockTablePrefix) > 0 {
			panic(fmt.Sprintf("intentInterleavingIter cannot be used with invalid local keys %s",
				k.String()))
		}
		kConstraint = constrainedToLocal
	} else if isExclusiveUpper && bytes.Equal(k, keys.LocalMax) {
		kConstraint = constrainedToLocal
	}
	if kConstraint != i.constraint {
		panic(fmt.Sprintf(
			"iterator with constraint=%d is being used with key %s that has constraint=%d",
			i.constraint, k.String(), kConstraint))
	}
}

func (i *intentInterleavingIter) tryDecodeKey() error {
	i.iterValid, i.err = i.iter.Valid()
	if i.iterValid {
		i.iterKey = i.iter.UnsafeKey()
	}
	if i.err != nil {
		i.valid = false
	}
	return i.err
}

// Assumes that i.err != nil. And i.iterValid and i.iterKey are up to date.
func (i *intentInterleavingIter) computePos() {
	if !i.iterValid && i.intentKey == nil {
		i.valid = false
		return
	}
	// INVARIANT: i.iterValid || i.intentKey != nil
	if !i.iterValid {
		i.intentCmp = -i.dir
		return
	}
	if i.intentKey == nil {
		i.intentCmp = i.dir
	} else {
		i.intentCmp = i.intentKey.Compare(i.iterKey.Key)
	}
}

func (i *intentInterleavingIter) tryDecodeLockKey(
	iterState pebble.IterValidityState, err error,
) error {
	if err != nil {
		i.err = err
		i.valid = false
		return err
	}
	i.intentIterState = iterState
	if iterState != pebble.IterValid {
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
			i.iter.Next()
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			var limitKey roachpb.Key
			if i.iterValid && !i.prefix {
				limitKey = i.makeUpperLimitKey()
			}
			iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
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
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			i.intentCmp = 0
			if !i.iterValid {
				i.err = errors.Errorf("intent has no provisional value")
				i.valid = false
				return
			}
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
			var limitKey roachpb.Key
			if !i.prefix {
				limitKey = i.makeUpperLimitKey()
			}
			iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			i.intentCmp = +1
			if util.RaceEnabled && iterState == pebble.IterValid {
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
		if !i.iterValid {
			i.err = errors.Errorf("iter expected to be at provisional value, but is exhausted")
			i.valid = false
			return
		}
		var limitKey roachpb.Key
		if !i.prefix {
			limitKey = i.makeUpperLimitKey()
		}
		iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
		if err = i.tryDecodeLockKey(iterState, err); err != nil {
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
		if err := i.tryDecodeKey(); err != nil {
			return
		}
		if i.intentIterState == pebble.IterAtLimit && i.iterValid && !i.prefix {
			// TODO(sumeer): could avoid doing this if i.iter has stepped to
			// different version of same key.
			limitKey := i.makeUpperLimitKey()
			iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
		}
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
		// Step the iter to NextKey(), i.e., past all the versions of this key.
		i.iter.NextKey()
		if err := i.tryDecodeKey(); err != nil {
			return
		}
		var limitKey roachpb.Key
		if i.iterValid && !i.prefix {
			limitKey = i.makeUpperLimitKey()
		}
		iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
		if err := i.tryDecodeLockKey(iterState, err); err != nil {
			return
		}
		i.computePos()
		return
	}
	// The iterator is positioned at iter. It could be a value or an intent,
	// though usually it will be a value.
	// Step the iter to NextKey(), i.e., past all the versions of this key.
	i.iter.NextKey()
	if err := i.tryDecodeKey(); err != nil {
		return
	}
	if i.intentIterState == pebble.IterAtLimit && i.iterValid && !i.prefix {
		limitKey := i.makeUpperLimitKey()
		iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
		if err = i.tryDecodeLockKey(iterState, err); err != nil {
			return
		}
	}
	i.computePos()
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
	*i = intentInterleavingIter{
		intentKeyAsNoTimestampMVCCKeyBacking: i.intentKeyAsNoTimestampMVCCKeyBacking,
		intentKeyBuf:                         i.intentKeyBuf,
		intentLimitKeyBuf:                    i.intentLimitKeyBuf,
	}
	intentInterleavingIterPool.Put(i)
}

func (i *intentInterleavingIter) SeekLT(key MVCCKey) {
	i.dir = -1
	i.valid = true
	i.err = nil

	if i.prefix {
		i.err = errors.Errorf("prefix iteration is not permitted with SeekLT")
		i.valid = false
		return
	}
	if i.constraint != notConstrained {
		i.checkConstraint(key.Key, true)
		if i.constraint == constrainedToLocal && bytes.Equal(key.Key, keys.LocalMax) {
			// Move it down to below the lock table so can iterate down cleanly into
			// the local key space. Note that we disallow anyone using a seek key
			// that is a local key above the lock table, and there should no keys in
			// the engine there either (at least not keys that we need to see using
			// an MVCCIterator).
			key.Key = keys.LocalRangeLockTablePrefix
		}
	}

	i.iter.SeekLT(key)
	if err := i.tryDecodeKey(); err != nil {
		return
	}
	var intentSeekKey roachpb.Key
	if key.Timestamp.IsEmpty() {
		// Common case.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key, i.intentKeyBuf)
	} else {
		// Seeking to a specific version, so need to see the intent. Since we need
		// to see the intent for key.Key, and we don't have SeekLE, call Next() on
		// the key before doing SeekLT.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key.Next(), i.intentKeyBuf)
	}
	var limitKey roachpb.Key
	if i.iterValid {
		limitKey = i.makeLowerLimitKey()
	}
	iterState, err := i.intentIter.SeekEngineKeyLTWithLimit(EngineKey{Key: intentSeekKey}, limitKey)
	if err = i.tryDecodeLockKey(iterState, err); err != nil {
		return
	}
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
			i.iter.Prev()
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			var limitKey roachpb.Key
			if i.iterValid {
				limitKey = i.makeLowerLimitKey()
			}
			iterState, err := i.intentIter.PrevEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
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
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			i.intentCmp = +1
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
			limitKey := i.makeLowerLimitKey()
			iterState, err := i.intentIter.PrevEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
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
		var limitKey roachpb.Key
		if i.iterValid {
			limitKey = i.makeLowerLimitKey()
		}
		intentIterState, err := i.intentIter.PrevEngineKeyWithLimit(limitKey)
		if err = i.tryDecodeLockKey(intentIterState, err); err != nil {
			return
		}
		if !i.iterValid {
			// It !i.iterValid, the intentIter can no longer be valid either.
			// Note that limitKey is nil in this case.
			if intentIterState != pebble.IterExhausted {
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
		if err := i.tryDecodeKey(); err != nil {
			return
		}
		if i.intentIterState == pebble.IterAtLimit && i.iterValid {
			// TODO(sumeer): could avoid doing this if i.iter has stepped to
			// different version of same key.
			limitKey := i.makeLowerLimitKey()
			iterState, err := i.intentIter.PrevEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
		}
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
	// Preceding call to SetUpperBound has confirmed that key != nil.
	if i.constraint != notConstrained {
		i.checkConstraint(key, true)
	}
	var intentUpperBound roachpb.Key
	intentUpperBound, i.intentKeyBuf = keys.LockTableSingleKey(key, i.intentKeyBuf)
	i.intentIter.SetUpperBound(intentUpperBound)
}

func (i *intentInterleavingIter) Stats() IteratorStats {
	stats := i.iter.Stats()
	intentStats := i.intentIter.Stats()
	stats.InternalDeleteSkippedCount += intentStats.InternalDeleteSkippedCount
	stats.TimeBoundNumSSTs += intentStats.TimeBoundNumSSTs
	for i := pebble.IteratorStatsKind(0); i < pebble.NumStatsKind; i++ {
		stats.Stats.ForwardSeekCount[i] += intentStats.Stats.ForwardSeekCount[i]
		stats.Stats.ReverseSeekCount[i] += intentStats.Stats.ReverseSeekCount[i]
		stats.Stats.ForwardStepCount[i] += intentStats.Stats.ForwardStepCount[i]
		stats.Stats.ReverseStepCount[i] += intentStats.Stats.ReverseStepCount[i]
	}
	return stats
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

// unsageMVCCIterator is used in RaceEnabled test builds to randomly inject
// changes to unsafe keys retrieved from MVCCIterators.
type unsafeMVCCIterator struct {
	MVCCIterator
	keyBuf        []byte
	rawKeyBuf     []byte
	rawMVCCKeyBuf []byte
}

func wrapInUnsafeIter(iter MVCCIterator) MVCCIterator {
	return &unsafeMVCCIterator{MVCCIterator: iter}
}

var _ MVCCIterator = &unsafeMVCCIterator{}

func (i *unsafeMVCCIterator) SeekGE(key MVCCKey) {
	i.mangleBufs()
	i.MVCCIterator.SeekGE(key)
}

func (i *unsafeMVCCIterator) Next() {
	i.mangleBufs()
	i.MVCCIterator.Next()
}

func (i *unsafeMVCCIterator) NextKey() {
	i.mangleBufs()
	i.MVCCIterator.NextKey()
}

func (i *unsafeMVCCIterator) SeekLT(key MVCCKey) {
	i.mangleBufs()
	i.MVCCIterator.SeekLT(key)
}

func (i *unsafeMVCCIterator) Prev() {
	i.mangleBufs()
	i.MVCCIterator.Prev()
}

func (i *unsafeMVCCIterator) UnsafeKey() MVCCKey {
	rv := i.MVCCIterator.UnsafeKey()
	i.keyBuf = append(i.keyBuf[:0], rv.Key...)
	rv.Key = i.keyBuf
	return rv
}

func (i *unsafeMVCCIterator) UnsafeRawKey() []byte {
	rv := i.MVCCIterator.UnsafeRawKey()
	i.rawKeyBuf = append(i.rawKeyBuf[:0], rv...)
	return i.rawKeyBuf
}

func (i *unsafeMVCCIterator) UnsafeRawMVCCKey() []byte {
	rv := i.MVCCIterator.UnsafeRawMVCCKey()
	i.rawMVCCKeyBuf = append(i.rawMVCCKeyBuf[:0], rv...)
	return i.rawMVCCKeyBuf
}

func (i *unsafeMVCCIterator) mangleBufs() {
	if rand.Intn(2) == 0 {
		for _, b := range [3][]byte{i.keyBuf, i.rawKeyBuf, i.rawMVCCKeyBuf} {
			for i := range b {
				b[i] = 0
			}
		}
	}
}
