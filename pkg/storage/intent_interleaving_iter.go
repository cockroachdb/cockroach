// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// wrappableReader is used to implement a wrapped Reader. A wrapped Reader
// should be used and immediately discarded. It maintains no state of its own
// between calls.
// Why do we not keep the wrapped reader as a member in the caller? Because
// different methods on Reader can need different wrappings depending on what
// they want to observe.
//
// TODO(sumeer): for allocation optimization we could expose a scratch space
// struct that the caller keeps on behalf of the wrapped reader. But can only
// do such an optimization when know that the wrappableReader will be used
// with external synchronization that prevents preallocated buffers from being
// modified concurrently. pebbleBatch.{MVCCGet,MVCCGetProto} have MVCCKey
// serialization allocation optimizations which we can't do below. But those
// are probably not performance sensitive, since the performance sensitive
// code probably uses an MVCCIterator.
type wrappableReader interface {
	Reader
}

// wrapReader wraps the provided reader, to return an implementation of MVCCIterator
// that supports MVCCKeyAndIntentsIterKind.
func wrapReader(r wrappableReader) *intentInterleavingReader {
	iiReader := intentInterleavingReaderPool.Get().(*intentInterleavingReader)
	*iiReader = intentInterleavingReader{wrappableReader: r}
	return iiReader
}

type intentInterleavingReader struct {
	wrappableReader
}

var _ Reader = &intentInterleavingReader{}

var intentInterleavingReaderPool = sync.Pool{
	New: func() interface{} {
		return &intentInterleavingReader{}
	},
}

// NewMVCCIterator implements the Reader interface. The
// intentInterleavingReader can be freed once this method returns.
func (imr *intentInterleavingReader) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	if (!opts.MinTimestamp.IsEmpty() || !opts.MaxTimestamp.IsEmpty()) &&
		iterKind == MVCCKeyAndIntentsIterKind {
		panic("cannot ask for interleaved intents when specifying timestamp hints")
	}
	if iterKind == MVCCKeyIterKind || opts.KeyTypes == IterKeyTypeRangesOnly {
		return imr.wrappableReader.NewMVCCIterator(ctx, MVCCKeyIterKind, opts)
	}
	return newIntentInterleavingIterator(ctx, imr.wrappableReader, opts)
}

func (imr *intentInterleavingReader) Free() {
	*imr = intentInterleavingReader{}
	intentInterleavingReaderPool.Put(imr)
}

type intentInterleavingIterConstraint int8

const (
	notConstrained intentInterleavingIterConstraint = iota
	constrainedToLocal
	constrainedToGlobal
)

// intentInterleavingIter makes separated intents appear as interleaved. It
// relies on the following assumptions:
//   - There can be no physically interleaved intents, i.e., all intents are
//     separated (in the lock table keyspace).
//   - An intent will have a corresponding provisional value.
//   - The only single key locks in the lock table key space are intents.
//
// Semantically, the functionality is equivalent to merging two MVCCIterators:
//   - A MVCCIterator on the MVCC key space.
//   - A MVCCIterator constructed by wrapping an EngineIterator on the lock table
//     key space where the EngineKey is transformed into the corresponding
//     intent key and appears as MVCCKey{Key: intentKey}.
//
// The implementation below is specialized to reduce unnecessary comparisons
// and iteration, by utilizing the aforementioned assumptions. The intentIter
// iterates over the lock table key space and iter over the MVCC key space.
// They are kept synchronized in the following way (for forward iteration):
//   - At the same MVCCKey.Key: the intentIter is at the intent and iter at the
//     provisional value.
//   - At different MVCCKey.Keys: the intentIter is ahead of iter, at the first
//     key after iter's MVCCKey.Key that has an intent.
//
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
// violates that in a subsequent SeekGE/SeekLT call.
//
// intentInterleavingIter ignores locks in the lock table keyspace with
// strengths other than lock.Intent (i.e. shared and exclusive locks). Future
// versions of the iterator may expose information to users about whether any
// non-intent locks were observed and, if so, which keys they were found on. For
// now, no such information is exposed.
type intentInterleavingIter struct {
	prefix     bool
	constraint intentInterleavingIterConstraint

	// iter is for iterating over MVCC keys.
	iter *pebbleIterator // MVCCIterator
	// The valid value from iter.Valid() after the last positioning call.
	iterValid bool
	// When iterValid = true, this contains the result of iter.UnsafeKey(). We
	// store it here to avoid repeatedly calling UnsafeKey() since it repeats
	// key parsing.
	iterKey MVCCKey

	// intentIter is for iterating over the lock table keyspace and finding
	// intents, so that intentInterleavingIter can make them look as if they
	// were interleaved.
	intentIter      *LockTableIterator // EngineIterator
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
	// When intentCmp == 0, this will be set to indicate whether iter is on an
	// unversioned position on a bare range key copositioned with the intent.
	// This will never happen in the forward direction due to
	// maybeSkipIntentRangeKey(). In the reverse direction, if an intent is
	// located on the start key of an overlapping range key, then we cannot step
	// iter past the range key to satisfy the usual intentCmp > 0 condition,
	// because we need the range keys to be exposed via e.g. RangeKeys(). We
	// therefore also have to consider isCurAtIntentIter to be true when iter is
	// positioned on a bare unversioned range key colocated with an intent,
	// i.e. i.dir < 0 && i.intentCmp == 0 && i.iterBareRangeAtIntent.
	//
	// NB: This value is not valid for intentCmp != 0.
	iterBareRangeAtIntent bool
	// rangeKeyChanged keeps track of RangeKeyChanged() for the current
	// iterator position. This can't simply call through to the parent
	// iterator for two reasons:
	//
	// - maybeSkipIntentRangeKey() may step the iterator forward from
	//   a bare range key onto a provisional value, which would cause
	//   RangeKeyChanged() to return false rather than true.
	//
	// - reverse iteration may prematurely move onto a range key when
	//   positioned on an intent not overlapping the range key.
	rangeKeyChanged bool
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
	return k.Compare(keys.LocalMax) < 0
}

func newIntentInterleavingIterator(
	ctx context.Context, reader Reader, opts IterOptions,
) (MVCCIterator, error) {
	if !opts.MinTimestamp.IsEmpty() || !opts.MaxTimestamp.IsEmpty() {
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

	// There cannot be any range keys across the lock table, so create the intent
	// iterator for point keys only, or return a normal MVCC iterator if only
	// range keys are requested.
	if opts.KeyTypes == IterKeyTypeRangesOnly {
		return reader.NewMVCCIterator(ctx, MVCCKeyIterKind, opts)
	}

	iiIter := intentInterleavingIterPool.Get().(*intentInterleavingIter)
	intentKeyBuf := iiIter.intentKeyBuf
	intentLimitKeyBuf := iiIter.intentLimitKeyBuf

	ltOpts := LockTableIteratorOptions{
		Prefix: opts.Prefix, MatchMinStr: lock.Intent, ReadCategory: opts.ReadCategory}
	if opts.LowerBound != nil {
		ltOpts.LowerBound, intentKeyBuf = keys.LockTableSingleKey(opts.LowerBound, intentKeyBuf)
	} else if !opts.Prefix {
		// Make sure we don't step outside the lock table key space. Note that
		// this is the case where the lower bound was not set and
		// constrainedToLocal.
		ltOpts.LowerBound = keys.LockTableSingleKeyStart
	}
	if opts.UpperBound != nil {
		ltOpts.UpperBound, intentLimitKeyBuf =
			keys.LockTableSingleKey(opts.UpperBound, intentLimitKeyBuf)
	} else if !opts.Prefix {
		// Make sure we don't step outside the lock table key space. Note that
		// this is the case where the upper bound was not set and
		// constrainedToGlobal.
		ltOpts.UpperBound = keys.LockTableSingleKeyEnd
	}

	// Note that we can reuse intentKeyBuf, intentLimitKeyBuf after
	// NewLockTableIter returns.
	intentIter, err := NewLockTableIterator(ctx, reader, ltOpts)
	if err != nil {
		return nil, err
	}

	// The creation of these iterators can race with concurrent mutations, which
	// may make them inconsistent with each other. So we clone here, to ensure
	// consistency (certain Reader implementations already ensure consistency,
	// and we use that when possible to save allocations).
	var iter *pebbleIterator
	if reader.ConsistentIterators() {
		mvccIter, err := reader.NewMVCCIterator(ctx, MVCCKeyIterKind, opts)
		if err != nil {
			return nil, err
		}
		iter = maybeUnwrapUnsafeIter(mvccIter).(*pebbleIterator)
	} else {
		iter = newPebbleIteratorByCloning(ctx, intentIter.CloneContext(), opts, StandardDurability)
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
	return iiIter, nil
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

// maybeSkipIntentRangeKey will step iter once forwards if iter is positioned on
// a bare range key with the same key position (either start key or seek key) as
// the current intentIter intent.
//
// This is necessary when intentIter lands on a new intent, to ensure iter is
// positioned on the provisional value instead of the bare range key. This must
// be done after positioning both iterators.
//
// NB: This is called before computePos(), and can't rely on intentCmp.
//
// REQUIRES: i.dir > 0
//
// gcassert:inline
func (i *intentInterleavingIter) maybeSkipIntentRangeKey() error {
	if util.RaceEnabled && i.dir < 0 {
		i.err = errors.AssertionFailedf("maybeSkipIntentRangeKey called in reverse")
		i.valid = false
		return i.err
	}
	if i.iterValid && i.intentKey != nil {
		return i.doMaybeSkipIntentRangeKey()
	}
	return nil
}

// doMaybeSkipIntentRangeKey is a helper for maybeSkipIntentRangeKey(), which
// allows mid-stack inlining of the former.
func (i *intentInterleavingIter) doMaybeSkipIntentRangeKey() error {
	if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
		// iter may be on a bare range key that will cover the provisional value,
		// in which case we can step onto it. We guard against emitting the wrong
		// range key for the intent if the provisional value turns out to be
		// missing by:
		//
		// 1. Before we step, make sure iter isn't ahead of intentIter. We have
		//    to do a key comparison anyway in case intentIter is ahead of iter.
		// 2. After we step, make sure we're on a point key covered by a range key.
		//    We don't need a key comparison (but do so under race), because if
		//    the provisional value is missing then we'll either land on a
		//    different point key below the range key (which will emit the
		//    correct range key), or we'll land on a different bare range key.
		//
		// TODO(erikgrinaker): in cases where we don't step iter, we can save
		// the result of the comparison in i.intentCmp to avoid another one.
		if intentCmp := i.intentKey.Compare(i.iterKey.Key); intentCmp < 0 {
			i.err = errors.Errorf("iter ahead of provisional value for intent %s (at %s)",
				i.intentKey, i.iterKey)
			i.valid = false
			return i.err
		} else if intentCmp == 0 {
			i.iter.Next()
			if err := i.tryDecodeKey(); err != nil {
				return err
			}
			hasPoint, hasRange = i.iter.HasPointAndRange()
			if !hasPoint || !hasRange {
				i.err = errors.Errorf("iter not on provisional value for intent %s", i.intentKey)
				i.valid = false
				return i.err
			}
		}
	}
	return nil
}

// maybeSuppressRangeKeyChanged will suppress i.rangeKeyChanged in the reverse
// direction if the underlying iterator has moved past an intent onto a
// different range key that should not be surfaced yet. Must be called after
// computePos().
//
// gcassert:inline
func (i *intentInterleavingIter) maybeSuppressRangeKeyChanged() {
	if util.RaceEnabled && i.dir > 0 {
		panic(errors.AssertionFailedf("maybeSuppressRangeKeyChanged called in forward direction"))
	}
	// NB: i.intentCmp implies isCurAtIntentIterReverse(), but cheaper.
	if i.rangeKeyChanged && i.intentCmp > 0 {
		i.doMaybeSuppressRangeKeyChanged()
	}
}

// doMaybeSuppressRangeKeyChanged is a helper for maybeSuppressRangeKeyChanged
// which allows mid-stack inlining of the former.
func (i *intentInterleavingIter) doMaybeSuppressRangeKeyChanged() {
	i.rangeKeyChanged = i.iter.RangeBounds().EndKey.Compare(i.intentKey) > 0
}

// shouldAdjustSeekRangeKeyChanged returns true if a seek (any kind) needs to
// adjust the RangeKeyChanged signal from the underlying iter. This is necessary
// when intentInterleavingIter was previously positioned on an intent in the
// reverse direction, with iter positioned on a previous range key that did
// not overlap the point key (suppressed via suppressRangeKeyChanged).
//
// In this case, a seek may incorrectly emit or omit a RangeKeyChanged signal
// when iter is seeked, since it's relative to iter's former position rather
// than the intent's (and thus intentInterleavingIter's) position.
//
// This situation is only possible when the intent does not overlap any range
// keys (otherwise, iter would either have stopped at a point key which overlaps
// the same range key, or at the range key's start bound). Thus, when this
// situation occurs, RangeKeyChanged must be set equal to iter's hasRange value
// after the seek: we know we were not previously positioned on a range key, so
// if hasRange is true then RangeKeyChanged must be true (we seeked onto a range
// key), and if hasRange is false then RangeKeyChanged must be false (we did not
// seek onto a range key).
//
// gcassert:inline
func (i *intentInterleavingIter) shouldAdjustSeekRangeKeyChanged() bool {
	if i.dir == -1 && i.intentCmp > 0 && i.valid && i.iterValid {
		return i.doShouldAdjustSeekRangeKeyChanged()
	}
	return false
}

// doShouldAdjustSeekRangeKeyChanged is a shouldAdjustSeekRangeKeyChanged
// helper, which allows mid-stack inlining of the former.
func (i *intentInterleavingIter) doShouldAdjustSeekRangeKeyChanged() bool {
	if _, iterHasRange := i.iter.HasPointAndRange(); iterHasRange {
		if _, hasRange := i.HasPointAndRange(); !hasRange {
			return true
		}
	}
	return false
}

// adjustSeekRangeKeyChanged adjusts i.rangeKeyChanged as described in
// shouldAdjustSeekRangeKeyChanged.
func (i *intentInterleavingIter) adjustSeekRangeKeyChanged() {
	if i.iterValid {
		_, hasRange := i.iter.HasPointAndRange()
		i.rangeKeyChanged = hasRange
	} else {
		i.rangeKeyChanged = false
	}
}

func (i *intentInterleavingIter) SeekGE(key MVCCKey) {
	adjustRangeKeyChanged := i.shouldAdjustSeekRangeKeyChanged()

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
	i.rangeKeyChanged = i.iter.RangeKeyChanged()
	if adjustRangeKeyChanged {
		i.adjustSeekRangeKeyChanged()
	}
	var intentSeekKey roachpb.Key
	if key.Timestamp.IsEmpty() {
		// Common case.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleKey(key.Key, i.intentKeyBuf)
	} else if !i.prefix {
		// Seeking to a specific version, so go past the intent.
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleNextKey(key.Key, i.intentKeyBuf)
	} else {
		// Else seeking to a particular version and using prefix iteration,
		// so don't expect to ever see the intent. NB: intentSeekKey is nil.
		i.intentKey = nil
	}
	if !i.iterValid && i.prefix {
		// The prefix seek below will also certainly fail, as we didn't find an
		// MVCC value here.
		intentSeekKey = nil
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
		if err := i.maybeSkipIntentRangeKey(); err != nil {
			return
		}
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
		if i.intentCmp == 0 {
			// We have to handle the case where intentIter is on an intent and iter is
			// on a bare range key at the same key position.
			//
			// In the forward direction, this should never happen: the caller should
			// have called maybeSkipIntentRangeKey() to step onto the provisional
			// value (or a later key, if the provisional value is absent, which we
			// will check later). The provisional value will be covered by the same
			// range keys as the intent.
			//
			// In the reverse direction, there are two cases:
			//
			// In the typical case, iter will be on the range key's unversioned start
			// key. We cannot move past this to satisfy intentCmp < 0 (the usual
			// condition for isCurAtIntentIter), because we need to expose those range
			// keys via e.g. RangeKeys().
			//
			// However, there is also the case where we're on a versioned key position
			// following a versioned SeekGE call, i.e. we're in the middle of
			// switching directions during a Prev() call. For example, we're on
			// position b@3 of [a-c)@3 with an intent at b. In this case, we should
			// not be considered located on the intent yet -- we'll land on it after a
			// subsequent Prev() call.
			//
			// We track this as iterBareRangeKeyAtIntent, assuming intentCmp == 0:
			//
			// hasRange && !hasPoint && Timestamp.IsEmpty()
			if i.dir > 0 {
				i.iterBareRangeAtIntent = false
			} else {
				hasPoint, hasRange := i.iter.HasPointAndRange()
				i.iterBareRangeAtIntent = !hasPoint && hasRange && i.iterKey.Timestamp.IsEmpty()
			}
		}
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
	if util.RaceEnabled && i.valid {
		if err := i.assertInvariants(); err != nil {
			return false, err
		}
	}
	return i.valid, i.err
}

func (i *intentInterleavingIter) Next() {
	if i.err != nil {
		return
	}
	if i.dir < 0 {
		// Switching from reverse to forward iteration.
		if util.RaceEnabled && i.prefix {
			panic(errors.AssertionFailedf("dir < 0 with prefix iteration"))
		}
		isCurAtIntent := i.isCurAtIntentIterReverse()
		i.dir = +1
		if !i.valid {
			// Both iterators are exhausted. We know that this is non-prefix
			// iteration, as reverse iteration is not supported with prefix
			// iteration. Since intentKey is synchronized with intentIter for
			// non-prefix iteration, step both forward.
			i.valid = true
			i.iter.Next()
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			i.rangeKeyChanged = i.iter.RangeKeyChanged()
			var limitKey roachpb.Key
			if i.iterValid {
				limitKey = i.makeUpperLimitKey()
			}
			iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			if err := i.maybeSkipIntentRangeKey(); err != nil {
				return
			}
			i.computePos()
			return
		}
		// At least one of the iterators is not exhausted.
		if isCurAtIntent {
			// Reverse iteration was positioned at the intent, so either (a) iter
			// precedes the intentIter, so must be at the lowest version of the
			// preceding key or exhausted, or (b) iter is at a bare range key whose
			// start key is colocated with the intent.
			// Step iter forward. It will now point to
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
			i.rangeKeyChanged = i.iter.RangeKeyChanged()
			i.intentCmp = 0
			if !i.iterValid {
				i.err = errors.Errorf("intent has no provisional value")
				i.valid = false
				return
			}
			if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
				// If there was a bare range key before the provisional value, iter
				// would have been positioned there prior to the i.iter.Next() call,
				// so it must now be at the provisional value, but it is not.
				i.err = errors.Errorf("intent has no provisional value")
				i.valid = false
				return
			}
		} else {
			// The intentIter precedes the iter. It could be for the same key, iff
			// this key has an intent, or an earlier key. Either way, stepping
			// forward will take it to an intent for a later key.
			limitKey := i.makeUpperLimitKey()
			iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			// NB: doesn't need maybeSkipIntentRangeKey() as intentCmp > 0.
			i.intentCmp = +1
		}
		// INVARIANT: i.valid
	}
	if !i.valid {
		return
	}
	if i.isCurAtIntentIterForward() {
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
		i.rangeKeyChanged = false // already surfaced at the intent
		// NB: doesn't need maybeSkipIntentRangeKey() as intentCmp > 0.
		i.intentCmp = +1
	} else {
		// Common case:
		// The iterator is positioned at iter, at an MVCC value.
		i.iter.Next()
		if err := i.tryDecodeKey(); err != nil {
			return
		}
		i.rangeKeyChanged = i.iter.RangeKeyChanged()
		if i.intentIterState == pebble.IterAtLimit && i.iterValid && !i.prefix {
			// TODO(sumeer): could avoid doing this if i.iter has stepped to
			// different version of same key.
			limitKey := i.makeUpperLimitKey()
			iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
		}
		// Whether we stepped the intentIter or not, we have stepped iter, and
		// iter could now be at a bare range key that is equal to the intentIter
		// key.
		if err := i.maybeSkipIntentRangeKey(); err != nil {
			return
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
	if i.isCurAtIntentIterForward() {
		// The iterator is positioned at an intent in intentIter. iter must be
		// positioned at the provisional value.
		if i.intentCmp != 0 {
			i.err = errors.Errorf("intentIter at intent, but iter not at provisional value")
			i.valid = false
			return
		}
		// Step the iter to NextKey(), i.e., past all the versions of this key.
		// Note that iter may already be exhausted, in which case calling NextKey
		// is a no-op.
		i.iter.NextKey()
		if err := i.tryDecodeKey(); err != nil {
			return
		}
		i.rangeKeyChanged = i.iter.RangeKeyChanged()
		var limitKey roachpb.Key
		if i.iterValid && !i.prefix {
			limitKey = i.makeUpperLimitKey()
		}
		iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
		if err := i.tryDecodeLockKey(iterState, err); err != nil {
			return
		}
		if err := i.maybeSkipIntentRangeKey(); err != nil {
			return
		}
		i.computePos()
		return
	}
	// Common case:
	// The iterator is positioned at iter, i.e., at a MVCC value.
	// Step the iter to NextKey(), i.e., past all the versions of this key.
	i.iter.NextKey()
	if err := i.tryDecodeKey(); err != nil {
		return
	}
	i.rangeKeyChanged = i.iter.RangeKeyChanged()
	if i.intentIterState == pebble.IterAtLimit && i.iterValid && !i.prefix {
		limitKey := i.makeUpperLimitKey()
		iterState, err := i.intentIter.NextEngineKeyWithLimit(limitKey)
		if err = i.tryDecodeLockKey(iterState, err); err != nil {
			return
		}
	}
	if err := i.maybeSkipIntentRangeKey(); err != nil {
		return
	}
	i.computePos()
}

// TODO(erikgrinaker): Consider computing this once and storing it as a struct
// field when repositioning the iterator, instead of repeatedly calling it. The
// forward/reverse methods are called at least once per step, with two more
// calls for UnsafeKey() and UnsafeValue(), and this has a measurable cost
// (especially in the reverse direction).
//
// gcassert:inline
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
	//   - intentCmp > 0. Returns true.
	//   - intentCmp = 0. Returns false unless copositioned with bare range key.
	//   - intentCmp < 0. Returns false.
	return (i.dir > 0 && i.isCurAtIntentIterForward()) || (i.dir < 0 && i.isCurAtIntentIterReverse())
}

// gcassert:inline
func (i *intentInterleavingIter) isCurAtIntentIterForward() bool {
	return i.intentCmp <= 0
}

// gcassert:inline
func (i *intentInterleavingIter) isCurAtIntentIterReverse() bool {
	return i.intentCmp > 0 || (i.intentCmp == 0 && i.iterBareRangeAtIntent)
}

func (i *intentInterleavingIter) UnsafeKey() MVCCKey {
	if i.isCurAtIntentIter() {
		return MVCCKey{Key: i.intentKey}
	}
	return i.iterKey
}

func (i *intentInterleavingIter) UnsafeValue() ([]byte, error) {
	if i.isCurAtIntentIter() {
		return i.intentIter.UnsafeValue()
	}
	return i.iter.UnsafeValue()
}

func (i *intentInterleavingIter) UnsafeLazyValue() pebble.LazyValue {
	if i.isCurAtIntentIter() {
		return i.intentIter.UnsafeLazyValue()
	}
	return i.iter.UnsafeLazyValue()
}

func (i *intentInterleavingIter) MVCCValueLenAndIsTombstone() (int, bool, error) {
	if i.isCurAtIntentIter() {
		return 0, false, errors.Errorf("not at MVCC value")
	}
	return i.iter.MVCCValueLenAndIsTombstone()
}

func (i *intentInterleavingIter) ValueLen() int {
	if i.isCurAtIntentIter() {
		return i.intentIter.ValueLen()
	}
	return i.iter.ValueLen()
}

func (i *intentInterleavingIter) Value() ([]byte, error) {
	if i.isCurAtIntentIter() {
		return i.intentIter.Value()
	}
	return i.iter.Value()
}

// HasPointAndRange implements SimpleMVCCIterator.
func (i *intentInterleavingIter) HasPointAndRange() (bool, bool) {
	var hasPoint, hasRange bool
	if i.iterValid {
		hasPoint, hasRange = i.iter.HasPointAndRange()
	}
	if i.isCurAtIntentIter() {
		hasPoint = true
		// In the reverse direction, if the intent itself does not overlap a range
		// key, then iter may be positioned on an earlier range key. Otherwise, iter
		// will always be positioned on the correct range key.
		//
		// Note the following implications:
		//
		//   hasRange → i.iterValid
		//   i.isCurAtIntentIter() && i.dir < 0 → i.intentCmp > 0 ||
		//		(i.intentCmp == 0 && i.iterBareRangeAtIntent)
		//
		// TODO(erikgrinaker): consider optimizing this comparison.
		if hasRange && i.dir < 0 {
			hasRange = i.intentCmp == 0 || i.iter.RangeBounds().EndKey.Compare(i.intentKey) > 0
		}
	}
	return hasPoint, hasRange
}

// RangeBounds implements SimpleMVCCIterator.
func (i *intentInterleavingIter) RangeBounds() roachpb.Span {
	return i.iter.RangeBounds()
}

// RangeKeys implements SimpleMVCCIterator.
func (i *intentInterleavingIter) RangeKeys() MVCCRangeKeyStack {
	if _, hasRange := i.HasPointAndRange(); !hasRange {
		return MVCCRangeKeyStack{}
	}
	return i.iter.RangeKeys()
}

// RangeKeyChanged implements SimpleMVCCIterator.
func (i *intentInterleavingIter) RangeKeyChanged() bool {
	return i.rangeKeyChanged
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
	adjustRangeKeyChanged := i.shouldAdjustSeekRangeKeyChanged()

	i.dir = -1
	i.valid = true
	i.err = nil

	if i.prefix {
		i.err = errors.Errorf("prefix iteration is not permitted with SeekLT")
		i.valid = false
		return
	}
	if i.constraint != notConstrained {
		// If the seek key of SeekLT is the boundary between the local and global
		// keyspaces, iterators constrained in either direction are permitted.
		// Iterators constrained to the local keyspace may be scanning from their
		// upper bound. Iterators constrained to the global keyspace may have found
		// a key on the boundary and may now be scanning before the key, using the
		// boundary as an exclusive upper bound.
		// NB: an iterator with bounds [L, U) is allowed to SeekLT over any key in
		// [L, U]. For local keyspace iterators, U can be LocalMax and for global
		// keyspace iterators L can be LocalMax.
		localMax := bytes.Equal(key.Key, keys.LocalMax)
		if !localMax {
			i.checkConstraint(key.Key, true)
		}
		if localMax && i.constraint == constrainedToLocal {
			// Move it down to below the lock table so can iterate down cleanly into
			// the local key space. Note that we disallow anyone using a seek key
			// that is a local key above the lock table, and there should be no keys
			// in the engine there either (at least not keys that we need to see using
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
		intentSeekKey, i.intentKeyBuf = keys.LockTableSingleNextKey(key.Key, i.intentKeyBuf)
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
	i.rangeKeyChanged = i.iter.RangeKeyChanged()
	if adjustRangeKeyChanged {
		i.adjustSeekRangeKeyChanged()
	}
	i.maybeSuppressRangeKeyChanged()
}

func (i *intentInterleavingIter) Prev() {
	if i.err != nil {
		return
	}
	// INVARIANT: !i.prefix
	if i.dir > 0 {
		// Switching from forward to reverse iteration.
		isCurAtIntent := i.isCurAtIntentIterForward()
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
			i.rangeKeyChanged = i.iter.RangeKeyChanged()
			i.maybeSuppressRangeKeyChanged()
			return
		}
		// At least one of the iterators is not exhausted.
		if isCurAtIntent {
			// iter is after the intentIter, so must be at the provisional value.
			// Step it backward. It will now point to a key that is before the
			// intent key, or a range key whose start key is colocated with the
			// intent, or be exhausted.
			//
			// Note that the code below does not specifically care if a bug (external
			// to this code) violates the invariant that the provisional value is the
			// highest timestamp key, but it does care that there is a timestamped
			// value for this key (which it checks below). The internal invariant of
			// this iterator implementation will ensure that iter is pointing to the
			// highest timestamped key.
			if i.intentCmp != 0 {
				i.err = errors.Errorf("iter not at provisional value, cmp: %d", i.intentCmp)
				i.valid = false
				return
			}
			i.iter.Prev()
			if err := i.tryDecodeKey(); err != nil {
				return
			}
			i.computePos()
			// TODO(sumeer): These calls to initialize and suppress rangeKeyChanged
			// are unnecessary since i.valid is true and we will overwrite tnis work
			// later in this function.
			i.rangeKeyChanged = i.iter.RangeKeyChanged()
			i.maybeSuppressRangeKeyChanged()
		} else {
			// The intentIter is after the iter. We don't know whether the iter key
			// has an intent. Note that the iter could itself be positioned at an
			// intent.
			limitKey := i.makeLowerLimitKey()
			iterState, err := i.intentIter.PrevEngineKeyWithLimit(limitKey)
			if err = i.tryDecodeLockKey(iterState, err); err != nil {
				return
			}
			i.computePos()
			// TODO(sumeer): This call to suppress rangeKeyChanged is unnecessary
			// since i.valid is true and we will overwrite tnis work later in this
			// function.
			i.maybeSuppressRangeKeyChanged()
		}
		// INVARIANT: i.valid
	}
	if !i.valid {
		return
	}
	if i.isCurAtIntentIterReverse() {
		// The iterator is positioned at an intent in intentIter, and iter is
		// exhausted, positioned at a versioned value of a preceding key, or
		// positioned on the start of a range key colocated with the intent.
		// Stepping intentIter backward will ensure that intentKey is <= the key
		// of iter (when neither is exhausted), but we may also need to step
		// off the bare range key if there is one, and account for the fact
		// that the range key may have already changed on the intent.
		if i.iterBareRangeAtIntent {
			i.iter.Prev()
			if err := i.tryDecodeKey(); err != nil {
				return
			}
		}
		// Two cases:
		// - i.iterBareRangeAtIntent: we have stepped iter backwards, and since
		//   we will no longer be at the intent, i.iter.RangeKeyChanged() should
		//   be used as the value of i.rangeKeyChanged.
		// - !i.iterBareRangeAtIntent: we have not stepped iter. If the range
		//   bounds of iter covered the current intent, we have already shown them
		//   to the client. So the only reason for i.rangeKeyChanged to be true is
		//   if the range bounds do not cover the current intent. That is the
		//   i.iter.RangeBounds().EndKey.Compare(i.intentKey) <= 0 condition
		//   below.
		i.rangeKeyChanged = i.iter.RangeKeyChanged() && (i.iterBareRangeAtIntent ||
			i.iter.RangeBounds().EndKey.Compare(i.intentKey) <= 0)
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
			i.computePos()
			if i.intentCmp > 0 {
				i.err = errors.Errorf("intentIter should not be after iter")
				i.valid = false
				return
			}
			// INVARIANT: i.intentCmp <= 0. So this call to
			// maybeSuppressRangeKeyChanged() will be a no-op.
			i.maybeSuppressRangeKeyChanged()
		}
	} else {
		// Common case:
		// The iterator is positioned at iter, i.e., at a MVCC value.
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
		i.rangeKeyChanged = i.iter.RangeKeyChanged()
		i.maybeSuppressRangeKeyChanged()
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
	value, err := i.UnsafeValue()
	if err != nil {
		return err
	}
	return protoutil.Unmarshal(value, msg)
}

func (i *intentInterleavingIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	return findSplitKeyUsingIterator(i, start, end, minSplitKey, targetSize)
}

func (i *intentInterleavingIter) Stats() IteratorStats {
	stats := i.iter.Stats()
	intentStats := i.intentIter.Stats()
	stats.Stats.Merge(intentStats.Stats)
	return stats
}

// IsPrefix implements the MVCCIterator interface.
func (i *intentInterleavingIter) IsPrefix() bool {
	return i.prefix
}

// assertInvariants asserts internal iterator invariants, returning an
// AssertionFailedf for any violations. It must be called on a valid iterator
// after a complete state transition.
func (i *intentInterleavingIter) assertInvariants() error {
	// Assert general MVCCIterator invariants.
	if err := assertMVCCIteratorInvariants(i); err != nil {
		return err
	}

	// The underlying iterator must not have errored.
	iterValid, err := i.iter.Valid()
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "valid iter but i.iter errored")
	}
	intentValid := i.intentKey != nil

	// At least one of the iterators must be valid. The iterator's validity state
	// should match i.iterValid.
	if !iterValid && !intentValid {
		return errors.AssertionFailedf("i.valid=%t but both iterators are invalid", i.valid)
	}
	if iterValid != i.iterValid {
		return errors.AssertionFailedf("i.iterValid=%t but i.iter.Valid=%t", i.iterValid, iterValid)
	}

	// i.dir must be either 1 or -1.
	if i.dir != 1 && i.dir != -1 {
		return errors.AssertionFailedf("i.dir=%v is not valid", i.dir)
	}

	// For valid iterators, the stored key must match the iterator key.
	if iterValid {
		if key := i.iter.UnsafeKey(); !i.iterKey.Equal(key) {
			return errors.AssertionFailedf("i.iterKey=%q does not match i.iter.UnsafeKey=%q",
				i.iterKey, key)
		}
	}
	if intentValid {
		intentKey := i.intentKey.Clone()
		if engineKey, err := i.intentIter.UnsafeEngineKey(); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "valid i.intentIter errored")
		} else if !engineKey.IsLockTableKey() {
			return errors.AssertionFailedf("i.intentIter on non-locktable key %s", engineKey)
		} else if key, err := keys.DecodeLockTableSingleKey(engineKey.Key); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode lock table key %s",
				engineKey)
		} else if !intentKey.Equal(key) {
			return errors.AssertionFailedf("i.intentKey %q != i.intentIter.UnsafeEngineKey() %q",
				intentKey, key)
		}
		// If i.intentKey is set (i.e. intentValid is true), then intentIterState
		// must be valid. The inverse is not always true.
		if i.intentIterState != pebble.IterValid {
			return errors.AssertionFailedf("i.intentKey=%q, but i.intentIterState=%v is not IterValid",
				i.intentKey, i.intentIterState)
		}
		// If i.intentKey is set, then i.intentKeyAsNoTimestampMVCCKey must either
		// be nil or equal to it with a \x00 byte appended.
		if i.intentKeyAsNoTimestampMVCCKey != nil &&
			!bytes.Equal(i.intentKeyAsNoTimestampMVCCKey, append(i.intentKey.Clone(), 0)) {
			return errors.AssertionFailedf(
				"i.intentKeyAsNoTimestampMVCCKey=%q differs from i.intentKey=%q",
				i.intentKeyAsNoTimestampMVCCKey, i.intentKey)
		}
	}

	// Check intentCmp depending on the iterator validity. We already know that
	// one of the iterators must be valid.
	if iterValid && intentValid {
		if cmp := i.intentKey.Compare(i.iterKey.Key); i.intentCmp != cmp {
			return errors.AssertionFailedf("i.intentCmp=%v does not match %v for intentKey=%q iterKey=%q",
				i.intentCmp, cmp, i.intentKey, i.iterKey)
		}
	} else if iterValid {
		if i.intentCmp != i.dir {
			return errors.AssertionFailedf("i.intentCmp=%v != i.dir=%v for invalid i.intentIter",
				i.intentCmp, i.dir)
		}
	} else if intentValid {
		if i.intentCmp != -i.dir {
			return errors.AssertionFailedf("i.intentCmp=%v == i.dir=%v for invalid i.iter",
				i.intentCmp, i.dir)
		}
	}

	// When on an intent in the forward direction, we must be on a provisional
	// value and any range key must cover it.
	if i.dir > 0 && i.isCurAtIntentIterForward() {
		if !iterValid {
			return errors.AssertionFailedf(
				"missing provisional value for i.intentKey=%q: i.iter exhausted", i.intentKey)
		} else if i.intentCmp != 0 {
			return errors.AssertionFailedf(
				"missing provisional value for i.intentKey=%q: i.intentCmp=%v is not 0",
				i.intentKey, i.intentCmp)
		} else if hasPoint, hasRange := i.iter.HasPointAndRange(); !hasPoint {
			return errors.AssertionFailedf(
				"missing provisional value for i.intentKey=%q: i.iter on bare range key",
				i.intentKey)
		} else if hasRange {
			if bounds := i.iter.RangeBounds(); !bounds.ContainsKey(i.intentKey) {
				return errors.AssertionFailedf("i.intentKey=%q not covered by i.iter range key %q",
					bounds, i.intentKey)
			}
		}
	}

	// Check i.iterBareRangeAtIntent, which is only valid for i.intentCmp == 0.
	if i.intentCmp == 0 {
		if i.dir > 0 && i.iterBareRangeAtIntent {
			return errors.AssertionFailedf("i.dir=%v can't have i.iterBareRangeAtIntent=%v",
				i.dir, i.iterBareRangeAtIntent)
		}
		if i.dir < 0 && i.iterBareRangeAtIntent {
			if hasPoint, hasRange := i.iter.HasPointAndRange(); hasPoint || !hasRange {
				return errors.AssertionFailedf("i.iterBareRangeAtIntent=%v but hasPoint=%t hasRange=%t",
					i.iterBareRangeAtIntent, hasPoint, hasRange)
			}
			// We've already asserted key equality for i.intentCmp == 0.
			if !i.iterKey.Timestamp.IsEmpty() {
				return errors.AssertionFailedf("i.iterBareRangeAtIntent=%v but i.iterKey has timestamp %s",
					i.iterBareRangeAtIntent, i.iterKey.Timestamp)
			}
		}
	}

	return nil
}

// unsafeMVCCIterator is used in RaceEnabled test builds to randomly inject
// changes to unsafe keys retrieved from MVCCIterators.
type unsafeMVCCIterator struct {
	MVCCIterator
	keyBuf        []byte
	rawKeyBuf     []byte
	rawMVCCKeyBuf []byte
}

// gcassert:inline
func maybeWrapInUnsafeIter(iter MVCCIterator) MVCCIterator {
	if util.RaceEnabled {
		return &unsafeMVCCIterator{MVCCIterator: iter}
	}
	return iter
}

// gcassert:inline
func maybeUnwrapUnsafeIter(iter MVCCIterator) MVCCIterator {
	if util.RaceEnabled {
		if unsafeIter, ok := iter.(*unsafeMVCCIterator); ok {
			return unsafeIter.MVCCIterator
		}
	}
	return iter
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
