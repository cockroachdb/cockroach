// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// mvccIncrementalIteratorMetamorphicTBI will randomly enable TBIs.
var mvccIncrementalIteratorMetamorphicTBI = metamorphic.ConstantWithTestBool(
	"mvcc-incremental-iter-tbi", true)

// MVCCIncrementalIterator iterates over the diff of the key range
// [startKey,endKey) and time range (startTime,endTime]. If a key was added or
// modified between startTime and endTime, the iterator will position at the
// most recent version (before or at endTime) of that key. If the key was most
// recently deleted, this is signaled with an empty value.
//
// Inline (unversioned) values are not supported, and may return an error or be
// omitted entirely. The iterator should not be used across such keys.
//
// Intents outside the time bounds are ignored. Intents inside the
// time bounds are handled according to the provided
// MVCCIncrementalIterIntentPolicy. By default, an error will be
// returned.
//
// Note: The endTime is inclusive to be consistent with the non-incremental
// iterator, where reads at a given timestamp return writes at that
// timestamp. The startTime is then made exclusive so that iterating time 1 to
// 2 and then 2 to 3 will only return values with time 2 once. An exclusive
// start time would normally make it difficult to scan timestamp 0, but
// CockroachDB uses that as a sentinel for key metadata anyway.
//
// Expected usage:
//
//	iter := NewMVCCIncrementalIterator(e, IterOptions{
//	    StartTime:  startTime,
//	    EndTime:    endTime,
//	    UpperBound: endKey,
//	})
//	defer iter.Close()
//	for iter.SeekGE(startKey); ; iter.Next() {
//	    ok, err := iter.Valid()
//	    if !ok { ... }
//	    [code using iter.Key() and iter.Value()]
//	}
//
// Note regarding the correctness of the time-bound iterator optimization:
//
// When using (t_s, t_e], say there is a version (committed or provisional)
// k@t where t is in that interval, that is visible to iter. All sstables
// containing k@t will be included in timeBoundIter. Note that there may be
// multiple sequence numbers for the key k@t at the storage layer, say k@t#n1,
// k@t#n2, where n1 > n2, some of which may be deleted, but the latest
// sequence number will be visible using iter (since not being visible would be
// a contradiction of the initial assumption that k@t is visible to iter).
// Since there is no delete across all sstables that deletes k@t#n1, there is
// no delete in the subset of sstables used by timeBoundIter that deletes
// k@t#n1, so the timeBoundIter will see k@t.
type MVCCIncrementalIterator struct {
	iter MVCCIterator

	// A time-bound iterator cannot be used by itself due to a bug in the time-
	// bound iterator (#28358). This was historically augmented with an iterator
	// without the time-bound optimization to act as a sanity iterator, but
	// issues remained (#43799), so now the iterator above is the main iterator
	// the timeBoundIter is used to check if any keys can be skipped by the main
	// iterator.
	timeBoundIter MVCCIterator

	startTime hlc.Timestamp
	endTime   hlc.Timestamp
	err       error
	valid     bool

	// For allocation avoidance, meta is used to store the timestamp of keys
	// regardless if they are metakeys.
	meta enginepb.MVCCMetadata

	// hasPoint and hasRange control whether the iterator should surface a point
	// or range key from the underlying iterator. If true, this implies that the
	// underlying iterator returns true as well. This can be used to hide point or
	// range keys where one key kind satisfies the time predicate but the other
	// one doesn't. Ignored following IgnoringTime() calls.
	hasPoint, hasRange bool

	// rangeKeys contains the filtered range keys at the current location.
	rangeKeys MVCCRangeKeyStack

	// rangeKeysIgnoringTime contains the complete range keys at the current location.
	rangeKeysIgnoringTime MVCCRangeKeyStack

	// rangeKeyChanged is true if i.rangeKeys changed during the previous
	// positioning operation.
	rangeKeyChanged bool

	// rangeKeyChangedIgnoringTime is true if i.rangeKeysIgnoringTime changed
	// during the previous positioning operation.
	rangeKeyChangedIgnoringTime bool

	// ignoringTime is true if the iterator is currently ignoring time bounds,
	// i.e. following a call to NextIgnoringTime().
	ignoringTime bool

	// Configuration passed in MVCCIncrementalIterOptions.
	intentPolicy MVCCIncrementalIterIntentPolicy

	// Optional collection of intents created on demand when first intent encountered.
	intents []roachpb.Intent

	// maxLockConflicts is a maximum number of conflicting locks collected before
	// returning LockConflictError. This setting only works under
	// MVCCIncrementalIterIntentPolicyAggregate. Caller must call TryGetIntentError
	// even when the collected intents is less than the threshold.
	//
	// The zero value indicates no limit.
	maxLockConflicts uint64

	// targetLockConflictBytes sets target bytes for collected intents with
	// LockConflictError. This setting will stop collecting intents when total intent
	// size exceeding the target threshold. This setting only work under
	// MVCCIncrementalIterIntentPolicyAggregate. Caller must call TryGetIntentError
	// even when the total collected intents size is less than the threshold.
	//
	// The zero value indicates no limit.
	targetLockConflictBytes uint64

	// collectedIntentBytes tracks the collected intents' memory usage, intent
	// collection could stop early if targetLockConflictBytes is reached. This
	// setting is only relevant under MVCCIncrementalIterIntentPolicyAggregate.
	collectedIntentBytes uint64
}

var _ SimpleMVCCIterator = &MVCCIncrementalIterator{}

// MVCCIncrementalIterIntentPolicy controls how the
// MVCCIncrementalIterator will handle intents that it encounters
// when iterating.
type MVCCIncrementalIterIntentPolicy int

const (
	// MVCCIncrementalIterIntentPolicyError will immediately
	// return an error for any intent found inside the given time
	// range.
	MVCCIncrementalIterIntentPolicyError MVCCIncrementalIterIntentPolicy = iota
	// MVCCIncrementalIterIntentPolicyAggregate will not fail on
	// first encountered intent, but will proceed further. All
	// found intents will be aggregated into a single
	// LockConflictError which would be updated during
	// iteration. The LockConflictError's intents size is
	// constrained by MaxLockConflicts setting. Consumer would
	// be free to decide if it wants to keep collecting entries
	// and intents or skip entries.
	MVCCIncrementalIterIntentPolicyAggregate
	// MVCCIncrementalIterIntentPolicyEmit will return intents to
	// the caller if they are inside or outside the time range.
	MVCCIncrementalIterIntentPolicyEmit
	// MVCCIncrementalIterIntentPolicyIgnore will not emit intents at all, by
	// disabling intent interleaving and filtering out any encountered intents.
	// This gives a minor performance improvement, but is only safe if the caller
	// has already checked the lock table prior to using the iterator. Otherwise,
	// any provisional values will be emitted, as they can't be disambiguated from
	// committed values.
	MVCCIncrementalIterIntentPolicyIgnore
)

// MVCCIncrementalIterOptions bundles options for NewMVCCIncrementalIterator.
type MVCCIncrementalIterOptions struct {
	KeyTypes IterKeyType
	StartKey roachpb.Key
	EndKey   roachpb.Key

	// Only keys within (StartTime,EndTime] will be emitted. EndTime defaults to
	// hlc.MaxTimestamp. The time-bound iterator optimization will only be used if
	// StartTime is set, since we assume EndTime will be near the current time.
	StartTime hlc.Timestamp
	EndTime   hlc.Timestamp

	// RangeKeyMaskingBelow will mask points keys covered by MVCC range tombstones
	// below the given timestamp. For more details, see IterOptions.
	//
	// NB: This masking also affects NextIgnoringTime(), which cannot see points
	// below MVCC range tombstones either.
	RangeKeyMaskingBelow hlc.Timestamp

	IntentPolicy MVCCIncrementalIterIntentPolicy

	// ReadCategory is used to map to a user-understandable category string, for
	// stats aggregation and metrics, and a Pebble-understandable QoS.
	ReadCategory fs.ReadCategory

	// MaxLockConflicts is a maximum number of conflicting locks collected before
	// returning LockConflictError. This setting only work under
	// MVCCIncrementalIterIntentPolicyAggregate. Caller must call TryGetIntentError
	// when the collected intents is less that the Threshold.
	//
	// The zero value indicates no limit.
	MaxLockConflicts uint64

	// TargetLockConflictBytes sets target bytes for collected intents with
	// LockConflictError. This setting will stop collecting intents when total intent
	// size exceeding the target threshold. This setting only work under
	// MVCCIncrementalIterIntentPolicyAggregate. Caller must call TryGetIntentError
	// even when the total collected intents size is less than the threshold.
	//
	// The zero value indicates no limit.
	TargetLockConflictBytes uint64
}

// NewMVCCIncrementalIterator creates an MVCCIncrementalIterator with the
// specified reader and options. The timestamp hint range should not be more
// restrictive than the start and end time range.
func NewMVCCIncrementalIterator(
	ctx context.Context, reader Reader, opts MVCCIncrementalIterOptions,
) (*MVCCIncrementalIterator, error) {
	// Default to MaxTimestamp for EndTime, since the code assumes it is set.
	if opts.EndTime.IsEmpty() {
		opts.EndTime = hlc.MaxTimestamp
	}

	// We assume EndTime is near the current time, so there is little to gain from
	// using a TBI unless StartTime is set. However, we always vary it in
	// metamorphic test builds, for better test coverage of both paths.
	useTBI := opts.StartTime.IsSet()
	if metamorphic.IsMetamorphicBuild() { // NB: always randomize when metamorphic
		useTBI = mvccIncrementalIteratorMetamorphicTBI
	}

	// Disable intent interleaving if requested.
	iterKind := MVCCKeyAndIntentsIterKind
	if opts.IntentPolicy == MVCCIncrementalIterIntentPolicyIgnore {
		iterKind = MVCCKeyIterKind
	}

	var iter MVCCIterator
	var err error
	var timeBoundIter MVCCIterator
	if useTBI {
		// An iterator without the timestamp hints is created to ensure that the
		// iterator visits every required version of every key that has changed.
		iter, err = reader.NewMVCCIterator(ctx, iterKind, IterOptions{
			KeyTypes:             opts.KeyTypes,
			LowerBound:           opts.StartKey,
			UpperBound:           opts.EndKey,
			RangeKeyMaskingBelow: opts.RangeKeyMaskingBelow,
			ReadCategory:         opts.ReadCategory,
		})
		if err != nil {
			return nil, err
		}
		// The timeBoundIter is only required to see versioned keys, since the
		// intents will be found by iter. It can also always enable range key
		// masking at the start time, since we never care about point keys below it
		// (the same isn't true for the main iterator, since it would break
		// NextIgnoringTime).
		tbiRangeKeyMasking := opts.RangeKeyMaskingBelow
		if tbiRangeKeyMasking.LessEq(opts.StartTime) && opts.KeyTypes == IterKeyTypePointsAndRanges {
			tbiRangeKeyMasking = opts.StartTime.Next()
		}
		timeBoundIter, err = reader.NewMVCCIterator(ctx, MVCCKeyIterKind, IterOptions{
			KeyTypes:   opts.KeyTypes,
			LowerBound: opts.StartKey,
			UpperBound: opts.EndKey,
			// The call to startTime.Next() converts our exclusive start bound into
			// the inclusive start bound that MinTimestampt expects.
			MinTimestamp:         opts.StartTime.Next(),
			MaxTimestamp:         opts.EndTime,
			RangeKeyMaskingBelow: tbiRangeKeyMasking,
			ReadCategory:         opts.ReadCategory,
		})
		if err != nil {
			iter.Close()
			return nil, err
		}
	} else {
		iter, err = reader.NewMVCCIterator(ctx, iterKind, IterOptions{
			KeyTypes:             opts.KeyTypes,
			LowerBound:           opts.StartKey,
			UpperBound:           opts.EndKey,
			RangeKeyMaskingBelow: opts.RangeKeyMaskingBelow,
			ReadCategory:         opts.ReadCategory,
		})
		if err != nil {
			return nil, err
		}
	}

	return &MVCCIncrementalIterator{
		iter:                    iter,
		startTime:               opts.StartTime,
		endTime:                 opts.EndTime,
		timeBoundIter:           timeBoundIter,
		intentPolicy:            opts.IntentPolicy,
		maxLockConflicts:        opts.MaxLockConflicts,
		targetLockConflictBytes: opts.TargetLockConflictBytes,
	}, nil
}

// SeekGE implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) SeekGE(startKey MVCCKey) {
	if i.timeBoundIter != nil {
		// Check which is the first key seen by the TBI.
		i.timeBoundIter.SeekGE(startKey)
		if ok, err := i.timeBoundIter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}
		unsafeTBIKey := i.timeBoundIter.UnsafeKey().Key
		if unsafeTBIKey.Compare(startKey.Key) > 0 {
			// If the first key that the TBI sees is ahead of the given startKey, we
			// can seek directly to the first version of the key.
			startKey = MakeMVCCMetadataKey(unsafeTBIKey.Clone())
		}
	}
	prevRangeKey := i.rangeKeys.Bounds.Key.Clone()
	i.iter.SeekGE(startKey)
	i.advance(true /* seeked */)
	i.rangeKeyChanged = !prevRangeKey.Equal(i.rangeKeys.Bounds.Key) // Is there a better way?
	i.rangeKeyChangedIgnoringTime = i.rangeKeyChanged
}

// Close implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) Close() {
	i.iter.Close()
	if i.timeBoundIter != nil {
		i.timeBoundIter.Close()
	}
}

// Next implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) Next() {
	i.iter.Next()
	i.advance(false /* seeked */)
}

// updateValid updates i.valid and i.err based on the underlying iterator, and
// returns true if valid.
// gcassert:inline
func (i *MVCCIncrementalIterator) updateValid() bool {
	i.valid, i.err = i.iter.Valid()
	return i.valid
}

// NextKey implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) NextKey() {
	i.iter.NextKey()
	i.advance(false /* seeked */)
}

// maybeSkipKeys checks if any keys can be skipped by using a time-bound
// iterator. If keys can be skipped, it will update the main iterator to point
// to the earliest version of the next candidate key. It is expected (but not
// required) that TBI is at a key <= main iterator key when calling
// maybeSkipKeys().
//
// Returns true if any of the iter positioning operations caused the range keys
// to change.
//
// NB: This logic will not handle TBI range key filtering properly -- the TBI
// may see different range key fragmentation than the regular iterator, causing
// it to skip past range key fragments. Range key filtering has therefore been
// disabled in pebbleMVCCIterator, since the performance gains are expected to
// be marginal, and the necessary seeks/processing here would likely negate it.
// See: https://github.com/cockroachdb/cockroach/issues/86260
func (i *MVCCIncrementalIterator) maybeSkipKeys() (rangeKeyChanged bool) {
	if i.timeBoundIter == nil {
		// If there is no time bound iterator, we cannot skip any keys.
		return false
	}
	tbiKey := i.timeBoundIter.UnsafeKey().Key
	iterKey := i.iter.UnsafeKey().Key
	if iterKey.Compare(tbiKey) > 0 {
		// If the iterKey got ahead of the TBI key, advance the TBI Key.
		//
		// We fast-path the case where the main iterator is referencing the next
		// key that would be visited by the TBI. In that case, after the following
		// NextKey call, we will have iterKey == tbiKey. This means that for the
		// incremental iterator to perform a Next or NextKey will require only 1
		// extra NextKey invocation while they remain in lockstep. This case will
		// be common if most keys are modified, or the modifications are clustered
		// in keyspace, which makes the incremental iterator optimization
		// ineffective. And so in this case we want to minimize the extra cost of
		// using the incremental iterator, by avoiding a SeekGE.
		i.timeBoundIter.NextKey()
		if ok, err := i.timeBoundIter.Valid(); !ok {
			i.valid, i.err = false, err
			return false
		}
		tbiKey = i.timeBoundIter.UnsafeKey().Key

		cmp := iterKey.Compare(tbiKey)

		if cmp > 0 {
			// If the tbiKey is still behind the iterKey, the TBI key may be seeing
			// phantom MVCCKey.Keys. These keys may not be seen by the main iterator
			// due to aborted transactions and keys which have been subsumed due to
			// range tombstones. In this case we can SeekGE() the TBI to the main iterator.
			seekKey := MakeMVCCMetadataKey(iterKey)
			i.timeBoundIter.SeekGE(seekKey)
			if ok, err := i.timeBoundIter.Valid(); !ok {
				i.valid, i.err = false, err
				return false
			}
			tbiKey = i.timeBoundIter.UnsafeKey().Key

			// If there is an MVCC range key across iterKey, then the TBI seek may get
			// stuck in the middle of the bare range key so we step forward.
			if hasPoint, hasRange := i.timeBoundIter.HasPointAndRange(); hasRange && !hasPoint {
				if !i.timeBoundIter.RangeBounds().Key.Equal(tbiKey) {
					i.timeBoundIter.Next()
					if ok, err := i.timeBoundIter.Valid(); !ok {
						i.valid, i.err = false, err
						return false
					}
					tbiKey = i.timeBoundIter.UnsafeKey().Key
				}
			}
			cmp = iterKey.Compare(tbiKey)
		}

		if cmp < 0 {
			// In the case that the next MVCC key that the TBI observes is not the
			// same as the main iterator, we may be able to skip over a large group
			// of keys. The main iterator is seeked to the TBI in hopes that many
			// keys were skipped. Note that a Seek is an order of magnitude more
			// expensive than a Next call, but the engine has low-level
			// optimizations that attempt to make it cheaper if the seeked key is
			// "nearby" (within the same sstable block).
			seekKey := MakeMVCCMetadataKey(tbiKey)
			i.iter.SeekGE(seekKey)
			if !i.updateValid() {
				return false
			}
			rangeKeyChanged := i.iter.RangeKeyChanged()

			// The seek may have landed in the middle of a bare range key, in which
			// case we should move on to the next key.
			if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
				if !i.iter.RangeBounds().Key.Equal(i.iter.UnsafeKey().Key) {
					i.iter.Next()
					if !i.updateValid() {
						return false
					}
					rangeKeyChanged = rangeKeyChanged || i.iter.RangeKeyChanged()
				}
			}
			return rangeKeyChanged
		}
	}
	return false
}

// updateMeta initializes i.meta. It sets i.err and returns an error on any
// errors, e.g. if it encounters an intent in the time span (startTime, endTime]
// or an inline value.
func (i *MVCCIncrementalIterator) updateMeta() error {
	unsafeKey := i.iter.UnsafeKey()
	if unsafeKey.IsValue() {
		// The key is an MVCC value and not an intent or inline.
		i.meta.Reset()
		i.meta.Timestamp = unsafeKey.Timestamp.ToLegacyTimestamp()
		return nil
	}

	// The key is a metakey (an intent or inline meta). If an inline meta, we
	// will handle below. If an intent meta, then this is used later to see if
	// the timestamp of this intent is within the incremental iterator's time
	// bounds.
	var v []byte
	v, i.err = i.iter.UnsafeValue()
	if i.err != nil {
		i.valid = false
		return i.err
	}
	if i.err = protoutil.Unmarshal(v, &i.meta); i.err != nil {
		i.valid = false
		return i.err
	}

	if i.meta.IsInline() {
		i.valid = false
		i.err = errors.Errorf("unexpected inline value found: %s", unsafeKey.Key)
		return i.err
	}

	if i.meta.Txn == nil {
		i.valid = false
		i.err = errors.Errorf("intent is missing a txn: %s", unsafeKey.Key)
	}

	metaTimestamp := i.meta.Timestamp.ToTimestamp()
	if i.startTime.Less(metaTimestamp) && metaTimestamp.LessEq(i.endTime) {
		switch i.intentPolicy {
		case MVCCIncrementalIterIntentPolicyError:
			i.err = &kvpb.LockConflictError{
				Locks: []roachpb.Lock{
					roachpb.MakeIntent(i.meta.Txn, i.iter.UnsafeKey().Key.Clone()).AsLock(),
				},
			}
			i.valid = false
			return i.err
		case MVCCIncrementalIterIntentPolicyAggregate:
			// We are collecting intents, so we need to save it and advance to its proposed value. Caller could then use a
			// value key to update proposed row counters for the sake of bookkeeping and advance more.
			intent := roachpb.MakeIntent(i.meta.Txn, i.iter.UnsafeKey().Key.Clone())
			i.intents = append(i.intents, intent)
			i.collectedIntentBytes += uint64(intent.Size())
			if i.targetLockConflictBytes > 0 && i.collectedIntentBytes >= i.targetLockConflictBytes {
				i.valid = false
				i.err = i.TryGetIntentError()
				return i.err
			}
			if i.maxLockConflicts > 0 && uint64(len(i.intents)) >= i.maxLockConflicts {
				i.valid = false
				i.err = i.TryGetIntentError()
				return i.err
			}
			return nil
		case MVCCIncrementalIterIntentPolicyEmit:
			// We will emit this intent to the caller.
			return nil
		case MVCCIncrementalIterIntentPolicyIgnore:
			// We don't expect to see this since we disabled intent interleaving.
			i.err = errors.AssertionFailedf("unexpected intent (interleaving disabled): %s", &i.meta)
			i.valid = false
			return i.err
		default:
			i.err = errors.AssertionFailedf("unknown intent policy: %d", i.intentPolicy)
			i.valid = false
			return i.err
		}
	}
	return nil
}

// updateRangeKeys updates the iterator with the current range keys, filtered by
// time span, and returns whether the position has point and/or range keys.
func (i *MVCCIncrementalIterator) updateRangeKeys() (bool, bool) {
	hasPoint, hasRange := i.iter.HasPointAndRange()
	if hasRange {
		// Clone full set of range keys into i.rangeKeysIgnoringTime.
		rangeKeys := i.iter.RangeKeys()
		rangeKeys.CloneInto(&i.rangeKeysIgnoringTime)

		// Keep trimmed subset in i.rangeKeys.
		i.rangeKeys = i.rangeKeysIgnoringTime
		i.rangeKeys.Trim(i.startTime.Next(), i.endTime)
		if i.rangeKeys.IsEmpty() {
			i.rangeKeys.Clear()
			hasRange = false
		}
	} else {
		i.rangeKeys.Clear()
		i.rangeKeysIgnoringTime.Clear()
	}
	return hasPoint, hasRange
}

// advance advances the main iterator until it is referencing a key within
// (start_time, end_time]. If seeked is true, the caller is a SeekGE operation,
// in which case we should emit the current range key position even if
// RangeKeyChanged() doesn't trigger.
//
// It populates i.err with an error if it encountered an inline value or an
// intent with a timestamp within the incremental iterator's bounds when the
// intent policy is MVCCIncrementalIterIntentPolicyError.
func (i *MVCCIncrementalIterator) advance(seeked bool) {
	i.ignoringTime = false
	i.rangeKeyChanged, i.rangeKeyChangedIgnoringTime = false, false
	hadRange, hadRangeIgnoringTime := !i.rangeKeys.IsEmpty(), !i.rangeKeysIgnoringTime.IsEmpty()
	for {
		if !i.updateValid() {
			return
		}

		// If the caller was a SeekGE operation, process the initial range key (if
		// any) even if RangeKeyChanged() does not fire.
		rangeKeyChanged := seeked || i.iter.RangeKeyChanged()
		seeked = false

		if i.maybeSkipKeys() {
			rangeKeyChanged = true
		}
		if !i.valid {
			return
		}

		// Process range keys.
		var newRangeKey bool
		if rangeKeyChanged {
			i.hasPoint, i.hasRange = i.updateRangeKeys()
			newRangeKey = i.hasRange

			// NB: !hasRange → !hasRange is not a change.
			i.rangeKeyChanged = hadRange || i.hasRange
			i.rangeKeyChangedIgnoringTime = hadRangeIgnoringTime || !i.rangeKeysIgnoringTime.IsEmpty()

			// If we're on a visible, bare range key then we're done. If the range key
			// was filtered out by the time bounds (the !hasPoint && !hasRange case),
			// then we move on to the next key.
			if !i.hasPoint {
				if !i.hasRange {
					i.iter.Next()
					continue
				}
				i.meta.Reset()
				return
			}

		} else if !i.hasPoint {
			// If the range key didn't change, and this wasn't a seek, then we must be
			// on a point key since the iterator won't surface anything else.
			i.hasPoint = true
		}

		// Process point keys.
		if err := i.updateMeta(); err != nil {
			return
		}

		// INVARIANT: we have an intent or an MVCC value.

		if i.meta.Txn != nil {
			switch i.intentPolicy {
			case MVCCIncrementalIterIntentPolicyEmit:
				// If our policy is emit, we may want this
				// intent. If it is outside our time bounds, it
				// will be filtered below.
			case MVCCIncrementalIterIntentPolicyError,
				MVCCIncrementalIterIntentPolicyAggregate,
				MVCCIncrementalIterIntentPolicyIgnore:
				// We have encountered an intent but it must lie outside the timestamp
				// span (startTime, endTime], or have been aggregated or ignored. In
				// either case, we want to advance past it, unless we're also on a new
				// range key that must be emitted.
				if newRangeKey {
					i.hasPoint = false
					return
				}
				i.iter.Next()
				continue
			}
		}

		// Note that MVCC keys are sorted by key, then by _descending_ timestamp
		// order with the exception of the metakey (timestamp 0) being sorted
		// first.
		//
		// If we encountered a new range key on this position, then we must emit it
		// even if the the point key should be skipped. This typically happens on a
		// filtered intent or when seeking directly to a filtered point version.
		metaTimestamp := i.meta.Timestamp.ToTimestamp()
		if newRangeKey {
			i.hasPoint = i.startTime.Less(metaTimestamp) && metaTimestamp.LessEq(i.endTime)
			return
		} else if i.endTime.Less(metaTimestamp) {
			i.iter.Next()
		} else if metaTimestamp.LessEq(i.startTime) {
			i.iter.NextKey()
		} else {
			// The current key is a valid user key and within the time bounds. We are
			// done.
			break
		}
	}
}

// Valid implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) Valid() (bool, error) {
	if util.RaceEnabled && i.valid {
		if err := i.assertInvariants(); err != nil {
			return false, err
		}
	}
	return i.valid, i.err
}

// UnsafeKey implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) UnsafeKey() MVCCKey {
	return i.iter.UnsafeKey()
}

// HasPointAndRange implements SimpleMVCCIterator.
//
// This only returns hasRange=true if there are filtered range keys present.
// Thus, it is possible for this to return hasPoint=false,hasRange=false
// following a NextIgnoringTime() call if positioned on a bare, filtered
// range key. In this case, the range keys are available via
// RangeKeysIgnoringTime().
func (i *MVCCIncrementalIterator) HasPointAndRange() (bool, bool) {
	return i.hasPoint, i.hasRange
}

// RangeBounds implements SimpleMVCCIterator.
//
// This only returns the filtered range key bounds. Thus, if a
// NextIgnoringTime() call moves onto an otherwise hidden range key, this will
// still return an empty span. These hidden range keys are available via
// RangeKeysIgnoringTime().
func (i *MVCCIncrementalIterator) RangeBounds() roachpb.Span {
	return i.rangeKeys.Bounds
}

// RangeKeys implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) RangeKeys() MVCCRangeKeyStack {
	return i.rangeKeys
}

// RangeKeysIgnoringTime returns the range keys at the current position,
// ignoring time bounds. This call is cheap, so callers do not need to perform
// their own caching.
func (i *MVCCIncrementalIterator) RangeKeysIgnoringTime() MVCCRangeKeyStack {
	return i.rangeKeysIgnoringTime
}

// RangeKeyChanged implements SimpleMVCCIterator.
//
// RangeKeyChanged only applies to the filtered set of range keys. If an
// IgnoringTime() operation reveals additional range keys or versions, these do
// not trigger RangeKeyChanged(). See also RangeKeyChangedIgnoringTime().
func (i *MVCCIncrementalIterator) RangeKeyChanged() bool {
	return i.rangeKeyChanged
}

// RangeKeyChangedIgnoringTime is like RangeKeyChanged, but returns true if the
// range keys returned by RangeKeysIgnoringTime() changed since the previous
// positioning operation -- in particular, after a Next(Key)IgnoringTime() call.
func (i *MVCCIncrementalIterator) RangeKeyChangedIgnoringTime() bool {
	return i.rangeKeyChangedIgnoringTime
}

// UnsafeValue implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) UnsafeValue() ([]byte, error) {
	if !i.hasPoint {
		return nil, nil
	}
	return i.iter.UnsafeValue()
}

// MVCCValueLenAndIsTombstone implements the SimpleMVCCIterator interface.
func (i *MVCCIncrementalIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	return i.iter.MVCCValueLenAndIsTombstone()
}

// ValueLen implements the SimpleMVCCIterator interface.
func (i *MVCCIncrementalIterator) ValueLen() int {
	return i.iter.ValueLen()
}

// updateIgnoreTime updates the iterator's metadata and handles intents depending on the iterator's
// intent policy.
func (i *MVCCIncrementalIterator) updateIgnoreTime() {
	i.ignoringTime = true
	i.rangeKeyChanged, i.rangeKeyChangedIgnoringTime = false, false
	hadRange := !i.rangeKeys.IsEmpty()
	for {
		if !i.updateValid() {
			return
		}

		if i.iter.RangeKeyChanged() {
			i.hasPoint, i.hasRange = i.updateRangeKeys()
			i.rangeKeyChanged = hadRange || i.hasRange // !hasRange → !hasRange is no change
			i.rangeKeyChangedIgnoringTime = true
			if !i.hasPoint {
				i.meta.Reset()
				return
			}
		} else if !i.hasPoint {
			i.hasPoint = true
		}

		if err := i.updateMeta(); err != nil {
			return
		}

		// We have encountered an intent but it does not lie in the timestamp span
		// (startTime, endTime] so we do not throw an error, and attempt to move to
		// the intent's corresponding provisional value.
		//
		// Note: it's important to surface the intent's provisional value as callers rely on observing
		// any value -- provisional, or not -- to make decisions. MVCClearTimeRange, for example,
		// flushes keys for deletion whenever it encounters a key outside (StartTime,EndTime].
		//
		// TODO(msbulter): investigate if it's clearer for the caller to emit the intent in
		// addition to the provisional value.
		if i.meta.Txn != nil && i.intentPolicy != MVCCIncrementalIterIntentPolicyEmit {
			i.iter.Next()
			continue
		}

		// We have a valid KV or an intent to emit.
		return
	}
}

// NextIgnoringTime returns the next key/value that would be encountered in a
// non-incremental iteration by moving the underlying non-TBI iterator forward.
// Intents within and outside the (StartTime, EndTime] time range are handled
// according to the iterator policy.
//
// NB: Range key methods only respect the filtered set of range keys. To access
// unfiltered range keys, use RangeKeysIgnoringTime(). This implies that if this
// call steps onto a range key that's entirely outside of the time bounds:
//
// * HasPointAndRange() will return false,false if on a bare range key.
//
//   - RangeKeyChanged() will not fire, unless stepping off of a range key
//     within the time bounds.
//
// * RangeBounds() and RangeKeys() will return empty results.
func (i *MVCCIncrementalIterator) NextIgnoringTime() {
	i.iter.Next()
	i.updateIgnoreTime()
}

// NextKeyIgnoringTime returns the next distinct key that would be encountered
// in a non-incremental iteration by moving the underlying non-TBI iterator
// forward. Intents within and outside the (StartTime, EndTime] time range are
// handled according to the iterator policy.
//
// NB: See NextIgnoringTime comment for important details about range keys.
func (i *MVCCIncrementalIterator) NextKeyIgnoringTime() {
	i.iter.NextKey()
	i.updateIgnoreTime()
}

// IgnoringTime returns true if the previous positioning operation ignored time
// bounds.
func (i *MVCCIncrementalIterator) IgnoringTime() bool {
	return i.ignoringTime
}

// TryGetIntentError returns kvpb.LockConflictError if intents were encountered
// during iteration and intent aggregation is enabled. Otherwise function
// returns nil. kvpb.LockConflictError will contain encountered intents, the
// collected intents are bounded by maxLockConflict or targetBytes constraint.
// TODO(nvanbenschoten): rename to TryGetLockConflictError.
func (i *MVCCIncrementalIterator) TryGetIntentError() error {
	if len(i.intents) == 0 {
		return nil
	}
	return &kvpb.LockConflictError{
		Locks: roachpb.AsLocks(i.intents),
	}
}

// Stats returns statistics about the iterator.
func (i *MVCCIncrementalIterator) Stats() IteratorStats {
	stats := i.iter.Stats()
	if i.timeBoundIter != nil {
		tbStats := i.timeBoundIter.Stats()
		stats.Stats.Merge(tbStats.Stats)
	}
	return stats
}

// assertInvariants asserts iterator invariants. The iterator must be valid.
func (i *MVCCIncrementalIterator) assertInvariants() error {
	// Check general SimpleMVCCIterator API invariants.
	if err := assertSimpleMVCCIteratorInvariants(i); err != nil {
		return err
	}

	// The underlying iterator must be valid when the MVCCIncrementalIterator is.
	if ok, err := i.iter.Valid(); err != nil || !ok {
		errMsg := err.Error()
		return errors.AssertionFailedf("i.iter is invalid with err=%s", errMsg)
	}

	iterKey := i.iter.UnsafeKey()

	// endTime must be set, and be at or after startTime.
	if i.endTime.IsEmpty() {
		return errors.AssertionFailedf("i.endTime not set")
	}
	if i.endTime.Less(i.startTime) {
		return errors.AssertionFailedf("i.endTime %s before i.startTime %s", i.endTime, i.startTime)
	}

	// If startTime is empty, the TBI should be disabled in non-metamorphic builds.
	if !metamorphic.IsMetamorphicBuild() && i.startTime.IsEmpty() && i.timeBoundIter != nil {
		return errors.AssertionFailedf("TBI enabled without i.startTime")
	}

	// If the TBI is enabled, its position should be <= iter unless iter is on an intent.
	if i.timeBoundIter != nil && iterKey.Timestamp.IsSet() {
		if ok, _ := i.timeBoundIter.Valid(); ok {
			if tbiKey := i.timeBoundIter.UnsafeKey(); tbiKey.Compare(iterKey) > 0 {
				return errors.AssertionFailedf("TBI at %q ahead of i.iter at %q", tbiKey, iterKey)
			}
		}
	}

	// i.meta should match the underlying iterator's key.
	if hasPoint, _ := i.iter.HasPointAndRange(); hasPoint {
		metaTS := i.meta.Timestamp.ToTimestamp()
		if iterKey.Timestamp.IsSet() && metaTS != iterKey.Timestamp {
			return errors.AssertionFailedf("i.meta.Timestamp %s differs from i.iter.UnsafeKey %s",
				metaTS, iterKey)
		}
		if metaTS.IsEmpty() && i.meta.Txn == nil {
			return errors.AssertionFailedf("empty i.meta for point key %s", iterKey)
		}
	} else {
		if i.meta.Timestamp.ToTimestamp().IsSet() || i.meta.Txn != nil {
			return errors.AssertionFailedf("i.iter hasPoint=false but non-empty i.meta %+v", i.meta)
		}
	}

	// Unlike most SimpleMVCCIterators, it's possible to return
	// hasPoint=false,hasRange=false following a NextIgnoringTime() call.
	hasPoint, hasRange := i.HasPointAndRange()
	if !hasPoint && !hasRange {
		if !i.ignoringTime {
			return errors.AssertionFailedf(
				"hasPoint=false,hasRange=false invalid when i.ignoringTime=false")
		}
		if i.RangeKeysIgnoringTime().IsEmpty() {
			return errors.AssertionFailedf(
				"hasPoint=false,hasRange=false and RangeKeysIgnoringTime() returned nothing")
		}
	}

	// Point keys and range keys must be within the time bounds, unless
	// we're ignoring time bounds.
	assertInRange := func(ts hlc.Timestamp, format string, args ...interface{}) error {
		if i.startTime.IsSet() && ts.LessEq(i.startTime) || i.endTime.Less(ts) {
			return errors.AssertionFailedf("%s not in range (%s-%s]",
				fmt.Sprintf(format, args...), i.startTime, i.endTime)
		}
		return nil
	}
	key := i.UnsafeKey()

	if hasPoint && !i.ignoringTime {
		if key.Timestamp.IsEmpty() {
			intent := key.Clone()
			intent.Timestamp = i.meta.Timestamp.ToTimestamp()
			if err := assertInRange(intent.Timestamp, "intent %s", intent); err != nil {
				return err
			}
		} else {
			if err := assertInRange(key.Timestamp, "point key %s", key); err != nil {
				return err
			}
		}
	}
	if hasRange {
		rangeKeys := i.RangeKeys()
		for _, v := range rangeKeys.Versions {
			if err := assertInRange(v.Timestamp, "range key %s", rangeKeys.AsRangeKey(v)); err != nil {
				return err
			}
		}
	}

	// Check that intents are processed according to intentPolicy.
	if hasPoint && key.Timestamp.IsEmpty() && i.intentPolicy != MVCCIncrementalIterIntentPolicyEmit {
		return errors.AssertionFailedf("emitted intent %s not allowed by i.intentPolicy %v",
			key, i.intentPolicy)
	}
	if len(i.intents) > 0 && i.intentPolicy != MVCCIncrementalIterIntentPolicyAggregate {
		return errors.AssertionFailedf("i.intents set but not allowed by i.intentPolicy %v",
			i.intentPolicy)
	}
	for _, intent := range i.intents {
		intentKey := MVCCKey{Key: intent.Key, Timestamp: intent.Txn.WriteTimestamp}
		if err := assertInRange(intentKey.Timestamp, "gathered intent %s", intentKey); err != nil {
			return err
		}
	}

	// RangeKeys() must be a subset of RangeKeysIgnoringTime().
	if hasRange {
		rangeKeys := i.RangeKeys()
		rangeKeysIgnoringTime := i.RangeKeysIgnoringTime()
		if !rangeKeys.Bounds.Equal(rangeKeysIgnoringTime.Bounds) {
			return errors.AssertionFailedf("RangeKeys=%s does not match RangeKeysIgnoringTime=%s",
				rangeKeys.Bounds, rangeKeysIgnoringTime.Bounds)
		}
		trimmedVersions := rangeKeysIgnoringTime.Versions
		trimmedVersions.Trim(rangeKeys.Oldest(), rangeKeys.Newest())
		if !rangeKeys.Versions.Equal(trimmedVersions) {
			return errors.AssertionFailedf("RangeKeys=%s not subset of RangeKeysIgnoringTime=%s",
				rangeKeys, rangeKeysIgnoringTime)
		}

	} else {
		// RangeKeysIgnoringTime must cover the current iterator position.
		if rangeKeys := i.RangeKeysIgnoringTime(); !rangeKeys.IsEmpty() {
			if !rangeKeys.Bounds.ContainsKey(key.Key) {
				return errors.AssertionFailedf("RangeKeysIgnoringTime %s does not cover position %s",
					rangeKeys.Bounds, key)
			}
		}
	}

	return nil
}
