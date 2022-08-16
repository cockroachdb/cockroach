// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// mvccIncrementalIteratorMetamorphicTBI will randomly enable TBIs.
var mvccIncrementalIteratorMetamorphicTBI = util.ConstantWithMetamorphicTestBool(
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
//    iter := NewMVCCIncrementalIterator(e, IterOptions{
//        StartTime:  startTime,
//        EndTime:    endTime,
//        UpperBound: endKey,
//    })
//    defer iter.Close()
//    for iter.SeekGE(startKey); ; iter.Next() {
//        ok, err := iter.Valid()
//        if !ok { ... }
//        [code using iter.Key() and iter.Value()]
//    }
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
	// one doesn't.
	hasPoint, hasRange bool

	// rangeKeysStart contains the last seen range key start bound. It is used
	// to detect changes to range keys.
	//
	// TODO(erikgrinaker): This pattern keeps coming up, and involves one
	// comparison for every covered point key. Consider exposing this from Pebble,
	// who has presumably already done these comparisons, so we can avoid them.
	rangeKeysStart roachpb.Key

	// ignoringTime is true if the iterator is currently ignoring time bounds,
	// i.e. following a call to NextIgnoringTime().
	ignoringTime bool

	// Configuration passed in MVCCIncrementalIterOptions.
	intentPolicy MVCCIncrementalIterIntentPolicy

	// Optional collection of intents created on demand when first intent encountered.
	intents []roachpb.Intent
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
	// WriteIntentError which would be updated during
	// iteration. Consumer would be free to decide if it wants to
	// keep collecting entries and intents or skip entries.
	MVCCIncrementalIterIntentPolicyAggregate
	// MVCCIncrementalIterIntentPolicyEmit will return intents to
	// the caller if they are inside or outside the time range.
	MVCCIncrementalIterIntentPolicyEmit
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
}

// NewMVCCIncrementalIterator creates an MVCCIncrementalIterator with the
// specified reader and options. The timestamp hint range should not be more
// restrictive than the start and end time range.
func NewMVCCIncrementalIterator(
	reader Reader, opts MVCCIncrementalIterOptions,
) *MVCCIncrementalIterator {
	// Default to MaxTimestamp for EndTime, since the code assumes it is set.
	if opts.EndTime.IsEmpty() {
		opts.EndTime = hlc.MaxTimestamp
	}

	// We assume EndTime is near the current time, so there is little to gain from
	// using a TBI unless StartTime is set. However, we always vary it in
	// metamorphic test builds, for better test coverage of both paths.
	useTBI := opts.StartTime.IsSet()
	if util.IsMetamorphicBuild() { // NB: always randomize when metamorphic
		useTBI = mvccIncrementalIteratorMetamorphicTBI
	}

	var iter MVCCIterator
	var timeBoundIter MVCCIterator
	if useTBI {
		// An iterator without the timestamp hints is created to ensure that the
		// iterator visits every required version of every key that has changed.
		iter = reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
			KeyTypes:             opts.KeyTypes,
			LowerBound:           opts.StartKey,
			UpperBound:           opts.EndKey,
			RangeKeyMaskingBelow: opts.RangeKeyMaskingBelow,
		})
		// The timeBoundIter is only required to see versioned keys, since the
		// intents will be found by iter. It can also always enable range key
		// masking at the start time, since we never care about point keys below it
		// (the same isn't true for the main iterator, since it would break
		// NextIgnoringTime).
		tbiRangeKeyMasking := opts.RangeKeyMaskingBelow
		if tbiRangeKeyMasking.LessEq(opts.StartTime) && opts.KeyTypes == IterKeyTypePointsAndRanges {
			tbiRangeKeyMasking = opts.StartTime.Next()
		}
		timeBoundIter = reader.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
			KeyTypes:   opts.KeyTypes,
			LowerBound: opts.StartKey,
			UpperBound: opts.EndKey,
			// The call to startTime.Next() converts our exclusive start bound into
			// the inclusive start bound that MinTimestampHint expects.
			MinTimestampHint:     opts.StartTime.Next(),
			MaxTimestampHint:     opts.EndTime,
			RangeKeyMaskingBelow: tbiRangeKeyMasking,
		})
	} else {
		iter = reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
			KeyTypes:             opts.KeyTypes,
			LowerBound:           opts.StartKey,
			UpperBound:           opts.EndKey,
			RangeKeyMaskingBelow: opts.RangeKeyMaskingBelow,
		})
	}

	return &MVCCIncrementalIterator{
		iter:          iter,
		startTime:     opts.StartTime,
		endTime:       opts.EndTime,
		timeBoundIter: timeBoundIter,
		intentPolicy:  opts.IntentPolicy,
	}
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
		tbiKey := i.timeBoundIter.Key().Key
		if tbiKey.Compare(startKey.Key) > 0 {
			// If the first key that the TBI sees is ahead of the given startKey, we
			// can seek directly to the first version of the key.
			startKey = MakeMVCCMetadataKey(tbiKey)
		}
	}
	i.iter.SeekGE(startKey)
	i.rangeKeysStart = nil
	i.advance()
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
	i.advance()
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
	i.advance()
}

// maybeSkipKeys checks if any keys can be skipped by using a time-bound
// iterator. If keys can be skipped, it will update the main iterator to point
// to the earliest version of the next candidate key.
// It is expected (but not required) that TBI is at a key <= main iterator key
// when calling maybeSkipKeys().
//
// NB: This logic will not handle TBI range key filtering properly -- the TBI
// may see different range key fragmentation than the regular iterator, causing
// it to skip past range key fragments. Range key filtering has therefore been
// disabled in pebbleMVCCIterator, since the performance gains are expected to
// be marginal, and the necessary seeks/processing here would likely negate it.
// See: https://github.com/cockroachdb/cockroach/issues/86260
func (i *MVCCIncrementalIterator) maybeSkipKeys() {
	if i.timeBoundIter == nil {
		// If there is no time bound iterator, we cannot skip any keys.
		return
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
			return
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
				return
			}
			tbiKey = i.timeBoundIter.UnsafeKey().Key

			// If there is an MVCC range key across iterKey, then the TBI seek may get
			// stuck in the middle of the bare range key so we step forward.
			if hasPoint, hasRange := i.timeBoundIter.HasPointAndRange(); hasRange && !hasPoint {
				if !i.timeBoundIter.RangeBounds().Key.Equal(tbiKey) {
					i.timeBoundIter.Next()
					if ok, err := i.timeBoundIter.Valid(); !ok {
						i.valid, i.err = false, err
						return
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
				return
			}

			// The seek may have landed in the middle of a bare range key, in which
			// case we should move on to the next key.
			if hasPoint, hasRange := i.iter.HasPointAndRange(); hasRange && !hasPoint {
				if !i.iter.RangeBounds().Key.Equal(i.iter.UnsafeKey().Key) {
					i.iter.Next()
					if !i.updateValid() {
						return
					}
				}
			}
		}
	}
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
	if i.err = protoutil.Unmarshal(i.iter.UnsafeValue(), &i.meta); i.err != nil {
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
			i.err = &roachpb.WriteIntentError{
				Intents: []roachpb.Intent{
					roachpb.MakeIntent(i.meta.Txn, i.iter.Key().Key),
				},
			}
			i.valid = false
			return i.err
		case MVCCIncrementalIterIntentPolicyAggregate:
			// We are collecting intents, so we need to save it and advance to its proposed value.
			// Caller could then use a value key to update proposed row counters for the sake of bookkeeping
			// and advance more.
			i.intents = append(i.intents, roachpb.MakeIntent(i.meta.Txn, i.iter.Key().Key))
			return nil
		case MVCCIncrementalIterIntentPolicyEmit:
			// We will emit this intent to the caller.
			return nil
		default:
			return errors.AssertionFailedf("unknown intent policy: %d", i.intentPolicy)
		}
	}
	return nil
}

// advance advances the main iterator until it is referencing a key within
// (start_time, end_time].
//
// It populates i.err with an error if it encountered an inline value or an
// intent with a timestamp within the incremental iterator's bounds when the
// intent policy is MVCCIncrementalIterIntentPolicyError.
func (i *MVCCIncrementalIterator) advance() {
	i.ignoringTime = false
	for {
		if !i.updateValid() {
			return
		}
		i.maybeSkipKeys()
		if !i.valid {
			return
		}

		// NB: Don't update i.hasRange directly -- we only change it when
		// i.rangeKeysStart changes, which allows us to retain i.hasRange=false if
		// we've already determined that the current range keys are outside of the
		// time bounds.
		hasPoint, hasRange := i.iter.HasPointAndRange()
		i.hasPoint = hasPoint

		// Process range keys.
		//
		// TODO(erikgrinaker): This needs to be optimized. For example, range keys
		// only change on unversioned keys (except after a SeekGE), which can save a
		// bunch of comparisons here. HasPointAndRange() has also been seen to have
		// a non-negligible cost even without any range keys.
		var newRangeKey bool
		if hasRange {
			if rangeStart := i.iter.RangeBounds().Key; !rangeStart.Equal(i.rangeKeysStart) {
				i.rangeKeysStart = append(i.rangeKeysStart[:0], rangeStart...)
				i.hasRange = i.iter.RangeKeys().HasBetween(i.startTime.Next(), i.endTime)
				newRangeKey = i.hasRange
			}
			// Else: keep i.hasRange from last i.rangeKeysStart change.
		} else {
			i.hasRange = false
		}

		// If we're on a visible, bare range key then we're done. If the range key
		// isn't visible either, then we keep going.
		if !i.hasPoint {
			if !i.hasRange {
				i.iter.Next()
				continue
			}
			i.meta.Reset()
			return
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
			case MVCCIncrementalIterIntentPolicyError, MVCCIncrementalIterIntentPolicyAggregate:
				// We have encountered an intent but it must lie outside the timestamp
				// span (startTime, endTime] or we have aggregated it. In either case,
				// we want to advance past it, unless we're also on a new range key that
				// must be emitted.
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
	return i.valid, i.err
}

// UnsafeKey implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) UnsafeKey() MVCCKey {
	return i.iter.UnsafeKey()
}

// HasPointAndRange implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) HasPointAndRange() (bool, bool) {
	return i.hasPoint, i.hasRange
}

// RangeBounds implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) RangeBounds() roachpb.Span {
	if !i.hasRange {
		return roachpb.Span{}
	}
	return i.iter.RangeBounds()
}

// RangeKeys implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) RangeKeys() MVCCRangeKeyStack {
	if !i.hasRange {
		return MVCCRangeKeyStack{}
	}
	// TODO(erikgrinaker): It may be worthwhile to clone and memoize this result
	// for the same range key. However, callers may avoid calling RangeKeys()
	// unnecessarily, and we may optimize parent iterators, so let's measure.
	rangeKeys := i.iter.RangeKeys()
	if !i.ignoringTime {
		rangeKeys.Trim(i.startTime.Next(), i.endTime)
	}
	return rangeKeys
}

// RangeKeyChanged implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) RangeKeyChanged() bool {
	panic("not implemented")
}

// UnsafeValue implements SimpleMVCCIterator.
func (i *MVCCIncrementalIterator) UnsafeValue() []byte {
	if !i.hasPoint {
		return nil
	}
	return i.iter.UnsafeValue()
}

// updateIgnoreTime updates the iterator's metadata and handles intents depending on the iterator's
// intent policy.
func (i *MVCCIncrementalIterator) updateIgnoreTime() {
	i.ignoringTime = true
	for {
		if !i.updateValid() {
			return
		}

		i.hasPoint, i.hasRange = i.iter.HasPointAndRange()
		if i.hasRange {
			// Make sure we update rangeKeysStart appropriately so that switching back
			// to regular iteration won't emit bare range keys twice.
			if rangeStart := i.iter.RangeBounds().Key; !rangeStart.Equal(i.rangeKeysStart) {
				i.rangeKeysStart = append(i.rangeKeysStart[:0], rangeStart...)
			}
		}

		if !i.hasPoint {
			return
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
func (i *MVCCIncrementalIterator) NextIgnoringTime() {
	i.iter.Next()
	i.updateIgnoreTime()
}

// NextKeyIgnoringTime returns the next distinct key that would be encountered
// in a non-incremental iteration by moving the underlying non-TBI iterator
// forward. Intents within and outside the (StartTime, EndTime] time range are
// handled according to the iterator policy.
func (i *MVCCIncrementalIterator) NextKeyIgnoringTime() {
	i.iter.NextKey()
	i.updateIgnoreTime()
}

// NumCollectedIntents returns number of intents encountered during iteration.
// This is only the case when intent aggregation is enabled, otherwise it is
// always 0.
func (i *MVCCIncrementalIterator) NumCollectedIntents() int {
	return len(i.intents)
}

// TryGetIntentError returns roachpb.WriteIntentError if intents were encountered
// during iteration and intent aggregation is enabled. Otherwise function
// returns nil. roachpb.WriteIntentError will contain all encountered intents.
func (i *MVCCIncrementalIterator) TryGetIntentError() error {
	if len(i.intents) == 0 {
		return nil
	}
	return &roachpb.WriteIntentError{
		Intents: i.intents,
	}
}
