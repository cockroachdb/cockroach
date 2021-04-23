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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// MVCCIncrementalIterator iterates over the diff of the key range
// [startKey,endKey) and time range (startTime,endTime]. If a key was added or
// modified between startTime and endTime, the iterator will position at the
// most recent version (before or at endTime) of that key. If the key was most
// recently deleted, this is signaled with an empty value.
//
// MVCCIncrementalIterator will return an error if either of the following are
// encountered:
//   1. An inline value (non-user data)
//   2. An intent whose timestamp lies within the time bounds
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
//    if err := iter.Error(); err != nil {
//      ...
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
//
// NOTE: This is not used by CockroachDB and has been preserved to serve as an
// oracle to prove the correctness of the new export logic.
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

	// Intent aggregation options.
	// Configuration passed in MVCCIncrementalIterOptions.
	enableWriteIntentAggregation bool
	// Optional collection of intents created on demand when first intent encountered.
	intents []roachpb.Intent
}

var _ SimpleMVCCIterator = &MVCCIncrementalIterator{}

// MVCCIncrementalIterOptions bundles options for NewMVCCIncrementalIterator.
type MVCCIncrementalIterOptions struct {
	EnableTimeBoundIteratorOptimization bool
	EndKey                              roachpb.Key
	// Keys visible by the MVCCIncrementalIterator must be within (StartTime,
	// EndTime]. Note that if {Min,Max}TimestampHints are specified in
	// IterOptions, the timestamp hints interval should include the start and end
	// time.
	StartTime hlc.Timestamp
	EndTime   hlc.Timestamp
	// If intent aggregation is enabled, iterator will not fail on first encountered
	// intent, but will proceed further. All found intents will be aggregated into
	// a single WriteIntentError which would be updated during iteration. Consumer
	// would be free to decide if it wants to keep collecting entries and intents or
	// skip entries.
	EnableWriteIntentAggregation bool
}

// NewMVCCIncrementalIterator creates an MVCCIncrementalIterator with the
// specified reader and options. The timestamp hint range should not be more
// restrictive than the start and end time range.
// TODO(pbardea): Add validation here and in C++ implementation that the
//  timestamp hints are not more restrictive than incremental iterator's
//  (startTime, endTime] interval.
func NewMVCCIncrementalIterator(
	reader Reader, opts MVCCIncrementalIterOptions,
) *MVCCIncrementalIterator {
	var iter MVCCIterator
	var timeBoundIter MVCCIterator
	if opts.EnableTimeBoundIteratorOptimization {
		// An iterator without the timestamp hints is created to ensure that the
		// iterator visits every required version of every key that has changed.
		iter = reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
			UpperBound: opts.EndKey,
		})
		// The timeBoundIter is only required to see versioned keys, since the
		// intents will be found by iter.
		timeBoundIter = reader.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
			UpperBound: opts.EndKey,
			// The call to startTime.Next() converts our exclusive start bound into
			// the inclusive start bound that MinTimestampHint expects.
			MinTimestampHint: opts.StartTime.Next(),
			MaxTimestampHint: opts.EndTime,
		})
	} else {
		iter = reader.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
			UpperBound: opts.EndKey,
		})
	}

	return &MVCCIncrementalIterator{
		iter:                         iter,
		startTime:                    opts.StartTime,
		endTime:                      opts.EndTime,
		timeBoundIter:                timeBoundIter,
		enableWriteIntentAggregation: opts.EnableWriteIntentAggregation,
	}
}

// SeekGE advances the iterator to the first key in the engine which is >= the
// provided key. startKey should be a metadata key to ensure that the iterator
// has a chance to observe any intents on the key if they are there.
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
	if ok, err := i.iter.Valid(); !ok {
		i.err = err
		i.valid = false
		return
	}
	i.err = nil
	i.valid = true
	i.advance()
}

// Close frees up resources held by the iterator.
func (i *MVCCIncrementalIterator) Close() {
	i.iter.Close()
	if i.timeBoundIter != nil {
		i.timeBoundIter.Close()
	}
}

// Next advances the iterator to the next key/value in the iteration. After this
// call, Valid() will be true if the iterator was not positioned at the last
// key.
func (i *MVCCIncrementalIterator) Next() {
	i.iter.Next()
	if !i.checkValidAndSaveErr() {
		return
	}
	i.advance()
}

// checkValidAndSaveErr checks if the underlying iter is valid after the operation
// and saves the error and validity state. Returns true if the underlying iterator
// is valid.
func (i *MVCCIncrementalIterator) checkValidAndSaveErr() bool {
	if ok, err := i.iter.Valid(); !ok {
		i.err = err
		i.valid = false
		return false
	}
	return true
}

// NextKey advances the iterator to the next key. This operation is distinct
// from Next which advances to the next version of the current key or the next
// key if the iterator is currently located at the last version for a key.
func (i *MVCCIncrementalIterator) NextKey() {
	i.iter.NextKey()
	if !i.checkValidAndSaveErr() {
		return
	}
	i.advance()
}

// maybeSkipKeys checks if any keys can be skipped by using a time-bound
// iterator. If keys can be skipped, it will update the main iterator to point
// to the earliest version of the next candidate key.
// It is expected that TBI is at a key <= main iterator key when calling
// maybeSkipKeys().
func (i *MVCCIncrementalIterator) maybeSkipKeys() {
	if i.timeBoundIter == nil {
		// If there is no time bound iterator, we cannot skip any keys.
		return
	}
	tbiKey := i.timeBoundIter.Key().Key
	iterKey := i.iter.Key().Key
	if iterKey.Compare(tbiKey) > 0 {
		// If the iterKey got ahead of the TBI key, advance the TBI Key.
		//
		// The case where iterKey == tbiKey, after this call, is the fast-path is
		// when the TBI and the main iterator are in lockstep. In this case, the
		// main iterator was referencing the next key that would be visited by the
		// TBI. This means that for the incremental iterator to perform a Next or
		// NextKey will require only 1 extra NextKey invocation while they remain in
		// lockstep. This could be common if most keys are modified or the
		// modifications are clustered in keyspace.
		//
		// NB: The Seek() below is expensive, so we aim to avoid it if both
		// iterators remain in lockstep as described above.
		i.timeBoundIter.NextKey()
		if ok, err := i.timeBoundIter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}
		tbiKey = i.timeBoundIter.Key().Key

		cmp := iterKey.Compare(tbiKey)

		if cmp > 0 {
			// If the tbiKey is still behind the iterKey, the TBI key may be seeing
			// phantom MVCCKey.Keys. These keys may not be seen by the main iterator
			// due to aborted transactions and keys which have been subsumed due to
			// range tombstones. In this case we can SeekGE() the TBI to the main iterator.
			seekKey := MakeMVCCMetadataKey(iterKey)
			i.timeBoundIter.SeekGE(seekKey)
			if ok, err := i.timeBoundIter.Valid(); !ok {
				i.err = err
				i.valid = false
				return
			}
			tbiKey = i.timeBoundIter.Key().Key
			cmp = iterKey.Compare(tbiKey)
		}

		if cmp < 0 {
			// In the case that the next MVCC key that the TBI observes is not the
			// same as the main iterator, we may be able to skip over a large group
			// of keys. The main iterator is seeked to the TBI in hopes that many
			// keys were skipped. Note that a Seek is an order of magnitude more
			// expensive than a Next call.
			seekKey := MakeMVCCMetadataKey(tbiKey)
			i.iter.SeekGE(seekKey)
			if !i.checkValidAndSaveErr() {
				return
			}
		}
	}
}

// initMetaAndCheckForIntentOrInlineError initializes i.meta, and throws an
// error if it encounters an intent in the timestamp span (startTime, endTime]
// or an inline meta.
// The method sets i.err with the error for future processing.
func (i *MVCCIncrementalIterator) initMetaAndCheckForIntentOrInlineError() error {
	unsafeKey := i.iter.UnsafeKey()
	if unsafeKey.IsValue() {
		// The key is an MVCC value and not an intent or inline.
		i.meta.Reset()
		i.meta.Timestamp = unsafeKey.Timestamp.ToLegacyTimestamp()
		return nil
	}

	// The key is a metakey (an intent or inline meta). If an inline meta, we
	// will error below. If an intent meta, then this is used later to see if
	// the timestamp of this intent is within the incremental iterator's time
	// bounds.
	if i.err = protoutil.Unmarshal(i.iter.UnsafeValue(), &i.meta); i.err != nil {
		i.valid = false
		return i.err
	}

	if i.meta.IsInline() {
		// Inline values are only used in non-user data. They're not needed
		// for backup, so they're not handled by this method. If one shows
		// up, throw an error so it's obvious something is wrong.
		i.valid = false
		i.err = errors.Errorf("inline values are unsupported by MVCCIncrementalIterator: %s",
			unsafeKey.Key)
		return i.err
	}

	metaTimestamp := i.meta.Timestamp.ToTimestamp()
	if i.meta.Txn == nil {
		i.valid = false
		i.err = errors.Errorf("intent is missing a txn: %s", unsafeKey.Key)
	}

	if i.startTime.Less(metaTimestamp) && metaTimestamp.LessEq(i.endTime) {
		if !i.enableWriteIntentAggregation {
			// If we don't plan to collect intents for resolving, we bail out here with a single intent.
			i.err = &roachpb.WriteIntentError{
				Intents: []roachpb.Intent{
					roachpb.MakeIntent(i.meta.Txn, i.iter.Key().Key),
				},
			}
			i.valid = false
			return i.err
		}
		// We are collecting intents, so we need to save it and advance to its proposed value.
		// Caller could then use a value key to update proposed row counters for the sake of bookkeeping
		// and advance more.
		i.intents = append(i.intents, roachpb.MakeIntent(i.meta.Txn, i.iter.Key().Key))
	}
	return nil
}

// advance advances the main iterator until it is referencing a key within
// (start_time, end_time].
// It populates i.err with an error if either of the following was encountered:
// a) an inline value
// b) an intent with a timestamp within the incremental iterator's bounds
func (i *MVCCIncrementalIterator) advance() {
	for {
		i.maybeSkipKeys()
		if !i.valid {
			return
		}

		if err := i.initMetaAndCheckForIntentOrInlineError(); err != nil {
			return
		}

		// We have encountered an intent but it does not lie in the timestamp span
		// (startTime, endTime] so we do not throw an error, and attempt to move to
		// the next valid KV.
		if i.meta.Txn != nil {
			i.iter.Next()
			if !i.checkValidAndSaveErr() {
				return
			}
			continue
		}

		// Note that MVCC keys are sorted by key, then by _descending_ timestamp
		// order with the exception of the metakey (timestamp 0) being sorted
		// first. See mvcc.h for more information.
		metaTimestamp := i.meta.Timestamp.ToTimestamp()
		if i.endTime.Less(metaTimestamp) {
			i.iter.Next()
		} else if metaTimestamp.LessEq(i.startTime) {
			i.iter.NextKey()
		} else {
			// The current key is a valid user key and within the time bounds. We are
			// done.
			break
		}

		if ok, err := i.iter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}
	}
}

// Valid must be called after any call to Reset(), Next(), or similar methods.
// It returns (true, nil) if the iterator points to a valid key (it is undefined
// to call Key(), Value(), or similar methods unless Valid() has returned (true,
// nil)). It returns (false, nil) if the iterator has moved past the end of the
// valid range, or (false, err) if an error has occurred. Valid() will never
// return true with a non-nil error.
func (i *MVCCIncrementalIterator) Valid() (bool, error) {
	return i.valid, i.err
}

// Key returns the current key.
func (i *MVCCIncrementalIterator) Key() MVCCKey {
	return i.iter.Key()
}

// Value returns the current value as a byte slice.
func (i *MVCCIncrementalIterator) Value() []byte {
	return i.iter.Value()
}

// UnsafeKey returns the same key as Key, but the memory is invalidated on the
// next call to {Next,Reset,Close}.
func (i *MVCCIncrementalIterator) UnsafeKey() MVCCKey {
	return i.iter.UnsafeKey()
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Reset,Close}.
func (i *MVCCIncrementalIterator) UnsafeValue() []byte {
	return i.iter.UnsafeValue()
}

// NextIgnoringTime returns the next key/value that would be encountered in a
// non-incremental iteration by moving the underlying non-TBI iterator forward.
// This method throws an error if it encounters an intent in the time range
// (startTime, endTime] or sees an inline value.
func (i *MVCCIncrementalIterator) NextIgnoringTime() {
	for {
		i.iter.Next()
		if ok, err := i.iter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}

		if err := i.initMetaAndCheckForIntentOrInlineError(); err != nil {
			return
		}

		// We have encountered an intent but it does not lie in the timestamp span
		// (startTime, endTime] so we do not throw an error, and attempt to move to
		// the next valid KV.
		if i.meta.Txn != nil {
			continue
		}

		// We have a valid KV.
		return
	}
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
