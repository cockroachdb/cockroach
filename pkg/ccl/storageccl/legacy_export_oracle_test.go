// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package storageccl

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

// MVCCIncrementalIterator iterates over the diff of the key range
// [startKey,endKey) and time range (startTime,endTime]. If a key was added or
// modified between startTime and endTime, the iterator will position at the
// most recent version (before or at endTime) of that key. If the key was most
// recently deleted, this is signaled with an empty value.
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
//    for iter.Seek(startKey); ; iter.Next() {
//        ok, err := iter.Valid()
//        if !ok { ... }
//        [code using iter.Key() and iter.Value()]
//    }
//    if err := iter.Error(); err != nil {
//      ...
//    }
//
// NOTE: This is not used by CockroachDB and has been preserved to serve as an
// oracle to prove the correctness of the new export logic.
type MVCCIncrementalIterator struct {
	// TODO(dan): Move all this logic into c++ and make this a thin wrapper.

	iter engine.Iterator

	// fields used for a workaround for a bug in the time-bound iterator
	// (#28358)
	upperBound roachpb.Key
	e          engine.Reader
	sanityIter engine.Iterator

	startTime hlc.Timestamp
	endTime   hlc.Timestamp
	err       error
	valid     bool

	// For allocation avoidance.
	meta enginepb.MVCCMetadata
}

var _ engine.SimpleIterator = &MVCCIncrementalIterator{}

// IterOptions bundles options for NewMVCCIncrementalIterator.
type IterOptions struct {
	StartTime                           hlc.Timestamp
	EndTime                             hlc.Timestamp
	UpperBound                          roachpb.Key
	WithStats                           bool
	EnableTimeBoundIteratorOptimization bool
}

// NewMVCCIncrementalIterator creates an MVCCIncrementalIterator with the
// specified engine and options.
func NewMVCCIncrementalIterator(e engine.Reader, opts IterOptions) *MVCCIncrementalIterator {
	io := engine.IterOptions{
		UpperBound: opts.UpperBound,
		WithStats:  opts.WithStats,
	}

	// Time-bound iterators only make sense to use if the start time is set.
	var sanityIter engine.Iterator
	if opts.EnableTimeBoundIteratorOptimization && !opts.StartTime.IsEmpty() {
		// The call to startTime.Next() converts our exclusive start bound into the
		// inclusive start bound that MinTimestampHint expects. This is strictly a
		// performance optimization; omitting the call would still return correct
		// results.
		io.MinTimestampHint = opts.StartTime.Next()
		io.MaxTimestampHint = opts.EndTime
		// It is necessary for correctness that sanityIter be created before iter.
		// This is because the provided Reader may not be a consistent snapshot, so
		// the two could end up observing different information. The hack around
		// sanityCheckMetadataKey only works properly if all possible discrepancies
		// between the two iterators lead to intents and values falling outside of
		// the timestamp range **from iter's perspective**. This allows us to simply
		// ignore discrepancies that we notice in advance(). See #34819.
		sanityIter = e.NewIterator(engine.IterOptions{
			UpperBound: opts.UpperBound,
		})
	}

	return &MVCCIncrementalIterator{
		e:          e,
		upperBound: opts.UpperBound,
		iter:       e.NewIterator(io),
		startTime:  opts.StartTime,
		endTime:    opts.EndTime,
		sanityIter: sanityIter,
	}
}

// Seek advances the iterator to the first key in the engine which is >= the
// provided key.
func (i *MVCCIncrementalIterator) Seek(startKey engine.MVCCKey) {
	i.iter.Seek(startKey)
	i.err = nil
	i.valid = true
	i.advance()
}

// Close frees up resources held by the iterator.
func (i *MVCCIncrementalIterator) Close() {
	i.iter.Close()
	if i.sanityIter != nil {
		i.sanityIter.Close()
	}
}

// Next advances the iterator to the next key/value in the iteration. After this
// call, Valid() will be true if the iterator was not positioned at the last
// key.
func (i *MVCCIncrementalIterator) Next() {
	i.iter.Next()
	i.advance()
}

// NextKey advances the iterator to the next MVCC key. This operation is
// distinct from Next which advances to the next version of the current key or
// the next key if the iterator is currently located at the last version for a
// key.
func (i *MVCCIncrementalIterator) NextKey() {
	i.iter.NextKey()
	i.advance()
}

func (i *MVCCIncrementalIterator) advance() {
	for {
		if !i.valid {
			return
		}
		if ok, err := i.iter.Valid(); !ok {
			i.err = err
			i.valid = false
			return
		}

		unsafeMetaKey := i.iter.UnsafeKey()
		if unsafeMetaKey.IsValue() {
			i.meta.Reset()
			i.meta.Timestamp = hlc.LegacyTimestamp(unsafeMetaKey.Timestamp)
		} else {
			// HACK(dan): Work around a known bug in the time-bound iterator.
			// For reasons described in #28358, a time-bound iterator will
			// sometimes see an unresolved intent where there is none. A normal
			// iterator doesn't have this problem, so we work around it by
			// double checking all non-value keys. If sanityCheckMetadataKey
			// returns false, then the intent was a phantom and we ignore it.
			// NB: this could return a newer intent than the one the time-bound
			// iterator saw; this is handled.
			unsafeValueBytes, ok, err := i.sanityCheckMetadataKey()
			if err != nil {
				i.err = err
				i.valid = false
				return
			} else if !ok {
				i.iter.Next()
				continue
			}

			if i.err = protoutil.Unmarshal(unsafeValueBytes, &i.meta); i.err != nil {
				i.valid = false
				return
			}
		}
		if i.meta.IsInline() {
			// Inline values are only used in non-user data. They're not needed
			// for backup, so they're not handled by this method. If one shows
			// up, throw an error so it's obvious something is wrong.
			i.valid = false
			i.err = errors.Errorf("inline values are unsupported by MVCCIncrementalIterator: %s",
				unsafeMetaKey.Key)
			return
		}

		metaTimestamp := hlc.Timestamp(i.meta.Timestamp)
		if i.meta.Txn != nil {
			if i.startTime.Less(metaTimestamp) && !i.endTime.Less(metaTimestamp) {
				i.err = &roachpb.WriteIntentError{
					Intents: []roachpb.Intent{{
						Span:   roachpb.Span{Key: i.iter.Key().Key},
						Status: roachpb.PENDING,
						Txn:    *i.meta.Txn,
					}},
				}
				i.valid = false
				return
			}
			i.iter.Next()
			continue
		}

		if i.endTime.Less(metaTimestamp) {
			i.iter.Next()
			continue
		}
		if !i.startTime.Less(metaTimestamp) {
			i.iter.NextKey()
			continue
		}

		break
	}
}

// sanityCheckMetadataKey looks up the current `i.iter` key using a normal,
// non-time-bound iterator and returns the value if the normal iterator also
// sees that exact key. Otherwise, it returns false. It's used in the workaround
// in `advance` for a time-bound iterator bug.
func (i *MVCCIncrementalIterator) sanityCheckMetadataKey() ([]byte, bool, error) {
	if i.sanityIter == nil {
		// If sanityIter is not set, it's because we're not using time-bound
		// iterator and we don't need the sanity check.
		return i.iter.UnsafeValue(), true, nil
	}
	unsafeKey := i.iter.UnsafeKey()
	i.sanityIter.Seek(unsafeKey)
	if ok, err := i.sanityIter.Valid(); err != nil {
		return nil, false, err
	} else if !ok {
		return nil, false, nil
	} else if !i.sanityIter.UnsafeKey().Equal(unsafeKey) {
		return nil, false, nil
	}
	return i.sanityIter.UnsafeValue(), true, nil
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

// UnsafeKey returns the same key as Key, but the memory is invalidated on the
// next call to {Next,Reset,Close}.
func (i *MVCCIncrementalIterator) UnsafeKey() engine.MVCCKey {
	return i.iter.UnsafeKey()
}

// UnsafeValue returns the same value as Value, but the memory is invalidated on
// the next call to {Next,Reset,Close}.
func (i *MVCCIncrementalIterator) UnsafeValue() []byte {
	return i.iter.UnsafeValue()
}
