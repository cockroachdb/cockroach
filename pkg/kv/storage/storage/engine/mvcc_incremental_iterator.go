// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
//    for iter.SeekGE(startKey); ; iter.Next() {
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
	iter Iterator

	// fields used for a workaround for a bug in the time-bound iterator
	// (#28358)
	upperBound roachpb.Key
	reader     Reader
	sanityIter Iterator

	startTime hlc.Timestamp
	endTime   hlc.Timestamp
	err       error
	valid     bool

	// For allocation avoidance.
	meta enginepb.MVCCMetadata
}

var _ SimpleIterator = &MVCCIncrementalIterator{}

// MVCCIncrementalIterOptions bundles options for NewMVCCIncrementalIterator.
type MVCCIncrementalIterOptions struct {
	IterOptions IterOptions
	StartTime   hlc.Timestamp
	EndTime     hlc.Timestamp
}

// NewMVCCIncrementalIterator creates an MVCCIncrementalIterator with the
// specified reader and options.
func NewMVCCIncrementalIterator(
	reader Reader, opts MVCCIncrementalIterOptions,
) *MVCCIncrementalIterator {
	var sanityIter Iterator
	if !opts.IterOptions.MinTimestampHint.IsEmpty() && !opts.IterOptions.MaxTimestampHint.IsEmpty() {
		// It is necessary for correctness that sanityIter be created before iter.
		// This is because the provided Reader may not be a consistent snapshot, so
		// the two could end up observing different information. The hack around
		// sanityCheckMetadataKey only works properly if all possible discrepancies
		// between the two iterators lead to intents and values falling outside of
		// the timestamp range **from iter's perspective**. This allows us to simply
		// ignore discrepancies that we notice in advance(). See #34819.
		sanityIter = reader.NewIterator(IterOptions{
			UpperBound: opts.IterOptions.UpperBound,
		})
	}

	return &MVCCIncrementalIterator{
		reader:     reader,
		upperBound: opts.IterOptions.UpperBound,
		iter:       reader.NewIterator(opts.IterOptions),
		startTime:  opts.StartTime,
		endTime:    opts.EndTime,
		sanityIter: sanityIter,
	}
}

// SeekGE advances the iterator to the first key in the engine which is >= the
// provided key.
func (i *MVCCIncrementalIterator) SeekGE(startKey MVCCKey) {
	i.iter.SeekGE(startKey)
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
			if i.startTime.Less(metaTimestamp) && metaTimestamp.LessEq(i.endTime) {
				i.err = &roachpb.WriteIntentError{
					Intents: []roachpb.Intent{
						roachpb.MakeIntent(i.meta.Txn, i.iter.Key().Key),
					},
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
		if metaTimestamp.LessEq(i.startTime) {
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
	i.sanityIter.SeekGE(unsafeKey)
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
