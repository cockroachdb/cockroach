// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// A CatchUpIterator is an iterator for catchUp-scans.
type CatchUpIterator struct {
	simpleCatchupIter
	close func()
}

// simpleCatchupIter is an extension of SimpleMVCCIterator that allows for the
// primary iterator to be implemented using a regular MVCCIterator or a
// (often) more efficient MVCCIncrementalIterator. When the caller wants to
// iterate to see older versions of a key, the desire of the caller needs to
// be expressed using one of two methods:
// - Next: when it wants to omit any versions that are not within the time
//   bounds.
// - NextIgnoringTime: when it wants to see the next older version even if it
//   is not within the time bounds.
type simpleCatchupIter interface {
	storage.SimpleMVCCIterator
	NextIgnoringTime()
}

type simpleCatchupIterAdapter struct {
	storage.SimpleMVCCIterator
}

func (i simpleCatchupIterAdapter) NextIgnoringTime() {
	i.SimpleMVCCIterator.Next()
}

var _ simpleCatchupIter = simpleCatchupIterAdapter{}

// NewCatchUpIterator returns a CatchUpIterator for the given Reader.
// If useTBI is true, a time-bound iterator will be used if possible,
// configured with a start time taken from the RangeFeedRequest.
func NewCatchUpIterator(
	reader storage.Reader, args *roachpb.RangeFeedRequest, useTBI bool, closer func(),
) *CatchUpIterator {
	ret := &CatchUpIterator{
		close: closer,
	}
	if useTBI {
		ret.simpleCatchupIter = storage.NewMVCCIncrementalIterator(reader, storage.MVCCIncrementalIterOptions{
			EnableTimeBoundIteratorOptimization: true,
			EndKey:                              args.Span.EndKey,
			// StartTime is exclusive but args.Timestamp
			// is inclusive.
			StartTime: args.Timestamp.Prev(),
			EndTime:   hlc.MaxTimestamp,
			// We want to emit intents rather than error
			// (the default behavior) so that we can skip
			// over the provisional values during
			// iteration.
			IntentPolicy: storage.MVCCIncrementalIterIntentPolicyEmit,
			// CatchUpScan currently emits all inline
			// values it encounters.
			//
			// TODO(ssd): Re-evalutate if this behavior is
			// still needed (#69357).
			InlinePolicy: storage.MVCCIncrementalIterInlinePolicyEmit,
		})
	} else {
		iter := reader.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
			UpperBound: args.Span.EndKey,
		})
		ret.simpleCatchupIter = simpleCatchupIterAdapter{SimpleMVCCIterator: iter}
	}

	return ret
}

// Close closes the iterator and calls the instantiator-supplied close
// callback.
func (i *CatchUpIterator) Close() {
	i.simpleCatchupIter.Close()
	if i.close != nil {
		i.close()
	}
}

// TODO(ssd): Clarify memory ownership. Currently, the memory backing
// the RangeFeedEvents isn't modified by the caller after this
// returns. However, we may revist this in #69596.
type outputEventFn func(e *roachpb.RangeFeedEvent) error

// CatchUpScan iterates over all changes for the given span of keys,
// starting at catchUpTimestamp. Keys and Values are emitted as
// RangeFeedEvents passed to the given outputFn. catchUpTimestamp is exclusive.
func (i *CatchUpIterator) CatchUpScan(
	startKey, endKey storage.MVCCKey,
	catchUpTimestamp hlc.Timestamp,
	withDiff bool,
	outputFn outputEventFn,
) error {
	var a bufalloc.ByteAllocator
	// MVCCIterator will encounter historical values for each key in
	// reverse-chronological order. To output in chronological order, store
	// events for the same key until a different key is encountered, then output
	// the encountered values in reverse. This also allows us to buffer events
	// as we fill in previous values.
	var lastKey roachpb.Key
	reorderBuf := make([]roachpb.RangeFeedEvent, 0, 5)
	addPrevToLastEvent := func(val []byte) {
		if l := len(reorderBuf); l > 0 {
			if reorderBuf[l-1].Val.PrevValue.IsPresent() {
				panic("RangeFeedValue.PrevVal unexpectedly set")
			}
			// TODO(sumeer): find out if it is deliberate that we are not populating
			// PrevValue.Timestamp.
			reorderBuf[l-1].Val.PrevValue.RawBytes = val
		}
	}

	outputEvents := func() error {
		for i := len(reorderBuf) - 1; i >= 0; i-- {
			e := reorderBuf[i]
			if err := outputFn(&e); err != nil {
				return err
			}
			reorderBuf[i] = roachpb.RangeFeedEvent{} // Drop references to values to allow GC
		}
		reorderBuf = reorderBuf[:0]
		return nil
	}
	// Iterate though all keys using Next. We want to publish all committed
	// versions of each key that are after the registration's startTS, so we
	// can't use NextKey.
	var meta enginepb.MVCCMetadata
	i.SeekGE(startKey)
	for {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		unsafeKey := i.UnsafeKey()
		unsafeVal := i.UnsafeValue()
		if !unsafeKey.IsValue() {
			// Found a metadata key.
			if err := protoutil.Unmarshal(unsafeVal, &meta); err != nil {
				return errors.Wrapf(err, "unmarshaling mvcc meta: %v", unsafeKey)
			}

			if !meta.IsInline() {
				// This is an MVCCMetadata key for an intent. The catchUp scan
				// only cares about committed values, so ignore this and skip past
				// the corresponding provisional key-value. To do this, iterate to
				// the provisional key-value, validate its timestamp, then iterate
				// again. If we arrived here with a preceding call to NextIgnoringTime
				// (in the with-diff case), it's possible that the intent is not within
				// the time bounds. Using `NextIgnoringTime` on the next line makes sure
				// that we are guaranteed to validate the version that belongs to the
				// intent.
				i.NextIgnoringTime()

				if ok, err := i.Valid(); err != nil {
					return errors.Wrap(err, "iterating to provisional value for intent")
				} else if !ok {
					return errors.Errorf("expected provisional value for intent")
				}
				if !meta.Timestamp.ToTimestamp().EqOrdering(i.UnsafeKey().Timestamp) {
					return errors.Errorf("expected provisional value for intent with ts %s, found %s",
						meta.Timestamp, i.UnsafeKey().Timestamp)
				}
				// Now move to the next key of interest. Note that if in the last
				// iteration of the loop we called `NextIgnoringTime`, the fact that we
				// hit an intent proves that there wasn't a previous value, so we can
				// (in fact, have to, to avoid surfacing unwanted keys) unconditionally
				// enforce time bounds.
				i.Next()
				continue
			}

			// If write is inline, it doesn't have a timestamp so we don't
			// filter on the registration's starting timestamp. Instead, we
			// return all inline writes.
			//
			// TODO(ssd): Do we want to continue to
			// support inline values here at all? TBI may
			// miss inline values completely and normal
			// iterators may result in the rangefeed not
			// seeing some intermediate values.
			unsafeVal = meta.RawBytes
		}

		// Ignore the version if it's not inline and its timestamp is at
		// or before the registration's (exclusive) starting timestamp.
		ts := unsafeKey.Timestamp
		ignore := !(ts.IsEmpty() || catchUpTimestamp.Less(ts))
		if ignore && !withDiff {
			// Skip all the way to the next key.
			// NB: fast-path to avoid value copy when !r.withDiff.
			i.NextKey()
			continue
		}

		// Determine whether the iterator moved to a new key.
		sameKey := bytes.Equal(unsafeKey.Key, lastKey)
		if !sameKey {
			// If so, output events for the last key encountered.
			if err := outputEvents(); err != nil {
				return err
			}
			a, lastKey = a.Copy(unsafeKey.Key, 0)
		}
		key := lastKey

		// INVARIANT: !ignore || withDiff
		//
		// Cases:
		//
		// - !ignore: we need to copy the unsafeVal to add to
		//   the reorderBuf to be output eventually,
		//   regardless of the value of withDiff
		//
		// - withDiff && ignore: we need to copy the unsafeVal
		//   only if there is already something in the
		//   reorderBuf for which we need to set the previous
		//   value.
		if !ignore || (withDiff && len(reorderBuf) > 0) {
			var val []byte
			a, val = a.Copy(unsafeVal, 0)
			if withDiff {
				// Update the last version with its
				// previous value (this version).
				addPrevToLastEvent(val)
			}

			if !ignore {
				// Add value to reorderBuf to be output.
				var event roachpb.RangeFeedEvent
				event.MustSetValue(&roachpb.RangeFeedValue{
					Key: key,
					Value: roachpb.Value{
						RawBytes:  val,
						Timestamp: ts,
					},
				})
				reorderBuf = append(reorderBuf, event)
			}
		}

		if ignore {
			// Skip all the way to the next key.
			i.NextKey()
		} else {
			// Move to the next version of this key (there may not be one, in which
			// case it will move to the next key).
			if withDiff {
				// Need to see the next version even if it is older than the time
				// bounds.
				i.NextIgnoringTime()
			} else {
				i.Next()
			}
		}
	}

	// Output events for the last key encountered.
	return outputEvents()
}
