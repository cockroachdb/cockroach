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
	storage.SimpleMVCCIterator

	useTBI bool
	close  func()
}

// NewCatchUpIterator returns a CatchUpIterator for the given Reader.
// If useTBI is true, a time-bound iterator will be used if possible,
// configured with a start time taken from the RangeFeedRequest.
func NewCatchUpIterator(
	reader storage.Reader, args *roachpb.RangeFeedRequest, useTBI bool, closer func(),
) *CatchUpIterator {
	ret := &CatchUpIterator{
		close:  closer,
		useTBI: useTBI,
	}
	if useTBI {
		ret.SimpleMVCCIterator = storage.NewMVCCIncrementalIterator(reader, storage.MVCCIncrementalIterOptions{
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
		ret.SimpleMVCCIterator = reader.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
			UpperBound: args.Span.EndKey,
		})
	}

	return ret
}

// Close closes the iterator and calls the instantiator-supplied close
// callback.
func (i *CatchUpIterator) Close() {
	i.SimpleMVCCIterator.Close()
	if i.close != nil {
		i.close()
	}
}

// TODO(ssd): Clarify memory ownership. Currently, the memory backing
// the RangeFeedEvents isn't modified by the caller after this
// returns. However, we may revist this in #69596.
type outputEventFn func(e *roachpb.RangeFeedEvent) error

// CatchUpScan iterates over all changes for the given span of keys,
// starting at catchUpTimestamp.  Keys and Values are emitted as
// RangeFeedEvents passed to the given outputFn.
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
	var valueBuf []byte

	reorderBuf := make([]roachpb.RangeFeedEvent, 0, 5)
	addPrevToLastEvent := func(val []byte) {
		if l := len(reorderBuf); l > 0 {
			if reorderBuf[l-1].Val.PrevValue.IsPresent() {
				panic("RangeFeedValue.PrevVal unexpectedly set")
			}
			reorderBuf[l-1].Val.PrevValue.RawBytes = val
		}
	}

	outputEvents := func() error {
		if i.useTBI && withDiff {
			// Our last entry will be erroneously missing a
			// previous value if it was outside the time window of
			// the incremental iterator.
			if len(valueBuf) > 0 && len(reorderBuf) > 0 {
				reorderBuf[len(reorderBuf)-1].Val.PrevValue.RawBytes = valueBuf
			}
		}
		for i := len(reorderBuf) - 1; i >= 0; i-- {
			e := reorderBuf[i]
			if err := outputFn(&e); err != nil {
				return err
			}
		}
		valueBuf = valueBuf[:0]
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
				// only cares about committed values, so ignore this and skip
				// past the corresponding provisional key-value. To do this,
				// scan to the timestamp immediately before (i.e. the key
				// immediately after) the provisional key.
				//
				// Make a copy since should not pass an unsafe key from the iterator
				// that provided it, when asking it to seek.
				a, unsafeKey.Key = a.Copy(unsafeKey.Key, 0)
				i.SeekGE(storage.MVCCKey{
					Key:       unsafeKey.Key,
					Timestamp: meta.Timestamp.ToTimestamp().Prev(),
				})
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
			// Move to the next version of this key.
			if i.useTBI && withDiff {
				// If we are using TBI and withDiff, we potentially need to capture the "PrevValue" for the key we just added.
				iter := i.SimpleMVCCIterator.(*storage.MVCCIncrementalIterator)
				if iter.NextSavingSkipped(key) {
					skippedValue := iter.UnsafeSkippedValue()
					valueBuf = append(valueBuf, skippedValue...)
				}

			} else {
				i.Next()
			}
		}
	}

	// Output events for the last key encountered.
	return outputEvents()
}
