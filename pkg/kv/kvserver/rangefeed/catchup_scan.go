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

// A CatchupIterator is an iterator for catchup-scans.
type CatchupIterator struct {
	storage.SimpleMVCCIterator

	// prevValIter is used to look up the previous values of keys
	// when using the TBI optimization, since the previous value
	// of the key is likely outside the time bounds of the TBI.
	//
	// TODO(ssd): It should be possible to use the underlying
	// non-timebound iterator in IncrementalIterator for this.
	prevValIter storage.SimpleMVCCIterator
	close       func()
}

// NewCatchupIterator returns a CatchupIterator for the given Reader.
// If useTBI is true, a time-bound iterator will be used, configured
// with a start time taken from the RangeFeedRequest.
func NewCatchupIterator(
	reader storage.Reader, args *roachpb.RangeFeedRequest, useTBI bool, closer func(),
) *CatchupIterator {
	ret := &CatchupIterator{
		close: closer,
	}
	if useTBI {
		ret.SimpleMVCCIterator = storage.NewMVCCIncrementalIterator(reader, storage.MVCCIncrementalIterOptions{
			EnableTimeBoundIteratorOptimization: true,
			EndKey:                              args.Span.EndKey,
			// StartTime is exclusive
			StartTime:    args.Timestamp.Prev(),
			EndTime:      hlc.MaxTimestamp,
			IntentPolicy: storage.MVCCIncrementalIterIntentPolicyEmit,
			InlinePolicy: storage.MVCCIncrementalIterInlinePolicyEmit,
		})
		if args.WithDiff {
			ret.prevValIter = reader.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
				UpperBound: args.Span.EndKey,
			})
		}
	} else {
		ret.SimpleMVCCIterator = reader.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{
			UpperBound: args.Span.EndKey,
		})
	}

	return ret
}

// Close closes the iterator and calls the instantiator-supplied close
// callback.
func (i *CatchupIterator) Close() {
	i.SimpleMVCCIterator.Close()
	if i.prevValIter != nil {
		i.prevValIter.Close()
	}
	if i.close != nil {
		i.close()
	}
}

// UnsafePrevValueForKey returns the value of the key at the previous
// timestamp. Returns `nil, nil` if the CatchupIterator was configured
// such that separately querying the previous value shouldn't be
// necessary.
func (i *CatchupIterator) UnsafePrevValueForKey(key storage.MVCCKey) ([]byte, error) {
	if i.prevValIter == nil {
		return nil, nil
	}

	// NOTE(ssd): I'm unsure if it is better to do this single
	// SeekGE to the modified key vs SeekGE to key and then
	// Next().
	i.prevValIter.SeekGE(storage.MVCCKey{
		Key:       key.Key,
		Timestamp: key.Timestamp.Prev(),
	})
	if ok, err := i.prevValIter.Valid(); err != nil {
		return nil, err
	} else if !ok {
		return nil, nil
	}

	unsafeKey := i.prevValIter.UnsafeKey()
	if !bytes.Equal(unsafeKey.Key, key.Key) {
		return nil, nil
	}
	return i.prevValIter.UnsafeValue(), nil
}

type outputEventFn func(e *roachpb.RangeFeedEvent) error

// CatchupScan iterates over all changes for the given span of keys,
// starting at catchupTimestamp.  Keys and Values are emitted as
// RangeFeedEvents passed to the given outputFn.
func (i *CatchupIterator) CatchupScan(
	startKey, endKey storage.MVCCKey,
	catchupTimestamp hlc.Timestamp,
	withDiff bool,
	outputFn outputEventFn,
) error {
	var a bufalloc.ByteAllocator
	// MVCCIterator will encounter historical values for each key in
	// reverse-chronological order. To output in chronological order, store
	// events for the same key until a different key is encountered, then output
	// the encountered values in reverse. This also allows us to buffer events
	// as we fill in previous values.
	var lastKey storage.MVCCKey
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
		// Our last entry will be erroneously missing a
		// previous value if it was outside the time window of
		// the incremental iterator.
		if l := len(reorderBuf); withDiff && l > 0 {
			prev, err := i.UnsafePrevValueForKey(lastKey)
			if err != nil {
				return err
			}
			if len(prev) > 0 {
				a, reorderBuf[len(reorderBuf)-1].Val.PrevValue.RawBytes = a.Copy(prev, 0)
			}
		}

		for i := len(reorderBuf) - 1; i >= 0; i-- {
			e := reorderBuf[i]
			if err := outputFn(&e); err != nil {
				return err
			}
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
		} else if !ok || !i.UnsafeKey().Less(endKey) {
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
				// This is an MVCCMetadata key for an intent. The catchup scan
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
			unsafeVal = meta.RawBytes
		}

		// Determine whether the iterator moved to a new key.
		sameKey := bytes.Equal(unsafeKey.Key, lastKey.Key)
		if !sameKey {
			// If so, output events for the last key encountered.
			if err := outputEvents(); err != nil {
				return err
			}
			a, lastKey.Key = a.Copy(unsafeKey.Key, 0)
		}
		key := lastKey.Key
		ts := unsafeKey.Timestamp
		lastKey.Timestamp = unsafeKey.Timestamp

		// Ignore the version if it's not inline and its timestamp is at
		// or before the registration's (exclusive) starting timestamp.
		ignore := !(ts.IsEmpty() || catchupTimestamp.Less(ts))
		if ignore && !withDiff {
			// Skip all the way to the next key.
			// NB: fast-path to avoid value copy when !r.withDiff.
			i.NextKey()
			continue
		}

		var val []byte
		a, val = a.Copy(unsafeVal, 0)
		if withDiff {
			// Update the last version with its previous value (this version).
			addPrevToLastEvent(val)
		}

		if ignore {
			// Skip all the way to the next key.
			i.NextKey()
		} else {
			// Move to the next version of this key.
			i.Next()

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

	// Output events for the last key encountered.
	return outputEvents()
}
