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

// CatchUpIterator is an iterator for catchup-scans.
type CatchUpIterator struct {
	simpleCatchupIter
	close     func()
	span      roachpb.Span
	startTime hlc.Timestamp // exclusive
}

// NewCatchUpIterator returns a CatchUpIterator for the given Reader over the
// given key/time span. startTime is exclusive.
//
// NB: startTime is exclusive, i.e. the first possible event will be emitted at
// Timestamp.Next().
func NewCatchUpIterator(
	reader storage.Reader, span roachpb.Span, startTime hlc.Timestamp, closer func(),
) *CatchUpIterator {
	return &CatchUpIterator{
		simpleCatchupIter: storage.NewMVCCIncrementalIterator(reader,
			storage.MVCCIncrementalIterOptions{
				KeyTypes:  storage.IterKeyTypePointsAndRanges,
				StartKey:  span.Key,
				EndKey:    span.EndKey,
				StartTime: startTime,
				EndTime:   hlc.MaxTimestamp,
				// We want to emit intents rather than error
				// (the default behavior) so that we can skip
				// over the provisional values during
				// iteration.
				IntentPolicy: storage.MVCCIncrementalIterIntentPolicyEmit,
			}),
		close:     closer,
		span:      span,
		startTime: startTime,
	}
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

// CatchUpScan iterates over all changes in the configured key/time span, and
// emits them as RangeFeedEvents via outputFn in chronological order.
//
// MVCC range tombstones are emitted at their start key, in chronological order.
// Because the start key itself is not timestamped, these will be ordered before
// all of the timestamped point keys that they overlap. For more details, see
// MVCC range key info on storage.SimpleMVCCIterator.
//
// For example, with MVCC range tombstones [a-f)@5 and [a-f)@3 overlapping point
// keys a@6, a@4, and b@2, the emitted order is [a-f)@3,[a-f)@5,a@4,a@6,b@2 because
// the start key "a" is ordered before all of the timestamped point keys.
func (i *CatchUpIterator) CatchUpScan(outputFn outputEventFn, withDiff bool) error {
	var a bufalloc.ByteAllocator
	// MVCCIterator will encounter historical values for each key in
	// reverse-chronological order. To output in chronological order, store
	// events for the same key until a different key is encountered, then output
	// the encountered values in reverse. This also allows us to buffer events
	// as we fill in previous values.
	reorderBuf := make([]roachpb.RangeFeedEvent, 0, 5)

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
	var lastKey roachpb.Key
	var meta enginepb.MVCCMetadata
	var rangeKeysStart roachpb.Key
	i.SeekGE(storage.MVCCKey{Key: i.span.Key})
	for {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		hasPoint, hasRange := i.HasPointAndRange()

		// Emit any new MVCC range tombstones when their start key is encountered.
		// Range keys can currently only be MVCC range tombstones.
		// We need to verify that the range tombstone is visible at the catch-up
		// timestamp, since we might have come here after a call to NextIgnoringTime.
		//
		// TODO(erikgrinaker): Find a faster/better way to detect range key changes
		// that doesn't involve constant comparisons. Pebble probably already knows,
		// we just need a way to ask it.
		// Note that byte slice comparison in Go is smart enough to immediately bail
		// if lengths are different. However, it isn't smart enough to compare from
		// the end, which would really help since our keys share prefixes.
		if hasRange {
			if rangeBounds := i.RangeBounds(); !rangeBounds.Key.Equal(rangeKeysStart) {
				rangeKeysStart = append(rangeKeysStart[:0], rangeBounds.Key...)

				// Emit events for these MVCC range tombstones, in chronological order.
				versions := i.RangeKeys().Versions
				for j := len(versions) - 1; j >= 0; j-- {
					if !i.startTime.LessEq(versions[j].Timestamp) {
						// This range tombstone isn't visible by this catch-up scan.
						continue
					}

					var span roachpb.Span
					a, span.Key = a.Copy(rangeBounds.Key, 0)
					a, span.EndKey = a.Copy(rangeBounds.EndKey, 0)
					err := outputFn(&roachpb.RangeFeedEvent{
						DeleteRange: &roachpb.RangeFeedDeleteRange{
							Span:      span,
							Timestamp: versions[j].Timestamp,
						},
					})
					if err != nil {
						return err
					}
				}
			}
		}

		// If there's no point key here (i.e. we found a bare range key above), then
		// step onto the next key. This may be a point key version at the same key
		// as the range key's start bound, or a later point/range key.
		if !hasPoint {
			i.Next()
			continue
		}

		unsafeKey := i.UnsafeKey()
		unsafeValRaw := i.UnsafeValue()
		if !unsafeKey.IsValue() {
			// Found a metadata key.
			if err := protoutil.Unmarshal(unsafeValRaw, &meta); err != nil {
				return errors.Wrapf(err, "unmarshaling mvcc meta: %v", unsafeKey)
			}

			// Inline values are unsupported by rangefeeds. MVCCIncrementalIterator
			// should have errored on them already.
			if meta.IsInline() {
				return errors.AssertionFailedf("unexpected inline key %s", unsafeKey)
			}

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

		mvccVal, err := storage.DecodeMVCCValue(unsafeValRaw)
		if err != nil {
			return errors.Wrapf(err, "decoding mvcc value: %v", unsafeKey)
		}
		unsafeVal := mvccVal.Value.RawBytes

		// Ignore the version if its timestamp is at or before the registration's
		// (exclusive) starting timestamp.
		ts := unsafeKey.Timestamp
		ignore := ts.LessEq(i.startTime)
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
				// Update the last version with its previous value (this version).
				if l := len(reorderBuf) - 1; l >= 0 {
					if reorderBuf[l].Val.PrevValue.IsPresent() {
						return errors.AssertionFailedf("unexpected previous value %s for key %s",
							reorderBuf[l].Val.PrevValue, key)
					}
					// If an MVCC range tombstone exists between this value and the next
					// one, we don't emit the value after all -- it should be a tombstone.
					//
					// TODO(erikgrinaker): We can't save range keys when we detect changes
					// to rangeKeysStart above, because NextIgnoringTime() could reveal
					// additional MVCC range tombstones below StartTime that cover this
					// point. We need to find a more performant way to handle this.
					if !hasRange || !i.RangeKeys().HasBetween(ts, reorderBuf[l].Val.Value.Timestamp) {
						// TODO(sumeer): find out if it is deliberate that we are not populating
						// PrevValue.Timestamp.
						reorderBuf[l].Val.PrevValue.RawBytes = val
					}
				}
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
