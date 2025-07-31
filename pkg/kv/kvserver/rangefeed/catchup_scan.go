// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// simpleCatchupIter is an extension of SimpleMVCCIterator that allows for the
// primary iterator to be implemented using a regular MVCCIterator or a
// (often) more efficient MVCCIncrementalIterator. When the caller wants to
// iterate to see older versions of a key, the desire of the caller needs to
// be expressed using one of two methods:
//   - Next: when it wants to omit any versions that are not within the time
//     bounds.
//   - NextIgnoringTime: when it wants to see the next older version even if it
//     is not within the time bounds.
type simpleCatchupIter interface {
	storage.SimpleMVCCIterator
	NextIgnoringTime()
	RangeKeyChangedIgnoringTime() bool
	RangeKeysIgnoringTime() storage.MVCCRangeKeyStack
}

type simpleCatchupIterAdapter struct {
	storage.SimpleMVCCIterator
}

func (i simpleCatchupIterAdapter) NextIgnoringTime() {
	i.SimpleMVCCIterator.Next()
}

func (i simpleCatchupIterAdapter) RangeKeyChangedIgnoringTime() bool {
	return i.SimpleMVCCIterator.RangeKeyChanged()
}

func (i simpleCatchupIterAdapter) RangeKeysIgnoringTime() storage.MVCCRangeKeyStack {
	return i.SimpleMVCCIterator.RangeKeys()
}

var _ simpleCatchupIter = simpleCatchupIterAdapter{}

// CatchUpIterator is an iterator for catchup-scans.
type CatchUpIterator struct {
	simpleCatchupIter
	close     func()
	span      roachpb.Span
	startTime hlc.Timestamp // exclusive
	pacer     *admission.Pacer
	OnEmit    func(key, endKey roachpb.Key, ts hlc.Timestamp, vh enginepb.MVCCValueHeader)
}

// NewCatchUpIterator returns a CatchUpIterator for the given Reader over the
// given key/time span. startTime is exclusive.
//
// NB: startTime is exclusive, i.e. the first possible event will be emitted at
// Timestamp.Next().
func NewCatchUpIterator(
	ctx context.Context,
	reader storage.Reader,
	span roachpb.Span,
	startTime hlc.Timestamp,
	closer func(),
	pacer *admission.Pacer,
) (*CatchUpIterator, error) {
	iter, err := storage.NewMVCCIncrementalIterator(ctx, reader,
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
			ReadCategory: fs.RangefeedReadCategory,
		})
	if err != nil {
		return nil, err
	}
	return &CatchUpIterator{
		simpleCatchupIter: iter,
		close:             closer,
		span:              span,
		startTime:         startTime,
		pacer:             pacer,
	}, nil
}

// Close closes the iterator and calls the instantiator-supplied close
// callback.
func (i *CatchUpIterator) Close() {
	i.simpleCatchupIter.Close()
	i.pacer.Close()
	if i.close != nil {
		i.close()
	}
}

// TODO(ssd): Clarify memory ownership. Currently, the memory backing
// the RangeFeedEvents isn't modified by the caller after this
// returns. However, we may revist this in #69596.
type outputEventFn func(e *kvpb.RangeFeedEvent) error

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
//
// TODO(sumeer): ctx is not used for SeekGE and Next. Fix by adding a method
// to SimpleMVCCIterator to replace the context.
func (i *CatchUpIterator) CatchUpScan(
	ctx context.Context,
	outputFn outputEventFn,
	withDiff bool,
	withFiltering bool,
	withOmitRemote bool,
) error {
	var a bufalloc.ByteAllocator
	// MVCCIterator will encounter historical values for each key in
	// reverse-chronological order. To output in chronological order, store
	// events for the same key until a different key is encountered, then output
	// the encountered values in reverse. This also allows us to buffer events
	// as we fill in previous values.
	reorderBuf := make([]kvpb.RangeFeedEvent, 0, 5)

	outputEvents := func() error {
		for i := len(reorderBuf) - 1; i >= 0; i-- {
			e := reorderBuf[i]
			if err := outputFn(&e); err != nil {
				return err
			}
			reorderBuf[i] = kvpb.RangeFeedEvent{} // Drop references to values to allow GC
		}
		reorderBuf = reorderBuf[:0]
		return nil
	}
	// Iterate though all keys using Next. We want to publish all committed
	// versions of each key that are after the registration's startTS, so we
	// can't use NextKey.
	var lastKey roachpb.Key
	var meta enginepb.MVCCMetadata
	i.SeekGE(storage.MVCCKey{Key: i.span.Key})

	every := log.Every(100 * time.Millisecond)
	for {
		if ok, err := i.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		if err := i.pacer.Pace(ctx); err != nil {
			// We're unable to pace things automatically -- shout loudly
			// semi-infrequently but don't fail the rangefeed itself.
			if every.ShouldLog() {
				log.Errorf(ctx, "automatic pacing: %v", err)
			}
		}

		// Emit any new MVCC range tombstones when their start key is encountered.
		// Range keys can currently only be MVCC range tombstones.
		//
		// NB: RangeKeyChangedIgnoringTime() may trigger because a previous
		// NextIgnoringTime() call moved onto an MVCC range tombstone outside of the
		// time bounds. In this case, HasPointAndRange() will return false,false and
		// we step forward.
		if i.RangeKeyChangedIgnoringTime() {
			hasPoint, hasRange := i.HasPointAndRange()
			if hasRange {
				// Emit events for these MVCC range tombstones, in chronological order.
				rangeKeys := i.RangeKeys()
				for j := rangeKeys.Len() - 1; j >= 0; j-- {
					var span roachpb.Span
					a, span.Key = a.Copy(rangeKeys.Bounds.Key, 0)
					a, span.EndKey = a.Copy(rangeKeys.Bounds.EndKey, 0)
					ts := rangeKeys.Versions[j].Timestamp
					err := outputFn(&kvpb.RangeFeedEvent{
						DeleteRange: &kvpb.RangeFeedDeleteRange{
							Span:      span,
							Timestamp: ts,
						},
					})
					if err != nil {
						return err
					}
					if i.OnEmit != nil {
						v, err := storage.DecodeMVCCValue(rangeKeys.Versions[j].Value)
						if err != nil {
							return err
						}
						i.OnEmit(span.Key, span.EndKey, ts, v.MVCCValueHeader)
					}
				}
			}
			// If there's no point key here (e.g. we found a bare range key above), then
			// step onto the next key. This may be a point key version at the same key
			// as the range key's start bound, or a later point/range key.
			if !hasPoint {
				i.Next()
				continue
			}
		}

		unsafeKey := i.UnsafeKey()
		unsafeValRaw, err := i.UnsafeValue()
		if err != nil {
			return err
		}
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
			if meta.Timestamp.ToTimestamp() != i.UnsafeKey().Timestamp {
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
					// The previous value may have already been set by an event with
					// either OmitInRangefeeds = true (and withFiltering = true) or
					// OriginID !=0 (and withOmitRemote = true). That event is not in
					// reorderBuf because we want to filter it out of the rangefeed, but
					// we still want to keep it as a previous value.
					if !reorderBuf[l].Val.PrevValue.IsPresent() {
						// However, don't emit a value if an MVCC range tombstone existed
						// between this value and the next one. The RangeKeysIgnoringTime()
						// call is cheap, no need for caching.
						rangeKeys := i.RangeKeysIgnoringTime()
						if rangeKeys.IsEmpty() || !rangeKeys.HasBetween(ts, reorderBuf[l].Val.Value.Timestamp) {
							// TODO(sumeer): find out if it is deliberate that we are not populating
							// PrevValue.Timestamp.
							reorderBuf[l].Val.PrevValue.RawBytes = val
						}
					}
				}
			}

			// The iterator may move to the next version for this key if at least one
			// of the conditions is met: 1) the value has the OmitInRangefeeds flag,
			// and this iterator has opted into filtering; 2) the value is from a
			// remote cluster (non zero originID), and the iterator has opted into
			// omitting remote values.
			if (mvccVal.OmitInRangefeeds && withFiltering) || (mvccVal.OriginID != 0 && withOmitRemote) {
				i.Next()
				continue
			}

			if !ignore {
				// Add value to reorderBuf to be output.
				var event kvpb.RangeFeedEvent
				event.MustSetValue(&kvpb.RangeFeedValue{
					Key: key,
					Value: roachpb.Value{
						RawBytes:  val,
						Timestamp: ts,
					},
				})
				reorderBuf = append(reorderBuf, event)
				if i.OnEmit != nil {
					i.OnEmit(key, nil, ts, mvccVal.MVCCValueHeader)
				}
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
