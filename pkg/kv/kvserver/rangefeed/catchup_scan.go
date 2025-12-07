// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangefeed

import (
	"bytes"
	"context"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/errors"
)

// SimpleCatchupIter is an extension of SimpleMVCCIterator that allows for the
// primary iterator to be implemented using a regular MVCCIterator or a
// (often) more efficient MVCCIncrementalIterator. When the caller wants to
// iterate to see older versions of a key, the desire of the caller needs to
// be expressed using one of two methods:
//   - Next: when it wants to omit any versions that are not within the time
//     bounds.
//   - NextIgnoringTime: when it wants to see the next older version even if it
//     is not within the time bounds.
type SimpleCatchupIter interface {
	storage.SimpleMVCCIterator
	NextIgnoringTime()
	RangeKeyChangedIgnoringTime() bool
	RangeKeysIgnoringTime() storage.MVCCRangeKeyStack
}

// EngSnapshot abstracts an engine snapshot for testing.
type EngSnapshot interface {
	Close()
	NewMVCCIncrementalIterator(
		context.Context, storage.MVCCIncrementalIterOptions) (SimpleCatchupIter, error)
}

type EngSnapshotAdapter struct {
	snap storage.Reader
}

var _ EngSnapshot = EngSnapshotAdapter{}

func (s EngSnapshotAdapter) Close() {
	s.snap.Close()
}

func (s EngSnapshotAdapter) NewMVCCIncrementalIterator(
	ctx context.Context, opts storage.MVCCIncrementalIterOptions,
) (SimpleCatchupIter, error) {
	iter, err := storage.NewMVCCIncrementalIterator(ctx, s.snap, opts)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

// CatchUpSnapshot is an engine snapshot for catchup-scans.
type CatchUpSnapshot struct {
	snap                 EngSnapshot
	iterOpts             storage.MVCCIncrementalIterOptions
	close                func()
	span                 roachpb.Span
	startTime            hlc.Timestamp // exclusive
	pacer                *admission.Pacer
	OnEmit               func(key, endKey roachpb.Key, ts hlc.Timestamp, vh enginepb.MVCCValueHeader)
	iterRecreateDuration time.Duration
}

// NewCatchUpSnapshot returns a CatchUpSnapshot for the given Engine over the
// given key/time span, where span represents a global key range. startTime is
// exclusive.
//
// NB: startTime is exclusive, i.e. the first possible event will be emitted at
// Timestamp.Next().
//
// iterRecreateDuration, if > 0, is the duration after which the
// MVCCIncrementalIterator should be closed and reopened (see the
// storage.snapshot.recreate_iter_duration cluster setting for details).
func NewCatchUpSnapshot(
	eng storage.Engine,
	span roachpb.Span,
	startTime hlc.Timestamp,
	closer func(),
	pacer *admission.Pacer,
	iterRecreateDuration time.Duration,
) *CatchUpSnapshot {
	iterOpts := storage.MVCCIncrementalIterOptions{
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
	}
	// TODO(sumeer): create a snapshot over span and the lock table spans
	// derived from it.
	snap := eng.NewSnapshot()
	return &CatchUpSnapshot{
		snap:                 EngSnapshotAdapter{snap},
		iterOpts:             iterOpts,
		close:                closer,
		span:                 span,
		startTime:            startTime,
		pacer:                pacer,
		iterRecreateDuration: iterRecreateDuration,
	}
}

// Close closes the iterator and calls the instantiator-supplied close
// callback.
func (i *CatchUpSnapshot) Close() {
	i.snap.Close()
	i.pacer.Close()
	if i.close != nil {
		i.close()
	}
}

// TODO(ssd): Clarify memory ownership. Currently, the memory backing
// the RangeFeedEvents isn't modified by the caller after this
// returns. However, we may revist this in #69596.
// TODO(dt): Does this really need to be a pointer to a struct containing all
// pointers? can we pass by value instead?
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
// NB: When the iterator is recreated while positioned on a range key, the
// same range key will be emitted multiple times. This is harmless. The range
// key bounds are the same even when the seek key is higher than the start of
// the range key. For example, with a range key [a, z), and iterator bound [d,
// j), an iterator seeked to d observes range key [d, j), and an iterator
// seeked to e also observes [d, j).
func (i *CatchUpSnapshot) CatchUpScan(
	ctx context.Context,
	emitFn outputEventFn,
	withDiff bool,
	withFiltering bool,
	withOmitRemote bool,
	bulkDeliverySize int,
) error {
	var a bufalloc.ByteAllocator
	// MVCCIterator will encounter historical values for each key in
	// reverse-chronological order. To output in chronological order, store
	// events for the same key until a different key is encountered, then output
	// the encountered values in reverse. This also allows us to buffer events
	// as we fill in previous values.
	reorderBuf := make([]kvpb.RangeFeedEvent, 0, 5)

	outputFn := emitFn
	var emitBufSize int
	var emitBuf []*kvpb.RangeFeedEvent

	if bulkDeliverySize > 0 {
		outputFn = func(event *kvpb.RangeFeedEvent) error {
			emitBuf = append(emitBuf, event)
			emitBufSize += event.Size()
			// If there are ~2MB of buffered events, flush them.
			if emitBufSize >= bulkDeliverySize {
				if err := emitFn(&kvpb.RangeFeedEvent{BulkEvents: &kvpb.RangeFeedBulkEvents{Events: emitBuf}}); err != nil {
					return err
				}
				emitBuf = make([]*kvpb.RangeFeedEvent, 0, len(emitBuf))
				emitBufSize = 0
			}
			return nil
		}
	}

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
	iter, err := i.snap.NewMVCCIncrementalIterator(ctx, i.iterOpts)
	if err != nil {
		return err
	}
	defer func() {
		if iter != nil {
			iter.Close()
		}
	}()
	lastIterCreateTime := crtime.NowMono()
	// Iterate though all keys using Next. We want to publish all committed
	// versions of each key that are after the registration's startTS, so we
	// can't use NextKey.
	var lastKey roachpb.Key
	var meta enginepb.MVCCMetadata
	iter.SeekGE(storage.MVCCKey{Key: i.span.Key})

	for {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		readmitted, err := i.pacer.Pace(ctx)
		if err != nil {
			return err
		}
		// Determine whether the iterator moved to a new key.
		unsafeKey := iter.UnsafeKey()
		sameKey := bytes.Equal(unsafeKey.Key, lastKey)
		if !sameKey {

			// If so, output events for the last key encountered.
			if err := outputEvents(); err != nil {
				return err
			}
			a, lastKey = a.Copy(unsafeKey.Key)
			// NB: we only recreate the iterator when we have moved to a new
			// roachpb.Key. This is because we will need to reposition the new
			// iterator using a seek, and the seek respects the time bounds.
			// Repositioning the iterator using a seek within the different versions
			// of a roachpb.Key is not desirable since we sometimes want to observe
			// a version that is outside the time bounds (see the calls to
			// NextIgnoringTime).
			recreateIter := false
			// recreateIter => now is initialized.
			var now crtime.Mono

			// Under race tests we want to recreate the iterator more often to
			// increase test coverage of this code. Doing this on every iteration
			// proved a bit slow.
			forceRecreateUnderTest := false
			if util.RaceEnabled {
				// Hash the pointer of the current iterator into the range [0, 3] to
				// give us a ~25% chance of recreating the iterator.
				//
				// unsafe.Pointer's documentation implies that this uintptr is not
				// technically guaranteed to be stable across multiple calls. So, we
				// could make a different decision for the same iterator. That's OK, the
				// goal here is to force recreation occasionally, nothing more.
				ptr := uintptr(unsafe.Pointer(&iter))
				forceRecreateUnderTest = util.FibHash(uint64(ptr), 2) == 0
			}

			// We sample the current time only when readmitted is true, to avoid
			// doing it in every iteration of the loop. In practice, readmitted is
			// true after every 100ms of cpu time, and there is no wall time limit
			// on how long one can wait in Pace. So there is a risk that the
			// iterator recreation is arbitrarily delayed. But we accept that risk
			// for now. The alternative would be to pass a context with a timeout to
			// Pace. We also need to consider the CPU cost of recreating the
			// iterator, and using the cpu time to amortize that cost seems
			// reasonable.
			shouldSampleTime := (readmitted && i.iterRecreateDuration > 0) || forceRecreateUnderTest
			if shouldSampleTime {
				// If enough walltime has elapsed, close iter and reopen. This
				// prevents the iter from holding on to Pebble memtables for too long,
				// which can cause OOMs.
				now = crtime.NowMono()
				recreateIter = now.Sub(lastIterCreateTime) > i.iterRecreateDuration || forceRecreateUnderTest
			}
			if recreateIter {
				lastIterCreateTime = now
				iter.Close()
				iter = nil
				iter, err = i.snap.NewMVCCIncrementalIterator(ctx, i.iterOpts)
				if err != nil {
					return err
				}
				// Resume from the same position. NB: we may have arrived at lastKey
				// using NextIgnoringTime() in the previous iteration of this loop,
				// and so lastKey may have a MVCC timestamp that is outside the time
				// window. So the following seek (which does respect the time window)
				// may land on a key > lastKey. Which is why we need to potentially
				// replace lastKey below and to consider the case that the iterator is
				// exhausted.
				iter.SeekGE(storage.MVCCKey{Key: lastKey})
				if ok, err := iter.Valid(); err != nil {
					return err
				} else if !ok {
					break
				}
				unsafeKey = iter.UnsafeKey()
				if !bytes.Equal(unsafeKey.Key, lastKey) {
					// The seek moved past lastKey, so replace lastKey with the new key.
					a, lastKey = a.Copy(unsafeKey.Key)
				}
			}
		}
		// Any stepping of the iterator below will immediately continue, i.e., key
		// is what we will be processing in this iteration of the loop, and is a
		// stable roachpb.Key.
		key := lastKey

		// Emit any new MVCC range tombstones when their start key is encountered.
		// Range keys can currently only be MVCC range tombstones.
		//
		// NB: RangeKeyChangedIgnoringTime() may trigger because a previous
		// NextIgnoringTime() call moved onto an MVCC range tombstone outside of the
		// time bounds. In this case, HasPointAndRange() will return false,false and
		// we step forward.
		if iter.RangeKeyChangedIgnoringTime() {
			hasPoint, hasRange := iter.HasPointAndRange()
			if hasRange {
				// Emit events for these MVCC range tombstones, in chronological order.
				rangeKeys := iter.RangeKeys()
				for j := rangeKeys.Len() - 1; j >= 0; j-- {
					var span roachpb.Span
					a, span.Key = a.Copy(rangeKeys.Bounds.Key)
					a, span.EndKey = a.Copy(rangeKeys.Bounds.EndKey)
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
				iter.Next()
				continue
			}
		}
		// Else since !iter.RangekeyChangedIgnoringTime, we must have stepped to a
		// new point key.

		if util.RaceEnabled {
			hasPoint, _ := iter.HasPointAndRange()
			if !hasPoint {
				return errors.AssertionFailedf("expected point key: %s", iter.UnsafeKey())
			}
		}
		unsafeValRaw, err := iter.UnsafeValue()
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
			iter.NextIgnoringTime()

			if ok, err := iter.Valid(); err != nil {
				return errors.Wrap(err, "iterating to provisional value for intent")
			} else if !ok {
				return errors.Errorf("expected provisional value for intent")
			}
			if meta.Timestamp.ToTimestamp() != iter.UnsafeKey().Timestamp {
				return errors.Errorf("expected provisional value for intent with ts %s, found %s",
					meta.Timestamp, iter.UnsafeKey().Timestamp)
			}
			if util.RaceEnabled && !bytes.Equal(key, iter.UnsafeKey().Key) {
				return errors.AssertionFailedf("expected key %s, and found %s",
					key, iter.UnsafeKey().Key)
			}
			// Now move to the next key of interest. Note that if in the last
			// iteration of the loop we called `NextIgnoringTime`, the fact that we
			// hit an intent proves that there wasn't a previous value, so we can
			// (in fact, have to, to avoid surfacing unwanted keys) unconditionally
			// enforce time bounds.
			iter.Next()
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
			iter.NextKey()
			continue
		}

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
			a, val = a.Copy(unsafeVal)
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
						rangeKeys := iter.RangeKeysIgnoringTime()
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
				iter.Next()
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
			iter.NextKey()
		} else {
			// Move to the next version of this key (there may not be one, in which
			// case it will move to the next key).
			if withDiff {
				// Need to see the next version even if it is older than the time
				// bounds.
				iter.NextIgnoringTime()
			} else {
				iter.Next()
			}
		}
	}

	// Output events for the last key encountered.
	if err := outputEvents(); err != nil {
		return err
	}
	// If bulk delivery has buffered anything for emission, flush it.
	if len(emitBuf) > 0 {
		// Flush any remaining buffered events.
		if err := emitFn(&kvpb.RangeFeedEvent{BulkEvents: &kvpb.RangeFeedBulkEvents{Events: emitBuf}}); err != nil {
			return err
		}
	}

	return nil
}
