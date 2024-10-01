// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.GC, declareKeysGC, GC)
}

func declareKeysGC(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	gcr := req.(*kvpb.GCRequest)
	if gcr.RangeKeys != nil {
		// When GC-ing MVCC range key tombstones, we need to serialize with
		// range key writes that overlap the MVCC range tombstone, as well as
		// the immediate left/right neighboring keys. This is because a range
		// key write has non-local effects, i.e. it can fragment or merge other
		// range keys at other timestamps and at its boundaries, and this has
		// a non-commutative effect on MVCC stats -- if someone writes a new
		// range key while we're GCing one below, the stats would come out wrong.
		//
		// To achieve this, we use a virtual latch key that will prevent any
		// range key write operations until GC request is done. All other range
		// tombstone write operations obtain a read latch on this key and can
		// run concurrently.
		latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
			Key: keys.MVCCRangeKeyGCKey(rs.GetRangeID()),
		})
	}
	// We have three distinct cases for GCClearRange.
	// First is when range has no timestamp which means we will attempt to delete
	// all data in range and that requires wide lock of the whole key span. This
	// is fine as we are discarding all data in range.
	// Second is deleting multiple versions of a single key when most recent data
	// remains. This case is not different from non-ranged GC for individual keys.
	// See explanation of correctness below why we can avoid obtaining locks.
	// Thirs case is deleting multiple keys, and for it we need to obtain a write
	// lock to prevent any addition of the keys that would be covered by the
	// request range span. We will obtain it on highest timestamp to avoid
	// interference with readers that want to read keys. It is safe to read as per
	// correctness explanation below because we only delete data below threshold
	// and that data is not readable.
	// This lock could create contention for writers, but in practice if range
	// contain live data, we will not create wide GCClearRange requests. And if
	// there's no live data for at least gc.ttl period, then it is not likely
	// there would be other writes to the range.
	// A corner case could be if writes were stopped for GCTTL and then resumed,
	// they could hit contention on the first GC run.
	if rk := gcr.ClearRange; rk != nil {
		if rk.StartKeyTimestamp.IsEmpty() || !rk.StartKey.Next().Equal(rk.EndKey) {
			latchSpans.AddMVCC(spanset.SpanReadWrite, roachpb.Span{Key: rk.StartKey, EndKey: rk.EndKey},
				hlc.MaxTimestamp)
		}
	}
	// The RangeGCThresholdKey is only written to if the
	// req.(*GCRequest).Threshold is set. However, we always declare an exclusive
	// access over this key in order to serialize with other GC requests.
	//
	// Correctness:
	// It is correct for a GC request to not declare exclusive access over the
	// keys being GCed because of the following:
	// 1. We define "correctness" to be the property that a reader reading at /
	// around the GC threshold will either see the correct results or receive an
	// error.
	// 2. Readers perform their command evaluation over a stable snapshot of the
	// storage engine. This means that the reader will not see the effects of a
	// subsequent GC run as long as it created a Pebble iterator before the GC
	// request.
	// 3. A reader checks the in-memory GC threshold of a Replica after it has
	// created this snapshot (i.e. after a Pebble iterator has been created).
	// 4. If the in-memory GC threshold is above the timestamp of the read, the
	// reader receives an error. Otherwise, the reader is guaranteed to see a
	// state of the storage engine that hasn't been affected by the GC request [5].
	// 5. GC requests bump the in-memory GC threshold of a Replica as a pre-apply
	// side effect. This means that if a reader checks the in-memory GC threshold
	// after it has created a Pebble iterator, it is impossible for the iterator
	// to point to a storage engine state that has been affected by the GC
	// request.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeGCThresholdKey(rs.GetRangeID())})
	// Needed for Range bounds checks in calls to EvalContext.ContainsKey.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
	// Needed for updating optional GC hint.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeGCHintKey(rs.GetRangeID())})
	latchSpans.DisableUndeclaredAccessAssertions()
	return nil
}

// Create latches and merge adjacent.
func mergeAdjacentSpans(spans []roachpb.Span) []roachpb.Span {
	if len(spans) == 0 {
		return nil
	}
	slices.SortFunc(spans, func(a, b roachpb.Span) int {
		return a.Key.Compare(b.Key)
	})
	j := 0
	for i := 1; i < len(spans); i++ {
		if spans[i].Key.Compare(spans[j].EndKey) < 0 {
			spans[j].EndKey = spans[i].EndKey
		} else {
			j++
		}
	}
	return spans[0 : j+1]
}

// GC iterates through the list of keys to garbage collect
// specified in the arguments. MVCCGarbageCollect is invoked on each
// listed key along with the expiration timestamp. The GC metadata
// specified in the args is persisted after GC.
func GC(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.GCRequest)
	h := cArgs.Header

	// We do not allow GC requests to bump the GC threshold at the same time that
	// they GC individual keys. This is because performing both of these actions
	// at the same time could lead to a race where a read request is allowed to
	// evaluate without error while also failing to see MVCC versions that were
	// concurrently GCed, which would be a form of data corruption.
	//
	// This race is possible because foreground traffic consults the in-memory
	// version of the GC threshold (r.mu.state.GCThreshold), which is updated
	// after (in handleGCThresholdResult), not atomically with, the application of
	// the GC request's WriteBatch to the LSM (in ApplyToStateMachine). This
	// allows a read request to see the effect of a GC on MVCC state without
	// seeing its effect on the in-memory GC threshold.
	//
	// The latching above looks like it will help here, but in practice it does
	// not for two reasons:
	// 1. the latches do not protect timestamps below the GC request's batch
	//    timestamp. This means that they only conflict with concurrent writes,
	//    but not all concurrent reads.
	// 2. the read could be served off a follower, which could be applying the
	//    GC request's effect from the raft log. Latches held on the leaseholder
	//    would have no impact on a follower read.
	if !args.Threshold.IsEmpty() &&
		(len(args.Keys) != 0 || len(args.RangeKeys) != 0 || args.ClearRange != nil) &&
		!cArgs.EvalCtx.EvalKnobs().AllowGCWithNewThresholdAndKeys {
		return result.Result{}, errors.AssertionFailedf(
			"GC request can set threshold or it can GC keys, but it is unsafe for it to do both")
	}

	// We do not allow removal of point or range keys combined with clear range
	// operation as they could cover the same set of keys.
	if (len(args.Keys) != 0 || len(args.RangeKeys) != 0) &&
		args.ClearRange != nil {
		return result.Result{}, errors.AssertionFailedf(
			"GC request can remove point and range keys or clear range, but it is unsafe for it to do both")
	}

	// All keys must be inside the current replica range. Keys outside
	// of this range in the GC request are dropped silently, which is
	// safe because they can simply be re-collected later on the correct
	// replica. Discrepancies here can arise from race conditions during
	// range splitting.
	globalKeys := make([]kvpb.GCRequest_GCKey, 0, len(args.Keys))
	// Local keys are rarer, so don't pre-allocate slice. We separate the two
	// kinds of keys since it is a requirement when calling MVCCGarbageCollect.
	var localKeys []kvpb.GCRequest_GCKey
	for _, k := range args.Keys {
		if cArgs.EvalCtx.ContainsKey(k.Key) {
			if keys.IsLocal(k.Key) {
				localKeys = append(localKeys, k)
			} else {
				globalKeys = append(globalKeys, k)
			}
		}
	}

	// Garbage collect the specified keys by expiration timestamps.
	for _, gcKeys := range [][]kvpb.GCRequest_GCKey{localKeys, globalKeys} {
		if err := storage.MVCCGarbageCollect(
			ctx, readWriter, cArgs.Stats, gcKeys, h.Timestamp,
		); err != nil {
			return result.Result{}, err
		}
	}

	desc := cArgs.EvalCtx.Desc()

	if cr := args.ClearRange; cr != nil {
		// Check if we are performing a fast path operation to try to remove all user
		// key data from the range. All data must be deleted by a range tombstone for
		// the operation to succeed.
		if cr.StartKeyTimestamp.IsEmpty() {
			if !cr.StartKey.Equal(desc.StartKey.AsRawKey()) || !cr.EndKey.Equal(desc.EndKey.AsRawKey()) {
				return result.Result{}, errors.Errorf("gc with clear range operation could only be used on the full range")
			}

			if err := storage.MVCCGarbageCollectWholeRange(ctx, readWriter, cArgs.Stats,
				cr.StartKey, cr.EndKey, cArgs.EvalCtx.GetGCThreshold(),
				cArgs.EvalCtx.GetMVCCStats()); err != nil {
				return result.Result{}, err
			}
		} else {
			// Otherwise garbage collect specified keys defined by clear range i.e. parts
			// of the whole range containing no live data.
			if err := storage.MVCCGarbageCollectPointsWithClearRange(ctx, readWriter, cArgs.Stats,
				cr.StartKey, cr.EndKey, cr.StartKeyTimestamp,
				cArgs.EvalCtx.GetGCThreshold()); err != nil {
				return result.Result{}, err
			}
		}
	}

	// Garbage collect range keys. Note that we pass latch range boundaries for
	// each key as we may need to merge range keys with adjacent ones, but we
	// are restricted on how far we are allowed to read.
	rangeKeys := makeCollectableGCRangesFromGCRequests(desc.StartKey.AsRawKey(),
		desc.EndKey.AsRawKey(), args.RangeKeys)
	if err := storage.MVCCGarbageCollectRangeKeys(ctx, readWriter, cArgs.Stats, rangeKeys); err != nil {
		return result.Result{}, err
	}

	var res result.Result

	gcThreshold := cArgs.EvalCtx.GetGCThreshold()
	// Optionally bump the GC threshold timestamp.
	if !args.Threshold.IsEmpty() {
		// Protect against multiple GC requests arriving out of order; we track
		// the maximum timestamp by forwarding the existing timestamp.
		updated := gcThreshold.Forward(args.Threshold)

		// Don't write the GC threshold key unless we have to.
		if updated {
			if err := MakeStateLoader(cArgs.EvalCtx).SetGCThreshold(
				ctx, readWriter, cArgs.Stats, &gcThreshold,
			); err != nil {
				return result.Result{}, err
			}

			res.Replicated.State = &kvserverpb.ReplicaState{
				GCThreshold: &gcThreshold,
			}
		}
	}

	// Check if optional GC hint on the range is expired (e.g. delete operation is
	// older than GC threshold) and remove it. Otherwise this range could be
	// unnecessarily GC'd with high priority again.
	// We should only do that when we are doing actual cleanup as we want to have
	// a hint when request is being handled.
	if len(args.Keys) != 0 || len(args.RangeKeys) != 0 || args.ClearRange != nil {
		sl := MakeStateLoader(cArgs.EvalCtx)
		hint, err := sl.LoadGCHint(ctx, readWriter)
		if err != nil {
			return result.Result{}, err
		}
		if hint.UpdateAfterGC(gcThreshold) {
			// NB: Replicated.State can already contain GCThreshold from above. Make
			// sure we don't accidentally remove it.
			if res.Replicated.State == nil {
				res.Replicated.State = &kvserverpb.ReplicaState{}
			}
			res.Replicated.State.GCHint = hint
			if err := sl.SetGCHint(ctx, readWriter, cArgs.Stats, hint); err != nil {
				return result.Result{}, err
			}
		}
	}

	return res, nil
}

// makeLookupBoundariesForGCRanges creates spans that could be used for latches
// and iterators when performing range tombstone garbage collection. Each of
// spans includes additional keys to the left and right of the GD'd range to
// ensure merging of range tombstones could be performed and at the same time
// no data is accessed outside of latches.
func makeLookupBoundariesForGCRanges(
	rangeStart, rangeEnd roachpb.Key, rangeKeys []kvpb.GCRequest_GCRangeKey,
) []roachpb.Span {
	spans := make([]roachpb.Span, len(rangeKeys))
	for i := range rangeKeys {
		l, r := rangeTombstonePeekBounds(rangeKeys[i].StartKey, rangeKeys[i].EndKey, rangeStart, rangeEnd)
		spans[i] = roachpb.Span{
			Key:    l,
			EndKey: r,
		}
	}
	return spans
}

// makeCollectableGCRangesFromGCRequests creates GC collectable ranges
// containing ranges to be removed as well as safe iteration boundaries.
// See makeLookupBoundariesForGCRanges for why additional boundaries are used.
func makeCollectableGCRangesFromGCRequests(
	rangeStart, rangeEnd roachpb.Key, rangeKeys []kvpb.GCRequest_GCRangeKey,
) []storage.CollectableGCRangeKey {
	latches := makeLookupBoundariesForGCRanges(rangeStart, rangeEnd, rangeKeys)
	collectableKeys := make([]storage.CollectableGCRangeKey, len(rangeKeys))
	for i, rk := range rangeKeys {
		collectableKeys[i] = storage.CollectableGCRangeKey{
			MVCCRangeKey: storage.MVCCRangeKey{
				StartKey:  rk.StartKey,
				EndKey:    rk.EndKey,
				Timestamp: rk.Timestamp,
			},
			LatchSpan: latches[i],
		}
	}
	return collectableKeys
}
