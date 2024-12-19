// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.DeleteRange, declareKeysDeleteRange, DeleteRange)
}

func declareKeysDeleteRange(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) error {
	args := req.(*kvpb.DeleteRangeRequest)
	if args.Inline {
		if err := DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset); err != nil {
			return err
		}
	} else {
		err := DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
		if err != nil {
			return err
		}
	}

	// When writing range tombstones, we must look for adjacent range tombstones
	// that we merge with or fragment, to update MVCC stats accordingly. But we
	// make sure to stay within the range bounds.
	if args.UseRangeTombstone {
		// NB: The range end key is not available, so this will pessimistically
		// latch up to args.EndKey.Next(). If EndKey falls on the range end key, the
		// span will be tightened during evaluation.
		// Even if we obtain latches beyond the end range here, it won't cause
		// contention with the subsequent range because latches are enforced per
		// range.
		l, r := rangeTombstonePeekBounds(args.Key, args.EndKey, rs.GetStartKey().AsRawKey(), nil)
		latchSpans.AddMVCC(spanset.SpanReadOnly, roachpb.Span{Key: l, EndKey: r}, header.Timestamp)

		// We need to read the range descriptor to determine the bounds during eval.
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.RangeDescriptorKey(rs.GetStartKey()),
		})

		// Obtain a read only lock on range key GC key to serialize with
		// range tombstone GC requests.
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.MVCCRangeKeyGCKey(rs.GetRangeID()),
		})

		if args.UpdateRangeDeleteGCHint {
			// If we are updating GC hint, add it to the latch span.
			latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
				Key: keys.RangeGCHintKey(rs.GetRangeID()),
			})
		}
	}
	return nil
}

const maxDeleteRangeBatchBytes = 32 << 20

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func DeleteRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.DeleteRangeRequest)
	reply := resp.(*kvpb.DeleteRangeResponse)

	if args.UpdateRangeDeleteGCHint && !args.UseRangeTombstone {
		// Check for prerequisite for gc hint. If it doesn't hold, this is incorrect
		// usage of hint.
		return result.Result{}, errors.AssertionFailedf(
			"GCRangeHint must only be used together with UseRangeTombstone")
	}

	if args.UseRangeTombstone {
		if cArgs.Header.Txn != nil {
			return result.Result{}, ErrTransactionUnsupported
		}
		if args.Inline {
			return result.Result{}, errors.AssertionFailedf("Inline can't be used with range tombstones")
		}
		if args.ReturnKeys {
			return result.Result{}, errors.AssertionFailedf(
				"ReturnKeys can't be used with range tombstones")
		}
	}

	hasPredicate := args.Predicates != (kvpb.DeleteRangePredicates{})
	if hasPredicate && args.IdempotentTombstone {
		return result.Result{}, errors.AssertionFailedf("IdempotentTombstone not compatible with Predicates")
	}

	switch {
	case hasPredicate:
		return deleteRangeWithPredicate(ctx, readWriter, cArgs, reply)
	case args.UseRangeTombstone:
		return deleteRangeUsingTombstone(ctx, readWriter, cArgs)
	default:
		return deleteRangeTransactional(ctx, readWriter, cArgs, reply)
	}
}

func deleteRangeUsingTombstone(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.DeleteRangeRequest)
	h := cArgs.Header
	desc := cArgs.EvalCtx.Desc()

	maybeUpdateGCHint := func(res *result.Result) error {
		if !args.UpdateRangeDeleteGCHint {
			return nil
		}
		sl := MakeStateLoader(cArgs.EvalCtx)
		hint, err := sl.LoadGCHint(ctx, readWriter)
		if err != nil {
			return err
		}

		updated := hint.ScheduleGCFor(h.Timestamp)
		// If the range tombstone covers the whole Range key span, update the
		// corresponding timestamp in GCHint to enable ClearRange optimization.
		if args.Key.Equal(desc.StartKey.AsRawKey()) && args.EndKey.Equal(desc.EndKey.AsRawKey()) {
			// NB: don't swap the order, we want to call the method unconditionally.
			updated = hint.ForwardLatestRangeDeleteTimestamp(h.Timestamp) || updated
		}
		if !updated {
			return nil
		}

		if err := sl.SetGCHint(ctx, readWriter, cArgs.Stats, hint); err != nil {
			return err
		}
		res.Replicated.State = &kvserverpb.ReplicaState{
			GCHint: hint,
		}
		return nil
	}

	leftPeekBound, rightPeekBound := rangeTombstonePeekBounds(
		args.Key, args.EndKey, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey())
	maxLockConflicts := storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV)
	targetLockConflictBytes := storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV)

	// If no predicate parameters are passed, use the fast path. If we're
	// deleting the entire Raft range, use an even faster path that avoids a
	// point key scan to update MVCC stats.
	var statsCovered *enginepb.MVCCStats
	if args.Key.Equal(desc.StartKey.AsRawKey()) && args.EndKey.Equal(desc.EndKey.AsRawKey()) {
		// NB: We take the fast path even if stats are estimates, because the
		// slow path will likely end up with similarly poor stats anyway.
		s := cArgs.EvalCtx.GetMVCCStats()
		statsCovered = &s
	}
	if err := storage.MVCCDeleteRangeUsingTombstone(ctx, readWriter, cArgs.Stats,
		args.Key, args.EndKey, h.Timestamp, cArgs.Now, leftPeekBound, rightPeekBound,
		args.IdempotentTombstone, maxLockConflicts, targetLockConflictBytes, statsCovered); err != nil {
		return result.Result{}, err
	}

	var res result.Result
	err := maybeUpdateGCHint(&res)
	return res, err
}

func deleteRangeWithPredicate(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.DeleteRangeRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.DeleteRangeResponse)

	if args.Predicates != (kvpb.DeleteRangePredicates{}) && !args.UseRangeTombstone {
		// This ensures predicate based DeleteRange piggybacks on the version gate,
		// roachpb api flags, and latch declarations used by the UseRangeTombstone.
		return result.Result{}, errors.AssertionFailedf(
			"UseRangeTombstones must be passed with predicate based Delete Range")
	}

	if h.MaxSpanRequestKeys == 0 {
		// In production, MaxSpanRequestKeys must be greater than zero to ensure
		// the DistSender serializes predicate based DeleteRange requests across
		// ranges. This ensures that only one resumeSpan gets returned to the
		// client.
		//
		// Also, note that DeleteRangeUsingTombstone requests pass the isAlone
		// flag in roachpb.api.proto, ensuring the requests do not intermingle with
		// other types of requests, preventing further resume span muddling.
		return result.Result{}, errors.AssertionFailedf(
			"MaxSpanRequestKeys must be greater than zero when using predicated based DeleteRange")
	}
	if args.Predicates.ImportEpoch > 0 && !args.Predicates.StartTime.IsEmpty() {
		return result.Result{}, errors.AssertionFailedf(
			"DeleteRangePredicate should not have both non-zero ImportEpoch and non-empty StartTime")
	}

	desc := cArgs.EvalCtx.Desc()

	leftPeekBound, rightPeekBound := rangeTombstonePeekBounds(
		args.Key, args.EndKey, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey())
	maxLockConflicts := storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV)
	targetLockConflictBytes := storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV)

	// TODO (msbutler): Tune the threshold once DeleteRange and DeleteRangeUsingTombstone have
	// been further optimized.
	defaultRangeTombstoneThreshold := int64(64)
	resumeSpan, err := storage.MVCCPredicateDeleteRange(ctx, readWriter, cArgs.Stats,
		args.Key, args.EndKey, h.Timestamp, cArgs.Now, leftPeekBound, rightPeekBound,
		args.Predicates, h.MaxSpanRequestKeys, maxDeleteRangeBatchBytes,
		defaultRangeTombstoneThreshold, maxLockConflicts, targetLockConflictBytes)
	if err != nil {
		return result.Result{}, err
	}

	if resumeSpan != nil {
		// Note: While MVCCPredicateDeleteRange _could_ return reply.NumKeys, as
		// the number of keys iterated through, doing so could lead to a
		// significant performance drawback. The DistSender would have used
		// NumKeys to subtract the number of keys processed by one range from the
		// MaxSpanRequestKeys limit given to the next range. In this case, that's
		// specifically not what we want, because this request does not use the
		// normal meaning of MaxSpanRequestKeys (i.e. number of keys to return).
		// Here, MaxSpanRequest keys is used to limit the number of tombstones
		// written. Thus, setting NumKeys would just reduce the limit available to
		// the next range for no good reason.
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = kvpb.RESUME_KEY_LIMIT
	}
	return result.Result{}, nil
}

func deleteRangeTransactional(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.DeleteRangeRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.DeleteRangeResponse)
	var timestamp hlc.Timestamp
	if !args.Inline {
		timestamp = h.Timestamp
	}

	opts := storage.MVCCWriteOptions{
		Txn:                            h.Txn,
		LocalTimestamp:                 cArgs.Now,
		Stats:                          cArgs.Stats,
		ReplayWriteTimestampProtection: h.AmbiguousReplayProtection,
		OmitInRangefeeds:               cArgs.OmitInRangefeeds,
		OriginID:                       h.WriteOptions.GetOriginID(),
		OriginTimestamp:                h.WriteOptions.GetOriginTimestamp(),
		MaxLockConflicts:               storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		TargetLockConflictBytes:        storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		Category:                       fs.BatchEvalReadCategory,
	}

	// NB: Even if args.ReturnKeys is false, we want to know which intents were
	// written if we're evaluating the DeleteRange for a transaction so that we
	// can update the Result's AcquiredLocks field.
	returnKeys := args.ReturnKeys || h.Txn != nil
	deleted, resumeSpan, num, acqs, err := storage.MVCCDeleteRange(
		ctx, readWriter, args.Key, args.EndKey,
		h.MaxSpanRequestKeys, timestamp,
		opts, returnKeys)
	if err != nil {
		return result.Result{}, err
	}
	if args.ReturnKeys {
		reply.Keys = deleted
	}
	reply.NumKeys = num
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = kvpb.RESUME_KEY_LIMIT
	}

	// If requested, replace point tombstones with range tombstones.
	if cArgs.EvalCtx.EvalKnobs().UseRangeTombstonesForPointDeletes && h.Txn == nil {
		if err := storage.ReplacePointTombstonesWithRangeTombstones(
			ctx, spanset.DisableReadWriterAssertions(readWriter),
			cArgs.Stats, args.Key, args.EndKey); err != nil {
			return result.Result{}, err
		}
	}

	return result.WithAcquiredLocks(acqs...), nil
}
