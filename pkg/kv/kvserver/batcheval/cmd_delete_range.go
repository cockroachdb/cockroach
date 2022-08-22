// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.DeleteRange, declareKeysDeleteRange, DeleteRange)
}

func declareKeysDeleteRange(
	rs ImmutableRangeState,
	header *roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
	maxOffset time.Duration,
) {
	args := req.(*roachpb.DeleteRangeRequest)
	if args.Inline {
		DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	} else {
		DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	}

	// When writing range tombstones, we must look for adjacent range tombstones
	// that we merge with or fragment, to update MVCC stats accordingly. But we
	// make sure to stay within the range bounds.
	if args.UseRangeTombstone {
		// NB: The range end key is not available, so this will pessimistically
		// latch up to args.EndKey.Next(). If EndKey falls on the range end key, the
		// span will be tightened during evaluation.
		l, r := rangeTombstonePeekBounds(args.Key, args.EndKey, rs.GetStartKey().AsRawKey(), nil)
		latchSpans.AddMVCC(spanset.SpanReadOnly, roachpb.Span{Key: l, EndKey: r}, header.Timestamp)

		// We need to read the range descriptor to determine the bounds during eval.
		latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.RangeDescriptorKey(rs.GetStartKey()),
		})

		if args.UpdateRangeDeleteGCHint {
			// If we are updating GC hint, add it to the latch span.
			latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
				Key: keys.RangeGCHintKey(rs.GetRangeID()),
			})
		}
	}
}

const maxDeleteRangeBatchBytes = 32 << 20

// DeleteRange deletes the range of key/value pairs specified by
// start and end keys.
func DeleteRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.DeleteRangeRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.DeleteRangeResponse)

	if args.Predicates != (roachpb.DeleteRangePredicates{}) && !args.UseRangeTombstone {
		// This ensures predicate based DeleteRange piggybacks on the version gate,
		// roachpb api flags, and latch declarations used by the UseRangeTombstone.
		return result.Result{}, errors.AssertionFailedf(
			"UseRangeTombstones must be passed with predicate based Delete Range")
	}

	if args.UpdateRangeDeleteGCHint && !args.UseRangeTombstone {
		// Check for prerequisite for gc hint. If it doesn't hold, this is incorrect
		// usage of hint.
		return result.Result{}, errors.AssertionFailedf(
			"GCRangeHint must only be used together with UseRangeTombstone")
	}

	// Use MVCC range tombstone if requested.
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

		desc := cArgs.EvalCtx.Desc()

		maybeUpdateGCHint := func(res *result.Result) error {
			if !args.UpdateRangeDeleteGCHint {
				return nil
			}
			// If GCHint was provided, then we need to check if this request meets
			// range gc criteria of removing all data. This is not an error as range
			// might have merged since request was sent and we don't want to fail
			// deletion.
			if !args.Key.Equal(desc.StartKey.AsRawKey()) || !args.EndKey.Equal(desc.EndKey.AsRawKey()) {
				return nil
			}
			sl := MakeStateLoader(cArgs.EvalCtx)
			hint, err := sl.LoadGCHint(ctx, readWriter)
			if err != nil {
				return err
			}
			if !hint.ForwardLatestRangeDeleteTimestamp(h.Timestamp) {
				return nil
			}
			canUseGCHint := cArgs.EvalCtx.ClusterSettings().Version.IsActive(ctx,
				clusterversion.GCHintInReplicaState)
			if updated, err := sl.SetGCHint(ctx, readWriter, cArgs.Stats, hint, canUseGCHint); err != nil || !updated {
				return err
			}
			res.Replicated.State = &kvserverpb.ReplicaState{
				GCHint: hint,
			}
			return nil
		}

		leftPeekBound, rightPeekBound := rangeTombstonePeekBounds(
			args.Key, args.EndKey, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey())
		maxIntents := storage.MaxIntentsPerWriteIntentError.Get(&cArgs.EvalCtx.ClusterSettings().SV)

		// If no predicate parameters are passed, use the fast path. If we're
		// deleting the entire Raft range, use an even faster path that avoids a
		// point key scan to update MVCC stats.
		if args.Predicates == (roachpb.DeleteRangePredicates{}) {
			var statsCovered *enginepb.MVCCStats
			if args.Key.Equal(desc.StartKey.AsRawKey()) && args.EndKey.Equal(desc.EndKey.AsRawKey()) {
				// NB: We take the fast path even if stats are estimates, because the
				// slow path will likely end up with similarly poor stats anyway.
				s := cArgs.EvalCtx.GetMVCCStats()
				statsCovered = &s
			}
			if err := storage.MVCCDeleteRangeUsingTombstone(ctx, readWriter, cArgs.Stats,
				args.Key, args.EndKey, h.Timestamp, cArgs.Now, leftPeekBound, rightPeekBound,
				args.IdempotentTombstone, maxIntents, statsCovered); err != nil {
				return result.Result{}, err
			}
			var res result.Result
			err := maybeUpdateGCHint(&res)
			return res, err
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
		if args.IdempotentTombstone {
			return result.Result{}, errors.AssertionFailedf(
				"IdempotentTombstone not compatible with Predicates")
		}
		// TODO (msbutler): Tune the threshold once DeleteRange and DeleteRangeUsingTombstone have
		// been further optimized.
		defaultRangeTombstoneThreshold := int64(64)
		resumeSpan, err := storage.MVCCPredicateDeleteRange(ctx, readWriter, cArgs.Stats,
			args.Key, args.EndKey, h.Timestamp, cArgs.Now, leftPeekBound, rightPeekBound,
			args.Predicates, h.MaxSpanRequestKeys, maxDeleteRangeBatchBytes,
			defaultRangeTombstoneThreshold, maxIntents)

		if resumeSpan != nil {
			reply.ResumeSpan = resumeSpan
			reply.ResumeReason = roachpb.RESUME_KEY_LIMIT

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
		}
		// Return result is always empty, since the reply is populated into the
		// passed in resp pointer.
		return result.Result{}, err
	}

	var timestamp hlc.Timestamp
	if !args.Inline {
		timestamp = h.Timestamp
	}
	// NB: Even if args.ReturnKeys is false, we want to know which intents were
	// written if we're evaluating the DeleteRange for a transaction so that we
	// can update the Result's AcquiredLocks field.
	returnKeys := args.ReturnKeys || h.Txn != nil
	deleted, resumeSpan, num, err := storage.MVCCDeleteRange(
		ctx, readWriter, cArgs.Stats, args.Key, args.EndKey,
		h.MaxSpanRequestKeys, timestamp, cArgs.Now, h.Txn, returnKeys)
	if err == nil && args.ReturnKeys {
		reply.Keys = deleted
	}
	reply.NumKeys = num
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}
	// NB: even if MVCC returns an error, it may still have written an intent
	// into the batch. This allows callers to consume errors like WriteTooOld
	// without re-evaluating the batch. This behavior isn't particularly
	// desirable, but while it remains, we need to assume that an intent could
	// have been written even when an error is returned. This is harmless if the
	// error is not consumed by the caller because the result will be discarded.
	return result.FromAcquiredLocks(h.Txn, deleted...), err
}
