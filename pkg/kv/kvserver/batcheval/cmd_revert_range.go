// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// clearRangeThreshold specifies the number of consecutive keys to clear where
// RevertRange will switch for issuing point key clear to clearing a range using
// a Pebble range tombstone. This constant hasn't been tuned here at all, but
// was just borrowed from `clearRangeData` where where this strategy originated.
const clearRangeThreshold = 64

func init() {
	RegisterReadWriteCommand(kvpb.RevertRange, declareKeysRevertRange, RevertRange)
}

func declareKeysRevertRange(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) error {
	args := req.(*kvpb.RevertRangeRequest)
	err := DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	if err != nil {
		return err
	}
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeGCThresholdKey(rs.GetRangeID())})
	// When clearing MVCC range tombstones, we must look for adjacent MVCC range
	// tombstones that we may merge with or fragment, to update MVCC stats
	// accordingly. But we make sure to stay within the range bounds.
	//
	// NB: The range end key is not available, so this will pessimistically
	// latch up to args.EndKey.Next(). If EndKey falls on the range end key, the
	// span will be tightened during evaluation.
	// Even if we obtain latches beyond the end range here, it won't cause
	// contention with the subsequent range because latches are enforced per
	// range.
	l, r := rangeTombstonePeekBounds(args.Key, args.EndKey, rs.GetStartKey().AsRawKey(), nil)
	latchSpans.AddMVCC(spanset.SpanReadOnly, roachpb.Span{Key: l, EndKey: r}, header.Timestamp)

	// Obtain a read only lock on range key GC key to serialize with
	// range tombstone GC requests.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
		Key: keys.MVCCRangeKeyGCKey(rs.GetRangeID()),
	})
	return nil
}

// isEmptyKeyTimeRange checks if the span has no writes in (since,until].
func isEmptyKeyTimeRange(
	ctx context.Context,
	readWriter storage.ReadWriter,
	from, to roachpb.Key,
	since, until hlc.Timestamp,
) (bool, error) {
	// Use a TBI to check if there is anything to delete -- the first key Seek hits
	// may not be in the time range but the fact the TBI found any key indicates
	// that there is *a* key in the SST that is in the time range. Thus we should
	// proceed to iteration that actually checks timestamps on each key.
	iter, err := readWriter.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		KeyTypes:     storage.IterKeyTypePointsAndRanges,
		LowerBound:   from,
		UpperBound:   to,
		MinTimestamp: since.Next(), // make exclusive
		MaxTimestamp: until,
		ReadCategory: fs.BatchEvalReadCategory,
	})
	if err != nil {
		return false, err
	}
	defer iter.Close()
	iter.SeekGE(storage.MVCCKey{Key: from})
	ok, err := iter.Valid()
	return !ok, err
}

const maxRevertRangeBatchBytes = 32 << 20

// RevertRange wipes all MVCC versions more recent than TargetTime (up to the
// command timestamp) of the keys covered by the specified span, adjusting the
// MVCC stats accordingly.
//
// Note: this should only be used when there is no user traffic writing to the
// target span at or above the target time.
func RevertRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	if cArgs.Header.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}
	log.VEventf(ctx, 2, "RevertRange %+v", cArgs.Args)

	args := cArgs.Args.(*kvpb.RevertRangeRequest)
	reply := resp.(*kvpb.RevertRangeResponse)
	desc := cArgs.EvalCtx.Desc()
	pd := result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			MVCCHistoryMutation: &kvserverpb.ReplicatedEvalResult_MVCCHistoryMutation{
				Spans: []roachpb.Span{{Key: args.Key, EndKey: args.EndKey}},
			},
		},
	}

	if empty, err := isEmptyKeyTimeRange(
		ctx, readWriter, args.Key, args.EndKey, args.TargetTime, cArgs.Header.Timestamp,
	); err != nil {
		return result.Result{}, err
	} else if empty {
		log.VEventf(ctx, 2, "no keys to clear in specified time range")
		return result.Result{}, nil
	}

	leftPeekBound, rightPeekBound := rangeTombstonePeekBounds(
		args.Key, args.EndKey, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey())
	maxLockConflicts := storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV)

	log.VEventf(ctx, 2, "clearing keys with timestamp (%v, %v]", args.TargetTime, cArgs.Header.Timestamp)

	resumeKey, err := storage.MVCCClearTimeRange(ctx, readWriter, cArgs.Stats, args.Key, args.EndKey,
		args.TargetTime, cArgs.Header.Timestamp, leftPeekBound, rightPeekBound,
		clearRangeThreshold, cArgs.Header.MaxSpanRequestKeys, maxRevertRangeBatchBytes, maxLockConflicts)
	if err != nil {
		return result.Result{}, err
	}

	if len(resumeKey) > 0 {
		reply.ResumeSpan = &roachpb.Span{Key: resumeKey, EndKey: args.EndKey.Clone()}
		log.VEventf(ctx, 2, "hit limit while clearing keys, resume span %s", reply.ResumeSpan)

		// If, and only if, we're returning a resume span do we want to return >0
		// NumKeys. Distsender will reduce the limit for subsequent requests by the
		// amount returned, but that doesn't really make sense for RevertRange:
		// there isn't some combined result set size we're trying to hit across many
		// requests; just because some earlier range ran X Clears that does not mean
		// we want the next range to run fewer than the limit chosen for the batch
		// size reasons. On the otherhand, we have to pass MaxKeys though if we
		// return a resume span to cause distsender to stop after this request, as
		// currently response combining's handling of resume spans prefers that
		// there only be one. Thus we just set it to MaxKeys when, and only when,
		// we're returning a ResumeSpan.
		reply.NumKeys = cArgs.Header.MaxSpanRequestKeys
		reply.ResumeReason = kvpb.RESUME_KEY_LIMIT
	}

	return pd, nil
}
