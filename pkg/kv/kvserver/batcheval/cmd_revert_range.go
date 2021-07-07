// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterReadWriteCommand(roachpb.RevertRange, declareKeysRevertRange, RevertRange)
}

func declareKeysRevertRange(
	rs ImmutableRangeState,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	DefaultDeclareIsolatedKeys(rs, header, req, latchSpans, lockSpans)
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeGCThresholdKey(rs.GetRangeID())})
}

// isEmptyKeyTimeRange checks if the span has no writes in (since,until].
func isEmptyKeyTimeRange(
	readWriter storage.ReadWriter, from, to roachpb.Key, since, until hlc.Timestamp,
) (bool, error) {
	// Use a TBI to check if there is anything to delete -- the first key Seek hits
	// may not be in the time range but the fact the TBI found any key indicates
	// that there is *a* key in the SST that is in the time range. Thus we should
	// proceed to iteration that actually checks timestamps on each key.
	iter := readWriter.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
		LowerBound: from, UpperBound: to,
		MinTimestampHint: since.Next() /* make exclusive */, MaxTimestampHint: until,
	})
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
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	if cArgs.Header.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}
	log.VEventf(ctx, 2, "RevertRange %+v", cArgs.Args)

	args := cArgs.Args.(*roachpb.RevertRangeRequest)
	reply := resp.(*roachpb.RevertRangeResponse)
	var pd result.Result

	if empty, err := isEmptyKeyTimeRange(
		readWriter, args.Key, args.EndKey, args.TargetTime, cArgs.Header.Timestamp,
	); err != nil {
		return result.Result{}, err
	} else if empty {
		log.VEventf(ctx, 2, "no keys to clear in specified time range")
		return result.Result{}, nil
	}

	log.VEventf(ctx, 2, "clearing keys with timestamp (%v, %v]", args.TargetTime, cArgs.Header.Timestamp)

	resume, err := storage.MVCCClearTimeRange(ctx, readWriter, cArgs.Stats, args.Key, args.EndKey,
		args.TargetTime, cArgs.Header.Timestamp, cArgs.Header.MaxSpanRequestKeys,
		maxRevertRangeBatchBytes,
		args.EnableTimeBoundIteratorOptimization)
	if err != nil {
		return result.Result{}, err
	}

	if resume != nil {
		log.VEventf(ctx, 2, "hit limit while clearing keys, resume span [%v, %v)", resume.Key, resume.EndKey)
		reply.ResumeSpan = resume

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
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	return pd, nil
}
