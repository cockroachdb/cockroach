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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.RevertRange, declareKeysRevertRange, RevertRange)
}

func declareKeysRevertRange(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	DefaultDeclareKeys(desc, header, req, spans)
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
}

// isEmptyKeyTimeRange checks if the span has no writes in (since,until].
func isEmptyKeyTimeRange(
	batch engine.ReadWriter, from, to roachpb.Key, since, until hlc.Timestamp,
) (bool, error) {
	// Use a TBI to check if there is anyting to delete -- the first key Seek hits
	// may not be in the time range but the fact the TBI found any key indicates
	// that there is *a* key in the SST that is in the time range. Thus we should
	// proceed to iteration that actually checks timestamps on each key.
	iter := batch.NewIterator(engine.IterOptions{
		LowerBound: from, UpperBound: to,
		MinTimestampHint: since.Next() /* make exclusive */, MaxTimestampHint: until,
	})
	defer iter.Close()
	iter.Seek(engine.MVCCKey{Key: from})
	ok, err := iter.Valid()
	return !ok, err
}

// RevertRange wipes all MVCC versions more recent than TargetTime (up to the
// command timestamp) of the keys covered by the specified span, adjusting the
// MVCC stats accordingly.
//
// Note: this should only be used when there is no user traffic writing to the
// target span at or above the target time.
func RevertRange(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	if cArgs.Header.Txn != nil {
		return result.Result{}, errors.New("cannot execute RevertRange within a transaction")
	}
	log.VEventf(ctx, 2, "RevertRange %+v", cArgs.Args)

	args := cArgs.Args.(*roachpb.RevertRangeRequest)
	reply := resp.(*roachpb.RevertRangeResponse)
	var pd result.Result

	if gc := cArgs.EvalCtx.GetGCThreshold(); !gc.Less(args.TargetTime) {
		return result.Result{}, errors.Errorf("cannot revert before replica GC threshold %v", gc)
	}

	if empty, err := isEmptyKeyTimeRange(
		batch, args.Key, args.EndKey, args.TargetTime, cArgs.Header.Timestamp,
	); err != nil {
		return result.Result{}, err
	} else if empty {
		log.VEventf(ctx, 2, "no keys to clear in specified time range")
		return result.Result{}, nil
	}

	log.VEventf(ctx, 2, "clearing keys with timestamp (%v, %v]", args.TargetTime, cArgs.Header.Timestamp)

	// Get the initial MVCC stats for all the keys in the affected span.
	statsBefore, err := computeStatsDelta(
		ctx, batch, cArgs, engine.MVCCKey{Key: args.Key}, engine.MVCCKey{Key: args.EndKey},
	)
	if err != nil {
		return result.Result{}, err
	}

	resume, err := engine.MVCCClearTimeRange(
		ctx, batch, args.Key, args.EndKey, args.TargetTime, cArgs.Header.Timestamp, cArgs.MaxKeys,
	)
	if err != nil {
		return result.Result{}, err
	}

	// Compute the true stats for the affected span after modification.
	iter := batch.NewIterator(engine.IterOptions{UpperBound: args.Key})
	statsAfter, err := iter.ComputeStats(
		engine.MVCCKey{Key: args.Key}, engine.MVCCKey{Key: args.EndKey}, cArgs.Header.Timestamp.WallTime,
	)
	iter.Close()
	if err != nil {
		return result.Result{}, err
	}

	// Compute the difference between the computed post-modification stats and the
	// initial stats for the span and
	statsAfter.Subtract(statsBefore)

	*cArgs.Stats = statsAfter

	if resume != nil {
		reply.ResumeSpan = resume
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	return pd, nil
}
