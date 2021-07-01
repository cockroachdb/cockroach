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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadOnlyCommand(roachpb.RecomputeStats, declareKeysRecomputeStats, RecomputeStats)
}

func declareKeysRecomputeStats(
	rs ImmutableRangeState, _ roachpb.Header, _ roachpb.Request, latchSpans, _ *spanset.SpanSet,
) {
	// We don't declare any user key in the range. This is OK since all we're doing is computing a
	// stats delta, and applying this delta commutes with other operations on the same key space.
	//
	// But we want two additional properties:
	// 1) prevent interleaving with splits, and
	// 2) prevent interleaving between different incarnations of `RecomputeStats`.
	//
	// This is achieved by declaring
	// 1) a read on the range descriptor key (thus blocking splits) and
	// 2) a write on a transaction anchored at the range descriptor key (thus blocking any other
	// incarnation of RecomputeStats).
	//
	// Note that we're also accessing the range stats key, but we don't declare it for the same
	// reasons as above.
	rdKey := keys.RangeDescriptorKey(rs.GetStartKey())
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: rdKey})
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.TransactionKey(rdKey, uuid.Nil)})
}

// RecomputeStats recomputes the MVCCStats stored for this range and adjust them accordingly,
// returning the MVCCStats delta obtained in the process.
func RecomputeStats(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	desc := cArgs.EvalCtx.Desc()
	args := cArgs.Args.(*roachpb.RecomputeStatsRequest)
	if !desc.StartKey.AsRawKey().Equal(args.Key) {
		return result.Result{}, errors.New("descriptor mismatch; range likely merged")
	}
	dryRun := args.DryRun

	args = nil // avoid accidental use below

	// Disable the assertions which check that all reads were previously declared.
	// See the comment in `declareKeysRecomputeStats` for details on this.
	reader = spanset.DisableReaderAssertions(reader)

	actualMS, err := rditer.ComputeStatsForRange(desc, reader, cArgs.Header.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, err
	}

	currentStats, err := MakeStateLoader(cArgs.EvalCtx).LoadMVCCStats(ctx, reader)
	if err != nil {
		return result.Result{}, err
	}

	delta := actualMS
	delta.Subtract(currentStats)

	if !dryRun {
		// TODO(tschottdorf): do we not want to run at all if we have estimates in
		// this range? I think we want to as this would give us much more realistic
		// stats for timeseries ranges (which go cold and the approximate stats are
		// wildly overcounting) and this is paced by the consistency checker, but it
		// means some extra engine churn.
		cArgs.Stats.Add(delta)
	}

	resp.(*roachpb.RecomputeStatsResponse).AddedDelta = enginepb.MVCCStatsDelta(delta)
	return result.Result{}, nil
}
