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
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.RecomputeStats, declareKeysRecomputeStats, RecomputeStats)
}

func declareKeysRecomputeStats(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
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
	rdKey := keys.RangeDescriptorKey(desc.StartKey)
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: rdKey})
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.TransactionKey(rdKey, uuid.Nil)})
}

// RecomputeStats recomputes the MVCCStats stored for this range and adjust them accordingly,
// returning the MVCCStats delta obtained in the process.
func RecomputeStats(
	ctx context.Context, _ engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	desc := cArgs.EvalCtx.Desc()
	args := cArgs.Args.(*roachpb.RecomputeStatsRequest)
	if !desc.StartKey.AsRawKey().Equal(args.Key) {
		return result.Result{}, errors.New("descriptor mismatch; range likely merged")
	}
	dryRun := args.DryRun

	args = nil // avoid accidental use below

	// Open a snapshot from which we will read everything (including the
	// MVCCStats). This is necessary because a batch does not provide us
	// with a consistent view of the data -- reading from the batch, we
	// could see skew between the stats recomputation and the MVCCStats
	// we read from the range state if concurrent writes are inflight[1].
	//
	// Note that in doing so, we also circumvent the assertions (present in both
	// the EvalContext and the batch in some builds) which check that all reads
	// were previously declared. See the comment in `declareKeysRecomputeStats`
	// for details on this.
	//
	// [1]: see engine.TestBatchReadLaterWrite.
	snap := cArgs.EvalCtx.Engine().NewSnapshot()
	defer snap.Close()

	actualMS, err := rditer.ComputeStatsForRange(desc, snap, cArgs.Header.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, err
	}

	currentStats, err := MakeStateLoader(cArgs.EvalCtx).LoadMVCCStats(ctx, snap)
	if err != nil {
		return result.Result{}, err
	}

	delta := actualMS
	delta.Subtract(currentStats)

	if !dryRun {
		// NB: this will never clear the ContainsEstimates flag. To be able to do this,
		// we would need to guarantee that no command that sets it is in-flight in
		// parallel with this command. This can be achieved by blocking all of the range
		// or by using our inside knowledge that dictates that ranges which contain no
		// timeseries writes never have the flag reset, or by making ContainsEstimates
		// a counter (and ensuring that we're the only one subtracting at any given
		// time).
		//
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
