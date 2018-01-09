// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package batcheval

import (
	"context"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
)

func init() {
	RegisterCommand(roachpb.AdjustStats, declareKeysAdjustStats, AdjustStats)
}

func declareKeysAdjustStats(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	// Declare only the target key, even though we're really iterating over a key range. This is OK
	// since all we're doing is computing a stats delta, and applying this delta commutes with other
	// operations on the same key space (except splits, which always touch the start key).
	DefaultDeclareKeys(desc, header, req, spans)
	// We read the range descriptor. As a side effect, this avoids interleaving with splits (which
	// shorten the range, so our recomputation would be bogus), though declaring the start key
	// (above this comment) already has that effect.
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
}

// AdjustStats recomputes the MVCCStats stored for this range and adjust them accordingly,
// returning the MVCCStats delta obtained in the process.
func AdjustStats(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	desc := cArgs.EvalCtx.Desc()

	args := cArgs.Args.(*roachpb.AdjustStatsRequest)
	reqSpan := roachpb.Span{
		Key:    args.Key,
		EndKey: desc.EndKey.AsRawKey(),
	}
	dryRun := args.DryRun
	args = nil // avoid accidental use below

	descSpan := roachpb.Span{
		Key:    desc.StartKey.AsRawKey(),
		EndKey: desc.EndKey.AsRawKey(),
	}

	if !descSpan.Equal(reqSpan) {
		return result.Result{}, errors.New("descriptor mismatch; range likely merged")
	}

	// If this is a SpanSetBatch, unwrap it. We're intentionally reading without
	// declaring the keys because we know this is safe: We declare the range
	// descriptor key and so nothing is splitting the range now, and all we are
	// doing is emitting a stats update, which commutes with any concurrent writer's
	// updates.
	//
	// NB: we could be computing these stats at any timestamp, but the byte ages
	// become unsuitable for human consumption when the timestamp is far away from
	// "real time", along with some risk of integer overflow.
	eng := spanset.UnwrapBatch(batch)
	actualMS, err := rditer.ComputeStatsForRange(desc, eng, cArgs.Header.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, err
	}

	delta := actualMS
	delta.Subtract(cArgs.EvalCtx.GetMVCCStats())

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

	resp.(*roachpb.AdjustStatsResponse).AddedDelta = enginepb.MVCCNetworkStats(delta)
	return result.Result{}, nil
}
