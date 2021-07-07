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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(roachpb.GC, declareKeysGC, GC)
}

func declareKeysGC(
	rs ImmutableRangeState,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	// Intentionally don't call DefaultDeclareKeys: the key range in the header
	// is usually the whole range (pending resolution of #7880).
	gcr := req.(*roachpb.GCRequest)
	for _, key := range gcr.Keys {
		if keys.IsLocal(key.Key) {
			latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: key.Key})
		} else {
			latchSpans.AddMVCC(spanset.SpanReadWrite, roachpb.Span{Key: key.Key}, header.Timestamp)
		}
	}
	// Be smart here about blocking on the threshold keys. The GC queue can send an empty
	// request first to bump the thresholds, and then another one that actually does work
	// but can avoid declaring these keys below.
	if !gcr.Threshold.IsEmpty() {
		latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeGCThresholdKey(rs.GetRangeID())})
	}
	// Needed for Range bounds checks in calls to EvalContext.ContainsKey.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
}

// GC iterates through the list of keys to garbage collect
// specified in the arguments. MVCCGarbageCollect is invoked on each
// listed key along with the expiration timestamp. The GC metadata
// specified in the args is persisted after GC.
func GC(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.GCRequest)
	h := cArgs.Header

	// All keys must be inside the current replica range. Keys outside
	// of this range in the GC request are dropped silently, which is
	// safe because they can simply be re-collected later on the correct
	// replica. Discrepancies here can arise from race conditions during
	// range splitting.
	globalKeys := make([]roachpb.GCRequest_GCKey, 0, len(args.Keys))
	// Local keys are rarer, so don't pre-allocate slice. We separate the two
	// kinds of keys since it is a requirement when calling MVCCGarbageCollect.
	var localKeys []roachpb.GCRequest_GCKey
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
	for _, gcKeys := range [][]roachpb.GCRequest_GCKey{localKeys, globalKeys} {
		if err := storage.MVCCGarbageCollect(
			ctx, readWriter, cArgs.Stats, gcKeys, h.Timestamp,
		); err != nil {
			return result.Result{}, err
		}
	}

	// Optionally bump the GC threshold timestamp.
	var res result.Result
	if !args.Threshold.IsEmpty() {
		oldThreshold := cArgs.EvalCtx.GetGCThreshold()

		// Protect against multiple GC requests arriving out of order; we track
		// the maximum timestamp by forwarding the existing timestamp.
		newThreshold := oldThreshold
		updated := newThreshold.Forward(args.Threshold)

		// Don't write the GC threshold key unless we have to. We also don't
		// declare the key unless we have to (to allow the GC queue to batch
		// requests more efficiently), and we must honor what we declare.
		if updated {
			if err := MakeStateLoader(cArgs.EvalCtx).SetGCThreshold(
				ctx, readWriter, cArgs.Stats, &newThreshold,
			); err != nil {
				return result.Result{}, err
			}

			res.Replicated.State = &kvserverpb.ReplicaState{
				GCThreshold: &newThreshold,
			}
		}
	}

	return res, nil
}
