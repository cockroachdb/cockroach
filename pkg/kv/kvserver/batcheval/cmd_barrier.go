// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(kvpb.Barrier, declareKeysBarrier, Barrier)
}

func declareKeysBarrier(
	_ ImmutableRangeState,
	_ *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// Barrier is special-cased in the concurrency manager to *not* actually
	// grab these latches. Instead, any conflicting latches with these are waited
	// on, but new latches aren't inserted.
	//
	// This will conflict with all writes and reads over the span, regardless of
	// timestamp. Note that this guarantees that concurrent writes will be flushed
	// out before this request is evaluated, but there is no guarantee regarding
	// flushing out of concurrent reads since they could be getting evaluated on a
	// follower. We don't currently need any guarantees regarding concurrent
	// reads, so this is acceptable.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, req.Header().Span())
	return nil
}

// Barrier evaluation is a no-op, but it still goes through Raft because of
// BatchRequest.RequiresConsensus(). The latch waiting happens in the latch
// manager, and the WithLeaseAppliedIndex info is populated during application.
func Barrier(
	_ context.Context, _ storage.ReadWriter, cArgs CommandArgs, response kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.BarrierRequest)
	resp := response.(*kvpb.BarrierResponse)
	resp.Timestamp = cArgs.EvalCtx.Clock().Now()

	return result.Result{
		Local: result.LocalResult{
			PopulateBarrierResponse: args.WithLeaseAppliedIndex,
		},
	}, nil
}
