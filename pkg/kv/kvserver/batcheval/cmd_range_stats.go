// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadOnlyCommand(kvpb.RangeStats, declareKeysRangeStats, RangeStats)
}

func declareKeysRangeStats(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	maxOffset time.Duration,
) error {
	err := DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	if err != nil {
		return err
	}
	// The request will return the descriptor and lease.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLeaseKey(rs.GetRangeID())})
	return nil
}

// RangeStats returns the MVCC statistics for a range.
func RangeStats(
	ctx context.Context, _ storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	reply := resp.(*kvpb.RangeStatsResponse)
	reply.MVCCStats = cArgs.EvalCtx.GetMVCCStats()

	maxQPS, qpsOK := cArgs.EvalCtx.GetMaxSplitQPS(ctx)
	maxCPU, cpuOK := cArgs.EvalCtx.GetMaxSplitCPU(ctx)
	// See comment on MaxQueriesPerSecond and MaxCPUPerSecond. -1 means !ok.
	reply.MaxCPUPerSecond, reply.MaxQueriesPerSecond = -1, -1
	// NB: We don't expect both cpuOk and qpsOK to be true, however we don't
	// prevent both being set.
	if qpsOK {
		reply.MaxQueriesPerSecond = maxQPS
	}
	if cpuOK {
		reply.MaxCPUPerSecond = maxCPU
	}

	reply.MaxQueriesPerSecondSet = true
	reply.RangeInfo = cArgs.EvalCtx.GetRangeInfo(ctx)
	return result.Result{}, nil
}
