// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadOnlyCommand(roachpb.RangeStats, declareKeysRangeStats, RangeStats)
}

func declareKeysRangeStats(
	rs ImmutableRangeState,
	header *roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
	maxOffset time.Duration,
) {
	DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	// The request will return the descriptor and lease.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLeaseKey(rs.GetRangeID())})
}

// RangeStats returns the MVCC statistics for a range.
func RangeStats(
	ctx context.Context, _ storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	reply := resp.(*roachpb.RangeStatsResponse)
	reply.MVCCStats = cArgs.EvalCtx.GetMVCCStats()
	reply.DeprecatedLastQueriesPerSecond = cArgs.EvalCtx.GetLastSplitQPS()
	if qps, ok := cArgs.EvalCtx.GetMaxSplitQPS(); ok {
		reply.MaxQueriesPerSecond = qps
	} else {
		// See comment on MaxQueriesPerSecond. -1 means !ok.
		reply.MaxQueriesPerSecond = -1
	}
	reply.MaxQueriesPerSecondSet = true
	reply.RangeInfo = cArgs.EvalCtx.GetRangeInfo(ctx)
	return result.Result{}, nil
}
