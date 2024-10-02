// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadOnlyCommand(kvpb.QueryLocks, declareKeysQueryLocks, QueryLocks)
}

func declareKeysQueryLocks(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// Latch on the range descriptor during evaluation of query locks.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
	return nil
}

// QueryLocks uses the concurrency manager to query the state of locks
// currently tracked by the in-memory lock table across a specified range of
// keys. The results are paginated according to the MaxSpanRequestKeys and
// TargetBytes specified in the request Header, setting the ResponseHeader's
// ResumeSpan and ResumeReason as necessary. Note that at a minimum, the
// response will include one result if at least one lock is found, ensuring
// that we do not allow empty responses due to byte limits.
func QueryLocks(
	ctx context.Context, _ storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.QueryLocksRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.QueryLocksResponse)

	concurrencyManager := cArgs.EvalCtx.GetConcurrencyManager()
	opts := concurrency.QueryLockTableOptions{
		MaxLocks:           h.MaxSpanRequestKeys,
		TargetBytes:        h.TargetBytes,
		IncludeUncontended: args.IncludeUncontended,
	}

	// Collect all LockStateInfo objects from the requested key span, up to the
	// target byte and max key limits specified in the request header.
	lockInfos, resumeState := concurrencyManager.QueryLockTableState(ctx, args.Span(), opts)

	// Set the results along with any resume reason/span for the client to
	// continue where this request met its limits.
	reply.Locks = lockInfos
	reply.NumKeys = int64(len(lockInfos))
	reply.NumBytes = resumeState.TotalBytes
	reply.ResumeReason = resumeState.ResumeReason
	reply.ResumeSpan = resumeState.ResumeSpan
	reply.ResumeNextBytes = resumeState.ResumeNextBytes

	return result.Result{}, nil
}
