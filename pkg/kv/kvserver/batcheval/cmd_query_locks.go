// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadOnlyCommand(roachpb.QueryLocks, declareKeysQueryLocks, QueryLocks)
}

func declareKeysQueryLocks(
	rs ImmutableRangeState,
	header *roachpb.Header,
	req roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
	maxOffset time.Duration,
) {
	DefaultDeclareKeys(rs, header, req, latchSpans, lockSpans, maxOffset)
	// Latch on the range descriptor during evaluation of query locks.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
}

func QueryLocks(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.QueryLocksRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.QueryLocksResponse)

	concurrencyManager := cArgs.EvalCtx.GetConcurrencyManager()
	var keySpan *roachpb.Span
	keyScope := spanset.SpanGlobal
	if args.Span().Valid() {
		s := args.Span()
		keySpan = &s
		if keys.IsLocal(s.Key) {
			keyScope = spanset.SpanLocal
		}
	}
	opts := concurrency.QueryLockTableOptions{
		KeyScope:           keyScope,
		MaxLocks:           h.MaxSpanRequestKeys,
		TargetBytes:        h.TargetBytes,
		IncludeUncontended: args.IncludeUncontended,
	}

	// Collect all LockStateInfo objects from the requested key span, up to the
	// target byte and max key limits specified in the request header.
	lockInfos, resumeState := concurrencyManager.QueryLockTableState(ctx, keySpan, opts)

	// Set the results along with any resume reason/span for the client to
	// continue where this request met its limits.
	reply.Locks = lockInfos
	reply.ResumeReason = resumeState.ResumeReason
	reply.ResumeSpan = resumeState.ResumeSpan
	reply.ResumeNextBytes = resumeState.ResumeNextBytes

	return result.Result{}, nil
}
