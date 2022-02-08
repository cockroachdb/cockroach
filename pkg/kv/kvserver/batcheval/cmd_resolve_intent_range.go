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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(roachpb.ResolveIntentRange, declareKeysResolveIntentRange, ResolveIntentRange)
}

func declareKeysResolveIntentRange(
	rs ImmutableRangeState,
	_ *roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
	_ time.Duration,
) {
	declareKeysResolveIntentCombined(rs, req, latchSpans)
}

// ResolveIntentRange resolves write intents in the specified
// key range according to the status of the transaction which created it.
func ResolveIntentRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ResolveIntentRangeRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}

	update := args.AsLockUpdate()
	numKeys, resumeSpan, err := storage.MVCCResolveWriteIntentRange(
		ctx, readWriter, ms, update, h.MaxSpanRequestKeys)
	if err != nil {
		return result.Result{}, err
	}
	reply := resp.(*roachpb.ResolveIntentRangeResponse)
	reply.NumKeys = numKeys
	if resumeSpan != nil {
		update.EndKey = resumeSpan.Key
		reply.ResumeSpan = resumeSpan
		// The given MaxSpanRequestKeys really specifies the number of intents
		// resolved, not the number of keys scanned. We could return
		// RESUME_INTENT_LIMIT here, but since the given limit is a key limit we
		// return RESUME_KEY_LIMIT for symmetry.
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}

	var res result.Result
	res.Local.ResolvedLocks = []roachpb.LockUpdate{update}
	res.Local.Metrics = resolveToMetricType(args.Status, args.Poison)

	if WriteAbortSpanOnResolve(args.Status, args.Poison, numKeys > 0) {
		if err := UpdateAbortSpan(ctx, cArgs.EvalCtx, readWriter, ms, args.IntentTxn, args.Poison); err != nil {
			return result.Result{}, err
		}
	}
	return res, nil
}
