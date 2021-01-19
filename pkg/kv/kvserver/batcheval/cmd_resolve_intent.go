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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func init() {
	RegisterReadWriteCommand(roachpb.ResolveIntent, declareKeysResolveIntent, ResolveIntent)
}

func declareKeysResolveIntentCombined(
	rs ImmutableRangeState, req roachpb.Request, latchSpans *spanset.SpanSet,
) {
	var status roachpb.TransactionStatus
	var txnID uuid.UUID
	var minTxnTS hlc.Timestamp
	switch t := req.(type) {
	case *roachpb.ResolveIntentRequest:
		status = t.Status
		txnID = t.IntentTxn.ID
		minTxnTS = t.IntentTxn.MinTimestamp
	case *roachpb.ResolveIntentRangeRequest:
		status = t.Status
		txnID = t.IntentTxn.ID
		minTxnTS = t.IntentTxn.MinTimestamp
	}
	latchSpans.AddMVCC(spanset.SpanReadWrite, req.Header().Span(), minTxnTS)
	if status == roachpb.ABORTED {
		// We don't always write to the abort span when resolving an ABORTED
		// intent, but we can't tell whether we will or not ahead of time.
		latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(rs.GetRangeID(), txnID)})
	}
}

func declareKeysResolveIntent(
	rs ImmutableRangeState, _ roachpb.Header, req roachpb.Request, latchSpans, _ *spanset.SpanSet,
) {
	declareKeysResolveIntentCombined(rs, req, latchSpans)
}

func resolveToMetricType(status roachpb.TransactionStatus, poison bool) *result.Metrics {
	var typ result.Metrics
	if status == roachpb.ABORTED {
		if poison {
			typ.ResolvePoison = 1
		} else {
			typ.ResolveAbort = 1
		}
	} else {
		typ.ResolveCommit = 1
	}
	return &typ
}

// ResolveIntent resolves a write intent from the specified key
// according to the status of the transaction which created it.
func ResolveIntent(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ResolveIntentRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}

	update := args.AsLockUpdate()
	ok, err := storage.MVCCResolveWriteIntent(ctx, readWriter, ms, update)
	if err != nil {
		return result.Result{}, err
	}

	var res result.Result
	res.Local.ResolvedLocks = []roachpb.LockUpdate{update}
	res.Local.Metrics = resolveToMetricType(args.Status, args.Poison)

	if WriteAbortSpanOnResolve(args.Status, args.Poison, ok) {
		if err := UpdateAbortSpan(ctx, cArgs.EvalCtx, readWriter, ms, args.IntentTxn, args.Poison); err != nil {
			return result.Result{}, err
		}
	}
	return res, nil
}
