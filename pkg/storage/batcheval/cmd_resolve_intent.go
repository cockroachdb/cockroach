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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func init() {
	RegisterReadWriteCommand(roachpb.ResolveIntent, declareKeysResolveIntent, ResolveIntent)
}

func declareKeysResolveIntentCombined(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	// TODO(nvanbenschoten): declare this span at the txn's MinTimestamp. See
	// lockTable.UpdateLocks for more.
	DefaultDeclareKeys(desc, header, req, spans)
	var status roachpb.TransactionStatus
	var txnID uuid.UUID
	switch t := req.(type) {
	case *roachpb.ResolveIntentRequest:
		status = t.Status
		txnID = t.IntentTxn.ID
	case *roachpb.ResolveIntentRangeRequest:
		status = t.Status
		txnID = t.IntentTxn.ID
	}
	if status == roachpb.ABORTED {
		// We don't always write to the abort span when resolving an ABORTED
		// intent, but we can't tell whether we will or not ahead of time.
		spans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.AbortSpanKey(header.RangeID, txnID)})
	}
}

func declareKeysResolveIntent(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	declareKeysResolveIntentCombined(desc, header, req, spans)
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
	ctx context.Context, readWriter engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ResolveIntentRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}

	intent := args.AsIntent()
	ok, err := engine.MVCCResolveWriteIntent(ctx, readWriter, ms, intent)
	if err != nil {
		return result.Result{}, err
	}

	var res result.Result
	res.Local.Metrics = resolveToMetricType(args.Status, args.Poison)

	if WriteAbortSpanOnResolve(args.Status, args.Poison, ok) {
		if err := UpdateAbortSpan(ctx, cArgs.EvalCtx, readWriter, ms, args.IntentTxn, args.Poison); err != nil {
			return result.Result{}, err
		}
	}
	return res, nil
}
