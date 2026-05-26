// Copyright 2014 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadOnlyCommand(kvpb.QueryTxn, declareKeysQueryTransaction, QueryTxn)
}

func declareKeysQueryTransaction(
	_ ImmutableRangeState,
	_ *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	qr := req.(*kvpb.QueryTxnRequest)
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.TransactionKey(qr.Txn.Key, qr.Txn.ID)})
	return nil
}

// QueryTxn fetches the current state of a transaction.
// This method is used to continually update the state of a txn
// which is blocked waiting to resolve a conflicting intent. It
// fetches the complete transaction record to determine whether
// priority or status has changed and also fetches a list of
// other txns which are waiting on this transaction in order
// to find dependency cycles.
func QueryTxn(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.QueryTxnRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.QueryTxnResponse)

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}
	if h.WriteTimestamp().Less(args.Txn.MinTimestamp) {
		// This condition must hold for the timestamp cache access in
		// SynthesizeTxnFromMeta to be safe.
		return result.Result{}, errors.AssertionFailedf("QueryTxn request timestamp %s less than txn MinTimestamp %s",
			h.Timestamp, args.Txn.MinTimestamp)
	}
	if !args.Key.Equal(args.Txn.Key) {
		return result.Result{}, errors.AssertionFailedf("QueryTxn request key %s does not match txn key %s",
			args.Key, args.Txn.Key)
	}
	key := keys.TransactionKey(args.Txn.Key, args.Txn.ID)

	// Fetch transaction record; if missing, attempt to synthesize one.
	ok, err := storage.MVCCGetProto(
		ctx, reader, key, hlc.Timestamp{}, &reply.QueriedTxn,
		storage.MVCCGetOptions{ReadCategory: fs.BatchEvalReadCategory},
	)
	if err != nil {
		return result.Result{}, err
	}
	if ok {
		reply.TxnRecordExists = true
	} else {
		// The transaction hasn't written a transaction record yet.
		// Attempt to synthesize it from the provided TxnMeta.
		reply.QueriedTxn = SynthesizeTxnFromMeta(ctx, cArgs.EvalCtx, args.Txn)
	}

	// Get the list of txns waiting on this txn.
	reply.WaitingTxns = cArgs.EvalCtx.GetConcurrencyManager().GetDependents(args.Txn.ID)
	return result.Result{}, nil
}
