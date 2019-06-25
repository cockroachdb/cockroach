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
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.QueryTxn, declareKeysQueryTransaction, QueryTxn)
}

func declareKeysQueryTransaction(
	_ *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	qr := req.(*roachpb.QueryTxnRequest)
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.TransactionKey(qr.Txn.Key, qr.Txn.ID)})
}

// QueryTxn fetches the current state of a transaction.
// This method is used to continually update the state of a txn
// which is blocked waiting to resolve a conflicting intent. It
// fetches the complete transaction record to determine whether
// priority or status has changed and also fetches a list of
// other txns which are waiting on this transaction in order
// to find dependency cycles.
func QueryTxn(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.QueryTxnRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.QueryTxnResponse)

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}
	// TODO(nvanbenschoten): old clusters didn't attach header timestamps to
	// QueryTxn requests, so only perform this check for clusters that will
	// always attach a valid timestamps.
	checkHeaderTS := cArgs.EvalCtx.ClusterSettings().Version.IsActive(cluster.VersionQueryTxnTimestamp)
	if h.Timestamp.Less(args.Txn.Timestamp) && checkHeaderTS {
		// This condition must hold for the timestamp cache access to be safe.
		return result.Result{}, errors.Errorf("request timestamp %s less than txn timestamp %s", h.Timestamp, args.Txn.Timestamp)
	}
	if !bytes.Equal(args.Key, args.Txn.Key) {
		return result.Result{}, errors.Errorf("request key %s does not match txn key %s", args.Key, args.Txn.Key)
	}
	key := keys.TransactionKey(args.Txn.Key, args.Txn.ID)

	// Fetch transaction record; if missing, attempt to synthesize one.
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.Timestamp{}, &reply.QueriedTxn, engine.MVCCGetOptions{},
	); err != nil {
		return result.Result{}, err
	} else if !ok {
		// The transaction hasn't written a transaction record yet.
		// Attempt to synthesize it from the provided TxnMeta.
		reply.QueriedTxn = SynthesizeTxnFromMeta(cArgs.EvalCtx, args.Txn)
	}
	// Get the list of txns waiting on this txn.
	reply.WaitingTxns = cArgs.EvalCtx.GetTxnWaitQueue().GetDependents(args.Txn.ID)
	return result.Result{}, nil
}
