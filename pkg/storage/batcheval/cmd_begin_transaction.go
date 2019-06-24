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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterCommand(roachpb.BeginTransaction, declareKeysBeginTransaction, BeginTransaction)
}

// declareKeysWriteTransaction is the shared portion of
// declareKeys{Begin,End,Heartbeat}Transaction.
func declareKeysWriteTransaction(
	_ *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		spans.Add(spanset.SpanReadWrite, roachpb.Span{
			Key: keys.TransactionKey(req.Header().Key, header.Txn.ID),
		})
	}
}

func declareKeysBeginTransaction(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	declareKeysWriteTransaction(desc, header, req, spans)
	spans.Add(spanset.SpanReadOnly, roachpb.Span{
		Key: keys.AbortSpanKey(header.RangeID, header.Txn.ID),
	})
}

// BeginTransaction writes the initial transaction record. Fails in
// the event that a transaction record is already written. This may
// occur if a transaction is started with a batch containing writes
// to different ranges, and the range containing the txn record fails
// to receive the write batch before a heartbeat or txn push is
// performed first and aborts the transaction.
func BeginTransaction(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.BeginTransactionRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.BeginTransactionResponse)

	if err := VerifyTransaction(h, args, roachpb.PENDING); err != nil {
		return result.Result{}, err
	}
	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)
	reply.Txn = h.Txn.Clone()

	// Check whether the transaction record already exists. If it already
	// exists, check its current status and react accordingly.
	var existingTxn roachpb.Transaction
	if ok, err := engine.MVCCGetProto(
		ctx, batch, key, hlc.Timestamp{}, &existingTxn, engine.MVCCGetOptions{},
	); err != nil {
		return result.Result{}, err
	} else if !ok {
		// Verify that it is safe to create the transaction record.
		if err := CanCreateTxnRecord(cArgs.EvalCtx, reply.Txn); err != nil {
			return result.Result{}, err
		}
	} else {
		switch existingTxn.Status {
		case roachpb.ABORTED:
			// Check whether someone has come in ahead and already aborted the
			// txn.
			return result.Result{}, roachpb.NewTransactionAbortedError(
				roachpb.ABORT_REASON_ABORTED_RECORD_FOUND,
			)

		case roachpb.PENDING:
			if h.Txn.Epoch > existingTxn.Epoch {
				// On a transaction retry there will be an extant txn record
				// but this run should have an upgraded epoch. The extant txn
				// record may have been pushed or otherwise updated, so update
				// this command's txn and rewrite the record.
				reply.Txn.Update(&existingTxn)
			} else {
				// Our txn record already exists. This is possible if the first
				// transaction heartbeat evaluated before this BeginTransaction
				// request or if the DistSender re-sent the batch. Either way,
				// this request will contain no new information about the
				// transaction, so treat the BeginTransaction as a no-op.
				return result.Result{}, nil
			}

		case roachpb.STAGING:
			// NB: we could support this case, but there isn't a reason to. No
			// cluster that's performing parallel commits should still be sending
			// BeginTransaction requests.
			fallthrough

		case roachpb.COMMITTED:
			return result.Result{}, roachpb.NewTransactionStatusError(
				fmt.Sprintf("BeginTransaction can't overwrite %s", existingTxn),
			)

		default:
			return result.Result{}, roachpb.NewTransactionStatusError(
				fmt.Sprintf("bad txn state: %s", existingTxn),
			)
		}
	}

	// Write the txn record.
	txnRecord := reply.Txn.AsRecord()
	return result.Result{}, engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.Timestamp{}, nil, &txnRecord)
}
