// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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

// DeclareKeysWriteTransaction is the shared portion of
// declareKeys{Begin,End,Heartbeat}Transaction
func DeclareKeysWriteTransaction(
	_ roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		spans.Add(spanset.SpanReadWrite, roachpb.Span{
			Key: keys.TransactionKey(req.Header().Key, header.Txn.ID),
		})
	}
}

func declareKeysBeginTransaction(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	DeclareKeysWriteTransaction(desc, header, req, spans)
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeTxnSpanGCThresholdKey(header.RangeID)})
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

	if err := VerifyTransaction(h, args); err != nil {
		return result.Result{}, err
	}
	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)
	clonedTxn := h.Txn.Clone()
	reply.Txn = &clonedTxn

	// Verify transaction does not already exist.
	tmpTxn := roachpb.Transaction{}
	ok, err := engine.MVCCGetProto(ctx, batch, key, hlc.Timestamp{}, true, nil, &tmpTxn)
	if err != nil {
		return result.Result{}, err
	}
	if ok {
		switch tmpTxn.Status {
		case roachpb.ABORTED:
			// Check whether someone has come in ahead and already aborted the
			// txn.
			return result.Result{}, roachpb.NewTransactionAbortedError()

		case roachpb.PENDING:
			if h.Txn.Epoch > tmpTxn.Epoch {
				// On a transaction retry there will be an extant txn record
				// but this run should have an upgraded epoch. The extant txn
				// record may have been pushed or otherwise updated, so update
				// this command's txn and rewrite the record.
				reply.Txn.Update(&tmpTxn)
			} else {
				// Our txn record already exists. This is either a client error, sending
				// a duplicate BeginTransaction, or it's an artifact of DistSender
				// re-sending a batch. Assume the latter and ask the client to restart.
				return result.Result{}, roachpb.NewTransactionRetryError(roachpb.RETRY_POSSIBLE_REPLAY)
			}

		case roachpb.COMMITTED:
			return result.Result{}, roachpb.NewTransactionStatusError(
				fmt.Sprintf("BeginTransaction can't overwrite %s", tmpTxn),
			)

		default:
			return result.Result{}, roachpb.NewTransactionStatusError(
				fmt.Sprintf("bad txn state: %s", tmpTxn),
			)
		}
	}

	threshold := cArgs.EvalCtx.GetTxnSpanGCThreshold()

	// Disallow creation of a transaction record if it's at a timestamp before
	// the TxnSpanGCThreshold, as in that case our transaction may already have
	// been aborted by a concurrent actor which encountered one of our intents
	// (which may have been written before this entry).
	//
	// See #9265.
	if reply.Txn.LastActive().Less(threshold) {
		return result.Result{}, roachpb.NewTransactionAbortedError()
	}

	// Write the txn record.
	reply.Txn.Writing = true
	return result.Result{}, engine.MVCCPutProto(ctx, batch, cArgs.Stats, key, hlc.Timestamp{}, nil, reply.Txn)
}
