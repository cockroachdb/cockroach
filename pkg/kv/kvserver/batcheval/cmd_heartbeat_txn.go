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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterReadWriteCommand(roachpb.HeartbeatTxn, declareKeysHeartbeatTransaction, HeartbeatTxn)
}

func declareKeysHeartbeatTransaction(
	desc *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	declareKeysWriteTransaction(desc, header, req, latchSpans)
}

// HeartbeatTxn updates the transaction status and heartbeat timestamp after
// receiving transaction heartbeat messages from coordinator. Returns the
// updated transaction and a nil error (For HeartbeatTxnRequests, even if an
// error occurs, we still only care about the updated transaction, which is why
// we always return a nil error)
//
// XXX: Make this backwards compatible, or version it by cluster setting.
func HeartbeatTxn(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	req := cArgs.Args.(*roachpb.HeartbeatTxnRequest)
	reply := resp.(*roachpb.HeartbeatTxnResponse)

	if err := VerifyTransaction(req.HeartbeatTxn, req, roachpb.PENDING, roachpb.STAGING); err != nil {
		return result.Result{}, err // XXX: How do these batch errors manifest to our interceptor?
	}

	if req.Now.IsEmpty() {
		return result.Result{}, fmt.Errorf("now not specified for heartbeat")
	}

	key := keys.TransactionKey(req.HeartbeatTxn.Key, req.HeartbeatTxn.ID)

	var txn roachpb.Transaction
	if ok, err := storage.MVCCGetProto(
		ctx, readWriter, key, hlc.Timestamp{}, &txn, storage.MVCCGetOptions{},
	); err != nil {
		return result.Result{}, err
	} else if !ok {
		// No existing transaction record was found - create one by writing
		// it below.
		txn = *req.HeartbeatTxn

		// Verify that it is safe to create the transaction record.
		if err := CanCreateTxnRecord(ctx, cArgs.EvalCtx, &txn); err != nil {
			// CanCreateTxnRecord returns a TransactionAbortedError, so in the
			// case of an error, we create a txn with ABORTED status and add it
			// to the reply
			txn.Status = roachpb.ABORTED
			reply.HeartbeatTxn = &txn
			reply.Error = err
			return result.Result{}, nil
			// XXX: What should we do with an error here? I feel like this
			// should be part of normal flow, not really an error condition.
			// What are we to do with `err` though? Where does it get used? It
			// used to be part of batch error essentially before, and if we're
			// folding it into regular processing, what are we missing?
		}
	}

	if !txn.Status.IsFinalized() {
		// NOTE: this only updates the LastHeartbeat. It doesn't update any other
		// field from h.Txn, even if it could. Whether that's a good thing or not
		// is up for debate.
		txn.LastHeartbeat.Forward(req.Now)
		txnRecord := txn.AsRecord()
		if err := storage.MVCCPutProto(ctx, readWriter, cArgs.Stats, key, hlc.Timestamp{}, nil, &txnRecord); err != nil {
			return result.Result{}, err
		}
	}

	reply.HeartbeatTxn = &txn
	return result.Result{}, nil
}
