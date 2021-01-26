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
	rs ImmutableRangeState,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	declareKeysWriteTransaction(rs, header, req, latchSpans)
}

// HeartbeatTxn updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. Returns the updated transaction.
func HeartbeatTxn(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.HeartbeatTxnRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.HeartbeatTxnResponse)

	if err := VerifyTransaction(h, args, roachpb.PENDING, roachpb.STAGING); err != nil {
		return result.Result{}, err
	}

	if args.Now.IsEmpty() {
		return result.Result{}, fmt.Errorf("now not specified for heartbeat")
	}

	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	var txn roachpb.Transaction
	if ok, err := storage.MVCCGetProto(
		ctx, readWriter, key, hlc.Timestamp{}, &txn, storage.MVCCGetOptions{},
	); err != nil {
		return result.Result{}, err
	} else if !ok {
		// No existing transaction record was found - create one by writing
		// it below.
		txn = *h.Txn

		// Verify that it is safe to create the transaction record.
		if err := CanCreateTxnRecord(ctx, cArgs.EvalCtx, &txn); err != nil {
			return result.Result{}, err
		}
	}

	if !txn.Status.IsFinalized() {
		// NOTE: this only updates the LastHeartbeat. It doesn't update any other
		// field from h.Txn, even if it could. Whether that's a good thing or not
		// is up for debate.
		txn.LastHeartbeat.Forward(args.Now)
		txnRecord := txn.AsRecord()
		if err := storage.MVCCPutProto(ctx, readWriter, cArgs.Stats, key, hlc.Timestamp{}, nil, &txnRecord); err != nil {
			return result.Result{}, err
		}
	}

	reply.Txn = &txn
	return result.Result{}, nil
}
