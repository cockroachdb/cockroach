// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnfeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterReadWriteCommand(kvpb.HeartbeatTxn, declareKeysHeartbeatTransaction, HeartbeatTxn)
}

func declareKeysHeartbeatTransaction(
	rs ImmutableRangeState,
	header *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	return declareKeysWriteTransaction(rs, header, req, latchSpans)
}

// HeartbeatTxn updates the transaction status and heartbeat
// timestamp after receiving transaction heartbeat messages from
// coordinator. Returns the updated transaction.
func HeartbeatTxn(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.HeartbeatTxnRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.HeartbeatTxnResponse)

	if err := VerifyTransaction(h, args, roachpb.PENDING, roachpb.PREPARED, roachpb.STAGING); err != nil {
		return result.Result{}, err
	}

	if args.Now.IsEmpty() {
		return result.Result{}, fmt.Errorf("now not specified for heartbeat")
	}

	key := keys.TransactionKey(h.Txn.Key, h.Txn.ID)

	var txn roachpb.Transaction
	if ok, err := storage.MVCCGetProto(
		ctx, readWriter, key, hlc.MaxTimestamp, &txn, storage.MVCCGetOptions{
			ReadCategory: fs.BatchEvalReadCategory,
		},
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

		// Bump the transaction's WriteTimestamp above the closed timestamp
		// to ensure the TxnFeed resolved timestamp never needs to regress
		// when a new transaction record appears. Unlike EndTxn, HeartbeatTxn
		// silently bumps rather than returning an error because heartbeats
		// bypass the span refresher interceptor.
		closedTS := cArgs.EvalCtx.GetClosedTimestampOlderThanStorageSnapshot()
		if !closedTS.IsEmpty() {
			txn.WriteTimestamp.Forward(closedTS.Next())
		}
	}

	// If the transaction is pending, take the opportunity to determine the
	// minimum timestamp that it will be allowed to commit at to account for any
	// transaction pushes. This can help inform the transaction coordinator of
	// pushes earlier than commit time, but is entirely best-effort.
	//
	// NOTE: we cannot do this if the transaction record is STAGING because it may
	// already be implicitly committed.
	if txn.Status == roachpb.PENDING {
		BumpToMinTxnCommitTS(ctx, cArgs.EvalCtx, &txn)
	}

	if !txn.Status.IsFinalized() {
		// NOTE: this only updates the LastHeartbeat. It doesn't update any other
		// field from h.Txn, even if it could. Whether that's a good thing or not
		// is up for debate.
		txn.LastHeartbeat.Forward(args.Now)
		txnRecord := txn.AsRecord()
		if err := storage.MVCCPutProto(ctx, readWriter, key, cArgs.EvalCtx.Clock().Now(), &txnRecord,
			storage.MVCCWriteOptions{Stats: cArgs.Stats, Category: fs.BatchEvalReadCategory}); err != nil {
			return result.Result{}, err
		}
	}

	reply.Txn = &txn
	var res result.Result
	if !txn.Status.IsFinalized() {
		// Emit a RECORD_WRITTEN op so the TxnFeed processor can track this
		// unresolved transaction for resolved timestamp computation. Heartbeats
		// create or update the transaction record for non-1PC transactions,
		// so this is the primary path for records that don't go through the
		// parallel commit (STAGING) path in EndTxn.
		if txnfeed.Enabled.Get(&cArgs.EvalCtx.ClusterSettings().SV) {
			res.Replicated.TxnFeedOps = &kvserverpb.TxnFeedOps{
				Ops: []kvserverpb.TxnFeedOp{{
					Type:           kvserverpb.TxnFeedOp_RECORD_WRITTEN,
					TxnID:          txn.ID,
					AnchorKey:      txn.Key,
					WriteTimestamp: txn.WriteTimestamp,
				}},
			}
		}
	}
	return res, nil
}
