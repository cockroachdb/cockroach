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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func init() {
	RegisterReadWriteCommand(kvpb.ResolveIntent, declareKeysResolveIntent, ResolveIntent)
}

func declareKeysResolveIntentCombined(
	rs ImmutableRangeState, req kvpb.Request, latchSpans *spanset.SpanSet,
) error {
	var status roachpb.TransactionStatus
	var txnID uuid.UUID
	var minTxnTS hlc.Timestamp
	switch t := req.(type) {
	case *kvpb.ResolveIntentRequest:
		status = t.Status
		txnID = t.IntentTxn.ID
		minTxnTS = t.IntentTxn.MinTimestamp
	case *kvpb.ResolveIntentRangeRequest:
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
	return nil
}

func declareKeysResolveIntent(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	return declareKeysResolveIntentCombined(rs, req, latchSpans)
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
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ResolveIntentRequest)
	h := cArgs.Header
	ms := cArgs.Stats

	if h.Txn != nil {
		return result.Result{}, ErrTransactionUnsupported
	}

	update := args.AsLockUpdate()
	if update.ClockWhilePending.NodeID != cArgs.EvalCtx.NodeID() {
		// The observation was from the wrong node. Ignore.
		update.ClockWhilePending = roachpb.ObservedTimestamp{}
	}
	ok, numBytes, resumeSpan, replLocksReleased, err := storage.MVCCResolveWriteIntent(
		ctx, readWriter, ms, update, storage.MVCCResolveWriteIntentOptions{TargetBytes: h.TargetBytes})
	if err != nil {
		return result.Result{}, err
	}
	reply := resp.(*kvpb.ResolveIntentResponse)
	reply.NumBytes = numBytes
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = kvpb.RESUME_BYTE_LIMIT
		return result.Result{}, nil
	}

	var res result.Result
	res.Local.ResolvedLocks = []roachpb.LockUpdate{update}
	res.Local.Metrics = resolveToMetricType(args.Status, args.Poison)

	// Handle replicated lock releases.
	if replLocksReleased && update.Status == roachpb.COMMITTED {
		// A replicated {shared, exclusive} lock was released for a committed
		// transaction. Now that the lock is no longer there, we still need to make
		// sure other transactions can't write underneath the transaction's commit
		// timestamp to the key. We return the transaction's commit timestamp on the
		// response and update the timestamp cache a few layers above to ensure
		// this.
		reply.ReplicatedLocksReleasedCommitTimestamp = update.Txn.WriteTimestamp
	}

	if WriteAbortSpanOnResolve(args.Status, args.Poison, ok) {
		if err := UpdateAbortSpan(ctx, cArgs.EvalCtx, readWriter, ms, args.IntentTxn, args.Poison); err != nil {
			return result.Result{}, err
		}
	}

	// If requested, replace point tombstones with range tombstones.
	if ok && cArgs.EvalCtx.EvalKnobs().UseRangeTombstonesForPointDeletes {
		if err := storage.ReplacePointTombstonesWithRangeTombstones(ctx,
			spanset.DisableReadWriterAssertions(readWriter), ms, update.Key, update.EndKey); err != nil {
			return result.Result{}, err
		}
	}

	return res, nil
}
