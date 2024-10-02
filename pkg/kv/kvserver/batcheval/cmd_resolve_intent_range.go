// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(kvpb.ResolveIntentRange, declareKeysResolveIntentRange, ResolveIntentRange)
}

func declareKeysResolveIntentRange(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	return declareKeysResolveIntentCombined(rs, req, latchSpans)
}

// ResolveIntentRange resolves write intents in the specified
// key range according to the status of the transaction which created it.
func ResolveIntentRange(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ResolveIntentRangeRequest)
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
	numKeys, numBytes, resumeSpan, resumeReason, replLocksReleased, err :=
		storage.MVCCResolveWriteIntentRange(ctx, readWriter, ms, update,
			storage.MVCCResolveWriteIntentRangeOptions{MaxKeys: h.MaxSpanRequestKeys, TargetBytes: h.TargetBytes},
		)
	if err != nil {
		return result.Result{}, err
	}
	reply := resp.(*kvpb.ResolveIntentRangeResponse)
	reply.NumKeys = numKeys
	reply.NumBytes = numBytes
	if resumeSpan != nil {
		update.EndKey = resumeSpan.Key
		reply.ResumeSpan = resumeSpan
		// The given MaxSpanRequestKeys really specifies the number of intents
		// resolved, not the number of keys scanned. We could return
		// RESUME_INTENT_LIMIT here, but since the given limit is a key limit we
		// return RESUME_KEY_LIMIT for symmetry.
		reply.ResumeReason = resumeReason
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
		//
		// NB: Doing so will update the timestamp cache over the entire key span the
		// request operated over -- we're losing fidelity about which key(s) had
		// locks. We could do a better job tracking and plumbing this information
		// up, but we choose not to.
		reply.ReplicatedLocksReleasedCommitTimestamp = update.Txn.WriteTimestamp
	}

	if WriteAbortSpanOnResolve(args.Status, args.Poison, numKeys > 0) {
		if err := UpdateAbortSpan(ctx, cArgs.EvalCtx, readWriter, ms, args.IntentTxn, args.Poison); err != nil {
			return result.Result{}, err
		}
	}

	// If requested, replace point tombstones with range tombstones.
	if cArgs.EvalCtx.EvalKnobs().UseRangeTombstonesForPointDeletes {
		if err := storage.ReplacePointTombstonesWithRangeTombstones(ctx,
			spanset.DisableReadWriterAssertions(readWriter), ms, args.Key, args.EndKey); err != nil {
			return result.Result{}, err
		}
	}

	return res, nil
}
