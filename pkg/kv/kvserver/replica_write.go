// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft"
)

// executeWriteBatch is the entry point for client requests which may mutate the
// range's replicated state. Requests taking this path are evaluated and ultimately
// serialized through Raft, but pass through additional machinery whose goal is
// to allow commands which commute to be proposed in parallel. The naive
// alternative, submitting requests to Raft one after another, paying massive
// latency, is only taken for commands whose effects may overlap.
//
// Concretely,
//
// - The timestamp cache is checked to determine if the command's affected keys
//   were accessed with a timestamp exceeding that of the command; if so, the
//   command's timestamp is incremented accordingly.
// - A RaftCommand is constructed. If proposer-evaluated KV is active,
//   the request is evaluated and the Result is placed in the
//   RaftCommand. If not, the request itself is added to the command.
// - The proposal is inserted into the Replica's in-flight proposals map,
//   a lease index is assigned to it, and it is submitted to Raft, returning
//   a channel.
// - The result of the Raft proposal is read from the channel and the command
//   registered with the timestamp cache, its latches are released, and
//   its result (which could be an error) is returned to the client.
//
// Returns either a response or an error, along with the provided concurrency
// guard if it is passing ownership back to the caller of the function.
//
// NB: changing BatchRequest to a pointer here would have to be done cautiously
// as this method makes the assumption that it operates on a shallow copy (see
// call to applyTimestampCache).
func (r *Replica) executeWriteBatch(
	ctx context.Context, ba *roachpb.BatchRequest, st kvserverpb.LeaseStatus, g *concurrency.Guard,
) (br *roachpb.BatchResponse, _ *concurrency.Guard, pErr *roachpb.Error) {
	startTime := timeutil.Now()

	// TODO(nvanbenschoten): unlike on the read-path (executeReadOnlyBatch), we
	// don't synchronize with r.readOnlyCmdMu here. Is that ok? What if the
	// replica is destroyed concurrently with a write? We won't be able to
	// successfully propose as the lease will presumably have changed, but what
	// if we hit an error during evaluation (e.g. a ConditionFailedError)?

	// Verify that the batch can be executed.
	// NB: we only need to check that the request is in the Range's key bounds
	// at proposal time, not at application time, because the spanlatch manager
	// will synchronize all requests (notably EndTxn with SplitTrigger) that may
	// cause this condition to change.
	if err := r.checkExecutionCanProceed(ctx, ba, g, &st); err != nil {
		return nil, g, roachpb.NewError(err)
	}

	minTS, untrack := r.store.cfg.ClosedTimestamp.Tracker.Track(ctx)
	defer untrack(ctx, 0, 0, 0) // covers all error returns below

	// Examine the timestamp cache for preceding commands which require this
	// command to move its timestamp forward. Or, in the case of a transactional
	// write, the txn timestamp and possible write-too-old bool.
	if bumped := r.applyTimestampCache(ctx, ba, minTS); bumped {
		// If we bump the transaction's timestamp, we must absolutely
		// tell the client in a response transaction (for otherwise it
		// doesn't know about the incremented timestamp). Response
		// transactions are set far away from this code, but at the time
		// of writing, they always seem to be set. Since that is a
		// likely target of future micro-optimization, this assertion is
		// meant to protect against future correctness anomalies.
		defer func() {
			if br != nil && ba.Txn != nil && br.Txn == nil {
				log.Fatalf(ctx, "assertion failed: transaction updated by "+
					"timestamp cache, but transaction returned in response; "+
					"updated timestamp would have been lost (recovered): "+
					"%s in batch %s", ba.Txn, ba,
				)
			}
		}()
	}
	log.Event(ctx, "applied timestamp cache")

	// Checking the context just before proposing can help avoid ambiguous errors.
	if err := ctx.Err(); err != nil {
		log.VEventf(ctx, 2, "%s before proposing: %s", err, ba.Summary())
		return nil, g, roachpb.NewError(errors.Wrap(err, "aborted before proposing"))
	}

	// Check that the lease is still valid before proposing to avoid discovering
	// this after replication and potentially missing out on the chance to retry
	// if the request is using AsyncConsensus. This is best-effort, but can help
	// in cases where the request waited arbitrarily long for locks acquired by
	// other transactions to be released while sequencing in the concurrency
	// manager.
	if curLease, _ := r.GetLease(); curLease.Sequence > st.Lease.Sequence {
		curLeaseCpy := curLease // avoid letting curLease escape
		err := newNotLeaseHolderError(&curLeaseCpy, r.store.StoreID(), r.Desc())
		log.VEventf(ctx, 2, "%s before proposing: %s", err, ba.Summary())
		return nil, g, roachpb.NewError(err)
	}

	// If the command is proposed to Raft, ownership of and responsibility for
	// the concurrency guard will be assumed by Raft, so provide the guard to
	// evalAndPropose.
	ch, abandon, maxLeaseIndex, pErr := r.evalAndPropose(ctx, ba, g, &st.Lease)
	if pErr != nil {
		if maxLeaseIndex != 0 {
			log.Fatalf(
				ctx, "unexpected max lease index %d assigned to failed proposal: %s, error %s",
				maxLeaseIndex, ba, pErr,
			)
		}
		return nil, g, pErr
	}
	g = nil // ownership passed to Raft, prevent misuse

	// A max lease index of zero is returned when no proposal was made or a lease was proposed.
	// In both cases, we don't need to communicate a MLAI. Furthermore, for lease proposals we
	// cannot communicate under the lease's epoch. Instead the code calls EmitMLAI explicitly
	// as a side effect of stepping up as leaseholder.
	if maxLeaseIndex != 0 {
		untrack(ctx, ctpb.Epoch(st.Lease.Epoch), r.RangeID, ctpb.LAI(maxLeaseIndex))
	}

	// If the command was accepted by raft, wait for the range to apply it.
	ctxDone := ctx.Done()
	shouldQuiesce := r.store.stopper.ShouldQuiesce()
	startPropTime := timeutil.Now()
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	slowTimer.Reset(base.SlowRequestThreshold)
	// NOTE: this defer was moved from a case in the select statement to here
	// because escape analysis does a better job avoiding allocations to the
	// heap when defers are unconditional. When this was in the slowTimer select
	// case, it was causing pErr to escape.
	defer func() {
		if slowTimer.Read {
			r.store.metrics.SlowRaftRequests.Dec(1)
			log.Infof(
				ctx,
				"slow command %s finished after %.2fs with error %v",
				ba,
				timeutil.Since(startPropTime).Seconds(),
				pErr,
			)
		}
	}()

	for {
		select {
		case propResult := <-ch:
			// Semi-synchronously process any intents that need resolving here in
			// order to apply back pressure on the client which generated them. The
			// resolution is semi-synchronous in that there is a limited number of
			// outstanding asynchronous resolution tasks allowed after which
			// further calls will block.
			if len(propResult.EncounteredIntents) > 0 {
				// TODO(peter): Re-proposed and canceled (but executed) commands can
				// both leave intents to GC that don't hit this code path. No good
				// solution presents itself at the moment and such intents will be
				// resolved on reads.
				if err := r.store.intentResolver.CleanupIntentsAsync(
					ctx, propResult.EncounteredIntents, true, /* allowSync */
				); err != nil {
					log.Warningf(ctx, "%v", err)
				}
			}
			if len(propResult.EndTxns) > 0 {
				if err := r.store.intentResolver.CleanupTxnIntentsAsync(
					ctx, r.RangeID, propResult.EndTxns, true, /* allowSync */
				); err != nil {
					log.Warningf(ctx, "%v", err)
				}
			}
			return propResult.Reply, nil, propResult.Err
		case <-slowTimer.C:
			slowTimer.Read = true
			r.store.metrics.SlowRaftRequests.Inc(1)

			log.Errorf(ctx, "range unavailable: %v",
				rangeUnavailableMessage(r.Desc(), r.store.cfg.NodeLiveness.GetIsLiveMap(),
					r.RaftStatus(), ba, timeutil.Since(startPropTime)))
		case <-ctxDone:
			// If our context was canceled, return an AmbiguousResultError,
			// which indicates to the caller that the command may have executed.
			abandon()
			log.VEventf(ctx, 2, "context cancellation after %0.1fs of attempting command %s",
				timeutil.Since(startTime).Seconds(), ba)
			return nil, nil, roachpb.NewError(roachpb.NewAmbiguousResultError(ctx.Err().Error()))
		case <-shouldQuiesce:
			// If shutting down, return an AmbiguousResultError, which indicates
			// to the caller that the command may have executed.
			abandon()
			log.VEventf(ctx, 2, "shutdown cancellation after %0.1fs of attempting command %s",
				timeutil.Since(startTime).Seconds(), ba)
			return nil, nil, roachpb.NewError(roachpb.NewAmbiguousResultError("server shutdown"))
		}
	}
}

func rangeUnavailableMessage(
	desc *roachpb.RangeDescriptor,
	lm IsLiveMap,
	rs *raft.Status,
	ba *roachpb.BatchRequest,
	dur time.Duration,
) string {
	cpy := *desc
	desc = &cpy
	desc.StartKey, desc.EndKey = nil, nil // scrub PII

	var liveReplicas, otherReplicas []roachpb.ReplicaDescriptor
	for _, rDesc := range desc.Replicas().All() {
		if lm[rDesc.NodeID].IsLive {
			liveReplicas = append(liveReplicas, rDesc)
		} else {
			otherReplicas = append(otherReplicas, rDesc)
		}
	}
	return fmt.Sprintf(`have been waiting %.2fs for proposing command %s.
This range is likely unavailable.
Please submit this message to Cockroach Labs support along with the following information:

Descriptor:  %s
Live:        %s
Non-live:    %s
Raft Status: %+v

and a copy of https://yourhost:8080/#/reports/range/%d

If you are using CockroachDB Enterprise, reach out through your
support contract. Otherwise, please open an issue at:

  https://github.com/cockroachdb/cockroach/issues/new/choose
`,
		dur.Seconds(),
		ba,
		desc,
		roachpb.MakeReplicaDescriptors(liveReplicas),
		roachpb.MakeReplicaDescriptors(otherReplicas),
		rs,
		desc.RangeID,
	)
}

// canAttempt1PCEvaluation looks at the batch and decides whether it can be
// executed as 1PC.
func (r *Replica) canAttempt1PCEvaluation(
	ctx context.Context, ba *roachpb.BatchRequest, latchSpans *spanset.SpanSet,
) (bool, *roachpb.Error) {
	if !isOnePhaseCommit(ba) {
		return false, nil
	}

	if ba.Timestamp != ba.Txn.WriteTimestamp {
		log.Fatalf(ctx, "unexpected 1PC execution with diverged timestamp. %s != %s",
			ba.Timestamp, ba.Txn.WriteTimestamp)
	}

	// The EndTxn checks whether the txn record can be created, but we're
	// eliding the EndTxn. So, we'll do the check instead.
	ok, minCommitTS, reason := r.CanCreateTxnRecord(ba.Txn.ID, ba.Txn.Key, ba.Txn.MinTimestamp)
	if !ok {
		newTxn := ba.Txn.Clone()
		newTxn.Status = roachpb.ABORTED
		return false, roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(reason), newTxn)
	}
	if ba.Timestamp.Less(minCommitTS) {
		ba.Txn.WriteTimestamp = minCommitTS
		// We can only evaluate at the new timestamp if we manage to bump the read
		// timestamp.
		return maybeBumpReadTimestampToWriteTimestamp(ctx, ba, latchSpans), nil
	}
	return true, nil
}

// evaluateWriteBatch evaluates the supplied batch.
//
// If the batch is transactional and has all the hallmarks of a 1PC commit (i.e.
// includes all intent writes & EndTxn, and there's nothing to suggest that the
// transaction will require retry or restart), the batch's txn is stripped and
// it's executed as an atomic batch write. If the writes cannot all be completed
// at the intended timestamp, the batch's txn is restored and it's re-executed
// in full. This allows it to lay down intents and return an appropriate
// retryable error.
func (r *Replica) evaluateWriteBatch(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
) (storage.Batch, enginepb.MVCCStats, *roachpb.BatchResponse, result.Result, *roachpb.Error) {
	log.Event(ctx, "executing read-write batch")

	// If the transaction has been pushed but it can commit at the higher
	// timestamp, let's evaluate the batch at the bumped timestamp. This will
	// allow it commit, and also it'll allow us to attempt the 1PC code path.
	maybeBumpReadTimestampToWriteTimestamp(ctx, ba, latchSpans)

	// Attempt 1PC execution, if applicable. If not transactional or there are
	// indications that the batch's txn will require retry, execute as normal.
	ok, pErr := r.canAttempt1PCEvaluation(ctx, ba, latchSpans)
	if pErr != nil {
		return nil, enginepb.MVCCStats{}, nil, result.Result{}, pErr
	}
	if ok {
		res := r.evaluate1PC(ctx, idKey, ba, latchSpans)
		switch res.success {
		case onePCSucceeded:
			return res.batch, res.stats, res.br, res.res, nil
		case onePCFailed:
			if res.pErr == nil {
				log.Fatalf(ctx, "1PC failed but no err. ba: %s", ba.String())
			}
			return nil, enginepb.MVCCStats{}, nil, result.Result{}, res.pErr
		case onePCFallbackToTransactionalEvaluation:
		}
	}

	ms := new(enginepb.MVCCStats)
	rec := NewReplicaEvalContext(r, latchSpans)
	batch, br, res, pErr := r.evaluateWriteBatchWithServersideRefreshes(
		ctx, idKey, rec, ms, ba, latchSpans, nil /* deadline */)
	return batch, *ms, br, res, pErr
}

type onePCSuccess int

const (
	// onePCSucceeded means that the 1PC evaluation succeeded and the results should be
	// returned to the client.
	onePCSucceeded onePCSuccess = iota
	// onePCFailed means that the 1PC evaluation failed and the attached error should be
	// returned to the client.
	onePCFailed
	// onePCFallbackToTransactionalEvaluation means that 1PC evaluation failed, but
	// regular transactional evaluation should be attempted.
	onePCFallbackToTransactionalEvaluation
)

type onePCResult struct {
	success onePCSuccess
	// pErr is set if success == onePCFailed. This is the error that should be
	// returned to the client for this request.
	pErr *roachpb.Error

	// The fields below are only set when success == onePCSucceeded.
	stats enginepb.MVCCStats
	br    *roachpb.BatchResponse
	res   result.Result
	batch storage.Batch
}

// evaluate1PC attempts to evaluate the batch as a 1PC transaction - meaning it
// attempts to evaluate the batch as a non-transactional request. This is only
// possible if the batch contains all of the transaction's writes, which the
// caller needs to ensure. If successful, evaluating the batch this way is more
// efficient - we're avoiding writing the transaction record and writing and the
// immediately deleting intents.
func (r *Replica) evaluate1PC(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
) (onePCRes onePCResult) {
	log.VEventf(ctx, 2, "attempting 1PC execution")

	var batch storage.Batch
	defer func() {
		// Close the batch unless it's passed to the caller (when the evaluation
		// succeeds).
		if onePCRes.success != onePCSucceeded {
			batch.Close()
		}
	}()

	// Try executing with transaction stripped.
	strippedBa := *ba
	strippedBa.Txn = nil
	strippedBa.Requests = ba.Requests[:len(ba.Requests)-1] // strip end txn req

	rec := NewReplicaEvalContext(r, latchSpans)
	var br *roachpb.BatchResponse
	var res result.Result
	var pErr *roachpb.Error

	arg, _ := ba.GetArg(roachpb.EndTxn)
	etArg := arg.(*roachpb.EndTxnRequest)
	canFwdTimestamp := batcheval.CanForwardCommitTimestampWithoutRefresh(ba.Txn, etArg)

	// Evaluate strippedBa. If the transaction allows, permit refreshes.
	ms := new(enginepb.MVCCStats)
	if canFwdTimestamp {
		batch, br, res, pErr = r.evaluateWriteBatchWithServersideRefreshes(
			ctx, idKey, rec, ms, &strippedBa, latchSpans, etArg.Deadline)
	} else {
		batch, br, res, pErr = r.evaluateWriteBatchWrapper(
			ctx, idKey, rec, ms, &strippedBa, latchSpans)
	}

	if pErr != nil || (!canFwdTimestamp && ba.Timestamp != br.Timestamp) {
		if pErr != nil {
			log.VEventf(ctx, 2,
				"1PC execution failed, falling back to transactional execution. pErr: %v", pErr.String())
		} else {
			log.VEventf(ctx, 2,
				"1PC execution failed, falling back to transactional execution; the batch was pushed")
		}
		return onePCResult{success: onePCFallbackToTransactionalEvaluation}
	}

	// 1PC execution was successful, let's synthesize an EndTxnResponse.

	clonedTxn := ba.Txn.Clone()
	clonedTxn.Status = roachpb.COMMITTED
	// Make sure the returned txn has the actual commit timestamp. This can be
	// different from ba.Txn's if the stripped batch was evaluated at a bumped
	// timestamp.
	clonedTxn.ReadTimestamp = br.Timestamp
	clonedTxn.WriteTimestamp = br.Timestamp

	// If the end transaction is not committed, clear the batch and mark the status aborted.
	if !etArg.Commit {
		clonedTxn.Status = roachpb.ABORTED
		batch.Close()
		batch = r.store.Engine().NewBatch()
		ms = new(enginepb.MVCCStats)
	} else {
		// Run commit trigger manually.
		innerResult, err := batcheval.RunCommitTrigger(ctx, rec, batch, ms, etArg, clonedTxn)
		if err != nil {
			return onePCResult{
				success: onePCFailed,
				pErr:    roachpb.NewErrorf("failed to run commit trigger: %s", err),
			}
		}
		if err := res.MergeAndDestroy(innerResult); err != nil {
			return onePCResult{
				success: onePCFailed,
				pErr:    roachpb.NewError(err),
			}
		}
	}

	// Even though the transaction is 1PC and hasn't written any intents, it may
	// have acquired unreplicated locks, so inform the concurrency manager that
	// it is finalized and than any unreplicated locks that it has acquired can
	// be released.
	res.Local.UpdatedTxns = []*roachpb.Transaction{clonedTxn}
	res.Local.ResolvedLocks = make([]roachpb.LockUpdate, len(etArg.LockSpans))
	for i, sp := range etArg.LockSpans {
		res.Local.ResolvedLocks[i] = roachpb.LockUpdate{
			Span:           sp,
			Txn:            clonedTxn.TxnMeta,
			Status:         clonedTxn.Status,
			IgnoredSeqNums: clonedTxn.IgnoredSeqNums,
		}
	}

	// Add placeholder responses for end transaction requests.
	br.Add(&roachpb.EndTxnResponse{OnePhaseCommit: true})
	br.Txn = clonedTxn
	return onePCResult{
		success: onePCSucceeded,
		stats:   *ms,
		br:      br,
		res:     res,
		batch:   batch,
	}
}

// evaluateWriteBatchWithServersideRefreshes invokes evaluateBatch and retries
// at a higher timestamp in the event of some retriable errors if allowed by the
// batch/txn.
//
// deadline, if not nil, specifies the highest timestamp (exclusive) at which
// the request can be evaluated. If ba is a transactional request, then dealine
// cannot be specified; a transaction's deadline comes from it's EndTxn request.
func (r *Replica) evaluateWriteBatchWithServersideRefreshes(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
	deadline *hlc.Timestamp,
) (batch storage.Batch, br *roachpb.BatchResponse, res result.Result, pErr *roachpb.Error) {
	goldenMS := *ms
	for retries := 0; ; retries++ {
		if retries > 0 {
			log.VEventf(ctx, 2, "server-side retry of batch")
		}
		if batch != nil {
			// Reset the stats.
			*ms = goldenMS
			batch.Close()
		}

		batch, br, res, pErr = r.evaluateWriteBatchWrapper(ctx, idKey, rec, ms, ba, latchSpans)

		var success bool
		if pErr == nil {
			wto := br.Txn != nil && br.Txn.WriteTooOld
			success = !wto
		} else {
			success = false
		}

		// If we can retry, set a higher batch timestamp and continue.
		// Allow one retry only; a non-txn batch containing overlapping
		// spans will always experience WriteTooOldError.
		if success || retries > 0 || !canDoServersideRetry(ctx, pErr, ba, br, latchSpans, deadline) {
			break
		}
	}
	return batch, br, res, pErr
}

// evaluateWriteBatchWrapper is a wrapper on top of evaluateBatch() which deals
// with filling out result.LogicalOpLog.
func (r *Replica) evaluateWriteBatchWrapper(
	ctx context.Context,
	idKey kvserverbase.CmdIDKey,
	rec batcheval.EvalContext,
	ms *enginepb.MVCCStats,
	ba *roachpb.BatchRequest,
	latchSpans *spanset.SpanSet,
) (storage.Batch, *roachpb.BatchResponse, result.Result, *roachpb.Error) {
	batch, opLogger := r.newBatchedEngine(latchSpans)
	br, res, pErr := evaluateBatch(ctx, idKey, batch, rec, ms, ba, false /* readOnly */)
	if pErr == nil {
		if opLogger != nil {
			res.LogicalOpLog = &kvserverpb.LogicalOpLog{
				Ops: opLogger.LogicalOps(),
			}
		}
	}
	return batch, br, res, pErr
}

// newBatchedEngine creates an engine.Batch. Depending on whether rangefeeds
// are enabled, it also returns an engine.OpLoggerBatch. If non-nil, then this
// OpLogger is attached to the returned engine.Batch, recording all operations.
// Its recording should be attached to the Result of request evaluation.
func (r *Replica) newBatchedEngine(spans *spanset.SpanSet) (storage.Batch, *storage.OpLoggerBatch) {
	batch := r.store.Engine().NewBatch()
	var opLogger *storage.OpLoggerBatch
	if r.isSystemRange() || RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
		// TODO(nvanbenschoten): once we get rid of the RangefeedEnabled
		// cluster setting we'll need a way to turn this on when any
		// replica (not just the leaseholder) wants it and off when no
		// replicas want it. This turns out to be pretty involved.
		//
		// The current plan is to:
		// - create a range-id local key that stores all replicas that are
		//   subscribed to logical operations, along with their corresponding
		//   liveness epoch.
		// - create a new command that adds or subtracts replicas from this
		//   structure. The command will be a write across the entire replica
		//   span so that it is serialized with all writes.
		// - each replica will add itself to this set when it first needs
		//   logical ops. It will then wait until it sees the replicated command
		//   that added itself pop out through Raft so that it knows all
		//   commands that are missing logical ops are gone.
		// - It will then proceed as normal, relying on the logical ops to
		//   always be included on the raft commands. When its no longer
		//   needs logical ops, it will remove itself from the set.
		// - The leaseholder will have a new queue to detect registered
		//   replicas that are no longer live and remove them from the
		//   set to prevent "leaking" subscriptions.
		// - The condition here to add logical logging will be:
		//     if len(replicaState.logicalOpsSubs) > 0 { ... }
		//
		// An alternative to this is the reduce the cost of the including
		// the logical op log to a negligible amount such that it can be
		// included on all raft commands, regardless of whether any replica
		// has a rangefeed running or not.
		//
		// Another alternative is to make the setting table/zone-scoped
		// instead of a fine-grained per-replica state.
		opLogger = storage.NewOpLoggerBatch(batch)
		batch = opLogger
	}
	if util.RaceEnabled {
		// During writes we may encounter a versioned value newer than the request
		// timestamp, and may have to retry at a higher timestamp. This is still
		// safe as we're only ever writing at timestamps higher than the timestamp
		// any write latch would be declared at. But because of this, we don't
		// assert on access timestamps using spanset.NewBatchAt.
		batch = spanset.NewBatch(batch, spans)
	}
	return batch, opLogger
}

// isOnePhaseCommit returns true iff the BatchRequest contains all writes in the
// transaction and ends with an EndTxn. One phase commits are disallowed if any
// of the following conditions are true:
// (1) the transaction has already been flagged with a write too old error
// (2) the transaction's commit timestamp has been forwarded
// (3) the transaction exceeded its deadline
// (4) the transaction is not in its first epoch and the EndTxn request does
//     not require one phase commit.
func isOnePhaseCommit(ba *roachpb.BatchRequest) bool {
	if ba.Txn == nil {
		return false
	}
	if !ba.IsCompleteTransaction() {
		return false
	}
	arg, _ := ba.GetArg(roachpb.EndTxn)
	etArg := arg.(*roachpb.EndTxnRequest)
	if retry, _, _ := batcheval.IsEndTxnTriggeringRetryError(ba.Txn, etArg); retry {
		return false
	}
	// If the transaction has already restarted at least once then it may have
	// left intents at prior epochs that need to be cleaned up during the
	// process of committing the transaction. Even if the current epoch could
	// perform a one phase commit, we don't allow it to because that could
	// prevent it from properly resolving intents from prior epochs and cause
	// it to abandon them instead.
	//
	// The exception to this rule is transactions that require a one phase
	// commit. We know that if they also required a one phase commit in past
	// epochs then they couldn't have left any intents that they now need to
	// clean up.
	return ba.Txn.Epoch == 0 || etArg.Require1PC
}
