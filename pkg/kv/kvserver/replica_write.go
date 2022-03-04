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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// migrateApplicationTimeout is the duration to wait for a Migrate command
// to be applied to all replicas.
//
// TODO(erikgrinaker): this, and the timeout handling, should be moved into a
// migration helper that manages checkpointing and retries as well.
var migrateApplicationTimeout = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"kv.migration.migrate_application.timeout",
	"timeout for a Migrate request to be applied across all replicas of a range",
	1*time.Minute,
	settings.PositiveDuration,
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
	ctx context.Context, ba *roachpb.BatchRequest, g *concurrency.Guard,
) (br *roachpb.BatchResponse, _ *concurrency.Guard, pErr *roachpb.Error) {
	startTime := timeutil.Now()

	// Even though we're not a read-only operation by definition, we have to
	// take out a read lock on readOnlyCmdMu while performing any reads during
	// pre-Raft evaluation (e.g. conditional puts), otherwise we can race with
	// replica removal and get evaluated on an empty replica. We must release
	// this lock before Raft execution, to avoid deadlocks.
	//
	// To avoid having to include an `RUnlock()` call in each early return before
	// Raft execution, use the local. Upon unlocking, the local must be zeroed
	// out.
	readOnlyCmdMu := &r.readOnlyCmdMu
	readOnlyCmdMu.RLock()
	defer func() {
		if readOnlyCmdMu != nil {
			readOnlyCmdMu.RUnlock()
		}
	}()

	// Verify that the batch can be executed.
	st, err := r.checkExecutionCanProceed(ctx, ba, g)
	if err != nil {
		return nil, g, roachpb.NewError(err)
	}

	// Check the breaker. Note that we do this after checkExecutionCanProceed,
	// so that NotLeaseholderError has precedence.
	if err := r.signallerForBatch(ba).Err(); err != nil {
		return nil, g, roachpb.NewError(err)
	}

	// Compute the transaction's local uncertainty limit using observed
	// timestamps, which can help avoid uncertainty restarts.
	ui := uncertainty.ComputeInterval(&ba.Header, st, r.Clock().MaxOffset())

	// Start tracking this request if it is an MVCC write (i.e. if it's the kind
	// of request that needs to obey the closed timestamp). The act of tracking
	// also gives us a closed timestamp, which we must ensure to evaluate above
	// of. We're going to pass in minTS to applyTimestampCache(), which bumps us
	// accordingly if necessary. We need to start tracking this request before we
	// know the final write timestamp at which this request will evaluate because
	// we need to atomically read the closed timestamp and start to be tracked.
	// TODO(andrei): The timestamp cache might bump us above the timestamp at which
	// we're registering with the proposalBuf. In that case, this request will be
	// tracked at an unnecessarily low timestamp which can block the closing of
	// this low timestamp for no reason. We should refactor such that the request
	// starts being tracked after we apply the timestamp cache.
	var minTS hlc.Timestamp
	var tok TrackedRequestToken
	if ba.AppliesTimestampCache() {
		minTS, tok = r.mu.proposalBuf.TrackEvaluatingRequest(ctx, ba.WriteTimestamp())
	}
	defer tok.DoneIfNotMoved(ctx)

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
		return nil, g, roachpb.NewError(errors.Wrapf(err, "aborted before proposing"))
	}

	// If the command is proposed to Raft, ownership of and responsibility for
	// the concurrency guard will be assumed by Raft, so provide the guard to
	// evalAndPropose.
	ch, abandon, _, pErr := r.evalAndPropose(ctx, ba, g, st, ui, tok.Move(ctx))
	if pErr != nil {
		if cErr, ok := pErr.GetDetail().(*roachpb.ReplicaCorruptionError); ok {
			// Need to unlock here because setCorruptRaftMuLock needs readOnlyCmdMu not held.
			readOnlyCmdMu.RUnlock()
			readOnlyCmdMu = nil

			r.raftMu.Lock()
			defer r.raftMu.Unlock()
			// This exits with a fatal error, but returns in tests.
			return nil, g, r.setCorruptRaftMuLocked(ctx, cErr)
		}
		return nil, g, pErr
	}
	g = nil // ownership passed to Raft, prevent misuse

	// We are done with pre-Raft evaluation at this point, and have to release the
	// read-only command lock to avoid deadlocks during Raft evaluation.
	readOnlyCmdMu.RUnlock()
	readOnlyCmdMu = nil

	// If the command was accepted by raft, wait for the range to apply it.
	ctxDone := ctx.Done()
	shouldQuiesce := r.store.stopper.ShouldQuiesce()

	for {
		select {
		case propResult := <-ch:
			// Semi-synchronously process any intents that need resolving here in
			// order to apply back pressure on the client which generated them. The
			// resolution is semi-synchronous in that there is a limited number of
			// outstanding asynchronous resolution tasks allowed after which
			// further calls will block.
			if len(propResult.EndTxns) > 0 {
				if err := r.store.intentResolver.CleanupTxnIntentsAsync(
					ctx, r.RangeID, propResult.EndTxns, true, /* allowSync */
				); err != nil {
					log.Warningf(ctx, "transaction cleanup failed: %v", err)
				}
			}
			if len(propResult.EncounteredIntents) > 0 {
				if err := r.store.intentResolver.CleanupIntentsAsync(
					ctx, propResult.EncounteredIntents, true, /* allowSync */
				); err != nil {
					log.Warningf(ctx, "intent cleanup failed: %v", err)
				}
			}
			if ba.Requests[0].GetMigrate() != nil && propResult.Err == nil {
				// Migrate is special since it wants commands to be durably
				// applied on all peers, which we achieve via waitForApplication.
				//
				// We don't have to worry about extant snapshots creating
				// replicas that start at an index before this Migrate request.
				// Snapshots that don't include the recipient (as specified by
				// replicaID and descriptor in the snap vs. the replicaID of the
				// raft instance) are discarded by the recipient, and we're
				// already checking against all replicas in the descriptor below
				// (which include learner replicas currently in the process of
				// receiving snapshots). Snapshots are also discarded unless
				// they move the LAI forward, so we're not worried about old
				// snapshots (with indexes preceding the MLAI here)
				// instantiating pre-migrated state in anyway. We also have a
				// separate mechanism to ensure replicas with older versions are
				// purged from the system[1]. This is driven by a higher-level
				// orchestration layer[2], these are the replicas that we don't
				// have a handle on here as they're eligible for GC (but may
				// still hit replica evaluation code paths with pre-migrated
				// state, unless explicitly purged).
				//
				// It's possible that between the proposal returning and the
				// call to r.Desc() below, the descriptor has already changed.
				// But the only thing that matters is that r.Desc() is at least
				// as up to date as the descriptor the command applied on
				// previously. If a replica got removed - fine,
				// waitForApplication will fail; we will have to cope with that.
				// If one got added - it was likely already a learner when we
				// migrated (in which case waitForApplication will know about
				// it). If that's not the case, we'll note that the Migrate
				// command also declares a read latch on the range descriptor.
				// The replication change will have thus serialized after the
				// migration, and so the snapshot will also include the
				// post-migration state.
				//
				// TODO(irfansharif): In a cluster that is constantly changing
				// its replica sets, it's possible to get into a situation
				// where a Migrate command never manages to complete - all it
				// takes is a single range in each attempt to throw things off.
				// Perhaps an error in waitForApplication should lead to a retry
				// of just the one RPC instead of propagating an error for the
				// entire migrate invocation.
				//
				// [1]: See PurgeOutdatedReplicas from the Migration service.
				// [2]: pkg/migration
				applicationErr := contextutil.RunWithTimeout(ctx, "wait for Migrate application",
					migrateApplicationTimeout.Get(&r.ClusterSettings().SV),
					func(ctx context.Context) error {
						desc := r.Desc()
						return waitForApplication(
							ctx, r.store.cfg.NodeDialer, desc.RangeID, desc.Replicas().Descriptors(),
							// We wait for an index >= that of the migration command.
							r.GetLeaseAppliedIndex())
					})
				propResult.Err = roachpb.NewError(applicationErr)
			}
			if propResult.Err != nil && ba.IsSingleProbeRequest() && errors.Is(
				propResult.Err.GoError(), noopOnProbeCommandErr.GoError(),
			) {
				// During command application, a Probe will fail due to the
				// mismatched lease (noop writes skip lease checks and so also don't
				// plumb the lease around in the first place) and they have special
				// casing to return a noopOnProbeCommandErr instead. So when we see
				// this error here, it means that the noop write succeeded.
				propResult.Reply, propResult.Err = ba.CreateReply(), nil
			}

			return propResult.Reply, nil, propResult.Err

		case <-ctxDone:
			// If our context was canceled, return an AmbiguousResultError,
			// which indicates to the caller that the command may have executed.
			//
			// If the batch contained an EndTxnRequest, asynchronously wait
			// around for the result for a while and try to clean up after the
			// txn. If the resolver's async task pool is full, just skip cleanup
			// by setting allowSync=false, since we won't be able to
			// backpressure clients.
			if _, ok := ba.GetArg(roachpb.EndTxn); ok {
				const taskName = "async txn cleanup"
				_ = r.store.stopper.RunAsyncTask(
					r.AnnotateCtx(context.Background()),
					taskName,
					func(ctx context.Context) {
						err := contextutil.RunWithTimeout(ctx, taskName, 20*time.Second,
							func(ctx context.Context) error {
								select {
								case propResult := <-ch:
									if len(propResult.EndTxns) > 0 {
										return r.store.intentResolver.CleanupTxnIntentsAsync(ctx,
											r.RangeID, propResult.EndTxns, false /* allowSync */)
									}
								case <-shouldQuiesce:
								case <-ctx.Done():
								}
								return ctx.Err()
							})
						if err != nil {
							log.Warningf(ctx, "transaction cleanup failed: %v", err)
							r.store.intentResolver.Metrics.FinalizedTxnCleanupFailed.Inc(1)
						}
					})
			}
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

// canAttempt1PCEvaluation looks at the batch and decides whether it can be
// executed as 1PC.
func (r *Replica) canAttempt1PCEvaluation(
	ctx context.Context, ba *roachpb.BatchRequest, g *concurrency.Guard,
) bool {
	if !isOnePhaseCommit(ba) {
		return false
	}

	if ba.Timestamp != ba.Txn.WriteTimestamp {
		log.Fatalf(ctx, "unexpected 1PC execution with diverged timestamp. %s != %s",
			ba.Timestamp, ba.Txn.WriteTimestamp)
	}

	// The EndTxn checks whether the txn record can be created, but we're
	// eliding the EndTxn. So, we'll do the check instead.
	//
	// Note that the returned reason does not distinguish between an existing
	// record (which should fall back to non-1PC EndTxn evaluation) and a
	// finalized record (which should return an error), so we ignore it here and
	// let EndTxn return an error as appropriate. This lets us avoid a disk read
	// to check for an existing record.
	ok, minCommitTS, _ := r.CanCreateTxnRecord(ctx, ba.Txn.ID, ba.Txn.Key, ba.Txn.MinTimestamp)
	if !ok {
		return false
	}
	if ba.Timestamp.Less(minCommitTS) {
		ba.Txn.WriteTimestamp = minCommitTS
		// We can only evaluate at the new timestamp if we manage to bump the read
		// timestamp.
		return maybeBumpReadTimestampToWriteTimestamp(ctx, ba, g)
	}
	return true
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
	ui uncertainty.Interval,
	g *concurrency.Guard,
) (storage.Batch, enginepb.MVCCStats, *roachpb.BatchResponse, result.Result, *roachpb.Error) {
	log.Event(ctx, "executing read-write batch")

	// If the transaction has been pushed but it can commit at the higher
	// timestamp, let's evaluate the batch at the bumped timestamp. This will
	// allow it commit, and also it'll allow us to attempt the 1PC code path.
	maybeBumpReadTimestampToWriteTimestamp(ctx, ba, g)

	// Attempt 1PC execution, if applicable. If not transactional or there are
	// indications that the batch's txn will require retry, execute as normal.
	if r.canAttempt1PCEvaluation(ctx, ba, g) {
		res := r.evaluate1PC(ctx, idKey, ba, g)
		switch res.success {
		case onePCSucceeded:
			return res.batch, res.stats, res.br, res.res, nil
		case onePCFailed:
			if res.pErr == nil {
				log.Fatalf(ctx, "1PC failed but no err. ba: %s", ba.String())
			}
			return nil, enginepb.MVCCStats{}, nil, result.Result{}, res.pErr
		case onePCFallbackToTransactionalEvaluation:
			// Fallthrough to transactional evaluation.
		}
	} else {
		// Deal with the Require1PC flag here, so that lower layers don't need to
		// care about it. Note that the point of Require1PC is that we don't want to
		// leave locks behind in case of retriable errors, so it's better to
		// terminate this request early.
		arg, ok := ba.GetArg(roachpb.EndTxn)
		if ok && arg.(*roachpb.EndTxnRequest).Require1PC {
			return nil, enginepb.MVCCStats{}, nil, result.Result{}, roachpb.NewError(kv.OnePCNotAllowedError{})
		}
	}

	if ba.Require1PC() {
		log.Fatalf(ctx,
			"Require1PC should not have gotten to transactional evaluation. ba: %s", ba.String())
	}

	ms := new(enginepb.MVCCStats)
	rec := NewReplicaEvalContext(r, g.LatchSpans())
	batch, br, res, pErr := r.evaluateWriteBatchWithServersideRefreshes(
		ctx, idKey, rec, ms, ba, ui, g, nil /* deadline */)
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
	//
	// Batches with the Require1PC flag set do not return this status.
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
	ctx context.Context, idKey kvserverbase.CmdIDKey, ba *roachpb.BatchRequest, g *concurrency.Guard,
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

	// The request is non-transactional, so there's no uncertainty.
	// TODO(nvanbenschoten): Is this correct? What if the request performs a read?
	// Is this relying on the batch being write-only?
	ui := uncertainty.Interval{}

	rec := NewReplicaEvalContext(r, g.LatchSpans())
	var br *roachpb.BatchResponse
	var res result.Result
	var pErr *roachpb.Error

	arg, _ := ba.GetArg(roachpb.EndTxn)
	etArg := arg.(*roachpb.EndTxnRequest)

	// Evaluate strippedBa. If the transaction allows, permit refreshes.
	ms := new(enginepb.MVCCStats)
	if ba.CanForwardReadTimestamp {
		batch, br, res, pErr = r.evaluateWriteBatchWithServersideRefreshes(
			ctx, idKey, rec, ms, &strippedBa, ui, g, etArg.Deadline)
	} else {
		batch, br, res, pErr = r.evaluateWriteBatchWrapper(
			ctx, idKey, rec, ms, &strippedBa, ui, g)
	}

	if pErr != nil || (!ba.CanForwardReadTimestamp && ba.Timestamp != br.Timestamp) {
		if pErr != nil {
			log.VEventf(ctx, 2,
				"1PC execution failed, falling back to transactional execution. pErr: %v", pErr.String())
		} else {
			log.VEventf(ctx, 2,
				"1PC execution failed, falling back to transactional execution; the batch was pushed")
		}
		if etArg.Require1PC {
			return onePCResult{success: onePCFailed, pErr: pErr}
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
				pErr:    roachpb.NewError(errors.Wrap(err, "failed to run commit trigger")),
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
	ui uncertainty.Interval,
	g *concurrency.Guard,
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

		batch, br, res, pErr = r.evaluateWriteBatchWrapper(ctx, idKey, rec, ms, ba, ui, g)

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
		if success || retries > 0 || !canDoServersideRetry(ctx, pErr, ba, br, g, deadline) {
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
	ui uncertainty.Interval,
	g *concurrency.Guard,
) (storage.Batch, *roachpb.BatchResponse, result.Result, *roachpb.Error) {
	batch, opLogger := r.newBatchedEngine(ba, g)
	br, res, pErr := evaluateBatch(ctx, idKey, batch, rec, ms, ba, ui, false /* readOnly */)
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
func (r *Replica) newBatchedEngine(
	ba *roachpb.BatchRequest, g *concurrency.Guard,
) (storage.Batch, *storage.OpLoggerBatch) {
	batch := r.store.Engine().NewBatch()
	if !batch.ConsistentIterators() {
		// This is not currently needed for correctness, but future optimizations
		// may start relying on this, so we assert here.
		panic("expected consistent iterators")
	}
	var opLogger *storage.OpLoggerBatch
	if r.isRangefeedEnabled() || RangefeedEnabled.Get(&r.store.cfg.Settings.SV) {
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
		batch = spanset.NewBatch(batch, g.LatchSpans())
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
