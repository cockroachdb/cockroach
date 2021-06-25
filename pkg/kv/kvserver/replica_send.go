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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

var optimisticEvalLimitedScans = settings.RegisterBoolSetting(
	"kv.concurrency.optimistic_eval_limited_scans.enabled",
	"when true, limited scans are optimistically evaluated in the sense of not checking for "+
		"conflicting latches or locks up front for the full key range of the scan, and instead "+
		"subsequently checking for conflicts only over the key range that was read",
	true,
)

// Send executes a command on this range, dispatching it to the
// read-only, read-write, or admin execution path as appropriate.
// ctx should contain the log tags from the store (and up).
func (r *Replica) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return r.sendWithRangeID(ctx, r.RangeID, &ba)
}

// sendWithRangeID takes an unused rangeID argument so that the range
// ID will be accessible in stack traces (both in panics and when
// sampling goroutines from a live server). This line is subject to
// the whims of the compiler and it can be difficult to find the right
// value, but as of this writing the following example shows a stack
// while processing range 21 (0x15) (the first occurrence of that
// number is the rangeID argument, the second is within the encoded
// BatchRequest, although we don't want to rely on that occurring
// within the portion printed in the stack trace):
//
// github.com/cockroachdb/cockroach/pkg/storage.(*Replica).sendWithRangeID(0xc420d1a000, 0x64bfb80, 0xc421564b10, 0x15, 0x153fd4634aeb0193, 0x0, 0x100000001, 0x1, 0x15, 0x0, ...)
func (r *Replica) sendWithRangeID(
	ctx context.Context, rangeID roachpb.RangeID, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	var br *roachpb.BatchResponse
	if r.leaseholderStats != nil && ba.Header.GatewayNodeID != 0 {
		r.leaseholderStats.record(ba.Header.GatewayNodeID)
	}

	// Add the range log tag.
	ctx = r.AnnotateCtx(ctx)

	// If the internal Raft group is not initialized, create it and wake the leader.
	r.maybeInitializeRaftGroup(ctx)

	isReadOnly := ba.IsReadOnly()
	if err := r.checkBatchRequest(ba, isReadOnly); err != nil {
		return nil, roachpb.NewError(err)
	}

	if err := r.maybeBackpressureBatch(ctx, ba); err != nil {
		return nil, roachpb.NewError(err)
	}
	if err := r.maybeRateLimitBatch(ctx, ba); err != nil {
		return nil, roachpb.NewError(err)
	}
	if err := r.maybeCommitWaitBeforeCommitTrigger(ctx, ba); err != nil {
		return nil, roachpb.NewError(err)
	}

	// NB: must be performed before collecting request spans.
	ba, err := maybeStripInFlightWrites(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	if filter := r.store.cfg.TestingKnobs.TestingRequestFilter; filter != nil {
		if pErr := filter(ctx, *ba); pErr != nil {
			return nil, pErr
		}
	}

	// Differentiate between read-write, read-only, and admin.
	var pErr *roachpb.Error
	if isReadOnly {
		log.Event(ctx, "read-only path")
		fn := (*Replica).executeReadOnlyBatch
		br, pErr = r.executeBatchWithConcurrencyRetries(ctx, ba, fn)
	} else if ba.IsWrite() {
		log.Event(ctx, "read-write path")
		fn := (*Replica).executeWriteBatch
		br, pErr = r.executeBatchWithConcurrencyRetries(ctx, ba, fn)
	} else if ba.IsAdmin() {
		log.Event(ctx, "admin path")
		br, pErr = r.executeAdminBatch(ctx, ba)
	} else if len(ba.Requests) == 0 {
		// empty batch; shouldn't happen (we could handle it, but it hints
		// at someone doing weird things, and once we drop the key range
		// from the header it won't be clear how to route those requests).
		log.Fatalf(ctx, "empty batch")
	} else {
		log.Fatalf(ctx, "don't know how to handle command %s", ba)
	}
	if pErr != nil {
		log.Eventf(ctx, "replica.Send got error: %s", pErr)
	} else {
		if filter := r.store.cfg.TestingKnobs.TestingResponseFilter; filter != nil {
			pErr = filter(ctx, *ba, br)
		}
	}

	// Return range information if it was requested. Note that we don't return it
	// on errors because the code doesn't currently support returning both a br
	// and a pErr here. Also, some errors (e.g. NotLeaseholderError) have custom
	// ways of returning range info.
	if pErr == nil {
		r.maybeAddRangeInfoToResponse(ctx, ba, br)
	}

	r.recordImpactOnRateLimiter(ctx, br)
	return br, pErr
}

// maybeCommitWaitBeforeCommitTrigger detects batches that are attempting to
// commit a transaction with a commit trigger and that will need to perform a
// commit-wait at some point. For reasons described below, transactions with
// commit triggers need to perform their commit wait sleep before their trigger
// runs, so this function eagerly performs that sleep before the batch moves on
// to evaluation. The function guarantees that if the transaction ends up
// committing with its current provisional commit timestamp, it will not need to
// commit wait any further.
func (r *Replica) maybeCommitWaitBeforeCommitTrigger(
	ctx context.Context, ba *roachpb.BatchRequest,
) error {
	args, hasET := ba.GetArg(roachpb.EndTxn)
	if !hasET {
		return nil
	}
	et := args.(*roachpb.EndTxnRequest)
	if !et.Commit || et.InternalCommitTrigger == nil {
		// Not committing with a commit trigger.
		return nil
	}
	txn := ba.Txn
	if txn.ReadTimestamp != txn.WriteTimestamp && !ba.CanForwardReadTimestamp {
		// The commit can not succeed.
		return nil
	}

	// A transaction is committing with a commit trigger. This means that it has
	// side-effects beyond those of the intents that it has written.
	//
	// If the transaction has a commit timestamp in the future of present time, it
	// will need to commit-wait before acknowledging the client. Typically, this
	// is performed in the TxnCoordSender after the transaction has committed and
	// resolved its intents (see TxnCoordSender.maybeCommitWait). It is safe to
	// wait after a future-time transaction has committed and resolved intents
	// without compromising linearizability because the uncertainty interval of
	// concurrent and later readers ensures atomic visibility of the effects of
	// the transaction. In other words, all of the transaction's intents will
	// become visible and will remain visible at once, which is sometimes called
	// "monotonic reads". This is true even if the resolved intents are at a high
	// enough timestamp such that they are not visible to concurrent readers
	// immediately after they are resolved, but only become visible sometime
	// during the writer's commit-wait sleep. This property is central to the
	// correctness of non-blocking transactions. See:
	//   https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20200811_non_blocking_txns.md
	//
	// However, if a transaction has a commit trigger, the side-effects of the
	// trigger will go into effect immediately after the EndTxn's Raft command is
	// applied to the Raft state machine. This poses a problem, because we do not
	// want part of a transaction's effects (e.g. its commit trigger) to become
	// visible to onlookers before the rest of its effects do (e.g. its intent
	// writes). To avoid this problem, we perform the commit-wait stage of a
	// transaction with a commit trigger early, before its commit triggers fire.
	// This results in the transaction waiting longer to commit and resolve its
	// intents, but is otherwise safe and effective.
	//
	// NOTE: it would be easier to perform this wait during the evaluation of the
	// corresponding EndTxn request instead of detecting the case here. However,
	// we intentionally do not commit wait during evaluation because we do not
	// want to sleep while holding latches and blocking other requests. So
	// instead, we commit wait here and then assert that transactions with commit
	// triggers do not need to commit wait further by the time they reach command
	// evaluation.
	//
	// NOTE: just like in TxnCoordSender.maybeCommitWait, we only need to perform
	// a commit-wait sleep if the commit timestamp is "synthetic". Otherwise, it
	// is known not to be in advance of present time.
	if !txn.WriteTimestamp.Synthetic {
		return nil
	}
	if !r.Clock().Now().Less(txn.WriteTimestamp) {
		return nil
	}

	waitUntil := txn.WriteTimestamp
	before := r.Clock().PhysicalTime()
	est := waitUntil.GoTime().Sub(before)
	log.VEventf(ctx, 1, "performing server-side commit-wait sleep for ~%s", est)

	if err := r.Clock().SleepUntil(ctx, waitUntil); err != nil {
		return err
	}

	after := r.Clock().PhysicalTime()
	log.VEventf(ctx, 1, "completed server-side commit-wait sleep, took %s", after.Sub(before))
	r.store.metrics.CommitWaitsBeforeCommitTrigger.Inc(1)
	return nil
}

// maybeAddRangeInfoToResponse populates br.RangeInfo if the client doesn't
// have up-to-date info about the range's descriptor and lease.
func (r *Replica) maybeAddRangeInfoToResponse(
	ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse,
) {
	// Compare the client's info with the replica's info to detect if the client
	// has stale knowledge. Note that the client can have more recent knowledge
	// than the replica in case this is a follower.
	cri := &ba.ClientRangeInfo
	ri := r.GetRangeInfo(ctx)
	needInfo := (cri.DescriptorGeneration < ri.Desc.Generation) ||
		(cri.LeaseSequence < ri.Lease.Sequence) ||
		(cri.ClosedTimestampPolicy != ri.ClosedTimestampPolicy)
	if !needInfo {
		return
	}
	log.VEventf(ctx, 3, "client had stale range info; returning an update")
	br.RangeInfos = []roachpb.RangeInfo{ri}

	// We're going to sometimes return info on the ranges coming right before or
	// right after r, if it looks like r came from a range that has recently split
	// and the client doesn't know about it. After a split, the client benefits
	// from learning about both resulting ranges.

	if cri.DescriptorGeneration >= ri.Desc.Generation {
		return
	}

	maybeAddRange := func(repl *Replica) {
		if repl.Desc().Generation != ri.Desc.Generation {
			// The next range does not look like it came from a split that produced
			// both r and this next range. Of course, this has false negatives (e.g.
			// if either the LHS or the RHS split multiple times since the client's
			// version). For best fidelity, the client could send the range's start
			// and end keys and the server could use that to return all overlapping
			// descriptors (like we do for RangeKeyMismatchErrors), but sending those
			// keys on every RPC seems too expensive.
			return
		}

		// Note that we return the lease even if it's expired. The kvclient can
		// use it as it sees fit.
		br.RangeInfos = append(br.RangeInfos, repl.GetRangeInfo(ctx))
	}

	if repl := r.store.lookupPrecedingReplica(ri.Desc.StartKey); repl != nil {
		maybeAddRange(repl)
	}
	if repl := r.store.LookupReplica(ri.Desc.EndKey); repl != nil {
		maybeAddRange(repl)
	}
}

// batchExecutionFn is a method on Replica that executes a BatchRequest. It is
// called with the batch, along with a guard for the latches protecting the
// request.
//
// The function will return either a batch response or an error. The function
// also has the option to pass ownership of the concurrency guard back to the
// caller. However, it does not need to. Instead, it can assume responsibility
// for releasing the concurrency guard it was provided by returning nil. This is
// useful is cases where the function:
// 1. eagerly released the concurrency guard after it determined that isolation
//    from conflicting requests was no longer needed.
// 2. is continuing to execute asynchronously and needs to maintain isolation
//    from conflicting requests throughout the lifetime of its asynchronous
//    processing. The most prominent example of asynchronous processing is
//    with requests that have the "async consensus" flag set. A more subtle
//    case is with requests that are acknowledged by the Raft machinery after
//    their Raft entry has been committed but before it has been applied to
//    the replicated state machine. In all of these cases, responsibility
//    for releasing the concurrency guard is handed to Raft.
//
// However, this option is not permitted if the function returns a "server-side
// concurrency retry error" (see isConcurrencyRetryError for more details). If
// the function returns one of these errors, it must also pass ownership of the
// concurrency guard back to the caller.
type batchExecutionFn func(
	*Replica, context.Context, *roachpb.BatchRequest, *concurrency.Guard,
) (*roachpb.BatchResponse, *concurrency.Guard, *roachpb.Error)

var _ batchExecutionFn = (*Replica).executeWriteBatch
var _ batchExecutionFn = (*Replica).executeReadOnlyBatch

// executeBatchWithConcurrencyRetries is the entry point for client (non-admin)
// requests that execute against the range's state. The method coordinates the
// execution of requests that may require multiple retries due to interactions
// with concurrent transactions.
//
// The method acquires latches for the request, which synchronizes it with
// conflicting requests. This permits the execution function to run without
// concern of coordinating with logically conflicting operations, although it
// still needs to worry about coordinating with non-conflicting operations when
// accessing shared data structures.
//
// If the execution function hits a concurrency error like a WriteIntentError or
// a TransactionPushError it will propagate the error back to this method, which
// handles the process of retrying batch execution after addressing the error.
func (r *Replica) executeBatchWithConcurrencyRetries(
	ctx context.Context, ba *roachpb.BatchRequest, fn batchExecutionFn,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// Determine the maximal set of key spans that the batch will operate on.
	// This is used below to sequence the request in the concurrency manager.
	latchSpans, lockSpans, requestEvalKind, err := r.collectSpans(ba)
	if err != nil {
		return nil, roachpb.NewError(err)
	}

	// Handle load-based splitting.
	r.recordBatchForLoadBasedSplitting(ctx, ba, latchSpans)

	// Try to execute command; exit retry loop on success.
	var g *concurrency.Guard
	defer func() {
		// NB: wrapped to delay g evaluation to its value when returning.
		if g != nil {
			r.concMgr.FinishReq(g)
		}
	}()
	for {
		// Exit loop if context has been canceled or timed out.
		if err := ctx.Err(); err != nil {
			return nil, roachpb.NewError(errors.Wrap(err, "aborted during Replica.Send"))
		}

		// Acquire latches to prevent overlapping requests from executing until
		// this request completes. After latching, wait on any conflicting locks
		// to ensure that the request has full isolation during evaluation. This
		// returns a request guard that must be eventually released.
		var resp []roachpb.ResponseUnion
		g, resp, pErr = r.concMgr.SequenceReq(ctx, g, concurrency.Request{
			Txn:             ba.Txn,
			Timestamp:       ba.Timestamp,
			Priority:        ba.UserPriority,
			ReadConsistency: ba.ReadConsistency,
			WaitPolicy:      ba.WaitPolicy,
			Requests:        ba.Requests,
			LatchSpans:      latchSpans, // nil if g != nil
			LockSpans:       lockSpans,  // nil if g != nil
		}, requestEvalKind)
		if pErr != nil {
			return nil, pErr
		} else if resp != nil {
			br = new(roachpb.BatchResponse)
			br.Responses = resp
			return br, nil
		}
		latchSpans, lockSpans = nil, nil // ownership released

		br, g, pErr = fn(r, ctx, ba, g)
		if pErr == nil {
			// Success.
			return br, nil
		} else if !isConcurrencyRetryError(pErr) {
			// Propagate error.
			return nil, pErr
		}

		// The batch execution func returned a server-side concurrency retry
		// error. It must have also handed back ownership of the concurrency
		// guard without having already released the guard's latches.
		g.AssertLatches()
		if filter := r.store.cfg.TestingKnobs.TestingConcurrencyRetryFilter; filter != nil {
			filter(ctx, *ba, pErr)
		}

		// Typically, retries are marked PessimisticEval. The one exception is a
		// pessimistic retry immediately after an optimistic eval which failed
		// when checking for conflicts, which is handled below. Note that an
		// optimistic eval failure for any other reason will also retry as
		// PessimisticEval.
		requestEvalKind = concurrency.PessimisticEval

		switch t := pErr.GetDetail().(type) {
		case *roachpb.WriteIntentError:
			// Drop latches, but retain lock wait-queues.
			if g, pErr = r.handleWriteIntentError(ctx, ba, g, pErr, t); pErr != nil {
				return nil, pErr
			}
		case *roachpb.TransactionPushError:
			// Drop latches, but retain lock wait-queues.
			if g, pErr = r.handleTransactionPushError(ctx, ba, g, pErr, t); pErr != nil {
				return nil, pErr
			}
		case *roachpb.IndeterminateCommitError:
			// Drop latches and lock wait-queues.
			latchSpans, lockSpans = g.TakeSpanSets()
			r.concMgr.FinishReq(g)
			g = nil
			// Then launch a task to handle the indeterminate commit error.
			if pErr = r.handleIndeterminateCommitError(ctx, ba, pErr, t); pErr != nil {
				return nil, pErr
			}
		case *roachpb.InvalidLeaseError:
			// Drop latches and lock wait-queues.
			latchSpans, lockSpans = g.TakeSpanSets()
			r.concMgr.FinishReq(g)
			g = nil
			// Then attempt to acquire the lease if not currently held by any
			// replica or redirect to the current leaseholder if currently held
			// by a different replica.
			if pErr = r.handleInvalidLeaseError(ctx, ba, pErr, t); pErr != nil {
				return nil, pErr
			}
		case *roachpb.MergeInProgressError:
			// Drop latches and lock wait-queues.
			latchSpans, lockSpans = g.TakeSpanSets()
			r.concMgr.FinishReq(g)
			g = nil
			// Then listen for the merge to complete.
			if pErr = r.handleMergeInProgressError(ctx, ba, pErr, t); pErr != nil {
				return nil, pErr
			}
		case *roachpb.OptimisticEvalConflictsError:
			// We are deliberately not dropping latches. Note that the latches are
			// also optimistically acquired, in the sense of being inserted but not
			// waited on. The next iteration will wait on these latches to ensure
			// acquisition, and then pessimistically check for locks while holding
			// these latches. If conflicting locks are found, the request will queue
			// for those locks and release latches.
			requestEvalKind = concurrency.PessimisticAfterFailedOptimisticEval
		default:
			log.Fatalf(ctx, "unexpected concurrency retry error %T", t)
		}
		// Retry...
	}
}

// isConcurrencyRetryError returns whether or not the provided error is a
// "concurrency retry error" that will be captured and retried by
// executeBatchWithConcurrencyRetries. Most concurrency retry errors are
// handled by dropping a request's latches, waiting for and/or ensuring that
// the condition which caused the error is handled, re-sequencing through the
// concurrency manager, and executing the request again. The one exception is
// OptimisticEvalConflictsError, where there is no need to drop latches, and
// the request can immediately proceed to retrying pessimistically.
func isConcurrencyRetryError(pErr *roachpb.Error) bool {
	switch pErr.GetDetail().(type) {
	case *roachpb.WriteIntentError:
		// If a request hits a WriteIntentError, it adds the conflicting intent
		// to the lockTable through a process called "lock discovery". It then
		// waits in the lock's wait-queue during its next sequencing pass.
	case *roachpb.TransactionPushError:
		// If a PushTxn request hits a TransactionPushError, it attempted to
		// push another transactions record but did not succeed. It enqueues the
		// pushee transaction in the txnWaitQueue and waits on the record to
		// change or expire during its next sequencing pass.
	case *roachpb.IndeterminateCommitError:
		// If a PushTxn hits a IndeterminateCommitError, it attempted to push an
		// expired transaction record in the STAGING state. It's unclear whether
		// the pushee is aborted or committed, so the request must kick off the
		// "transaction recovery procedure" to resolve this ambiguity before
		// retrying.
	case *roachpb.InvalidLeaseError:
		// If a request hits an InvalidLeaseError, the replica it is being
		// evaluated against does not have a valid lease under which it can
		// serve the request. The request cannot proceed until a new lease is
		// acquired. If the acquisition process determines that the lease now
		// lives elsewhere, the request should be redirected (using a
		// NotLeaseHolderError) to the new leaseholder.
	case *roachpb.MergeInProgressError:
		// If a request hits a MergeInProgressError, the replica it is being
		// evaluated against is in the process of being merged into its left-hand
		// neighbor. The request cannot proceed until the range merge completes,
		// either successfully or unsuccessfully, so it waits before retrying.
		// If the merge does complete successfully, the retry will be rejected
		// with an error that will propagate back to the client.
	case *roachpb.OptimisticEvalConflictsError:
		// Optimistic evaluation encountered a conflict. The request will
		// immediately retry pessimistically.
	default:
		return false
	}
	return true
}

// maybeAttachLease is used to augment a concurrency retry error with
// information about the lease that the operation which hit this error was
// operating under.
func maybeAttachLease(pErr *roachpb.Error, lease *roachpb.Lease) *roachpb.Error {
	if wiErr, ok := pErr.GetDetail().(*roachpb.WriteIntentError); ok {
		wiErr.LeaseSequence = lease.Sequence
		return roachpb.NewErrorWithTxn(wiErr, pErr.GetTxn())
	}
	return pErr
}

func (r *Replica) handleWriteIntentError(
	ctx context.Context,
	ba *roachpb.BatchRequest,
	g *concurrency.Guard,
	pErr *roachpb.Error,
	t *roachpb.WriteIntentError,
) (*concurrency.Guard, *roachpb.Error) {
	if r.store.cfg.TestingKnobs.DontPushOnWriteIntentError {
		return g, pErr
	}
	// g's latches will be dropped, but it retains its spot in lock wait-queues.
	return r.concMgr.HandleWriterIntentError(ctx, g, t.LeaseSequence, t)
}

func (r *Replica) handleTransactionPushError(
	ctx context.Context,
	ba *roachpb.BatchRequest,
	g *concurrency.Guard,
	pErr *roachpb.Error,
	t *roachpb.TransactionPushError,
) (*concurrency.Guard, *roachpb.Error) {
	// On a transaction push error, retry immediately if doing so will enqueue
	// into the txnWaitQueue in order to await further updates to the unpushed
	// txn's status. We check ShouldPushImmediately to avoid retrying
	// non-queueable PushTxnRequests (see #18191).
	dontRetry := r.store.cfg.TestingKnobs.DontRetryPushTxnFailures
	if !dontRetry && ba.IsSinglePushTxnRequest() {
		pushReq := ba.Requests[0].GetInner().(*roachpb.PushTxnRequest)
		dontRetry = txnwait.ShouldPushImmediately(pushReq)
	}
	if dontRetry {
		return g, pErr
	}
	// g's latches will be dropped, but it retains its spot in lock wait-queues
	// (though a PushTxn shouldn't be in any lock wait-queues).
	return r.concMgr.HandleTransactionPushError(ctx, g, t), nil
}

func (r *Replica) handleIndeterminateCommitError(
	ctx context.Context,
	ba *roachpb.BatchRequest,
	pErr *roachpb.Error,
	t *roachpb.IndeterminateCommitError,
) *roachpb.Error {
	if r.store.cfg.TestingKnobs.DontRecoverIndeterminateCommits {
		return pErr
	}
	// On an indeterminate commit error, attempt to recover and finalize the
	// stuck transaction. Retry immediately if successful.
	if _, err := r.store.recoveryMgr.ResolveIndeterminateCommit(ctx, t); err != nil {
		// Do not propagate ambiguous results; assume success and retry original op.
		if errors.HasType(err, (*roachpb.AmbiguousResultError)(nil)) {
			return nil
		}
		// Propagate new error. Preserve the error index.
		newPErr := roachpb.NewError(err)
		newPErr.Index = pErr.Index
		return newPErr
	}
	// We've recovered the transaction that blocked the push; retry command.
	return nil
}

func (r *Replica) handleInvalidLeaseError(
	ctx context.Context, ba *roachpb.BatchRequest, _ *roachpb.Error, t *roachpb.InvalidLeaseError,
) *roachpb.Error {
	// On an invalid lease error, attempt to acquire a new lease. If in the
	// process of doing so, we determine that the lease now lives elsewhere,
	// redirect.
	_, pErr := r.redirectOnOrAcquireLeaseForRequest(ctx, ba.Timestamp)
	if pErr == nil {
		// Lease valid. Retry command.
		return nil
	}
	// If we failed to acquire the lease, check to see whether the request can
	// still be served as a follower read on this replica. Doing so here will
	// not be necessary once we break the dependency between closed timestamps
	// and leases and address the TODO in checkExecutionCanProceed to check the
	// closed timestamp before consulting the lease.

	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.canServeFollowerReadRLocked(ctx, ba, pErr.GoError()) {
		// Follower read possible. Retry command.
		return nil
	}
	return pErr
}

func (r *Replica) handleMergeInProgressError(
	ctx context.Context,
	ba *roachpb.BatchRequest,
	pErr *roachpb.Error,
	t *roachpb.MergeInProgressError,
) *roachpb.Error {
	// A merge was in progress. We need to retry the command after the merge
	// completes, as signaled by the closing of the replica's mergeComplete
	// channel. Note that the merge may have already completed, in which case
	// its mergeComplete channel will be nil.
	mergeCompleteCh := r.getMergeCompleteCh()
	if mergeCompleteCh == nil {
		// Merge no longer in progress. Retry the command.
		return nil
	}
	// Check to see if the request is a lease transfer. If so, reject it
	// immediately instead of a waiting for the merge to complete. This is
	// necessary because the merge may need to acquire a range lease in order to
	// complete if it still needs to perform its Subsume request, which it
	// likely will if this lease transfer revoked the leaseholder's existing
	// range lease. Any concurrent lease acquisition attempt will be blocked on
	// this lease transfer because a replica only performs a single lease
	// operation at a time, so we reject to prevent a deadlock.
	//
	// NOTE: it would not be sufficient to check for an in-progress merge in
	// AdminTransferLease because the range may notice the in-progress merge
	// after the lease transfer is initiated but before the lease transfer
	// acquires latches.
	if ba.IsSingleTransferLeaseRequest() {
		return roachpb.NewErrorf("cannot transfer lease while merge in progress")
	}
	log.Event(ctx, "waiting on in-progress range merge")
	select {
	case <-mergeCompleteCh:
		// Merge complete. Retry the command.
		return nil
	case <-ctx.Done():
		return roachpb.NewError(errors.Wrap(ctx.Err(), "aborted during merge"))
	case <-r.store.stopper.ShouldQuiesce():
		return roachpb.NewError(&roachpb.NodeUnavailableError{})
	}
}

// executeAdminBatch executes the command directly. There is no interaction
// with the spanlatch manager or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the lease holder replica. Batch support here is
// limited to single-element batches; everything else catches an error.
func (r *Replica) executeAdminBatch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(ba.Requests) != 1 {
		return nil, roachpb.NewErrorf("only single-element admin batches allowed")
	}

	args := ba.Requests[0].GetInner()
	if sp := tracing.SpanFromContext(ctx); sp != nil {
		sp.SetOperationName(reflect.TypeOf(args).String())
	}

	// Verify that the batch can be executed, which includes verifying that the
	// current replica has the range lease.
	// NB: we pass nil for the spanlatch guard because we haven't acquired
	// latches yet. This is ok because each individual request that the admin
	// request sends will acquire latches.
	for {
		if err := ctx.Err(); err != nil {
			return nil, roachpb.NewError(err)
		}

		_, err := r.checkExecutionCanProceed(ctx, ba, nil /* g */)
		if err == nil {
			break
		}
		switch {
		case errors.HasType(err, (*roachpb.InvalidLeaseError)(nil)):
			// If the replica does not have the lease, attempt to acquire it, or
			// redirect to the current leaseholder by returning an error.
			_, pErr := r.redirectOnOrAcquireLeaseForRequest(ctx, ba.Timestamp)
			if pErr != nil {
				return nil, pErr
			}
			// Retry...
		default:
			return nil, roachpb.NewError(err)
		}
	}

	var resp roachpb.Response
	var pErr *roachpb.Error
	switch tArgs := args.(type) {
	case *roachpb.AdminSplitRequest:
		var reply roachpb.AdminSplitResponse
		reply, pErr = r.AdminSplit(ctx, *tArgs, "manual")
		resp = &reply

	case *roachpb.AdminUnsplitRequest:
		var reply roachpb.AdminUnsplitResponse
		reply, pErr = r.AdminUnsplit(ctx, *tArgs, "manual")
		resp = &reply

	case *roachpb.AdminMergeRequest:
		var reply roachpb.AdminMergeResponse
		reply, pErr = r.AdminMerge(ctx, *tArgs, "manual")
		resp = &reply

	case *roachpb.AdminTransferLeaseRequest:
		pErr = roachpb.NewError(r.AdminTransferLease(ctx, tArgs.Target))
		resp = &roachpb.AdminTransferLeaseResponse{}

	case *roachpb.AdminChangeReplicasRequest:
		chgs := tArgs.Changes()
		desc, err := r.ChangeReplicas(ctx, &tArgs.ExpDesc, SnapshotRequest_REBALANCE, kvserverpb.ReasonAdminRequest, "", chgs)
		pErr = roachpb.NewError(err)
		if pErr != nil {
			resp = &roachpb.AdminChangeReplicasResponse{}
		} else {
			resp = &roachpb.AdminChangeReplicasResponse{
				Desc: *desc,
			}
		}

	case *roachpb.AdminRelocateRangeRequest:
		err := r.store.AdminRelocateRange(ctx, *r.Desc(), tArgs.VoterTargets, tArgs.NonVoterTargets)
		pErr = roachpb.NewError(err)
		resp = &roachpb.AdminRelocateRangeResponse{}

	case *roachpb.CheckConsistencyRequest:
		var reply roachpb.CheckConsistencyResponse
		reply, pErr = r.CheckConsistency(ctx, *tArgs)
		resp = &reply

	case *roachpb.AdminScatterRequest:
		reply, err := r.adminScatter(ctx, *tArgs)
		pErr = roachpb.NewError(err)
		resp = &reply

	case *roachpb.AdminVerifyProtectedTimestampRequest:
		reply, err := r.adminVerifyProtectedTimestamp(ctx, *tArgs)
		pErr = roachpb.NewError(err)
		resp = &reply

	default:
		return nil, roachpb.NewErrorf("unrecognized admin command: %T", args)
	}

	if pErr != nil {
		return nil, pErr
	}

	br := &roachpb.BatchResponse{}
	br.Add(resp)
	br.Txn = resp.Header().Txn
	return br, nil
}

// checkBatchRequest verifies BatchRequest validity requirements. In particular,
// the batch must have an assigned timestamp, and either all requests must be
// read-only, or none.
//
// TODO(tschottdorf): should check that request is contained in range and that
// EndTxn only occurs at the very end.
func (r *Replica) checkBatchRequest(ba *roachpb.BatchRequest, isReadOnly bool) error {
	if ba.Timestamp.IsEmpty() {
		// For transactional requests, Store.Send sets the timestamp. For non-
		// transactional requests, the client sets the timestamp. Either way, we
		// need to have a timestamp at this point.
		return errors.New("Replica.checkBatchRequest: batch does not have timestamp assigned")
	}
	consistent := ba.ReadConsistency == roachpb.CONSISTENT
	if isReadOnly {
		if !consistent && ba.Txn != nil {
			// Disallow any inconsistent reads within txns.
			return errors.Errorf("cannot allow %v reads within a transaction", ba.ReadConsistency)
		}
	} else if !consistent {
		return errors.Errorf("%v mode is only available to reads", ba.ReadConsistency)
	}

	return nil
}

func (r *Replica) collectSpans(
	ba *roachpb.BatchRequest,
) (latchSpans, lockSpans *spanset.SpanSet, requestEvalKind concurrency.RequestEvalKind, _ error) {
	latchSpans, lockSpans = spanset.New(), spanset.New()
	r.mu.RLock()
	desc := r.descRLocked()
	liveCount := r.mu.state.Stats.LiveCount
	r.mu.RUnlock()
	// TODO(bdarnell): need to make this less global when local
	// latches are used more heavily. For example, a split will
	// have a large read-only span but also a write (see #10084).
	// Currently local spans are the exception, so preallocate for the
	// common case in which all are global. We rarely mix read and
	// write commands, so preallocate for writes if there are any
	// writes present in the batch.
	//
	// TODO(bdarnell): revisit as the local portion gets its appropriate
	// use.
	if ba.IsLocking() {
		latchGuess := len(ba.Requests)
		if et, ok := ba.GetArg(roachpb.EndTxn); ok {
			// EndTxn declares a global write for each of its lock spans.
			latchGuess += len(et.(*roachpb.EndTxnRequest).LockSpans) - 1
		}
		latchSpans.Reserve(spanset.SpanReadWrite, spanset.SpanGlobal, latchGuess)
		lockSpans.Reserve(spanset.SpanReadWrite, spanset.SpanGlobal, len(ba.Requests))
	} else {
		latchSpans.Reserve(spanset.SpanReadOnly, spanset.SpanGlobal, len(ba.Requests))
		lockSpans.Reserve(spanset.SpanReadOnly, spanset.SpanGlobal, len(ba.Requests))
	}

	// Note that we are letting locking readers be considered for optimistic
	// evaluation. This is correct, though not necessarily beneficial.
	considerOptEval := ba.IsReadOnly() && ba.IsAllTransactional() && ba.Header.MaxSpanRequestKeys > 0 &&
		optimisticEvalLimitedScans.Get(&r.ClusterSettings().SV)
	// When considerOptEval, these are computed below and used to decide whether
	// to actually do optimistic evaluation.
	hasScans := false
	numGets := 0

	// For non-local, MVCC spans we annotate them with the request timestamp
	// during declaration. This is the timestamp used during latch acquisitions.
	// For read requests this works as expected, reads are performed at the same
	// timestamp. During writes however, we may encounter a versioned value newer
	// than the request timestamp, and may have to retry at a higher timestamp.
	// This is still safe as we're only ever writing at timestamps higher than the
	// timestamp any write latch would be declared at.
	batcheval.DeclareKeysForBatch(desc, ba.Header, latchSpans)
	for _, union := range ba.Requests {
		inner := union.GetInner()
		if cmd, ok := batcheval.LookupCommand(inner.Method()); ok {
			cmd.DeclareKeys(desc, ba.Header, inner, latchSpans, lockSpans)
			if considerOptEval {
				switch inner.(type) {
				case *roachpb.ScanRequest, *roachpb.ReverseScanRequest:
					hasScans = true
				case *roachpb.GetRequest:
					numGets++
				}
			}
		} else {
			return nil, nil, concurrency.PessimisticEval, errors.Errorf("unrecognized command %s", inner.Method())
		}
	}

	// Commands may create a large number of duplicate spans. De-duplicate
	// them to reduce the number of spans we pass to the spanlatch manager.
	for _, s := range [...]*spanset.SpanSet{latchSpans, lockSpans} {
		s.SortAndDedup()

		// If any command gave us spans that are invalid, bail out early
		// (before passing them to the spanlatch manager, which may panic).
		if err := s.Validate(); err != nil {
			return nil, nil, concurrency.PessimisticEval, err
		}
	}

	requestEvalKind = concurrency.PessimisticEval
	if considerOptEval {
		// Evaluate batches optimistically if they have a key limit which is less
		// than the upper bound on number of keys that can be returned for this
		// batch. For scans, the upper bound is the number of live keys on the
		// Range. For gets, it is the minimum of he number of live keys on the
		// Range and the number of gets. Ignoring write latches and locks can be
		// beneficial because it can help avoid waiting on writes to keys that the
		// batch will never actually need to read due to the overestimate of its
		// key bounds. Only after it is clear exactly what spans were read do we
		// verify whether there were any conflicts with concurrent writes.
		//
		// This case is not uncommon; for example, a Scan which requests the entire
		// range but has a limit of 1 result. We want to avoid allowing overly broad
		// spans from backing up the latch manager, or encountering too much contention
		// in the lock table.
		//
		// The heuristic is upper bound = k * liveCount, where k <= 1. The use of
		// k=1 below is an un-tuned setting.
		//
		// This heuristic is crude in that it looks at the live count for the
		// whole range, which may be much wider than the spans requested.
		// Additionally, it does not consider TargetBytes.
		const k = 1
		upperBoundKeys := k * liveCount
		if !hasScans && int64(numGets) < upperBoundKeys {
			upperBoundKeys = int64(numGets)
		}
		if ba.Header.MaxSpanRequestKeys < upperBoundKeys {
			requestEvalKind = concurrency.OptimisticEval
		}
	}

	return latchSpans, lockSpans, requestEvalKind, nil
}

// endCmds holds necessary information to end a batch after command processing,
// either after a write request has achieved consensus and been applied to Raft
// or after a read-only request has finished evaluation.
type endCmds struct {
	repl *Replica
	g    *concurrency.Guard
	st   kvserverpb.LeaseStatus // empty for follower reads
}

// move moves the endCmds into the return value, clearing and making a call to
// done on the receiver a no-op.
func (ec *endCmds) move() endCmds {
	res := *ec
	*ec = endCmds{}
	return res
}

// done releases the latches acquired by the command and updates the timestamp
// cache using the final timestamp of each command.
//
// No-op if the receiver has been zeroed out by a call to move. Idempotent and
// is safe to call more than once.
func (ec *endCmds) done(
	ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) {
	if ec.repl == nil {
		// The endCmds were cleared.
		return
	}
	defer ec.move() // clear

	// Update the timestamp cache. Each request within the batch is considered
	// in turn; only those marked as affecting the cache are processed. However,
	// only do so if the request is consistent and was operating on the
	// leaseholder under a valid range lease.
	if ba.ReadConsistency == roachpb.CONSISTENT && ec.st.State == kvserverpb.LeaseState_VALID {
		ec.repl.updateTimestampCache(ctx, &ec.st, ba, br, pErr)
	}

	// Release the latches acquired by the request and exit lock wait-queues.
	// Must be done AFTER the timestamp cache is updated. ec.g is only set when
	// the Raft proposal has assumed responsibility for the request.
	if ec.g != nil {
		ec.repl.concMgr.FinishReq(ec.g)
	}
}
