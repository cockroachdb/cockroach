// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"reflect"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/poison"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvadmission"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/replicastats"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/grunning"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

var optimisticEvalLimitedScans = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"kv.concurrency.optimistic_eval_limited_scans.enabled",
	"when true, limited scans are optimistically evaluated in the sense of not checking for "+
		"conflicting latches or locks up front for the full key range of the scan, and instead "+
		"subsequently checking for conflicts only over the key range that was read",
	true,
)

// Send executes a command on this range, dispatching it to the
// read-only, read-write, or admin execution path as appropriate.
// ctx should contain the log tags from the store (and up).
//
// A rough schematic for the path requests take through a Replica
// is presented below, with a focus on where requests may spend
// most of their time (once they arrive at the Node.Batch endpoint).
//
//	                  DistSender (tenant)
//	                       │
//	                       ┆ (RPC)
//	                       │
//	                       ▼
//	                  Node.Batch (host cluster)
//	                       │
//	                       ▼
//	               Admission control
//	                       │
//	                       ▼
//	                  Replica.Send
//	                       │
//	                 Circuit breaker
//	                       │
//	                       ▼
//	       Replica.maybeBackpressureBatch (if Range too large)
//	                       │
//	                       ▼
//	Replica.maybeRateLimitBatch (tenant rate limits)
//	                       │
//	                       ▼
//	  Replica.maybeCommitWaitBeforeCommitTrigger (if committing with commit-trigger)
//	                       │
//
// read-write ◄─────────────────────────┴────────────────────────► read-only
//
//	│                                                               │
//	│                                                               │
//	├─────────────► executeBatchWithConcurrencyRetries ◄────────────┤
//	│               (handles leases and txn conflicts)              │
//	│                                                               │
//	▼                                                               │
//
// executeWriteBatch                                                   │
//
//	│                                                               │
//	▼                                                               ▼
//
// evalAndPropose         (turns the BatchRequest        executeReadOnlyBatch
//
//	│                   into pebble WriteBatch)
//	│
//	├──────────────────► (writes that can use async consensus do not
//	│                     wait for replication and are done here)
//	│
//	├──────────────────► maybeAcquireProposalQuota
//	│                    (applies backpressure in case of
//	│                     lagging Raft followers)
//	│
//	│
//	▼
//
// handleRaftReady        (drives the Raft loop, first appending to the log
//
//	to commit the command, then signaling proposer and
//	applying the command)
func (r *Replica) Send(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	br, writeBytes, pErr := r.SendWithWriteBytes(ctx, ba)
	writeBytes.Release()
	return br, pErr
}

// SendWithWriteBytes is the implementation of Send with an additional
// *StoreWriteBytes return value.
func (r *Replica) SendWithWriteBytes(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvadmission.StoreWriteBytes, *kvpb.Error) {
	if r.store.cfg.Settings.CPUProfileType() == cluster.CPUProfileWithLabels {
		defer pprof.SetGoroutineLabels(ctx)
		// Note: the defer statement captured the previous context.
		ctx = pprof.WithLabels(ctx, pprof.Labels("range_str", r.rangeStr.ID()))
		pprof.SetGoroutineLabels(ctx)
	}
	// Add the range log tag.
	ctx = r.AnnotateCtx(ctx)

	// Record the CPU time processing the request for this replica. This is
	// recorded regardless of errors that are encountered.
	startCPU := grunning.Time()
	defer r.MeasureReqCPUNanos(startCPU)

	isReadOnly := ba.IsReadOnly()
	if err := r.checkBatchRequest(ba, isReadOnly); err != nil {
		return nil, nil, kvpb.NewError(err)
	}

	if err := r.maybeBackpressureBatch(ctx, ba); err != nil {
		return nil, nil, kvpb.NewError(err)
	}
	if err := r.maybeRateLimitBatch(ctx, ba); err != nil {
		return nil, nil, kvpb.NewError(err)
	}
	if err := r.maybeCommitWaitBeforeCommitTrigger(ctx, ba); err != nil {
		return nil, nil, kvpb.NewError(err)
	}

	// NB: must be performed before collecting request spans.
	ba, err := maybeStripInFlightWrites(ba)
	if err != nil {
		return nil, nil, kvpb.NewError(err)
	}

	if filter := r.store.cfg.TestingKnobs.TestingRequestFilter; filter != nil {
		if pErr := filter(ctx, ba); pErr != nil {
			return nil, nil, pErr
		}
	}

	// Differentiate between read-write, read-only, and admin.
	var br *kvpb.BatchResponse
	var pErr *kvpb.Error
	var writeBytes *kvadmission.StoreWriteBytes
	if isReadOnly {
		log.Event(ctx, "read-only path")
		fn := (*Replica).executeReadOnlyBatch
		br, _, pErr = r.executeBatchWithConcurrencyRetries(ctx, ba, fn)
	} else if ba.IsWrite() {
		log.Event(ctx, "read-write path")
		fn := (*Replica).executeWriteBatch
		br, writeBytes, pErr = r.executeBatchWithConcurrencyRetries(ctx, ba, fn)
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
			pErr = filter(ctx, ba, br)
		}
	}

	if pErr == nil {
		// Return range information if it was requested. Note that we don't return it
		// on errors because the code doesn't currently support returning both a br
		// and a pErr here. Also, some errors (e.g. NotLeaseholderError) have custom
		// ways of returning range info.
		r.maybeAddRangeInfoToResponse(ctx, ba, br)
		// Handle load-based splitting, if necessary.
		r.recordBatchForLoadBasedSplitting(ctx, ba, br, int(grunning.Difference(startCPU, grunning.Time())))
	}

	// Record summary throughput information about the batch request for
	// accounting.
	r.recordBatchRequestLoad(ctx, ba)
	r.recordRequestWriteBytes(writeBytes)
	r.recordImpactOnRateLimiter(ctx, br, isReadOnly)
	return br, writeBytes, pErr
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
	ctx context.Context, ba *kvpb.BatchRequest,
) error {
	args, hasET := ba.GetArg(kvpb.EndTxn)
	if !hasET {
		return nil
	}
	et := args.(*kvpb.EndTxnRequest)
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
	if txn.WriteTimestamp.LessEq(r.Clock().Now()) {
		// No wait fast-path. This is the common case for most transactions. Only
		// transactions who have their commit timestamp bumped into the future will
		// need to wait.
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
	ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse,
) {
	// Only return range info if ClientRangeInfo is non-empty. In particular, we
	// don't want to populate this for lease requests, since these bypass
	// DistSender and never use ClientRangeInfo.
	//
	// From 23.2, all DistSenders ensure ExplicitlyRequested is set when otherwise
	// empty.
	if ba.ClientRangeInfo == (roachpb.ClientRangeInfo{}) {
		return
	}

	// Compare the client's info with the replica's info to detect if the client
	// has stale knowledge. Note that the client can have more recent knowledge
	// than the replica in case this is a follower.
	cri := &ba.ClientRangeInfo
	ri := r.GetRangeInfo(ctx)
	needInfo := cri.ExplicitlyRequested ||
		(cri.DescriptorGeneration < ri.Desc.Generation) ||
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
//  1. eagerly released the concurrency guard after it determined that isolation
//     from conflicting requests was no longer needed.
//  2. is continuing to execute asynchronously and needs to maintain isolation
//     from conflicting requests throughout the lifetime of its asynchronous
//     processing. The most prominent example of asynchronous processing is
//     with requests that have the "async consensus" flag set. A more subtle
//     case is with requests that are acknowledged by the Raft machinery after
//     their Raft entry has been committed but before it has been applied to
//     the replicated state machine. In all of these cases, responsibility
//     for releasing the concurrency guard is handed to Raft.
//
// However, this option is not permitted if the function returns a "server-side
// concurrency retry error" (see isConcurrencyRetryError for more details). If
// the function returns one of these errors, it must also pass ownership of the
// concurrency guard back to the caller.
type batchExecutionFn func(
	*Replica, context.Context, *kvpb.BatchRequest, *concurrency.Guard,
) (*kvpb.BatchResponse, *concurrency.Guard, *kvadmission.StoreWriteBytes, *kvpb.Error)

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
// If the execution function hits a concurrency error like a LockConflictError or
// a TransactionPushError it will propagate the error back to this method, which
// handles the process of retrying batch execution after addressing the error.
func (r *Replica) executeBatchWithConcurrencyRetries(
	ctx context.Context, ba *kvpb.BatchRequest, fn batchExecutionFn,
) (br *kvpb.BatchResponse, writeBytes *kvadmission.StoreWriteBytes, pErr *kvpb.Error) {
	// Try to execute command; exit retry loop on success.
	var latchSpans *spanset.SpanSet
	var lockSpans *lockspanset.LockSpanSet
	var requestEvalKind concurrency.RequestEvalKind
	var g *concurrency.Guard
	defer func() {
		// NB: wrapped to delay g evaluation to its value when returning.
		if g != nil {
			r.concMgr.FinishReq(ctx, g)
		}
	}()
	pp := poison.Policy_Error
	if r.signallerForBatch(ba).C() == nil {
		// The request wishes to ignore the circuit breaker, i.e. attempt to propose
		// commands and wait even if the circuit breaker is tripped.
		pp = poison.Policy_Wait
	}
	for {
		// Exit loop if context has been canceled or timed out.
		if err := ctx.Err(); err != nil {
			return nil, nil, kvpb.NewError(errors.Wrap(err, "aborted during Replica.Send"))
		}

		// Determine the maximal set of key spans that the batch will operate on.
		// This is used below to sequence the request in the concurrency manager.
		//
		// Only do so if the latchSpans and lockSpans are not being preserved from a
		// prior iteration, either directly or in a concurrency guard that we intend
		// to re-use during sequencing.
		if latchSpans == nil && g == nil {
			var err error
			latchSpans, lockSpans, requestEvalKind, err = r.collectSpans(ba)
			if err != nil {
				return nil, nil, kvpb.NewError(err)
			}
		}

		// Acquire latches to prevent overlapping requests from executing until
		// this request completes. After latching, wait on any conflicting locks
		// to ensure that the request has full isolation during evaluation. This
		// returns a request guard that must be eventually released.
		var resp []kvpb.ResponseUnion
		g, resp, pErr = r.concMgr.SequenceReq(ctx, g, concurrency.Request{
			Txn:             ba.Txn,
			Timestamp:       ba.Timestamp,
			NonTxnPriority:  ba.UserPriority,
			ReadConsistency: ba.ReadConsistency,
			WaitPolicy:      ba.WaitPolicy,
			LockTimeout:     ba.LockTimeout,
			DeadlockTimeout: ba.DeadlockTimeout,
			AdmissionHeader: ba.AdmissionHeader,
			PoisonPolicy:    pp,
			Requests:        ba.Requests,
			LatchSpans:      latchSpans, // nil if g != nil
			LockSpans:       lockSpans,  // nil if g != nil
			BaFmt:           ba,
		}, requestEvalKind)
		if pErr != nil {
			if poisonErr := (*poison.PoisonedError)(nil); errors.As(pErr.GoError(), &poisonErr) {
				// It's possible that intent resolution accessed txn info anchored on a
				// different range and hit a poisoned latch there, in which case we want
				// to propagate its ReplicaUnavailableError instead of creating one for
				// this range (which likely isn't tripped).
				if !errors.HasType(pErr.GoError(), (*kvpb.ReplicaUnavailableError)(nil)) {
					// NB: we make the breaker error (which may be nil at this point, but
					// usually is not) a secondary error, meaning it is not in the error
					// chain. That is fine; the important bits to investigate
					// programmatically are the ReplicaUnavailableError (which contains the
					// descriptor) and the *PoisonedError (which contains the concrete
					// subspan that caused this request to fail). We mark
					// circuit.ErrBreakerOpen into the chain as well so that we have the
					// invariant that all replica circuit breaker errors contain both
					// ErrBreakerOpen and ReplicaUnavailableError.
					pErr = kvpb.NewError(r.replicaUnavailableError(errors.CombineErrors(
						errors.Mark(poisonErr, circuit.ErrBreakerOpen),
						r.breaker.Signal().Err(),
					)))
				}
			}
			return nil, nil, pErr
		} else if resp != nil {
			br = new(kvpb.BatchResponse)
			br.Responses = resp
			return br, nil, nil
		}
		latchSpans, lockSpans = nil, nil // ownership released

		br, g, writeBytes, pErr = fn(r, ctx, ba, g)
		if pErr == nil {
			// Success.
			return br, writeBytes, nil
		} else if !isConcurrencyRetryError(pErr) {
			// Propagate error.
			return nil, nil, pErr
		}

		log.VErrEventf(ctx, 2, "concurrency retry error: %s", pErr)

		// The batch execution func returned a server-side concurrency retry error.
		// It may have either handed back ownership of the concurrency guard without
		// having already released the guard's latches, or in case of certain types
		// of read-only requests (see `canReadOnlyRequestDropLatchesBeforeEval`), it
		// may have released the guard's latches.
		dropLatchesAndLockWaitQueues := func(reuseLatchAndLockSpans bool) {
			if g != nil {
				latchSpans, lockSpans = nil, nil
				if reuseLatchAndLockSpans {
					latchSpans, lockSpans = g.TakeSpanSets()
				}
				r.concMgr.FinishReq(ctx, g)
				g = nil
			}
		}

		if filter := r.store.cfg.TestingKnobs.TestingConcurrencyRetryFilter; filter != nil {
			filter(ctx, ba, pErr)
		}

		// Typically, retries are marked PessimisticEval. The one exception is a
		// pessimistic retry immediately after an optimistic eval which failed
		// when checking for conflicts, which is handled below. Note that an
		// optimistic eval failure for any other reason will also retry as
		// PessimisticEval.
		requestEvalKind = concurrency.PessimisticEval

		switch t := pErr.GetDetail().(type) {
		case *kvpb.LockConflictError:
			// Drop latches, but retain lock wait-queues.
			g.AssertLatches()
			if g, pErr = r.handleLockConflictError(ctx, ba, g, pErr, t); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.TransactionPushError:
			// Drop latches, but retain lock wait-queues.
			g.AssertLatches()
			if g, pErr = r.handleTransactionPushError(ctx, ba, g, pErr, t); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.IndeterminateCommitError:
			dropLatchesAndLockWaitQueues(true /* reuseLatchAndLockSpans */)
			// Then launch a task to handle the indeterminate commit error. No error
			// is returned if the transaction is recovered successfully to either a
			// COMMITTED or ABORTED state.
			if pErr = r.handleIndeterminateCommitError(ctx, ba, pErr, t); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.ReadWithinUncertaintyIntervalError:
			// If the batch is able to perform a server-side retry in order to avoid
			// the uncertainty error, it will have a new timestamp. Force a refresh of
			// the latch and lock spans.
			dropLatchesAndLockWaitQueues(false /* reuseLatchAndLockSpans */)
			// Attempt to adjust the batch's timestamp to avoid the uncertainty error
			// and allow for a server-side retry. For transactional requests, there
			// are strict conditions that must be met for this to be permitted. For
			// non-transactional requests, this is always allowed. If successful, an
			// updated BatchRequest will be returned. If unsuccessful, the provided
			// read within uncertainty interval error will be returned so that we can
			// propagate it.
			ba, pErr = r.handleReadWithinUncertaintyIntervalError(ctx, ba, pErr, t)
			if pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.InvalidLeaseError:
			dropLatchesAndLockWaitQueues(true /* reuseLatchAndLockSpans */)
			// Then attempt to acquire the lease if not currently held by any
			// replica or redirect to the current leaseholder if currently held
			// by a different replica.
			if pErr = r.handleInvalidLeaseError(ctx, ba); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.MergeInProgressError:
			dropLatchesAndLockWaitQueues(true /* reuseLatchAndLockSpans */)
			// Then listen for the merge to complete.
			if pErr = r.handleMergeInProgressError(ctx, ba, pErr, t); pErr != nil {
				return nil, nil, pErr
			}
		case *kvpb.OptimisticEvalConflictsError:
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
func isConcurrencyRetryError(pErr *kvpb.Error) bool {
	switch pErr.GetDetail().(type) {
	case *kvpb.LockConflictError:
		// If a request hits a LockConflictError, it adds the conflicting intent
		// to the lockTable through a process called "lock discovery". It then
		// waits in the lock's wait-queue during its next sequencing pass.
	case *kvpb.TransactionPushError:
		// If a PushTxn request hits a TransactionPushError, it attempted to
		// push another transactions record but did not succeed. It enqueues the
		// pushee transaction in the txnWaitQueue and waits on the record to
		// change or expire during its next sequencing pass.
	case *kvpb.IndeterminateCommitError:
		// If a PushTxn hits a IndeterminateCommitError, it attempted to push an
		// expired transaction record in the STAGING state. It's unclear whether
		// the pushee is aborted or committed, so the request must kick off the
		// "transaction recovery procedure" to resolve this ambiguity before
		// retrying.
	case *kvpb.ReadWithinUncertaintyIntervalError:
		// If a request hits a ReadWithinUncertaintyIntervalError, it was performing
		// a non-locking read [1] and encountered a (committed or provisional) write
		// within the uncertainty interval of the reader. Depending on the state of
		// the request (see conditions in canDoServersideRetry), it may be able to
		// adjust its timestamp and retry on the server.
		//
		// This is similar to other server-side retries that we allow below
		// latching, like for WriteTooOld errors. However, because uncertainty
		// errors are specific to non-locking reads, they can not [2] be retried
		// without first dropping and re-acquiring their read latches at a higher
		// timestamp. This is unfortunate for uncertainty errors, as it leads to
		// some extra work.
		//
		// On the other hand, it is more important for other forms of retry errors
		// to be handled without dropping latches because they could be starved by
		// repeated conflicts. For instance, if WriteTooOld errors caused a write
		// request to drop and re-acquire latches, it is possible that the request
		// could return after each retry to find a new WriteTooOld conflict, never
		// managing to complete. This is not the case for uncertainty errors, which
		// can not occur indefinitely. A request (transactional or otherwise) has a
		// fixed uncertainty window and, once exhausted, will never hit an
		// uncertainty error again.
		//
		// [1] if a locking read observes a write at a later timestamp, it returns a
		// WriteTooOld error. It's uncertainty interval does not matter.
		// [2] in practice, this is enforced by tryBumpBatchTimestamp's call to
		// (*concurrency.Guard).IsolatedAtLaterTimestamps.
	case *kvpb.InvalidLeaseError:
		// If a request hits an InvalidLeaseError, the replica it is being
		// evaluated against does not have a valid lease under which it can
		// serve the request. The request cannot proceed until a new lease is
		// acquired. If the acquisition process determines that the lease now
		// lives elsewhere, the request should be redirected (using a
		// NotLeaseHolderError) to the new leaseholder.
	case *kvpb.MergeInProgressError:
		// If a request hits a MergeInProgressError, the replica it is being
		// evaluated against is in the process of being merged into its left-hand
		// neighbor. The request cannot proceed until the range merge completes,
		// either successfully or unsuccessfully, so it waits before retrying.
		// If the merge does complete successfully, the retry will be rejected
		// with an error that will propagate back to the client.
	case *kvpb.OptimisticEvalConflictsError:
		// Optimistic evaluation encountered a conflict. The request will
		// immediately retry pessimistically.
	default:
		return false
	}
	return true
}

// maybeAttachLease is used to augment a concurrency retry error with
// information about the lease that the operation which hit this error was
// operating under. If the operation was performed on a follower that does not
// hold the lease (e.g. a follower read), the provided lease will be empty.
func maybeAttachLease(pErr *kvpb.Error, lease *roachpb.Lease) *kvpb.Error {
	if lcErr, ok := pErr.GetDetail().(*kvpb.LockConflictError); ok {
		// If we hit an intent on the leaseholder, attach information about the
		// lease to LockConflictErrors, which is necessary to keep the lock-table
		// in sync with the applied state.
		//
		// However, if we hit an intent during a follower read, the lock-table will
		// be disabled, so we won't be able to use it to wait for the resolution of
		// the intent. Instead of waiting locally, we replace the LockConflictError
		// with an InvalidLeaseError so that the request will be redirected to the
		// leaseholder. Beyond implementation constraints, waiting for conflicting
		// intents on the leaseholder instead of on a follower is preferable
		// because:
		// - the leaseholder is notified of and reactive to lock-table state
		//   transitions.
		// - the leaseholder is able to more efficiently resolve intents, if
		//   necessary, without the risk of multiple follower<->leaseholder
		//   round-trips compounding. If the follower was to attempt to resolve
		//   multiple intents during a follower read then the PushTxn and
		//   ResolveIntent requests would quickly be more expensive (in terms of
		//   latency) than simply redirecting the entire read request to the
		//   leaseholder and letting the leaseholder coordinate the intent
		//   resolution.
		// - after the leaseholder has received a response from a ResolveIntent
		//   request, it has a guarantee that the intent resolution has been applied
		//   locally and that no future read will observe the intent. This is not
		//   true on follower replicas. Due to the asynchronous nature of Raft, both
		//   due to quorum voting and due to async commit acknowledgement from
		//   leaders to followers, it is possible for a ResolveIntent request to
		//   complete and then for a future read on a follower to observe the
		//   pre-resolution state of the intent. This effect is transient and will
		//   eventually disappear once the follower catches up on its Raft log, but
		//   it creates an opportunity for momentary thrashing if a follower read
		//   was to resolve an intent and then immediately attempt to read again.
		//
		// This behavior of redirecting follower read attempts to the leaseholder
		// replica if they encounter conflicting intents on a follower means that
		// follower read eligibility is a function of the "resolved timestamp" over
		// a read's key span, and not just the "closed timestamp" over its key span.
		// Architecturally, this is consistent with Google Spanner, who maintains a
		// concept of "safe time", "paxos safe time", "transaction manager safe
		// time". "safe time" is analogous to the "resolved timestamp" in
		// CockroachDB and "paxos safe time" is analogous to the "closed timestamp"
		// in CockroachDB. In Spanner, it is the "safe time" of a replica that
		// determines follower read eligibility.
		if lease.Empty() /* followerRead */ {
			return kvpb.NewErrorWithTxn(&kvpb.InvalidLeaseError{}, pErr.GetTxn())
		}
		lcErr.LeaseSequence = lease.Sequence
		return kvpb.NewErrorWithTxn(lcErr, pErr.GetTxn())
	}
	return pErr
}

func (r *Replica) handleLockConflictError(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	pErr *kvpb.Error,
	t *kvpb.LockConflictError,
) (*concurrency.Guard, *kvpb.Error) {
	if r.store.cfg.TestingKnobs.DontPushOnLockConflictError {
		return g, pErr
	}
	// g's latches will be dropped, but it retains its spot in lock wait-queues.
	return r.concMgr.HandleLockConflictError(ctx, g, t.LeaseSequence, t)
}

func (r *Replica) handleTransactionPushError(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	g *concurrency.Guard,
	pErr *kvpb.Error,
	t *kvpb.TransactionPushError,
) (*concurrency.Guard, *kvpb.Error) {
	// On a transaction push error, retry immediately if doing so will enqueue
	// into the txnWaitQueue in order to await further updates to the unpushed
	// txn's status. We check ShouldPushImmediately to avoid retrying
	// non-queueable PushTxnRequests (see #18191).
	dontRetry := r.store.cfg.TestingKnobs.DontRetryPushTxnFailures
	if !dontRetry && ba.IsSinglePushTxnRequest() {
		pushReq := ba.Requests[0].GetInner().(*kvpb.PushTxnRequest)
		dontRetry = txnwait.ShouldPushImmediately(pushReq, t.PusheeTxn.Status, ba.WaitPolicy)
	}
	if dontRetry {
		return g, pErr
	}
	// g's latches will be dropped, but it retains its spot in lock wait-queues
	// (though a PushTxn shouldn't be in any lock wait-queues).
	return r.concMgr.HandleTransactionPushError(ctx, g, t), nil
}

func (r *Replica) handleIndeterminateCommitError(
	ctx context.Context, ba *kvpb.BatchRequest, pErr *kvpb.Error, t *kvpb.IndeterminateCommitError,
) *kvpb.Error {
	if r.store.cfg.TestingKnobs.DontRecoverIndeterminateCommits {
		return pErr
	}
	// On an indeterminate commit error, attempt to recover and finalize the
	// stuck transaction. Retry immediately if successful.
	if _, err := r.store.recoveryMgr.ResolveIndeterminateCommit(ctx, t); err != nil {
		// Do not propagate ambiguous results; assume success and retry original op.
		if errors.HasType(err, (*kvpb.AmbiguousResultError)(nil)) {
			return nil
		}
		// Propagate new error. Preserve the error index.
		newPErr := kvpb.NewError(err)
		newPErr.Index = pErr.Index
		return newPErr
	}
	// We've recovered the transaction that blocked the request; retry.
	return nil
}

func (r *Replica) handleReadWithinUncertaintyIntervalError(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	pErr *kvpb.Error,
	t *kvpb.ReadWithinUncertaintyIntervalError,
) (*kvpb.BatchRequest, *kvpb.Error) {
	// Attempt a server-side retry of the request. Note that we pass nil for
	// latchSpans, because we have already released our latches and plan to
	// re-acquire them if the retry is allowed.
	var ok bool
	ba, ok = canDoServersideRetry(ctx, pErr, ba, nil /* g */, hlc.Timestamp{} /* deadline */)
	if !ok {
		r.store.Metrics().ReadWithinUncertaintyIntervalErrorServerSideRetryFailure.Inc(1)
		return nil, pErr
	}
	r.store.Metrics().ReadWithinUncertaintyIntervalErrorServerSideRetrySuccess.Inc(1)

	if ba.Txn == nil {
		// If the request is non-transactional and it was refreshed into the future
		// after observing a value with a timestamp in the future, immediately sleep
		// until its new read timestamp becomes present. We don't need to do this
		// for transactional requests because they will do this during their
		// commit-wait sleep after committing.
		//
		// See TxnCoordSender.maybeCommitWait for a discussion about why doing this
		// is necessary to preserve real-time ordering for transactions that write
		// into the future.
		var cancel func()
		ctx, cancel = r.store.Stopper().WithCancelOnQuiesce(ctx)
		defer cancel()
		if err := r.Clock().SleepUntil(ctx, ba.Timestamp); err != nil {
			return nil, kvpb.NewError(err)
		}
	}
	return ba, nil
}

func (r *Replica) handleInvalidLeaseError(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
	// On an invalid lease error, attempt to acquire a new lease. If in the
	// process of doing so, we determine that the lease now lives elsewhere,
	// redirect.
	_, pErr := r.redirectOnOrAcquireLeaseForRequest(ctx, ba.Timestamp, r.signallerForBatch(ba))
	// If we managed to get a lease (i.e. pErr == nil), the request evaluation
	// will be retried.
	return pErr
}

func (r *Replica) handleMergeInProgressError(
	ctx context.Context, ba *kvpb.BatchRequest, pErr *kvpb.Error, t *kvpb.MergeInProgressError,
) *kvpb.Error {
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
		return kvpb.NewErrorf("cannot transfer lease while merge in progress")
	}
	log.Event(ctx, "waiting on in-progress range merge")
	select {
	case <-mergeCompleteCh:
		// Merge complete. Retry the command.
		return nil
	case <-ctx.Done():
		return kvpb.NewError(errors.Wrap(ctx.Err(), "aborted during merge"))
	case <-r.store.stopper.ShouldQuiesce():
		return kvpb.NewError(&kvpb.NodeUnavailableError{})
	}
}

// executeAdminBatch executes the command directly. There is no interaction
// with the spanlatch manager or the timestamp cache, as admin commands
// are not meant to consistently access or modify the underlying data.
// Admin commands must run on the lease holder replica. Batch support here is
// limited to single-element batches; everything else catches an error.
func (r *Replica) executeAdminBatch(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if len(ba.Requests) != 1 {
		return nil, kvpb.NewErrorf("only single-element admin batches allowed")
	}

	args := ba.Requests[0].GetInner()

	sArg := reflect.TypeOf(args).String()
	ctx = logtags.AddTag(ctx, sArg, "")

	ctx, sp := tracing.EnsureChildSpan(ctx, r.AmbientContext.Tracer, sArg)
	defer sp.Finish()

	// Verify that the batch can be executed, which includes verifying that the
	// current replica has the range lease.
	// NB: we pass nil for the spanlatch guard because we haven't acquired
	// latches yet. This is ok because each individual request that the admin
	// request sends will acquire latches.
	for {
		if err := ctx.Err(); err != nil {
			return nil, kvpb.NewError(err)
		}

		_, err := r.checkExecutionCanProceedRWOrAdmin(ctx, ba, nil /* g */)
		if err == nil {
			err = r.signallerForBatch(ba).Err()
		}
		if err == nil {
			break
		}
		switch {
		case errors.HasType(err, (*kvpb.InvalidLeaseError)(nil)):
			// If the replica does not have the lease, attempt to acquire it, or
			// redirect to the current leaseholder by returning an error.
			_, pErr := r.redirectOnOrAcquireLeaseForRequest(ctx, ba.Timestamp, r.signallerForBatch(ba))
			if pErr != nil {
				return nil, pErr
			}
			// Retry...
		default:
			return nil, kvpb.NewError(err)
		}
	}

	var resp kvpb.Response
	var pErr *kvpb.Error
	switch tArgs := args.(type) {
	case *kvpb.AdminSplitRequest:
		var reply kvpb.AdminSplitResponse
		reply, pErr = r.AdminSplit(ctx, *tArgs, manualAdminReason)
		resp = &reply

	case *kvpb.AdminUnsplitRequest:
		var reply kvpb.AdminUnsplitResponse
		reply, pErr = r.AdminUnsplit(ctx, *tArgs, manualAdminReason)
		resp = &reply

	case *kvpb.AdminMergeRequest:
		var reply kvpb.AdminMergeResponse
		reply, pErr = r.AdminMerge(ctx, *tArgs, manualAdminReason)
		resp = &reply

	case *kvpb.AdminTransferLeaseRequest:
		pErr = kvpb.NewError(r.AdminTransferLease(ctx, tArgs.Target, tArgs.BypassSafetyChecks))
		resp = &kvpb.AdminTransferLeaseResponse{}

	case *kvpb.AdminChangeReplicasRequest:
		chgs := tArgs.Changes()
		desc, err := r.ChangeReplicas(ctx, &tArgs.ExpDesc, kvserverpb.ReasonAdminRequest, "", chgs)
		pErr = kvpb.NewError(err)
		if pErr != nil {
			resp = &kvpb.AdminChangeReplicasResponse{}
		} else {
			resp = &kvpb.AdminChangeReplicasResponse{
				Desc: *desc,
			}
		}

	case *kvpb.AdminRelocateRangeRequest:
		// Transferring the lease to the first voting replica in the target slice is
		// pre-22.1 behavior.
		// We revert to that behavior if the request is coming
		// from a 21.2 node that doesn't yet know about this change in contract.
		transferLeaseToFirstVoter := !tArgs.TransferLeaseToFirstVoterAccurate
		// We also revert to that behavior if the caller specifically asked for it.
		transferLeaseToFirstVoter = transferLeaseToFirstVoter || tArgs.TransferLeaseToFirstVoter
		err := r.AdminRelocateRange(
			ctx, *r.Desc(), tArgs.VoterTargets, tArgs.NonVoterTargets, transferLeaseToFirstVoter,
		)
		pErr = kvpb.NewError(err)
		resp = &kvpb.AdminRelocateRangeResponse{}

	case *kvpb.CheckConsistencyRequest:
		var reply kvpb.CheckConsistencyResponse
		reply, pErr = r.CheckConsistency(ctx, *tArgs)
		resp = &reply

	case *kvpb.AdminScatterRequest:
		reply, err := r.adminScatter(ctx, *tArgs)
		pErr = kvpb.NewError(err)
		resp = &reply

	case *kvpb.AdminVerifyProtectedTimestampRequest:
		reply, err := r.adminVerifyProtectedTimestamp(ctx, *tArgs)
		pErr = kvpb.NewError(err)
		resp = &reply

	default:
		return nil, kvpb.NewErrorf("unrecognized admin command: %T", args)
	}

	if pErr != nil {
		return nil, pErr
	}

	br := &kvpb.BatchResponse{}
	br.Add(resp)
	br.Txn = resp.Header().Txn
	return br, nil
}

func (r *Replica) recordBatchRequestLoad(ctx context.Context, ba *kvpb.BatchRequest) {
	if r.loadStats == nil {
		log.VEventf(
			ctx,
			3,
			"Unable to record load of batch request for r%d, load stats is not initialized",
			ba.Header.RangeID,
		)
		return
	}

	// adjustedQPS is the adjusted number of queries per second, that is a cost
	// estimate of a BatchRequest. See getBatchRequestQPS() for the
	// calculation.
	adjustedQPS := r.getBatchRequestQPS(ctx, ba)

	r.loadStats.RecordBatchRequests(adjustedQPS, ba.Header.GatewayNodeID)
	r.loadStats.RecordRequests(float64(len(ba.Requests)))
}

// getBatchRequestQPS calculates the cost estimation of a BatchRequest. The
// estimate returns Queries Per Second (QPS), representing the abstract
// resource cost associated with this request. BatchRequests are calculated as
// 1 QPS, unless an AddSSTableRequest exists, in which case the sum of all
// AddSSTableRequest's data size is divided by a factor and added to QPS. This
// specific treatment of QPS is a special case to account for the mismatch
// between AddSSTableRequest and other requests in terms of resource use.
func (r *Replica) getBatchRequestQPS(ctx context.Context, ba *kvpb.BatchRequest) float64 {
	var count float64 = 1

	// For divisors less than 1, use the default treatment of QPS.
	requestFact := replicastats.AddSSTableRequestSizeFactor.Get(&r.store.cfg.Settings.SV)
	if requestFact < 1 {
		return count
	}

	var addSSTSize float64 = 0
	for _, req := range ba.Requests {
		switch t := req.GetInner().(type) {
		case *kvpb.AddSSTableRequest:
			addSSTSize += float64(len(t.Data))
		default:
			continue
		}
	}

	count += addSSTSize / float64(requestFact)
	return count
}

// recordRequestWriteBytes records the write bytes from a replica batch
// request.
func (r *Replica) recordRequestWriteBytes(writeBytes *kvadmission.StoreWriteBytes) {
	if writeBytes == nil {
		return
	}
	// TODO(kvoli): Consider recording the ingested bytes (AddSST) separately
	// to the write bytes.
	r.loadStats.RecordWriteBytes(float64(writeBytes.WriteBytes + writeBytes.IngestedBytes))
}

// checkBatchRequest verifies BatchRequest validity requirements. In particular,
// the batch must have an assigned timestamp, and either all requests must be
// read-only, or none.
//
// TODO(tschottdorf): should check that request is contained in range and that
// EndTxn only occurs at the very end.
func (r *Replica) checkBatchRequest(ba *kvpb.BatchRequest, isReadOnly bool) error {
	if ba.Timestamp.IsEmpty() {
		// For transactional requests, Store.Send sets the timestamp. For non-
		// transactional requests, the client sets the timestamp. Either way, we
		// need to have a timestamp at this point.
		return errors.New("Replica.checkBatchRequest: batch does not have timestamp assigned")
	}
	consistent := ba.ReadConsistency == kvpb.CONSISTENT
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
	ba *kvpb.BatchRequest,
) (
	latchSpans *spanset.SpanSet,
	lockSpans *lockspanset.LockSpanSet,
	requestEvalKind concurrency.RequestEvalKind,
	_ error,
) {
	latchSpans, lockSpans = spanset.New(), lockspanset.New()
	r.mu.RLock()
	desc := r.descRLocked()
	liveCount := r.shMu.state.Stats.LiveCount
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
		if et, ok := ba.GetArg(kvpb.EndTxn); ok {
			// EndTxn declares a global write for each of its lock spans.
			latchGuess += len(et.(*kvpb.EndTxnRequest).LockSpans) - 1
		}
		latchSpans.Reserve(spanset.SpanReadWrite, spanset.SpanGlobal, latchGuess)
		// TODO(arul): Use the correct locking strength here.
		lockSpans.Reserve(lock.Intent, len(ba.Requests))
	} else {
		latchSpans.Reserve(spanset.SpanReadOnly, spanset.SpanGlobal, len(ba.Requests))
		lockSpans.Reserve(lock.None, len(ba.Requests))
	}

	// Note that we are letting locking readers be considered for optimistic
	// evaluation. This is correct, though not necessarily beneficial.
	considerOptEval := ba.IsReadOnly() && ba.IsAllTransactional() &&
		optimisticEvalLimitedScans.Get(&r.ClusterSettings().SV)

	// If the request is using a SkipLocked wait policy, we always perform
	// optimistic evaluation. In Replica.collectSpansRead, SkipLocked reads are
	// able to constraint their read spans down to point reads on just those keys
	// that were returned and were not already locked. This means that there is a
	// good chance that some or all of the write latches that the SkipLocked read
	// would have blocked on won't overlap with the keys that the request ends up
	// returning, so they won't conflict when checking for optimistic conflicts.
	//
	// Concretely, SkipLocked reads can ignore write latches when:
	// 1. a key is first being written to non-transactionally (or through 1PC)
	//    and its write is in flight.
	// 2. a key is locked and its value is being updated by a write from its
	//    transaction that is in flight.
	// 3. a key is locked and the lock is being removed by intent resolution.
	//
	// In all three of these cases, optimistic evaluation improves concurrency
	// because the SkipLocked request does not return the key that is currently
	// write latched. However, SkipLocked reads will fail optimistic evaluation
	// if they return a key that is write latched. For instance, it can fail if
	// it returns a key that is being updated without first being locked.
	optEvalForSkipLocked := considerOptEval && ba.Header.WaitPolicy == lock.WaitPolicy_SkipLocked

	// If the request is using a key limit, we may want it to perform optimistic
	// evaluation, depending on how large the limit is.
	considerOptEvalForLimit := considerOptEval && ba.Header.MaxSpanRequestKeys > 0
	// When considerOptEvalForLimit, these are computed below and used to decide
	// whether to actually do optimistic evaluation.
	hasScans := false
	numGets := 0

	// For non-local, MVCC spans we annotate them with the request timestamp
	// during declaration. This is the timestamp used during latch acquisitions.
	// For read requests this works as expected, reads are performed at the same
	// timestamp. During writes however, we may encounter a versioned value newer
	// than the request timestamp, and may have to retry at a higher timestamp.
	// This is still safe as we're only ever writing at timestamps higher than the
	// timestamp any write latch would be declared at.
	err := batcheval.DeclareKeysForBatch(desc, &ba.Header, latchSpans)
	if err != nil {
		return nil, nil, concurrency.PessimisticEval, err
	}
	for _, union := range ba.Requests {
		inner := union.GetInner()
		if cmd, ok := batcheval.LookupCommand(inner.Method()); ok {
			err := cmd.DeclareKeys(desc, &ba.Header, inner, latchSpans, lockSpans, r.Clock().MaxOffset())
			if err != nil {
				return nil, nil, concurrency.PessimisticEval, err
			}
			if considerOptEvalForLimit {
				switch inner.(type) {
				case *kvpb.ScanRequest, *kvpb.ReverseScanRequest:
					hasScans = true
				case *kvpb.GetRequest:
					numGets++
				}
			}
		} else {
			return nil, nil, concurrency.PessimisticEval, errors.Errorf("unrecognized command %s", inner.Method())
		}
	}

	// Commands may create a large number of duplicate spans. De-duplicate
	// them to reduce the number of spans we pass to the {spanlatch,Lock}Manager.
	latchSpans.SortAndDedup()
	lockSpans.SortAndDeDup()

	// If any command gave us spans that are invalid, bail out early
	// (before passing them to the {spanlatch,Lock}Manager, which may panic).
	if err := latchSpans.Validate(); err != nil {
		return nil, nil, concurrency.PessimisticEval, err
	}
	if err := lockSpans.Validate(); err != nil {
		return nil, nil, concurrency.PessimisticEval, err
	}

	optEvalForLimit := false
	if considerOptEvalForLimit {
		// Evaluate batches optimistically if they have a key limit which is less
		// than the upper bound on number of keys that can be returned for this
		// batch. For scans, the upper bound is the number of live keys on the
		// Range. For gets, it is the minimum of the number of live keys on the
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
			optEvalForLimit = true
		}
	}

	requestEvalKind = concurrency.PessimisticEval
	if optEvalForSkipLocked || optEvalForLimit {
		requestEvalKind = concurrency.OptimisticEval
	}

	return latchSpans, lockSpans, requestEvalKind, nil
}

// endCmds holds necessary information to end a batch after command processing,
// either after a write request has achieved consensus and been applied to Raft
// or after a read-only request has finished evaluation.
type endCmds struct {
	repl             *Replica
	g                *concurrency.Guard
	st               kvserverpb.LeaseStatus // empty for follower reads
	replicatingSince time.Time
}

// makeUnreplicatedEndCmds sets up an endCmds to track an unreplicated,
// that is, read-only, command.
func makeUnreplicatedEndCmds(
	repl *Replica, g *concurrency.Guard, st kvserverpb.LeaseStatus,
) endCmds {
	return makeReplicatedEndCmds(repl, g, st, time.Time{})
}

// makeReplicatedEndCmds initializes an endCmds representing a command that
// needs to undergo replication. This is not used for read-only commands
// (including read-write commands that end up not queueing any mutations to the
// state machine).
func makeReplicatedEndCmds(
	repl *Replica, g *concurrency.Guard, st kvserverpb.LeaseStatus, replicatingSince time.Time,
) endCmds {
	return endCmds{repl: repl, g: g, st: st, replicatingSince: replicatingSince}
}

func makeEmptyEndCmds() endCmds {
	return endCmds{}
}

// move moves the endCmds into the return value, clearing and making a call to
// done on the receiver a no-op.
func (ec *endCmds) move() endCmds {
	res := *ec
	*ec = makeEmptyEndCmds()
	return res
}

// poison marks the Guard held by the endCmds as poisoned, which
// induces fail-fast behavior for requests waiting for our latches.
// This method must only be called for commands in the Replica.mu.proposals
// map and the Replica mutex must be held throughout.
func (ec *endCmds) poison() {
	if ec.repl == nil {
		// Already cleared. This path may no longer be hit thanks to a re-work
		// of reproposals[1]. The caller to poison holds the replica mutex and
		// the command is in r.mu.proposals, meaning the command hasn't been
		// signaled by log application yet, i.e. latches must not have been
		// released (even if refreshProposalsLocked has already signaled the
		// client with an ambiguous result).
		//
		// [1]: https://github.com/cockroachdb/cockroach/pull/106750
		//
		// TODO(repl): verify that and put an assertion here. This is similar to
		// the TODO on ProposalData.endCmds.
		return
	}
	ec.repl.concMgr.PoisonReq(ec.g)
}

// done releases the latches acquired by the command and updates the timestamp
// cache using the final timestamp of each command. If `br` is nil, it is
// assumed that `done` is being called by a request that's dropping its latches
// before evaluation.
//
// No-op if the receiver has been zeroed out by a call to move. Idempotent and
// is safe to call more than once.
func (ec *endCmds) done(
	ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse, pErr *kvpb.Error,
) {
	if ec.repl == nil {
		// The endCmds were cleared. This may no longer be necessary, see the comment on
		// ProposalData.endCmds.
		return
	}
	defer ec.move() // clear

	// Update the timestamp cache. Each request within the batch is considered in
	// turn; only those marked as affecting the cache are processed. However, only
	// do so if the request is consistent and was operating on the leaseholder
	// under a valid range lease.
	if ba.ReadConsistency == kvpb.CONSISTENT && ec.st.State == kvserverpb.LeaseState_VALID {
		ec.repl.updateTimestampCache(ctx, ba, br, pErr)
	}

	if ts := ec.replicatingSince; !ts.IsZero() {
		ec.repl.store.metrics.RaftReplicationLatency.RecordValue(timeutil.Since(ts).Nanoseconds())
	}

	// Release the latches acquired by the request and exit lock wait-queues. Must
	// be done AFTER the timestamp cache is updated. ec.g is set both for reads
	// and for writes. For writes, it is set only when the Raft proposal has
	// assumed responsibility for the request.
	//
	// TODO(replication): at the time of writing, there is no code path in which
	// this method is called and the Guard is not set. Consider removing this
	// check and upgrading the previous observation to an invariant.
	if ec.g != nil {
		ec.repl.concMgr.FinishReq(ctx, ec.g)
	}
}
