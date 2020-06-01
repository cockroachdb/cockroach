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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	opentracing "github.com/opentracing/opentracing-go"
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
	ctx, cleanup := tracing.EnsureContext(ctx, r.AmbientContext.Tracer, "replica send")
	defer cleanup()

	// If the internal Raft group is not initialized, create it and wake the leader.
	r.maybeInitializeRaftGroup(ctx)

	isReadOnly := ba.IsReadOnly()
	useRaft := !isReadOnly && ba.IsWrite()

	if err := r.checkBatchRequest(ba, isReadOnly); err != nil {
		return nil, roachpb.NewError(err)
	}

	if err := r.maybeBackpressureBatch(ctx, ba); err != nil {
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
	if useRaft {
		log.Event(ctx, "read-write path")
		fn := (*Replica).executeWriteBatch
		br, pErr = r.executeBatchWithConcurrencyRetries(ctx, ba, fn)
	} else if isReadOnly {
		log.Event(ctx, "read-only path")
		fn := (*Replica).executeReadOnlyBatch
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
	return br, pErr
}

// batchExecutionFn is a method on Replica that is able to execute a
// BatchRequest. It is called with the batch, along with the status of
// the lease that the batch is operating under and a guard for the
// latches protecting the request.
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
	*Replica, context.Context, *roachpb.BatchRequest, kvserverpb.LeaseStatus, *concurrency.Guard,
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
	// Try to execute command; exit retry loop on success.
	var g *concurrency.Guard
	var latchSpans, lockSpans *spanset.SpanSet
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

		// Determine the lease under which to evaluate the request.
		var status kvserverpb.LeaseStatus
		if !ba.ReadConsistency.RequiresReadLease() {
			// Get a clock reading for checkExecutionCanProceed.
			status.Timestamp = r.Clock().Now()
		} else if ba.IsSingleSkipLeaseCheckRequest() {
			// For lease commands, use the provided previous lease for verification.
			status.Lease = ba.GetPrevLeaseForLeaseRequest()
			status.Timestamp = r.Clock().Now()
		} else {
			// If the request is a write or a consistent read, it requires the
			// range lease or permission to serve via follower reads.
			if status, pErr = r.redirectOnOrAcquireLease(ctx); pErr != nil {
				if nErr := r.canServeFollowerRead(ctx, ba, pErr); nErr != nil {
					return nil, nErr
				}
			}
		}
		// Limit the transaction's maximum timestamp using observed timestamps.
		r.limitTxnMaxTimestamp(ctx, ba, status)

		// Determine the maximal set of key spans that the batch will operate
		// on. We only need to do this once and we make sure to do so after we
		// have limited the transaction's maximum timestamp.
		if latchSpans == nil {
			var err error
			latchSpans, lockSpans, err = r.collectSpans(ba)
			if err != nil {
				return nil, roachpb.NewError(err)
			}

			// Handle load-based splitting.
			r.recordBatchForLoadBasedSplitting(ctx, ba, latchSpans)
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
			Requests:        ba.Requests,
			LatchSpans:      latchSpans,
			LockSpans:       lockSpans,
		})
		if pErr != nil {
			return nil, pErr
		} else if resp != nil {
			br = new(roachpb.BatchResponse)
			br.Responses = resp
			return br, nil
		}

		if filter := r.store.cfg.TestingKnobs.TestingLatchFilter; filter != nil {
			if pErr := filter(ctx, *ba); pErr != nil {
				return nil, pErr
			}
		}

		br, g, pErr = fn(r, ctx, ba, status, g)
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
			r.concMgr.FinishReq(g)
			g = nil
			// Then launch a task to handle the indeterminate commit error.
			if pErr = r.handleIndeterminateCommitError(ctx, ba, pErr, t); pErr != nil {
				return nil, pErr
			}
		case *roachpb.MergeInProgressError:
			// Drop latches and lock wait-queues.
			r.concMgr.FinishReq(g)
			g = nil
			// Then listen for the merge to complete.
			if pErr = r.handleMergeInProgressError(ctx, ba, pErr, t); pErr != nil {
				return nil, pErr
			}
		default:
			log.Fatalf(ctx, "unexpected concurrency retry error %T", t)
		}
		// Retry...
	}
}

// isConcurrencyRetryError returns whether or not the provided error is a
// "server-side concurrency retry error" that will be captured and retried by
// executeBatchWithConcurrencyRetries. Server-side concurrency retry errors are
// handled by dropping a request's latches, waiting for and/or ensuring that the
// condition which caused the error is handled, re-sequencing through the
// concurrency manager, and executing the request again.
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
	case *roachpb.MergeInProgressError:
		// If a request hits a MergeInProgressError, the replica it is being
		// evaluted against is in the process of being merged into its left-hand
		// neighbor. The request cannot proceed until the range merge completes,
		// either successfully or unsuccessfully, so it waits before retrying.
		// If the merge does complete successfully, the retry will be rejected
		// with an error that will propagate back to the client.
	default:
		return false
	}
	return true
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
	return r.concMgr.HandleWriterIntentError(ctx, g, t)
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
	log.Event(ctx, "waiting on in-progress merge")
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
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		sp.SetOperationName(reflect.TypeOf(args).String())
	}

	// Admin commands always require the range lease.
	status, pErr := r.redirectOnOrAcquireLease(ctx)
	if pErr != nil {
		return nil, pErr
	}
	// Note there is no need to limit transaction max timestamp on admin requests.

	// Verify that the batch can be executed.
	// NB: we pass nil for the spanlatch guard because we haven't acquired
	// latches yet. This is ok because each individual request that the admin
	// request sends will acquire latches.
	if err := r.checkExecutionCanProceed(ctx, ba, nil /* g */, &status); err != nil {
		return nil, roachpb.NewError(err)
	}

	var resp roachpb.Response
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
		err := r.store.AdminRelocateRange(ctx, *r.Desc(), tArgs.Targets)
		pErr = roachpb.NewError(err)
		resp = &roachpb.AdminRelocateRangeResponse{}

	case *roachpb.CheckConsistencyRequest:
		var reply roachpb.CheckConsistencyResponse
		reply, pErr = r.CheckConsistency(ctx, *tArgs)
		resp = &reply

	case *roachpb.ImportRequest:
		cArgs := batcheval.CommandArgs{
			EvalCtx: NewReplicaEvalContext(r, todoSpanSet),
			Header:  ba.Header,
			Args:    args,
		}
		var err error
		resp, err = importCmdFn(ctx, cArgs)
		pErr = roachpb.NewError(err)

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

	if ba.Header.ReturnRangeInfo {
		returnRangeInfo(resp, r)
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
	if ba.Timestamp == (hlc.Timestamp{}) {
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
) (latchSpans, lockSpans *spanset.SpanSet, _ error) {
	latchSpans, lockSpans = new(spanset.SpanSet), new(spanset.SpanSet)
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
		guess := len(ba.Requests)
		if et, ok := ba.GetArg(roachpb.EndTxn); ok {
			// EndTxn declares a global write for each of its lock spans.
			guess += len(et.(*roachpb.EndTxnRequest).LockSpans) - 1
		}
		latchSpans.Reserve(spanset.SpanReadWrite, spanset.SpanGlobal, guess)
	} else {
		latchSpans.Reserve(spanset.SpanReadOnly, spanset.SpanGlobal, len(ba.Requests))
	}

	// For non-local, MVCC spans we annotate them with the request timestamp
	// during declaration. This is the timestamp used during latch acquisitions.
	// For read requests this works as expected, reads are performed at the same
	// timestamp. During writes however, we may encounter a versioned value newer
	// than the request timestamp, and may have to retry at a higher timestamp.
	// This is still safe as we're only ever writing at timestamps higher than the
	// timestamp any write latch would be declared at.
	desc := r.Desc()
	batcheval.DeclareKeysForBatch(desc, ba.Header, latchSpans)
	for _, union := range ba.Requests {
		inner := union.GetInner()
		if cmd, ok := batcheval.LookupCommand(inner.Method()); ok {
			cmd.DeclareKeys(desc, ba.Header, inner, latchSpans, lockSpans)
		} else {
			return nil, nil, errors.Errorf("unrecognized command %s", inner.Method())
		}
	}

	// Commands may create a large number of duplicate spans. De-duplicate
	// them to reduce the number of spans we pass to the spanlatch manager.
	for _, s := range [...]*spanset.SpanSet{latchSpans, lockSpans} {
		s.SortAndDedup()

		// If any command gave us spans that are invalid, bail out early
		// (before passing them to the spanlatch manager, which may panic).
		if err := s.Validate(); err != nil {
			return nil, nil, err
		}
	}

	return latchSpans, lockSpans, nil
}

// limitTxnMaxTimestamp limits the batch transaction's max timestamp
// so that it respects any timestamp already observed on this node.
// This prevents unnecessary uncertainty interval restarts caused by
// reading a value written at a timestamp between txn.Timestamp and
// txn.MaxTimestamp. The replica lease's start time is also taken into
// consideration to ensure that a lease transfer does not result in
// the observed timestamp for this node being inapplicable to data
// previously written by the former leaseholder. To wit:
//
// 1. put(k on leaseholder n1), gateway chooses t=1.0
// 2. begin; read(unrelated key on n2); gateway chooses t=0.98
// 3. pick up observed timestamp for n2 of t=0.99
// 4. n1 transfers lease for range with k to n2 @ t=1.1
// 5. read(k) on leaseholder n2 at ReadTimestamp=0.98 should get
//    ReadWithinUncertaintyInterval because of the write in step 1, so
//    even though we observed n2's timestamp in step 3 we must expand
//    the uncertainty interval to the lease's start time, which is
//    guaranteed to be greater than any write which occurred under
//    the previous leaseholder.
func (r *Replica) limitTxnMaxTimestamp(
	ctx context.Context, ba *roachpb.BatchRequest, status kvserverpb.LeaseStatus,
) {
	if ba.Txn == nil {
		return
	}
	// For calls that read data within a txn, we keep track of timestamps
	// observed from the various participating nodes' HLC clocks. If we have
	// a timestamp on file for this Node which is smaller than MaxTimestamp,
	// we can lower MaxTimestamp accordingly. If MaxTimestamp drops below
	// ReadTimestamp, we effectively can't see uncertainty restarts anymore.
	// TODO(nvanbenschoten): This should use the lease's node id.
	obsTS, ok := ba.Txn.GetObservedTimestamp(ba.Replica.NodeID)
	if !ok {
		return
	}
	// If the lease is valid, we use the greater of the observed
	// timestamp and the lease start time, up to the max timestamp. This
	// ensures we avoid incorrect assumptions about when data was
	// written, in absolute time on a different node, which held the
	// lease before this replica acquired it.
	// TODO(nvanbenschoten): Do we ever need to call this when
	//   status.State != VALID?
	if status.State == kvserverpb.LeaseState_VALID {
		obsTS.Forward(status.Lease.Start)
	}
	if obsTS.Less(ba.Txn.MaxTimestamp) {
		// Copy-on-write to protect others we might be sharing the Txn with.
		txnClone := ba.Txn.Clone()
		// The uncertainty window is [ReadTimestamp, maxTS), so if that window
		// is empty, there won't be any uncertainty restarts.
		if obsTS.LessEq(ba.Txn.ReadTimestamp) {
			log.Event(ctx, "read has no clock uncertainty")
		}
		txnClone.MaxTimestamp.Backward(obsTS)
		ba.Txn = txnClone
	}
}
