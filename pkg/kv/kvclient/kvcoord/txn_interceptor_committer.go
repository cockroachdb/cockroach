// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

var parallelCommitsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.parallel_commits_enabled",
	"if enabled, transactional commits will be parallelized with transactional writes",
	true,
	settings.WithName("kv.transaction.parallel_commits.enabled"),
)

// txnCommitter is a txnInterceptor that concerns itself with committing and
// rolling back transactions. It intercepts EndTxn requests and coordinates
// their execution. This is accomplished either by issuing them directly with
// proper addressing if they are alone, eliding them if they are not needed, or
// coordinating their execution in parallel with the rest of their batch if they
// are part of a larger set of requests.
//
// The third operation listed, which we define as a "parallel commit", is the
// most interesting. Marking a transaction record as committed in parallel with
// writing the rest of the transaction's intents is a clear win in terms of
// latency - in theory it removes the cost of an entire consensus round-trip
// from a transaction. However, doing so safely comes with extra complication.
// It requires an extension to the transaction model, additional client-side
// logic, buy-in from concurrency control, and specialized support from a
// transaction recovery mechanism. txnCommitter is responsible for the parallel
// commit-specific client-side logic.
//
// Parallel commits works by defining a committed transaction as a transaction
// that meets one of the two following commit conditions:
//  1. a transaction is *explicitly committed* if it has a transaction record with
//     a COMMITTED status
//  2. a transaction is *implicitly committed* if it has a transaction record with
//     a STAGING status and intents written for all writes declared as "in-flight"
//     on the transaction record at equal or lower timestamps than the transaction
//     record's commit timestamp
//
// A transaction may move from satisfying the implicit commit condition to
// satisfying the explicit commit condition. This is desirable because it moves
// the commit condition from a distributed condition to one local to the
// transaction record. Regardless, once either commit condition is satisfied, a
// transaction will remain committed in perpetuity both to itself and to all
// concurrent observers.
//
// The txnCommitter interceptor's role in this is to determine the set of writes
// that will be in-flight during a parallel commit. It collects this set from
// both the writes and the query intent requests that it finds present in the
// same batch as the committing end transaction request. The writes in this
// batch indicate a new intent write and the query intent requests indicate a
// previous pipelined intent write that has not yet been proven as successful.
// Before issuing the batch, the txnCommitter attaches this set to the end
// transaction request.
//
// The txnCommitter then collects the response of the batch when it returns.
// Based on the outcome of the requests in the batch, the interceptor determines
// whether the transaction successfully committed by satisfying the implicit
// commit condition.
//
// If all requests in the batch succeeded (including the EndTxn request) then
// the implicit commit condition is satisfied. The interceptor returns a
// successful response up then stack and launches an async task to make the
// commit explicit by moving the transaction record's status from STAGING to
// COMMITTED.
//
// If all requests did not succeed then the implicit commit condition is not
// satisfied and the transaction is still in-progress (and could still be
// committed or aborted at a later time). There are a number of reasons why
// some of the requests in the final batch may have failed:
//
//   - intent writes (error): these requests may fail to write an intent due to
//     a logical error like a ConditionFailedError during evaluation. In these
//     cases, the txnCommitter will receive an error and can conclude that
//     intent was never written and so the implicit commit condition is not
//     satisfied. The error is returned to the client.
//
//   - intent writes (successful but pushed): these requests may also succeed at
//     writing an intent but fail to write it at the desired (staging) timestamp
//     because they run into the timestamp cache or another committed value
//     (forms of contention). In these cases, the txnCommitter will receive a
//     successful response, but can determine (isTxnCommitImplicit) that the
//     transaction does not satisfy the implicit commit condition because one or
//     more of its intents are written with a timestamp higher than the staging
//     transaction record's. It will retry the transaction commit by re-issuing
//     the EndTxn request (retryTxnCommitAfterFailedParallelCommit) to attempt
//     to move the transaction directly to the explicitly committed state. This
//     retry is called a "parallel commit auto-retry".
//
//   - query intents: these requests may fail because they discover that one of the
//     previously issued writes has failed; either because it never left an intent
//     or because it left one at too high of a timestamp. In this case, the request
//     will return an error because the requests all have the ErrorIfMissing option
//     set. It will also prevent the write from ever succeeding in the future, which
//     ensures that the transaction will never suddenly become implicitly committed
//     at a later point due to the write eventually succeeding (e.g. after a replay).
//
//   - end txn: this request may fail with a TransactionRetryError for any number of
//     reasons, such as if the transaction's provisional commit timestamp has been
//     pushed past its read timestamp. In all of these cases, an error will be
//     returned and the transaction record will not be staged.
//
// If it is unknown whether all of the requests in the final batch succeeded
// (e.g. due to a network error) then an AmbiguousResultError is returned. The
// logic to enforce this is in DistSender.
//
// In all cases, the interceptor abstracts away the details of this from all
// interceptors above it in the coordinator interceptor stack.
type txnCommitter struct {
	st         *cluster.Settings
	stopper    *stop.Stopper
	wrapped    lockedSender
	metrics    *TxnMetrics
	mu         sync.Locker
	disable1PC bool
}

// SendLocked implements the lockedSender interface.
func (tc *txnCommitter) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	tc.maybeDisable1PC(ba)
	// If the batch does not include an EndTxn request, pass it through.
	rArgs, hasET := ba.GetArg(kvpb.EndTxn)
	if !hasET {
		return tc.wrapped.SendLocked(ctx, ba)
	}
	et := rArgs.(*kvpb.EndTxnRequest)

	if err := tc.validateEndTxnBatch(ba); err != nil {
		return nil, kvpb.NewError(err)
	}
	// Determine whether we can elide the EndTxn entirely. We can do so if the
	// transaction is read-only, which we determine based on whether the EndTxn
	// request contains any writes.
	if len(et.LockSpans) == 0 && len(et.InFlightWrites) == 0 {
		return tc.sendLockedWithElidedEndTxn(ctx, ba, et)
	}

	// Assign the transaction's key to the Request's header.
	if et.Key != nil {
		return nil, kvpb.NewError(errors.AssertionFailedf("client must not assign Key to EndTxn"))
	}
	et.Key = ba.Txn.Key
	et.Disable1PC = tc.disable1PC // disable the 1PC optimization, if necessary

	// Determine whether the commit request can be run in parallel with the rest
	// of the requests in the batch. If not, move the in-flight writes currently
	// attached to the EndTxn request to the LockSpans and clear the in-flight
	// write set; no writes will be in-flight concurrently with the EndTxn
	// request.
	if len(et.InFlightWrites) > 0 && !tc.canCommitInParallel(ba, et) {
		// NB: when parallel commits is disabled, this is the best place to
		// detect whether the batch has only distinct spans. We can set this
		// flag based on whether any of previously declared in-flight writes
		// in this batch overlap with each other. This will have (rare) false
		// negatives when the in-flight writes overlap with existing lock
		// spans, but never false positives.
		et.LockSpans, ba.Header.DistinctSpans = mergeIntoSpans(et.LockSpans, et.InFlightWrites)
		// Disable parallel commits.
		et.InFlightWrites = nil
	}

	// If the EndTxn request is a rollback, pass it through.
	if !et.Commit {
		return tc.wrapped.SendLocked(ctx, ba)
	}

	// Send the adjusted batch through the wrapped lockedSender. Unlocks while
	// sending then re-locks.
	br, pErr := tc.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		// If the batch resulted in an error but the EndTxn request succeeded,
		// staging the transaction record in the process, downgrade the status
		// back to PENDING. Even though the transaction record may have a status
		// of STAGING, we know that the transaction failed to implicitly commit,
		// so interceptors above the txnCommitter in the stack don't need to be
		// made aware that the record is staging.
		pErr = maybeRemoveStagingStatusInErr(pErr)
		return nil, pErr
	}

	// Determine next steps based on the status of the transaction.
	switch br.Txn.Status {
	case roachpb.STAGING:
		// Continue with STAGING-specific validation and cleanup.
	case roachpb.PREPARED:
		// The transaction is prepared.
		return br, nil
	case roachpb.COMMITTED:
		// The transaction is explicitly committed. This is possible if all
		// in-flight writes were sent to the same range as the EndTxn request,
		// in a single batch. In this case, a range can determine that all
		// in-flight writes will succeed with the EndTxn and can decide to skip
		// the STAGING state.
		//
		// This is also possible if we never attached any in-flight writes to
		// the EndTxn request, either because canCommitInParallel returned false
		// or because there were no unproven in-flight writes (see txnPipeliner)
		// and there were no writes in the batch request.
		return br, nil
	default:
		return nil, kvpb.NewErrorf("unexpected response status without error: %v", br.Txn)
	}

	// Determine whether the transaction satisfies the implicit commit condition.
	// If not, it needs to retry the EndTxn request, and possibly also refresh if
	// it is serializable.
	implicitCommit, err := isTxnCommitImplicit(br)
	if err != nil {
		return nil, kvpb.NewError(err)
	}

	// Retry the EndTxn request (and nothing else) if the transaction does not
	// satisfy the implicit commit condition. This EndTxn request will not be
	// in-flight concurrently with any other writes (they all succeeded), so it
	// will move the transaction record directly to the COMMITTED state.
	//
	// Note that we leave the transaction record that we wrote in the STAGING
	// state, which is not ideal. But as long as we continue heartbeating the
	// txn record, it being PENDING or STAGING does not make a difference.
	if !implicitCommit {
		return tc.retryTxnCommitAfterFailedParallelCommit(ctx, ba, br)
	}

	// If the transaction doesn't need to retry then it is implicitly committed!
	// We're the only ones who know that though -- other concurrent transactions
	// will need to go through the full status resolution process to make a
	// determination about the status of our STAGING transaction. To avoid this,
	// we transition to an explicitly committed transaction as soon as possible.
	// This also has the side-effect of kicking off intent resolution.
	mergedLockSpans, _ := mergeIntoSpans(et.LockSpans, et.InFlightWrites)
	tc.makeTxnCommitExplicitAsync(ctx, br.Txn, mergedLockSpans)

	// Switch the status on the batch response's transaction to COMMITTED. No
	// interceptor above this one in the stack should ever need to deal with
	// transaction proto in the STAGING state.
	br.Txn = cloneWithStatus(br.Txn, roachpb.COMMITTED)
	return br, nil
}

// validateEndTxnBatch runs sanity checks on a commit or rollback request.
func (tc *txnCommitter) validateEndTxnBatch(ba *kvpb.BatchRequest) error {
	// Check that we don't combine a limited DeleteRange with a commit. We cannot
	// attempt to run such a batch as a 1PC because, if it gets split and thus
	// doesn't run as a 1PC, resolving the intents will be very expensive.
	// Resolving the intents would require scanning the whole key span, which
	// might be much larger than the span of keys deleted before the limit was
	// hit. Requests that actually run as 1PC don't have this problem, as they
	// don't write and resolve intents. So, we make an exception and allow batches
	// that set Require1PC - those are guaranteed to either execute as 1PC or
	// fail. See also #37457.
	if ba.Header.MaxSpanRequestKeys == 0 {
		return nil
	}
	e, endTxn := ba.GetArg(kvpb.EndTxn)
	_, delRange := ba.GetArg(kvpb.DeleteRange)
	if delRange && endTxn && !e.(*kvpb.EndTxnRequest).Require1PC {
		return errors.Errorf("possible 1PC batch cannot contain EndTxn without setting Require1PC; see #37457")
	}
	// Check that the EndTxn request doesn't require a 1PC when we've previously
	// determined 1PC should be disabled.
	if e.(*kvpb.EndTxnRequest).Require1PC && tc.disable1PC {
		return errors.AssertionFailedf(
			"cannot require 1PC when for transactions that acquire replicated locks",
		)
	}
	return nil
}

// sendLockedWithElidedEndTxn sends the provided batch without its EndTxn
// request. However, if the EndTxn request is alone in the batch, nothing will
// be sent at all. Either way, the result of the EndTxn will be synthesized and
// returned in the batch response.
//
// The method is used for read-only transactions, which never need to write a
// transaction record.
func (tc *txnCommitter) sendLockedWithElidedEndTxn(
	ctx context.Context, ba *kvpb.BatchRequest, et *kvpb.EndTxnRequest,
) (br *kvpb.BatchResponse, pErr *kvpb.Error) {
	// Send the batch without its final request, which we know to be the EndTxn
	// request that we're eliding. If this would result in us sending an empty
	// batch, mock out a reply instead of sending anything.
	ba = ba.ShallowCopy()
	ba.Requests = ba.Requests[:len(ba.Requests)-1]
	if len(ba.Requests) > 0 {
		br, pErr = tc.wrapped.SendLocked(ctx, ba)
		if pErr != nil {
			return nil, pErr
		}
	} else {
		br = &kvpb.BatchResponse{}
		// NB: there's no need to clone the txn proto here because we already
		// call cloneWithStatus below.
		br.Txn = ba.Txn
	}

	// Check if the (read-only) txn was pushed above its deadline, if the
	// transaction is trying to commit.
	if et.Commit {
		deadline := et.Deadline
		if !deadline.IsEmpty() && deadline.LessEq(br.Txn.WriteTimestamp) {
			return nil, generateTxnDeadlineExceededErr(ba.Txn, deadline)
		}
	}

	// Update the response's transaction proto. This normally happens on the
	// server and is sent back in response headers, but in this case the EndTxn
	// request was optimized away. The caller may still inspect the transaction
	// struct, so we manually update it here to emulate a true transaction.
	status := roachpb.ABORTED
	if et.Commit {
		status = roachpb.COMMITTED
	}
	br.Txn = cloneWithStatus(br.Txn, status)

	// Synthesize and append an EndTxn response.
	br.Add(&kvpb.EndTxnResponse{})
	return br, nil
}

// canCommitInParallel determines whether the batch can issue its committing
// EndTxn in parallel with the rest of its requests and with any in-flight
// writes, which all should have corresponding QueryIntent requests in the
// batch.
func (tc *txnCommitter) canCommitInParallel(ba *kvpb.BatchRequest, et *kvpb.EndTxnRequest) bool {
	if !parallelCommitsEnabled.Get(&tc.st.SV) {
		return false
	}

	// We're trying to parallel commit, not parallel abort.
	if !et.Commit {
		return false
	}

	// We don't support a parallel prepare.
	if et.Prepare {
		return false
	}

	// If the transaction has a commit trigger, we don't allow it to commit in
	// parallel with writes. There's no fundamental reason for this restriction,
	// but for now it's not worth the complication.
	if et.InternalCommitTrigger != nil {
		return false
	}

	// Check whether every request in the batch is compatable with a parallel
	// commit. If any are incompatible then we cannot perform a parallel commit.
	// We ignore the last request in the slice because we know it is the EndTxn.
	for _, ru := range ba.Requests[:len(ba.Requests)-1] {
		req := ru.GetInner()
		switch {
		case kvpb.CanParallelCommit(req):
			//  The request can be part of a batch that is committed in parallel.

		case req.Method() == kvpb.QueryIntent:
			// QueryIntent requests are compatible with parallel commits. The
			// intents being queried are also attached to the EndTxn request's
			// InFlightWrites set and are visible to the status resolution
			// process for STAGING transactions. Populating InFlightWrites has
			// already been done by the txnPipeliner.

		default:
			// All other request types, notably Get, Scan and DeleteRange requests,
			// are incompatible with parallel commits because their outcome is not
			// taken into consideration by the status resolution process for STAGING
			// transactions.
			return false
		}
	}
	return true
}

// mergeIntoSpans merges all provided sequenced writes into the span slice. It
// then sorts the spans and merges an that overlap. The function does not mutate
// the provided span slice. Returns true iff all of the spans are distinct.
func mergeIntoSpans(s []roachpb.Span, ws []roachpb.SequencedWrite) ([]roachpb.Span, bool) {
	m := make([]roachpb.Span, len(s)+len(ws))
	copy(m, s)
	for i, w := range ws {
		m[len(s)+i] = roachpb.Span{Key: w.Key}
	}
	return roachpb.MergeSpans(&m)
}

// isTxnCommitImplicit determines whether the transaction has satisfied the
// implicit commit requirements. It is used to determine whether the transaction
// needs to retry its EndTxn based on the response to a parallel commit attempt.
func isTxnCommitImplicit(br *kvpb.BatchResponse) (bool, error) {
	if len(br.Responses) == 0 {
		return false, errors.AssertionFailedf("no responses in BatchResponse: %v", br)
	}
	lastResp := br.Responses[len(br.Responses)-1].GetInner()
	etResp, ok := lastResp.(*kvpb.EndTxnResponse)
	if !ok {
		return false, errors.AssertionFailedf("unexpected response in BatchResponse: %v", lastResp)
	}
	if etResp.StagingTimestamp.IsEmpty() {
		return false, errors.AssertionFailedf("empty StagingTimestamp in EndTxnResponse: %v", etResp)
	}
	// If the timestamp that the transaction record was staged at is less than
	// the timestamp of the transaction in the batch response then one of the
	// concurrent writes was pushed to a higher timestamp. This violates the
	// "implicit commit" condition and neither the transaction coordinator nor
	// any other concurrent actor will consider this transaction to be committed
	// as is.
	failed := etResp.StagingTimestamp.Less(br.Txn.WriteTimestamp)
	return !failed, nil
}

// retryTxnCommitAfterFailedParallelCommit retries the batch's EndTxn request
// after the batch has previously succeeded (with the response br), but failed
// to qualify for the implicit commit condition. This EndTxn request will not be
// in-flight concurrently with any other writes (they all succeeded), so it will
// move the transaction record directly to the COMMITTED state.
//
// If successful, the response for the re-issued EndTxn request is stitched back
// together with the rest of the BatchResponse and returned.
func (tc *txnCommitter) retryTxnCommitAfterFailedParallelCommit(
	ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse,
) (*kvpb.BatchResponse, *kvpb.Error) {
	log.Eventf(ctx, "parallel commit failed; retrying EndTxn request")
	tc.metrics.ParallelCommitAutoRetries.Inc(1)

	// Issue a batch containing only the EndTxn request.
	etIdx := len(ba.Requests) - 1
	baSuffix := ba.ShallowCopy()
	baSuffix.Requests = ba.Requests[etIdx:]
	baSuffix.Txn = cloneWithStatus(br.Txn, roachpb.PENDING)
	// Update the EndTxn request to move the in-flight writes currently attached
	// to the EndTxn request to the LockSpans and clear the in-flight write set;
	// the writes already succeeded and will not be in-flight concurrently with
	// the EndTxn request.
	{
		et := baSuffix.Requests[0].GetEndTxn().ShallowCopy().(*kvpb.EndTxnRequest)
		et.LockSpans, _ = mergeIntoSpans(et.LockSpans, et.InFlightWrites)
		et.InFlightWrites = nil
		baSuffix.Requests[0].MustSetInner(et)
	}
	brSuffix, pErr := tc.wrapped.SendLocked(ctx, baSuffix)
	if pErr != nil {
		// If the request determined that the transaction record had been staging,
		// but then fails to commit the transaction, downgrade the status back to
		// PENDING. We issued the request with a PENDING status, so we typically
		// don't expect this to happen. However, it can happen if the error is
		// constructed using the proto from the transaction record, as is the case
		// for TransactionRetryErrors returned from request evaluation.
		pErr = maybeRemoveStagingStatusInErr(pErr)
		return nil, pErr
	}

	// Combine the responses.
	br.Responses[etIdx] = kvpb.ResponseUnion{}
	if err := br.Combine(ctx, brSuffix, []int{etIdx}, ba); err != nil {
		return nil, kvpb.NewError(err)
	}
	if br.Txn == nil || !br.Txn.Status.IsFinalized() {
		return nil, kvpb.NewError(errors.AssertionFailedf(
			"txn status not finalized after successful retried EndTxn: %v", br.Txn))
	}
	return br, nil
}

// makeTxnCommitExplicitAsync launches an async task that attempts to move the
// transaction from implicitly committed (STAGING status with all intents
// written) to explicitly committed (COMMITTED status). It does so by sending a
// second EndTxnRequest, this time with no InFlightWrites attached.
func (tc *txnCommitter) makeTxnCommitExplicitAsync(
	ctx context.Context, txn *roachpb.Transaction, lockSpans []roachpb.Span,
) {
	// TODO(nvanbenschoten): consider adding tracing for this request.
	// TODO(nvanbenschoten): add a timeout to this request.
	// TODO(nvanbenschoten): consider making this semi-synchronous to
	//   backpressure client writes when these start to slow down. This
	//   would be similar to what we do for intent resolution.
	log.VEventf(ctx, 2, "making txn commit explicit: %s", txn)
	asyncCtx := context.Background()
	// If ctx is exempt from cost control, the explicit commit ctx should be
	// exempt as well.
	if multitenant.HasTenantCostControlExemption(ctx) {
		asyncCtx = multitenant.WithTenantCostControlExemption(asyncCtx)
	}
	if err := tc.stopper.RunAsyncTask(
		asyncCtx, "txnCommitter: making txn commit explicit", func(ctx context.Context) {
			tc.mu.Lock()
			defer tc.mu.Unlock()
			if err := makeTxnCommitExplicitLocked(ctx, tc.wrapped, txn, lockSpans); err != nil {
				log.Errorf(ctx, "making txn commit explicit failed for %s: %v", txn, err)
			}
		},
	); err != nil {
		log.VErrEventf(ctx, 1, "failed to make txn commit explicit: %v", err)
	}
}

func makeTxnCommitExplicitLocked(
	ctx context.Context, s lockedSender, txn *roachpb.Transaction, lockSpans []roachpb.Span,
) error {
	// Clone the txn to prevent data races.
	txn = txn.Clone()

	// Construct a new batch with just an EndTxn request.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn}
	et := kvpb.EndTxnRequest{Commit: true}
	et.Key = txn.Key
	et.LockSpans = lockSpans
	ba.Add(&et)

	_, pErr := s.SendLocked(ctx, ba)
	if pErr != nil {
		switch t := pErr.GetDetail().(type) {
		case *kvpb.TransactionStatusError:
			// Detect whether the error indicates that someone else beat
			// us to explicitly committing the transaction record.
			if t.Reason == kvpb.TransactionStatusError_REASON_TXN_COMMITTED {
				return nil
			}
		case *kvpb.TransactionRetryError:
			logFunc := log.Errorf
			if util.RaceEnabled {
				logFunc = log.Fatalf
			}
			logFunc(ctx, "unexpected retry error when making commit explicit for %s: %v", txn, t)
		}
		return pErr.GoError()
	}
	return nil
}

// maybeDisable1PC checks if the supplied batch would require us to disable 1PC
// when it's time to commit the transaction. A transaction that has acquired one
// or more replicated locks is not allowed to commit using 1PC; everyone else,
// if they're able to (determined on the server), is.
//
// Replicated locks must be held until and provide protection up till their
// transaction's commit timestamp[1]. We ensure this by bumping the timestamp
// cache to the transaction's commit timestamp for all locked keys when
// resolving locks. Let's consider external and local replicated locks
// separately:
//
// - External locks: 1PC transactions do not write a transaction record. This
// means if any of its external locks are resolved by another transaction
// they'll be resolved as if the transaction were aborted, thus not providing us
// protection until the transaction's commit timestamp.
// - Local locks: we have all the information to locally resolve replicated
// locks and bump the timestamp cache correctly if we're only dealing with local
// replicated locks. However, the mechanics of 1PC transactions prevent us from
// hitting it in the common case, where we're acquiring a replicated lock and
// writing to the same key. 1PC transactions work by stripping the batch of its
// EndTxnRequest and running it as a non-transactional batch. This means that
// without some elbow grease, 1PC is bound to fail when it discovers its own
// replicated lock. For now, we disable 1PC on the client for local locks as
// well -- this can be optimized in the future.
// TODO(arul): file an issue about improving things for local locks.
//
// [1] This distinction is currently moot for serializable transactions, as they
// refresh all their reads (locked and unlocked) before committing. Doing so
// bumps the timestamp cache. However, one can imagine a world where
// serializable transactions do not need to refresh keys they acquired
// replicated locks on. In such a world, we would be relying on lock resolution
// to bump the timestamp cache to the commit timestamp of the transaction.
func (tc *txnCommitter) maybeDisable1PC(ba *kvpb.BatchRequest) {
	if tc.disable1PC {
		return // already disabled; early return
	}
	for _, req := range ba.Requests {
		if readOnlyReq, ok := req.GetInner().(kvpb.LockingReadRequest); ok {
			_, dur := readOnlyReq.KeyLocking()
			if dur == lock.Replicated {
				tc.disable1PC = true
				return
			}
		}
	}
}

// setWrapped implements the txnInterceptor interface.
func (tc *txnCommitter) setWrapped(wrapped lockedSender) { tc.wrapped = wrapped }

// populateLeafInputState is part of the txnInterceptor interface.
func (*txnCommitter) populateLeafInputState(*roachpb.LeafTxnInputState) {}

// populateLeafFinalState is part of the txnInterceptor interface.
func (*txnCommitter) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (*txnCommitter) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked implements the txnInterceptor interface.
func (tc *txnCommitter) epochBumpedLocked() {}

// createSavepointLocked is part of the txnInterceptor interface.
func (*txnCommitter) createSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (*txnCommitter) rollbackToSavepointLocked(context.Context, savepoint) {}

// closeLocked implements the txnInterceptor interface.
func (tc *txnCommitter) closeLocked() {}

func cloneWithStatus(txn *roachpb.Transaction, s roachpb.TransactionStatus) *roachpb.Transaction {
	clone := txn.Clone()
	clone.Status = s
	return clone
}

func maybeRemoveStagingStatusInErr(pErr *kvpb.Error) *kvpb.Error {
	if txn := pErr.GetTxn(); txn != nil && txn.Status == roachpb.STAGING {
		pErr.SetTxn(cloneWithStatus(txn, roachpb.PENDING))
	}
	return pErr
}
