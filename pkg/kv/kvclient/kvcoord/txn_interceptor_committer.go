// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

var parallelCommitsEnabled = settings.RegisterBoolSetting(
	"kv.transaction.parallel_commits_enabled",
	"if enabled, transactional commits will be parallelized with transactional writes",
	true,
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
// 1. a transaction is *explicitly committed* if it has a transaction record with
//    a COMMITTED status
// 2. a transaction is *implicitly committed* if it has a transaction record with
//    a STAGING status and intents written for all writes declared as "in-flight"
//    on the transaction record at equal or lower timestamps than the transaction
//    record's commit timestamp
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
// - intent writes: these requests may fail to write an intent due to a logical
//     error like a ConditionFailedError. They also could have succeeded at writing
//     an intent but failed to write it at the desired timestamp because they ran
//     into the timestamp cache or another committed value. In the first case, the
//     txnCommitter will receive an error. In the second, it will generate one in
//     needTxnRetryAfterStaging.
// - query intents: these requests may fail because they discover that one of the
//     previously issued writes has failed; either because it never left an intent
//     or because it left one at too high of a timestamp. In this case, the request
//     will return an error because the requests all have the ErrorIfMissing option
//     set. It will also prevent the write from ever succeeding in the future, which
//     ensures that the transaction will never suddenly become implicitly committed
//     at a later point due to the write eventually succeeding (e.g. after a replay).
// - end txn: this request may fail with a TransactionRetryError for any number of
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
	st      *cluster.Settings
	stopper *stop.Stopper
	wrapped lockedSender
	mu      sync.Locker
}

// SendLocked implements the lockedSender interface.
func (tc *txnCommitter) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// If the batch does not include an EndTxn request, pass it through.
	rArgs, hasET := ba.GetArg(roachpb.EndTxn)
	if !hasET {
		return tc.wrapped.SendLocked(ctx, ba)
	}
	et := rArgs.(*roachpb.EndTxnRequest)

	if err := tc.validateEndTxnBatch(ba); err != nil {
		return nil, roachpb.NewError(err)
	}

	// Determine whether we can elide the EndTxn entirely. We can do so if the
	// transaction is read-only, which we determine based on whether the EndTxn
	// request contains any writes.
	if len(et.LockSpans) == 0 && len(et.InFlightWrites) == 0 {
		return tc.sendLockedWithElidedEndTxn(ctx, ba, et)
	}

	// Assign the transaction's key to the Request's header if it isn't already
	// set. This is the only place where EndTxnRequest.Key is assigned, but we
	// could be dealing with a re-issued batch after a refresh. Remember, the
	// committer is below the span refresh on the interceptor stack.
	var etAttempt endTxnAttempt
	if et.Key == nil {
		et.Key = ba.Txn.Key
		etAttempt = endTxnFirstAttempt
	} else {
		// If this is a retry, we'll disable parallel commit. Since the previous
		// attempt might have partially succeeded (i.e. the batch might have been
		// split into sub-batches and some of them might have evaluated
		// successfully), there might be intents laying around. If we'd perform a
		// parallel commit, and the batch gets split again, and the STAGING txn
		// record were written before we evaluate some of the other sub-batche. We
		// could technically enter the "implicitly committed" state before all the
		// sub-batches are evaluated and this is problematic: there's a race between
		// evaluating those requests and other pushers coming along and
		// transitioning the txn to explicitly committed (and cleaning up all the
		// intents), and the evaluations of the outstanding sub-batches. If the
		// randos win, then the re-evaluations will fail because we don't have
		// idempotency of evaluations across a txn commit (for example, the
		// re-evaluations might notice that their transaction is already committed
		// and get confused).
		etAttempt = endTxnRetry
		if len(et.InFlightWrites) > 0 {
			// Make a copy of the EndTxn, since we're going to change it below to
			// disable the parallel commit.
			etCpy := *et
			ba.Requests[len(ba.Requests)-1].MustSetInner(&etCpy)
			et = &etCpy
		}
	}

	// Determine whether the commit request can be run in parallel with the rest
	// of the requests in the batch. If not, move the in-flight writes currently
	// attached to the EndTxn request to the LockSpans and clear the in-flight
	// write set; no writes will be in-flight concurrently with the EndTxn
	// request.
	if len(et.InFlightWrites) > 0 && !tc.canCommitInParallel(ctx, ba, et, etAttempt) {
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
		if txn := pErr.GetTxn(); txn != nil && txn.Status == roachpb.STAGING {
			pErr.SetTxn(cloneWithStatus(txn, roachpb.PENDING))
		}
		return nil, pErr
	}

	// Determine next steps based on the status of the transaction.
	switch br.Txn.Status {
	case roachpb.STAGING:
		// Continue with STAGING-specific validation and cleanup.
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
		return nil, roachpb.NewErrorf("unexpected response status without error: %v", br.Txn)
	}

	// Determine whether the transaction needs to either retry or refresh. When
	// the EndTxn request evaluated while STAGING the transaction record, it
	// performed this check. However, the transaction proto may have changed due
	// to writes evaluated concurrently with the EndTxn even if none of those
	// writes returned an error. Remember that the transaction proto we see here
	// could be a combination of protos from responses, all merged by
	// DistSender.
	if pErr := needTxnRetryAfterStaging(br); pErr != nil {
		log.VEventf(ctx, 2, "parallel commit failed since some writes were pushed. "+
			"Synthesized err: %s", pErr)
		return nil, pErr
	}

	// If the transaction doesn't need to retry then it is implicitly committed!
	// We're the only ones who know that though -- other concurrent transactions
	// will need to go through the full status resolution process to make a
	// determination about the status of our STAGING transaction. To avoid this,
	// we transition to an explicitly committed transaction as soon as possible.
	// This also has the side-effect of kicking off intent resolution.
	mergedLockSpans, _ := mergeIntoSpans(et.LockSpans, et.InFlightWrites)
	tc.makeTxnCommitExplicitAsync(ctx, br.Txn, mergedLockSpans, ba.CanForwardReadTimestamp)

	// Switch the status on the batch response's transaction to COMMITTED. No
	// interceptor above this one in the stack should ever need to deal with
	// transaction proto in the STAGING state.
	br.Txn = cloneWithStatus(br.Txn, roachpb.COMMITTED)
	return br, nil
}

// validateEndTxnBatch runs sanity checks on a commit or rollback request.
func (tc *txnCommitter) validateEndTxnBatch(ba roachpb.BatchRequest) error {
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
	e, endTxn := ba.GetArg(roachpb.EndTxn)
	_, delRange := ba.GetArg(roachpb.DeleteRange)
	if delRange && endTxn && !e.(*roachpb.EndTxnRequest).Require1PC {
		return errors.Errorf("possible 1PC batch cannot contain EndTxn without setting Require1PC; see #37457")
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
	ctx context.Context, ba roachpb.BatchRequest, et *roachpb.EndTxnRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// Send the batch without its final request, which we know to be the EndTxn
	// request that we're eliding. If this would result in us sending an empty
	// batch, mock out a reply instead of sending anything.
	ba.Requests = ba.Requests[:len(ba.Requests)-1]
	if len(ba.Requests) > 0 {
		br, pErr = tc.wrapped.SendLocked(ctx, ba)
		if pErr != nil {
			return nil, pErr
		}
	} else {
		br = &roachpb.BatchResponse{}
		// NB: there's no need to clone the txn proto here because we already
		// call cloneWithStatus below.
		br.Txn = ba.Txn
	}

	// Check if the (read-only) txn was pushed above its deadline, if the
	// transaction is trying to commit.
	if et.Commit {
		deadline := et.Deadline
		if deadline != nil && deadline.LessEq(br.Txn.WriteTimestamp) {
			return nil, generateTxnDeadlineExceededErr(ba.Txn, *deadline)
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
	br.Add(&roachpb.EndTxnResponse{})
	return br, nil
}

// endTxnAttempt specifies whether it's the first time that we're attempting to
// evaluate an EndTxn request or whether it's a retry (i.e. after a successful
// refresh). There are some precautions we need to take when sending out
// retries.
type endTxnAttempt int

const (
	endTxnFirstAttempt endTxnAttempt = iota
	endTxnRetry
)

// canCommitInParallel determines whether the batch can issue its committing
// EndTxn in parallel with the rest of its requests and with any in-flight
// writes, which all should have corresponding QueryIntent requests in the
// batch.
func (tc *txnCommitter) canCommitInParallel(
	ctx context.Context, ba roachpb.BatchRequest, et *roachpb.EndTxnRequest, etAttempt endTxnAttempt,
) bool {
	if !parallelCommitsEnabled.Get(&tc.st.SV) {
		return false
	}

	if etAttempt == endTxnRetry {
		log.VEventf(ctx, 2, "retrying batch not eligible for parallel commit")
		return false
	}

	// We're trying to parallel commit, not parallel abort.
	if !et.Commit {
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
		case roachpb.IsIntentWrite(req):
			if roachpb.IsRange(req) {
				// Similar to how we can't pipeline ranged writes, we also can't
				// commit in parallel with them. The reason for this is that the
				// status resolution process for STAGING transactions wouldn't
				// know where to look for the corresponding intents.
				return false
			}
			// All other point writes are included in the EndTxn request's
			// InFlightWrites set and are visible to the status resolution
			// process for STAGING transactions. Populating InFlightWrites
			// has already been done by the txnPipeliner.

		case req.Method() == roachpb.QueryIntent:
			// QueryIntent requests are compatable with parallel commits. The
			// intents being queried are also attached to the EndTxn request's
			// InFlightWrites set and are visible to the status resolution
			// process for STAGING transactions. Populating InFlightWrites has
			// already been done by the txnPipeliner.

		default:
			// All other request types, notably Get and Scan requests, are
			// incompatible with parallel commits because their outcome is
			// not taken into consideration by the status resolution process
			// for STAGING transactions.
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

// needTxnRetryAfterStaging determines whether the transaction needs to refresh
// (see txnSpanRefresher) or retry based on the batch response of a parallel
// commit attempt.
func needTxnRetryAfterStaging(br *roachpb.BatchResponse) *roachpb.Error {
	if len(br.Responses) == 0 {
		return roachpb.NewErrorf("no responses in BatchResponse: %v", br)
	}
	lastResp := br.Responses[len(br.Responses)-1].GetInner()
	etResp, ok := lastResp.(*roachpb.EndTxnResponse)
	if !ok {
		return roachpb.NewErrorf("unexpected response in BatchResponse: %v", lastResp)
	}
	if etResp.StagingTimestamp.IsEmpty() {
		return roachpb.NewErrorf("empty StagingTimestamp in EndTxnResponse: %v", etResp)
	}
	if etResp.StagingTimestamp.Less(br.Txn.WriteTimestamp) {
		// If the timestamp that the transaction record was staged at
		// is less than the timestamp of the transaction in the batch
		// response then one of the concurrent writes was pushed to
		// a higher timestamp. This violates the "implicit commit"
		// condition and neither the transaction coordinator nor any
		// other concurrent actor will consider this transaction to
		// be committed as is.
		// Note that we leave the transaction record that we wrote in the STAGING
		// state, which is not ideal. But as long as we continue heartbeating the
		// txn record, it being PENDING or STAGING does not make a difference.
		reason := roachpb.RETRY_SERIALIZABLE
		if br.Txn.WriteTooOld {
			reason = roachpb.RETRY_WRITE_TOO_OLD
		}
		err := roachpb.NewTransactionRetryError(
			reason, "serializability failure concurrent with STAGING")
		txn := cloneWithStatus(br.Txn, roachpb.PENDING)
		return roachpb.NewErrorWithTxn(err, txn)
	}
	return nil
}

// makeTxnCommitExplicitAsync launches an async task that attempts to move the
// transaction from implicitly committed (STAGING status with all intents
// written) to explicitly committed (COMMITTED status). It does so by sending a
// second EndTxnRequest, this time with no InFlightWrites attached.
func (tc *txnCommitter) makeTxnCommitExplicitAsync(
	ctx context.Context, txn *roachpb.Transaction, lockSpans []roachpb.Span, canFwdRTS bool,
) {
	// TODO(nvanbenschoten): consider adding tracing for this request.
	// TODO(nvanbenschoten): add a timeout to this request.
	// TODO(nvanbenschoten): consider making this semi-synchronous to
	//   backpressure client writes when these start to slow down. This
	//   would be similar to what we do for intent resolution.
	log.VEventf(ctx, 2, "making txn commit explicit: %s", txn)
	if err := tc.stopper.RunAsyncTask(
		context.Background(), "txnCommitter: making txn commit explicit", func(ctx context.Context) {
			tc.mu.Lock()
			defer tc.mu.Unlock()
			if err := makeTxnCommitExplicitLocked(ctx, tc.wrapped, txn, lockSpans, canFwdRTS); err != nil {
				log.Errorf(ctx, "making txn commit explicit failed for %s: %v", txn, err)
			}
		},
	); err != nil {
		log.VErrEventf(ctx, 1, "failed to make txn commit explicit: %v", err)
	}
}

func makeTxnCommitExplicitLocked(
	ctx context.Context,
	s lockedSender,
	txn *roachpb.Transaction,
	lockSpans []roachpb.Span,
	canFwdRTS bool,
) error {
	// Clone the txn to prevent data races.
	txn = txn.Clone()

	// Construct a new batch with just an EndTxn request.
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: txn, CanForwardReadTimestamp: canFwdRTS}
	et := roachpb.EndTxnRequest{Commit: true}
	et.Key = txn.Key
	et.LockSpans = lockSpans
	ba.Add(&et)

	_, pErr := s.SendLocked(ctx, ba)
	if pErr != nil {
		switch t := pErr.GetDetail().(type) {
		case *roachpb.TransactionStatusError:
			// Detect whether the error indicates that someone else beat
			// us to explicitly committing the transaction record.
			if t.Reason == roachpb.TransactionStatusError_REASON_TXN_COMMITTED {
				return nil
			}
		case *roachpb.TransactionRetryError:
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

// setWrapped implements the txnInterceptor interface.
func (tc *txnCommitter) setWrapped(wrapped lockedSender) { tc.wrapped = wrapped }

// populateLeafInputState is part of the txnInterceptor interface.
func (*txnCommitter) populateLeafInputState(*roachpb.LeafTxnInputState) {}

// populateLeafFinalState is part of the txnInterceptor interface.
func (*txnCommitter) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (*txnCommitter) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) {}

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
