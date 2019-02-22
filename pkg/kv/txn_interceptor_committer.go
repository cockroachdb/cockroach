// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package kv

import (
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var parallelCommitsEnabled = settings.RegisterBoolSetting(
	"kv.transaction.parallel_commits",
	"if enabled, transactional commits will be parallelized with transactional writes",
	true,
)

// txnCommitter is a txnInterceptor that concerns itself with committing and
// rolling back transactions. It intercepts EndTransaction requests and
// coordinates their execution. This is accomplished either by issuing them
// directly with proper addressing, eliding them when they are not needed, or
// coordinating the execution by issuing EndTransaction requests in parallel
// with the rest of their batch.
//
// The third operation listed, which we define as a "parallel commit", is the
// most interesting. Marking a transaction record as committed in parallel with
// writing the rest of a transaction's intents is a clear win in terms of
// latency - in theory it removes the cost of an entire consensus round-trip
// from a transaction. However, doing so safely comes with extra complication.
// It requires an extension to the transaction model, additional client-side
// logic, buy-in from concurrency control, and specialized support from a
// transaction recovery mechanism. txnCommitter is responsible for this parallel
// commit-specific client-side logic.
//
// Parallel commits works by defining a committed transaction as a transaction
// that meets one of the two following commit conditions:
// 1. a transaction is *explicitly committed* if it has a transaction record with
//    a COMMITTED status
// 2. a transaction is *implicitly committed* if it has a transaction record with
//    a STAGING status and intents written for all writes declared as "in-flight"
//    on the transaction record
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
// commit condition. If all writes in the batch succeeded then the implicit
// commit condition is satisfied. The interceptor then launches an async task to
// make the commit explicit by moving the transaction record's status from
// STAGING to COMMITTED. If all writes did not succeed, either because they did
// not write and intent or because they wrote an intent at a higher timestamp,
// then the implicit commit condition is not satisfied and the transaction is
// still PENDING. Either way, the interceptor abstracts away the details of this
// from all interceptors above it in the interceptor stack.
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
	// If the batch does not include an EndTransaction request, pass it through.
	rArgs, hasET := ba.GetArg(roachpb.EndTransaction)
	if !hasET {
		return tc.wrapped.SendLocked(ctx, ba)
	}
	et := rArgs.(*roachpb.EndTransactionRequest)

	// Determine whether we can elide the EndTransaction entirely. We can do
	// so if the transaction is read-only, which we determine based on whether
	// the EndTransaction request contains any intents.
	if len(et.IntentSpans) == 0 {
		return tc.sendLockedWithElidedEndTransaction(ctx, ba, et)
	}

	// Assign the transaction's key to the Request's header if it isn't already
	// set. This is the only place where EndTransactionRequest.Key is assigned,
	// but we could be dealing with a re-issued batch after a refresh. Remember,
	// the committer is below the span refresh on the interceptor stack.
	if et.Key == nil {
		et.Key = ba.Txn.Key
	}

	// If the EndTransaction request is a rollback, pass it through.
	if !et.Commit {
		return tc.wrapped.SendLocked(ctx, ba)
	}

	// Determine whether the commit can be run in parallel with the rest of the
	// writes in the batch. If so, attach the writes that will be in-flight
	// concurrently with the EndTransaction request as the EndTransaction's
	// promised intents.
	if et.InFlightWrites == nil {
		if ok, inFlightWrites := tc.canCommitInParallelWithWrites(ba, et); ok {
			et.InFlightWrites = inFlightWrites
		}
	}

	// Send the adjusted batch through the wrapped lockedSender. Unlocks while
	// sending then re-locks.
	br, pErr := tc.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		// If the batch resulted in an error but the EndTransaction request
		// succeeded, staging the transaction record in the process, downgrade
		// the status back to PENDING.
		if txn := pErr.GetTxn(); txn != nil && txn.Status == roachpb.STAGING {
			pErr.SetTxn(cloneWithStatus(txn, roachpb.PENDING))
		}
		// Same deal with MixedSuccessErrors.
		// TODO(nvanbenschoten): Merge code? Wait for MixedSuccessError to go away?
		if aPSErr, ok := pErr.GetDetail().(*roachpb.MixedSuccessError); ok {
			if txn := aPSErr.Wrapped.GetTxn(); txn != nil && txn.Status == roachpb.STAGING {
				aPSErr.Wrapped.SetTxn(cloneWithStatus(txn, roachpb.PENDING))
			}
		}
		return nil, pErr
	}

	// Determine next steps based on the status of the transaction.
	switch br.Txn.Status {
	case roachpb.STAGING:
		// Continue with STAGING-specific validation and cleanup.
	case roachpb.COMMITTED:
		// The transaction is explicitly committed. This is possible if all
		// promised writes were sent to the same range as the EndTransaction
		// request, in a single batch. In this case, a range can determine that
		// all promised writes will succeed with the EndTransaction and can
		// decide to skip the STAGING state.
		//
		// This is also possible if we never attached any promised writes to the
		// EndTransaction request, either because canCommitInParallelWithWrites
		// returned false or because there were no unproven outstanding writes
		// (see txnPipeliner) and there were no writes in the batch request.
		return br, nil
	default:
		return nil, roachpb.NewErrorf("unexpected response status without error: %v", br.Txn)
	}

	// Determine whether the transaction needs to either retry or refresh.
	if ok, reason := tc.needTxnRetry(&ba, br); ok {
		err := roachpb.NewTransactionRetryError(reason)
		txn := cloneWithStatus(br.Txn, roachpb.PENDING)
		return nil, roachpb.NewErrorWithTxn(err, txn)
	}

	// If the transaction doesn't need to retry then it is implicitly committed!
	// We're the only ones who know that though - other concurrent transactions
	// will need to go through the full status resolution process to make a
	// determination about the status of our STAGING transaction. To avoid this,
	// we transition to an explicitly committed transaction as soon as possible.
	// This also has the side-effect of kicking off intent resolution.
	tc.makeTxnCommitExplicitAsync(ctx, br.Txn)

	// Switch the status on the batch response's transaction to COMMITTED. No
	// interceptor above this one in the stack should ever need to deal with
	// transaction proto in the STAGING state.
	br.Txn = cloneWithStatus(br.Txn, roachpb.COMMITTED)
	return br, nil
}

// sendLockedWithElidedEndTransaction sends the provided batch without its
// EndTransaction request. However, if the EndTransaction request is alone in
// the batch, nothing will be sent at all. Either way, the result of the
// EndTransaction will be synthesized and returned in the batch response.
//
// The method is used for read-only transactions, which never need to write a
// transaction record.
func (tc *txnCommitter) sendLockedWithElidedEndTransaction(
	ctx context.Context, ba roachpb.BatchRequest, et *roachpb.EndTransactionRequest,
) (br *roachpb.BatchResponse, pErr *roachpb.Error) {
	// Send the batch without its final request, which we know to be the
	// EndTransaction request that we're eliding. If this would result in us
	// sending an empty batch, mock out a reply instead of sending anything.
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

	// Check if the (read-only) txn was pushed above its deadline.
	if et.Deadline != nil && et.Deadline.Less(br.Txn.Timestamp) {
		return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(
			"deadline exceeded before transaction finalization"), br.Txn)
	}

	// Update the response's transaction proto. This normally happens on the
	// server and is sent back in response headers, but in this case the
	// EndTransaction request was optimized away. The caller may still inspect
	// the transaction struct, so we manually update it here to emulate a true
	// transaction.
	status := roachpb.ABORTED
	if et.Commit {
		status = roachpb.COMMITTED
	}
	br.Txn = cloneWithStatus(br.Txn, status)

	// Synthesize and append an EndTransaction response.
	resp := &roachpb.EndTransactionResponse{}
	resp.Txn = br.Txn
	br.Add(resp)
	return br, nil
}

// canCommitInParallelWithWrites determines whether the batch can issue its
// committing EndTransaction in parallel with other writes. If so, it returns
// the set of writes that may still be in-flight when the EndTransaction is
// evaluated. These writes should be declared are requirements for the
// transaction to move from a STAGING status to a COMMITTED status.
func (tc *txnCommitter) canCommitInParallelWithWrites(
	ba roachpb.BatchRequest, et *roachpb.EndTransactionRequest,
) (bool, map[int32]int32) {
	if !parallelCommitsEnabled.Get(&tc.st.SV) {
		return false, nil
	}

	// If the transaction has a commit trigger, we don't allow it to commit in
	// parallel with writes. There's no fundamental reason for this restriction,
	// but for now it's not worth the complication.
	if et.InternalCommitTrigger != nil {
		return false, nil
	}

	// See EndTransactionRequest.InFlightWrites.
	var inFlightWrites map[int32]int32
	for _, ru := range ba.Requests {
		req := ru.GetInner()
		h := req.Header()

		var k roachpb.Key
		var seq int32
		switch {
		case roachpb.IsTransactionWrite(req):
			// Similarly to how we can't pipelining ranged writes, we also can't
			// commit in parallel with them. The reason for this is that the status
			// resolution process for STAGING transactions wouldn't know where to
			// look for the intents.
			if roachpb.IsRange(req) {
				return false, nil
			}
			k = h.Key
			seq = h.Sequence
		case req.Method() == roachpb.QueryIntent:
			// A QueryIntent isn't a write, but it indicates that there is an
			// in-flight write that needs to complete as part of the transaction.
			qi := req.(*roachpb.QueryIntentRequest)
			k = h.Key
			seq = qi.Txn.Sequence
		default:
			continue
		}

		idx := intentSliceIndexForKey(et.IntentSpans, k)
		if idx == -1 {
			// If we can't find the corresponding intent span for a key then its
			// likely that the intent was condensed into a larger intent span by the
			// txnIntentCollector. In that case, we return false and give up on
			// committing in parallel with the writes.
			//
			// This sounds unfortunate, but it actually has a desirable property. It
			// allows us to piggyback the decision of whether to perform a parallel
			// commit on the kv.transaction.max_intents_bytes setting. This is nice
			// both because it bounds the size of InFlightWrites to a size proportional
			// to the bound on the size of IntentSpans and because it prevents the cost
			// of recovery from an abandoned STAGING transaction from growing without
			// bound.
			//
			// TODO(nvanbenschoten): Should we just create a separate setting instead?
			return false, nil
		}
		if inFlightWrites == nil {
			inFlightWrites = make(map[int32]int32)
		}
		inFlightWrites[seq] = int32(idx)
	}
	return true, inFlightWrites
}

// intentSliceIndexForKey determines the index of the key in the provided slice
// of spans. If the key does not exist as a single-key span in the slice, the
// function returns -1.
func intentSliceIndexForKey(s []roachpb.Span, k roachpb.Key) int {
	i := sort.Search(len(s), func(i int) bool { return k.Compare(s[i].Key) <= 0 })
	if i == len(s) || !k.Equal(s[i].Key) || len(s[i].EndKey) > 0 {
		return -1
	}
	return i
}

// needTxnRetry determines whether the transaction needs to refresh (see
// txnSpanRefresher) or retry based on the batch response of a parallel
// commit attempt.
//
// TODO(nvanbenschoten): explain this better.
func (*txnCommitter) needTxnRetry(
	ba *roachpb.BatchRequest, br *roachpb.BatchResponse,
) (bool, roachpb.TransactionRetryReason) {
	// TODO(nvanbenschoten): Explain cases.
	if br.Txn.WriteTooOld {
		return true, roachpb.RETRY_WRITE_TOO_OLD
	} else if ba.Txn.Timestamp.Less(br.Txn.Timestamp) {
		return true, roachpb.RETRY_SERIALIZABLE
	}
	return false, 0
}

// makeTxnCommitExplicitAsync launches an async task that attempts to move the
// transaction from implicitly committed (STAGING status with all intents
// written) to explicitly committed (COMMITTED status). It does so by sending a
// second EndTransactionRequest, this time with no InFlightWrites attached.
func (tc *txnCommitter) makeTxnCommitExplicitAsync(ctx context.Context, txn *roachpb.Transaction) {
	log.VEventf(ctx, 2, "making txn commit explicit: %s", txn)
	// ctx, sp := tracing.ForkCtxSpan(ctx, "making txn commit explicit")
	if err := tc.stopper.RunAsyncTask(
		context.Background(), "txnCommitter: making txn commit explicit", func(ctx context.Context) {
			// defer tracing.FinishSpan(sp)
			tc.mu.Lock()
			defer tc.mu.Unlock()
			if err := makeTxnCommitExplicitLocked(ctx, tc.wrapped, txn); err != nil {
				log.VErrEventf(ctx, 1, "making txn commit explicit failed for %s: %v", txn, err)
			}
		},
	); err != nil {
		log.Warning(ctx, err)
		// tracing.FinishSpan(sp)
	}
}

func makeTxnCommitExplicitLocked(
	ctx context.Context, s lockedSender, txn *roachpb.Transaction,
) error {
	// Construct a new batch with just an EndTransaction request. We don't
	// need to include any intents because the transaction record already
	// contains them.
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: txn}
	et := roachpb.EndTransactionRequest{Commit: true}
	et.Key = txn.Key
	ba.Add(&et)

	// Retry until the
	const maxAttempts = 5
	return retry.WithMaxAttempts(ctx, base.DefaultRetryOptions(), maxAttempts, func() error {
		_, pErr := s.SendLocked(ctx, ba)
		if pErr == nil {
			return nil
		}
		// TODO(nvanbenschoten): Detect errors that indicate that someone else
		// has cleaned us up.
		return pErr.GoError()
	})
}

// setWrapped implements the txnInterceptor interface.
func (tc *txnCommitter) setWrapped(wrapped lockedSender) { tc.wrapped = wrapped }

// populateMetaLocked implements the txnReqInterceptor interface.
func (tc *txnCommitter) populateMetaLocked(meta *roachpb.TxnCoordMeta) {}

// augmentMetaLocked implements the txnReqInterceptor interface.
func (tc *txnCommitter) augmentMetaLocked(meta roachpb.TxnCoordMeta) {}

// epochBumpedLocked implements the txnReqInterceptor interface.
func (tc *txnCommitter) epochBumpedLocked() {}

// closeLocked implements the txnReqInterceptor interface.
func (tc *txnCommitter) closeLocked() {}

func cloneWithStatus(txn *roachpb.Transaction, s roachpb.TransactionStatus) *roachpb.Transaction {
	clone := txn.Clone()
	txn = &clone
	txn.Status = s
	return txn
}
