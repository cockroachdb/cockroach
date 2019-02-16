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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// txnCommitter is a txnInterceptor that concerns itself with committing and
// rolling back transactions. It intercepts EndTransaction requests and
// coordinates their execution. This is either accomplished by issuing them
// directly with proper addressing, eliding them when they are not needed, or
// (eventually) coordinating the execution of committing EndTransaction requests
// in parallel with the rest of their batch.
//
// NOTE: the primary justification for introducing this interceptor is that it
// will house all of the coordinator-side logic for parallel commits. Once
// complete, there will be a nice description of parallel commits here.
type txnCommitter struct {
	wrapped lockedSender
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

	// Pass the adjusted batch through the wrapped lockedSender.
	return tc.wrapped.SendLocked(ctx, ba)
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
