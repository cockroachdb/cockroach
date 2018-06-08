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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// txnBatchWrapper is a txnReqInterceptor that catches requests that produced
// OpRequiresTxnErrors and re-runs them in the context of a transaction.
//
// TODO(tschottdorf): this handling is somewhat awkward but unless we want to
// give this error back to the client, our options are limited. We'll have to
// run the whole thing for them, or any restart will still end up at the client
// which will not be prepared to be handed a Txn.
// TODO(andrei): if we lifted the retry loop for non-transaction requests that
// hit OpRequiresTxnErrors into client.DB then we wouldn't have to send them
// through a TxnCoordSender at all. This would allow us to get rid of this
// interceptor.
type txnBatchWrapper struct {
	tcf *TxnCoordSenderFactory
}

var _ txnReqInterceptor = &txnBatchWrapper{}

// beforeSendLocked implements the txnReqInterceptor interface.
func (*txnBatchWrapper) beforeSendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (roachpb.BatchRequest, *roachpb.Error) {
	// No-op.
	return ba, nil
}

// maybeRetrySend implements the txnReqInterceptor interface.
func (bw *txnBatchWrapper) maybeRetrySend(
	ctx context.Context, ba *roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) (*roachpb.BatchResponse, *roachpb.Error) {
	if _, ok := pErr.GetDetail().(*roachpb.OpRequiresTxnError); !ok {
		return br, pErr
	}

	// Run a one-off transaction with that single command.
	log.VEventf(ctx, 2, "%s: auto-wrapping in txn and re-executing", ba)
	// TODO(bdarnell): need to be able to pass other parts of DBContext
	// through here.
	dbCtx := client.DefaultDBContext()
	dbCtx.UserPriority = ba.UserPriority
	dbCtx.Stopper = bw.tcf.stopper
	tmpDB := client.NewDBWithContext(bw.tcf.AmbientContext, bw.tcf, bw.tcf.clock, dbCtx)
	err := tmpDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetDebugName("auto-wrap")
		b := txn.NewBatch()
		b.Header = ba.Header
		for _, arg := range ba.Requests {
			req := arg.GetInner().ShallowCopy()
			b.AddRawRequest(req)
		}
		err := txn.CommitInBatch(ctx, b)
		br = b.RawResponse()
		return err
	})
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	br.Txn = nil // hide the evidence
	return br, nil
}

// afterSendLocked implements the txnReqInterceptor interface.
func (*txnBatchWrapper) afterSendLocked(
	ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// No-op.
	return br, pErr
}

// populateMetaLocked implements the txnReqInterceptor interface.
func (*txnBatchWrapper) populateMetaLocked(meta *roachpb.TxnCoordMeta) {}

// augmentMetaLocked implements the txnReqInterceptor interface.
func (*txnBatchWrapper) augmentMetaLocked(meta roachpb.TxnCoordMeta) {}

// epochRetryLocked implements the txnReqInterceptor interface.
func (*txnBatchWrapper) epochRetryLocked() {}

// closeLocked implements the txnReqInterceptor interface.
func (*txnBatchWrapper) closeLocked() {}
