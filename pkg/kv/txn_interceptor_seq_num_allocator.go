// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

// txnSeqNumAllocator is a txnInterceptor in charge of allocating sequence
// numbers to all the individual requests in batches.
//
// Sequence numbers serve a few roles in the transaction model:
//
// 1. they are used to enforce an ordering between read and write operations in a
//    single transaction that go to the same key. Each read request that travels
//    through the interceptor is assigned the sequence number of the most recent
//    write. Each write request that travels through the interceptor is assigned
//    a sequence number larger than any previously allocated.
//
//    This is true even for leaf transaction coordinators. In their case, they are
//    provided the sequence number of the most recent write during construction.
//    Because they only perform read operations and never issue writes, they assign
//    each read this sequence number without ever incrementing their own counter.
//    In this way, sequence numbers are maintained correctly across a distributed
//    tree of transaction coordinators.
//
// 2. they are used to uniquely identify write operations. Because every write
//    request is given a new sequence number, the tuple (txn_id, txn_epoch, seq)
//    uniquely identifies a write operation across an entire cluster. This property
//    is exploited when determining the status of an individual write by looking
//    for its intent. We perform such an operation using the QueryIntent request
//    type when pipelining transactional writes. We will do something similar
//    during the recovery stage of implicitly committed transactions.
//
// 3. they are used to determine whether a batch contains the entire write set
//    for a transaction. See BatchRequest.IsCompleteTransaction.
//
// 4. they are used to provide idempotency for replays and re-issues. The MVCC
//    layer is sequence number-aware and ensures that reads at a given sequence
//    number ignore writes in the same transaction at larger sequence numbers.
//    Likewise, writes at a sequence number become no-ops if an intent with the
//    same sequence is already present. If an intent with the same sequence is not
//    already present but an intent with a larger sequence number is, an error is
//    returned. Likewise, if an intent with the same sequence is present but its
//    value is different than what we recompute, an error is returned.
//
type txnSeqNumAllocator struct {
	wrapped lockedSender
	seqGen  enginepb.TxnSeq
}

// SendLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	for _, ru := range ba.Requests {
		// Only increment the sequence number generator for requests that
		// will leave intents or requests that will commit the transaction.
		// This enables ba.IsCompleteTransaction to work properly.
		req := ru.GetInner()
		if roachpb.IsTransactionWrite(req) || req.Method() == roachpb.EndTxn {
			s.seqGen++
		}

		oldHeader := req.Header()
		oldHeader.Sequence = s.seqGen
		ru.GetInner().SetHeader(oldHeader)
	}

	return s.wrapped.SendLocked(ctx, ba)
}

// setWrapped is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) setWrapped(wrapped lockedSender) { s.wrapped = wrapped }

// populateLeafInputState is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) populateLeafInputState(tis *roachpb.LeafTxnInputState) {
	tis.Txn.Sequence = s.seqGen
}

// populateLeafFinalState is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) populateLeafFinalState(tfs *roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) importLeafFinalState(tfs *roachpb.LeafTxnFinalState) {}

// epochBumpedLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) epochBumpedLocked() {
	s.seqGen = 0
}

// closeLocked is part of the txnInterceptor interface.
func (*txnSeqNumAllocator) closeLocked() {}
