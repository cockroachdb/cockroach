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

// txnSeqNumAllocator is a txnInterceptor in charge of allocating sequence
// numbers to all the individual requests in batches.
//
// Sequence numbers serve a few roles in the transaction model:
// 1. they are used to enforce an ordering between read and write operations in a
//    single transaction that go to the same key. Each read request that travels
//    through the interceptor is assigned the current largest sequence number. Each
//    write request that travels through the interceptor is assigned a sequence
//    number larger than any previously allocated.
// 2. they are used to uniquely identify write operation. Because every write
//    request is given a new sequence number, the tuple (txn_id, txn_epoch, seq)
//    uniquely identifies a write operation across an entire cluster.
// 3. they are used to determine whether a batch contains the entire write set
//    for a transaction. See BatchRequest.IsCompleteTransaction.
// 4. they are used to provide idempotency for replays and re-issues. The MVCC
//    layer is sequence number-aware and ensures that reads at a given sequence
//    number ignore writes in the same transaction at larger sequence numbers.
//    Likewise, writes at a sequence number become no-ops if the result of the
//    write is already present. If the result of the write is not already present
//    but the result of a write at a larger sequence number is, an error is
//    returned.
//
type txnSeqNumAllocator struct {
	wrapped lockedSender
	seqGen  int32

	// commandCount indicates how many requests have been sent through
	// this transaction. Reset on retryable txn errors.
	// TODO(andrei): let's get rid of this. It should be maintained
	// in the SQL level.
	// TODO(nvanbenschoten): convince Andrei to address this TODO.
	commandCount int32
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
		if roachpb.IsTransactionWrite(req) || req.Method() == roachpb.EndTransaction {
			s.seqGen++
		}

		oldHeader := req.Header()
		oldHeader.Sequence = s.seqGen
		ru.GetInner().SetHeader(oldHeader)
	}

	s.commandCount += int32(len(ba.Requests))

	return s.wrapped.SendLocked(ctx, ba)
}

// setWrapped is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) setWrapped(wrapped lockedSender) { s.wrapped = wrapped }

// populateMetaLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) populateMetaLocked(meta *roachpb.TxnCoordMeta) {
	meta.CommandCount = s.commandCount
	meta.Txn.Sequence = s.seqGen
}

// augmentMetaLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) augmentMetaLocked(meta roachpb.TxnCoordMeta) {
	s.commandCount += meta.CommandCount
	if meta.Txn.Sequence > s.seqGen {
		s.seqGen = meta.Txn.Sequence
	}
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) epochBumpedLocked() {
	s.seqGen = 0
	s.commandCount = 0
}

// closeLocked is part of the txnInterceptor interface.
func (*txnSeqNumAllocator) closeLocked() {}
