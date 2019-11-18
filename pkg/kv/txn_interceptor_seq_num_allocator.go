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
	"github.com/cockroachdb/errors"
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

	// writeSeq is the current write seqnum, or the value last assigned
	// to a write operation in a batch. It remains at 0 until the first
	// write operation is encountered.
	writeSeq enginepb.TxnSeq

	// readSeqPlusOne is either:
	// - 0 to indicate that read operations should read at the latest
	//   write seqnum (read-own-write behavior).
	// - >0 to indicate that read operations should read at a
	//   fixed readSeq = (readSeqPlusOne - 1), also called
	//   "step-wise" behavior.
	// We use a +1 offset so that the default value 0 can be
	// used as sentinel to disable step-wise behavior.
	readSeqPlusOne enginepb.TxnSeq

	// commandCount indicates how many requests have been sent through
	// this transaction. Reset on retryable txn errors.
	// TODO(andrei): let's get rid of this. It should be maintained
	// in the SQL level.
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
			s.writeSeq++
		}

		oldHeader := req.Header()
		// Default case: operate at the current seqnum.
		oldHeader.Sequence = s.writeSeq
		if s.readSeqPlusOne > 0 && roachpb.IsReadOnly(req) {
			// For read operations, if the step-wise execution mode has been
			// enabled, then we want the read operation to read at the read
			// seqnum, not the latest write seqnum.
			oldHeader.Sequence = s.readSeqPlusOne - 1
		}
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
	meta.Txn.Sequence = s.writeSeq
	meta.ReadSeqNumPlusOne = s.readSeqPlusOne
}

// augmentMetaLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) augmentMetaLocked(meta roachpb.TxnCoordMeta) {
	s.commandCount += meta.CommandCount
	if meta.Txn.Sequence > s.writeSeq {
		s.writeSeq = meta.Txn.Sequence
	}
	if meta.ReadSeqNumPlusOne > s.readSeqPlusOne {
		s.readSeqPlusOne = meta.ReadSeqNumPlusOne
	}
}

// stepLocked bumps the read seqnum to the current write seqnum.
// Used by the TxnCoordSender's Step() method.
func (s *txnSeqNumAllocator) stepLocked() error {
	if s.readSeqPlusOne-1 > s.writeSeq {
		return errors.AssertionFailedf("cannot step() after mistaken initialization (%d,%d)", s.writeSeq, s.readSeqPlusOne)
	}
	s.readSeqPlusOne = s.writeSeq + 1
	return nil
}

// disableSteppingLocked cancels the stepping behavior and
// restores read-latest-write behavior.
// Used by the TxnCoordSender's DisableStepping() method.
func (s *txnSeqNumAllocator) disableSteppingLocked() error {
	s.readSeqPlusOne = 0
	return nil
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) epochBumpedLocked() {
	s.commandCount = 0
	s.writeSeq = 0
	if s.readSeqPlusOne > 0 {
		s.readSeqPlusOne = 1
	} else {
		s.readSeqPlusOne = 0
	}
}

// closeLocked is part of the txnInterceptor interface.
func (*txnSeqNumAllocator) closeLocked() {}
