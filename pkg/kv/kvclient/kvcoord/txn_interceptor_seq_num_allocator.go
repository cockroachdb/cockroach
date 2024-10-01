// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/errors"
)

// txnSeqNumAllocator is a txnInterceptor in charge of allocating sequence
// numbers to all the individual requests in batches.
//
// Sequence numbers serve a few roles in the transaction model:
//
//  1. they are used to enforce an ordering between read and write operations in a
//     single transaction that go to the same key. Each read request that travels
//     through the interceptor is assigned the sequence number of the most recent
//     write. Each write request that travels through the interceptor is assigned
//     a sequence number larger than any previously allocated.
//
//     This is true even for leaf transaction coordinators. In their case, they are
//     provided the sequence number of the most recent write during construction.
//     Because they only perform read operations and never issue writes, they assign
//     each read this sequence number without ever incrementing their own counter.
//     In this way, sequence numbers are maintained correctly across a distributed
//     tree of transaction coordinators.
//
//  2. they are used to uniquely identify write operations. Because every write
//     request is given a new sequence number, the tuple (txn_id, txn_epoch, seq)
//     uniquely identifies a write operation across an entire cluster. This property
//     is exploited when determining the status of an individual write by looking
//     for its intent. We perform such an operation using the QueryIntent request
//     type when pipelining transactional writes. We will do something similar
//     during the recovery stage of implicitly committed transactions.
//
//  3. they are used to determine whether a batch contains the entire write set
//     for a transaction. See BatchRequest.IsCompleteTransaction.
//
//  4. they are used to provide idempotency for replays and re-issues. The MVCC
//     layer is sequence number-aware and ensures that reads at a given sequence
//     number ignore writes in the same transaction at larger sequence numbers.
//     Likewise, writes at a sequence number become no-ops if an intent with the
//     same sequence is already present. If an intent with the same sequence is not
//     already present but an intent with a larger sequence number is, an error is
//     returned. Likewise, if an intent with the same sequence is present but its
//     value is different than what we recompute, an error is returned.
type txnSeqNumAllocator struct {
	wrapped lockedSender

	// writeSeq is the current write seqnum, i.e. the value last assigned
	// to a write operation in a batch. It remains at 0 until the first
	// write or savepoint operation is encountered.
	writeSeq enginepb.TxnSeq

	// readSeq is the sequence number at which to perform read-only operations.
	readSeq enginepb.TxnSeq

	// steppingMode indicates whether to operate in stepping mode or
	// read-own-writes:
	// - in read-own-writes, the readSeq is advanced automatically after each
	//   write operation. All subsequent reads, even those in the same batch,
	//   will read at the newest sequence number and observe all prior writes.
	// - when stepping, the readSeq is only advanced when the client calls
	//   TxnCoordSender.Step. Reads will only observe writes performed before
	//   the last call to TxnCoordSender.Step.
	steppingMode kv.SteppingMode
}

// SendLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	if err := s.checkReadSeqNotIgnoredLocked(ba); err != nil {
		return nil, kvpb.NewError(err)
	}

	for _, ru := range ba.Requests {
		req := ru.GetInner()
		oldHeader := req.Header()
		// Only increment the sequence number generator for requests that
		// will leave intents or requests that will commit the transaction.
		// This enables ba.IsCompleteTransaction to work properly.
		//
		// Note: requests that perform writes using write intents and the EndTxn
		// request cannot operate at a past sequence number. This also applies to
		// combined read/intent-write requests (e.g. CPuts) -- these always read at
		// the latest write sequence number as well.
		//
		// Requests that do not perform intent writes use the read sequence number.
		// Notably, this includes Get/Scan/ReverseScan requests that acquire
		// replicated locks, even though they go through raft.
		if kvpb.IsIntentWrite(req) || req.Method() == kvpb.EndTxn {
			if err := s.stepWriteSeqLocked(ctx); err != nil {
				return nil, kvpb.NewError(err)
			}
			oldHeader.Sequence = s.writeSeq
		} else {
			oldHeader.Sequence = s.readSeq
		}

		req.SetHeader(oldHeader)
	}

	return s.wrapped.SendLocked(ctx, ba)
}

// setWrapped is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) setWrapped(wrapped lockedSender) { s.wrapped = wrapped }

// populateLeafInputState is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) populateLeafInputState(tis *roachpb.LeafTxnInputState) {
	tis.Txn.Sequence = s.writeSeq
	tis.SteppingModeEnabled = bool(s.steppingMode)
	tis.ReadSeqNum = s.readSeq
}

// initializeLeaf loads the read seqnum for a leaf transaction.
func (s *txnSeqNumAllocator) initializeLeaf(tis *roachpb.LeafTxnInputState) {
	s.steppingMode = kv.SteppingMode(tis.SteppingModeEnabled)
	s.readSeq = tis.ReadSeqNum
}

// populateLeafFinalState is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) populateLeafFinalState(tfs *roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) importLeafFinalState(
	ctx context.Context, tfs *roachpb.LeafTxnFinalState,
) error {
	return nil
}

// manualStepReadSeqLocked bumps the read seqnum to the current write seqnum.
// Used by the TxnCoordSender's Step() method.
func (s *txnSeqNumAllocator) manualStepReadSeqLocked(ctx context.Context) error {
	if s.steppingMode != kv.SteppingEnabled {
		return errors.AssertionFailedf("stepping mode is not enabled")
	}
	return s.stepReadSeqLocked(ctx)
}

// maybeAutoStepReadSeqLocked bumps the readSeq to the current write seqnum,
// if manual stepping is disabled and the txnSeqNumAllocator is expected to
// automatically step the readSeq. Otherwise, the method is a no-op.
//
//gcassert:inline
func (s *txnSeqNumAllocator) maybeAutoStepReadSeqLocked(ctx context.Context) error {
	if s.steppingMode == kv.SteppingEnabled {
		return nil // only manual stepping allowed
	}
	return s.stepReadSeqLocked(ctx)
}

// stepReadSeqLocked bumps the read seqnum to the current write seqnum.
func (s *txnSeqNumAllocator) stepReadSeqLocked(ctx context.Context) error {
	if s.readSeq > s.writeSeq {
		return errors.AssertionFailedf(
			"cannot stepReadSeqLocked() after mistaken initialization (%d,%d)", s.writeSeq, s.readSeq)
	}
	s.readSeq = s.writeSeq
	return nil
}

// stepWriteSeqLocked increments the write seqnum.
func (s *txnSeqNumAllocator) stepWriteSeqLocked(ctx context.Context) error {
	s.writeSeq++
	return s.maybeAutoStepReadSeqLocked(ctx)
}

// checkReadSeqNotIgnoredLocked verifies that the read seqnum is not in the
// ignored seqnum list of the provided batch request.
func (s *txnSeqNumAllocator) checkReadSeqNotIgnoredLocked(ba *kvpb.BatchRequest) error {
	if enginepb.TxnSeqIsIgnored(s.readSeq, ba.Txn.IgnoredSeqNums) {
		return errors.AssertionFailedf(
			"read sequence number %d but sequence number is ignored %v after savepoint rollback",
			s.readSeq, ba.Txn.IgnoredSeqNums)
	}
	return nil
}

// configureSteppingLocked configures the stepping mode.
//
// When enabling stepping from the non-enabled state, the read seqnum
// is set to the current write seqnum, as if a snapshot was taken at
// the point stepping was enabled.
//
// The read seqnum is otherwise not modified when trying to enable
// stepping when it was previously enabled already. This is the
// behavior needed to provide the documented API semantics of
// sender.ConfigureStepping() (see client/sender.go).
func (s *txnSeqNumAllocator) configureSteppingLocked(
	newMode kv.SteppingMode,
) (prevMode kv.SteppingMode) {
	prevMode, s.steppingMode = s.steppingMode, newMode
	if prevMode == kv.SteppingDisabled && newMode == kv.SteppingEnabled {
		s.readSeq = s.writeSeq
	}
	return prevMode
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) epochBumpedLocked() {
	// Note: we do not touch steppingModeEnabled here: if stepping mode
	// was enabled on the txn, it remains enabled.
	s.writeSeq = 0
	s.readSeq = 0
}

// createSavepointLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) createSavepointLocked(ctx context.Context, sp *savepoint) {
	sp.seqNum = s.writeSeq
}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (s *txnSeqNumAllocator) rollbackToSavepointLocked(context.Context, savepoint) {
	// Nothing to restore. The seq nums keep increasing. The TxnCoordSender has
	// added a range of sequence numbers to the ignored list. It may have also
	// manually stepped the write seqnum to distinguish the ignored range from
	// any future operations.
}

// closeLocked is part of the txnInterceptor interface.
func (*txnSeqNumAllocator) closeLocked() {}
