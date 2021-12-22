// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// savepoint captures the state in the TxnCoordSender necessary to restore that
// state upon a savepoint rollback.
type savepoint struct {
	// active is a snapshot of TxnCoordSender.active.
	active bool

	// txnID and epoch are set for savepoints with the active field set.
	// txnID and epoch are used to disallow rollbacks past transaction restarts.
	// Savepoints without the active field set are allowed to be used to rollback
	// past transaction restarts too, because it's trivial to rollback to the
	// beginning of the transaction.
	txnID uuid.UUID
	epoch enginepb.TxnEpoch

	// seqNum represents the write seq num at the time the savepoint was created.
	// On rollback, it configures the txn to ignore all seqnums from this value
	// until the most recent seqnum.
	seqNum enginepb.TxnSeq

	// txnSpanRefresher fields.
	refreshSpans   []roachpb.Span
	refreshInvalid bool
}

var _ kv.SavepointToken = (*savepoint)(nil)

// statically allocated savepoint marking the beginning of a transaction. Used
// to avoid allocations for such savepoints.
var initialSavepoint = savepoint{}

// Initial implements the client.SavepointToken interface.
func (s *savepoint) Initial() bool {
	return !s.active
}

// CreateSavepoint is part of the client.TxnSender interface.
func (tc *TxnCoordSender) CreateSavepoint(ctx context.Context) (kv.SavepointToken, error) {
	if tc.typ != kv.RootTxn {
		return nil, errors.AssertionFailedf("cannot get savepoint in non-root txn")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.assertNotFinalized(); err != nil {
		return nil, err
	}

	if tc.mu.txnState != txnPending {
		return nil, ErrSavepointOperationInErrorTxn
	}

	if !tc.mu.active {
		// Return a preallocated savepoint for the common case of savepoints placed
		// at the beginning of transactions.
		return &initialSavepoint, nil
	}

	s := &savepoint{
		active: true, // we've handled the not-active case above
		txnID:  tc.mu.txn.ID,
		epoch:  tc.mu.txn.Epoch,
	}
	for _, reqInt := range tc.interceptorStack {
		reqInt.createSavepointLocked(ctx, s)
	}

	return s, nil
}

// RollbackToSavepoint is part of the client.TxnSender interface.
func (tc *TxnCoordSender) RollbackToSavepoint(ctx context.Context, s kv.SavepointToken) error {
	if tc.typ != kv.RootTxn {
		return errors.AssertionFailedf("cannot rollback savepoint in non-root txn")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.assertNotFinalized(); err != nil {
		return err
	}

	// We don't allow rollback to savepoint after errors (except after
	// ConditionFailedError and WriteIntentError, which are special-cased
	// elsewhere and don't move the txn to the txnError state). In particular, we
	// cannot allow rollbacks to savepoint after ambiguous errors where it's
	// possible for a previously-successfully written intent to have been pushed
	// at a timestamp higher than the coordinator's WriteTimestamp. Doing so runs
	// the risk that we'll commit at the lower timestamp, at which point the
	// respective intent will be discarded. See
	// https://github.com/cockroachdb/cockroach/issues/47587.
	//
	// TODO(andrei): White-list more errors.
	if tc.mu.txnState == txnError {
		return unimplemented.New("rollback_error", "cannot rollback to savepoint after error")
	}

	sp := s.(*savepoint)
	err := tc.checkSavepointLocked(sp)
	if err != nil {
		if errors.Is(err, errSavepointInvalidAfterTxnRestart) {
			err = roachpb.NewTransactionRetryWithProtoRefreshError(
				"cannot rollback to savepoint after a transaction restart",
				tc.mu.txn.ID,
				tc.mu.txn,
			)
		}
		return err
	}

	tc.mu.active = sp.active

	for _, reqInt := range tc.interceptorStack {
		reqInt.rollbackToSavepointLocked(ctx, *sp)
	}

	// If there's been any more writes since the savepoint was created, they'll
	// need to be ignored.
	if sp.seqNum < tc.interceptorAlloc.txnSeqNumAllocator.writeSeq {
		tc.mu.txn.AddIgnoredSeqNumRange(
			enginepb.IgnoredSeqNumRange{
				Start: sp.seqNum + 1, End: tc.interceptorAlloc.txnSeqNumAllocator.writeSeq,
			})
	}

	return nil
}

// ReleaseSavepoint is part of the client.TxnSender interface.
func (tc *TxnCoordSender) ReleaseSavepoint(ctx context.Context, s kv.SavepointToken) error {
	if tc.typ != kv.RootTxn {
		return errors.AssertionFailedf("cannot release savepoint in non-root txn")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.txnState != txnPending {
		return ErrSavepointOperationInErrorTxn
	}

	sp := s.(*savepoint)
	err := tc.checkSavepointLocked(sp)
	if errors.Is(err, errSavepointInvalidAfterTxnRestart) {
		err = roachpb.NewTransactionRetryWithProtoRefreshError(
			"cannot release savepoint after a transaction restart",
			tc.mu.txn.ID,
			tc.mu.txn,
		)
	}
	return err
}

type errSavepointOperationInErrorTxn struct{}

// ErrSavepointOperationInErrorTxn is reported when CreateSavepoint()
// or ReleaseSavepoint() is called over a txn currently in error.
var ErrSavepointOperationInErrorTxn error = errSavepointOperationInErrorTxn{}

func (err errSavepointOperationInErrorTxn) Error() string {
	return "cannot create or release savepoint after an error has occurred"
}

func (tc *TxnCoordSender) assertNotFinalized() error {
	if tc.mu.txnState == txnFinalized {
		return errors.AssertionFailedf("operation invalid for finalized txns")
	}
	return nil
}

var errSavepointInvalidAfterTxnRestart = errors.New("savepoint invalid after transaction restart")

// checkSavepointLocked checks whether the provided savepoint is still valid.
// Returns errSavepointInvalidAfterTxnRestart if the savepoint is not an
// "initial" one and the transaction has restarted since the savepoint was
// created.
func (tc *TxnCoordSender) checkSavepointLocked(s *savepoint) error {
	// Only savepoints taken before any activity are allowed to be used after a
	// transaction restart.
	if s.Initial() {
		return nil
	}
	if s.txnID != tc.mu.txn.ID {
		return errSavepointInvalidAfterTxnRestart
	}
	if s.epoch != tc.mu.txn.Epoch {
		return errSavepointInvalidAfterTxnRestart
	}

	if s.seqNum < 0 || s.seqNum > tc.interceptorAlloc.txnSeqNumAllocator.writeSeq {
		return errors.AssertionFailedf("invalid savepoint: got %d, expected 0-%d",
			s.seqNum, tc.interceptorAlloc.txnSeqNumAllocator.writeSeq)
	}

	return nil
}
