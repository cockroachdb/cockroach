// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

// savepoint captures the state in the TxnCoordSender necessary to restore that
// state upon a savepoint rollback.
type savepoint struct {
	// seqNum represents the write seq num at the time the savepoint was created.
	// On rollback, it configures the txn to ignore all seqnums from this value
	// until the most recent seqnum.
	seqNum enginepb.TxnSeq

	// txnSpanRefresher fields
	refreshSpans     []roachpb.Span
	refreshInvalid   bool
	refreshSpanBytes int64

	// txnPipeliner fields
	ifWrites *btree.BTree // can be nil if no reads were tracked
	ifBytes  int64

	// txnID is used to verify that a rollback is not used to paper
	// over a txn abort error.
	txnID uuid.UUID
	// epoch is used to verify that a savepoint rollback is not
	// used to paper over a retry error.
	// TODO(knz,andrei): expand savepoint rollbacks to recover
	// from retry errors.
	// TODO(knz,andrei): remove the epoch mechanism entirely in
	// favor of seqnums and savepoint rollbacks.
	epoch enginepb.TxnEpoch
}

var _ client.SavepointToken = (*savepoint)(nil)

// SavepointToken implements the client.SavepointToken interface.
func (s *savepoint) SavepointToken() {}

// CreateSavepoint is part of the client.TxnSender interface.
func (tc *TxnCoordSender) CreateSavepoint(ctx context.Context) (client.SavepointToken, error) {
	if tc.typ != client.RootTxn {
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

	s := &savepoint{
		txnID: tc.mu.txn.ID,
		epoch: tc.mu.txn.Epoch,
	}
	for _, reqInt := range tc.interceptorStack {
		reqInt.createSavepoint(nil, s)
	}

	return s, nil
}

// RollbackToSavepoint is part of the client.TxnSender interface.
func (tc *TxnCoordSender) RollbackToSavepoint(ctx context.Context, s client.SavepointToken) error {
	if tc.typ != client.RootTxn {
		return errors.AssertionFailedf("cannot rollback savepoint in non-root txn")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if err := tc.assertNotFinalized(); err != nil {
		return err
	}

	st, err := tc.checkSavepointLocked(s, "rollback")
	if err != nil {
		return err
	}

	if tc.mu.txnState == txnFinalized {
		return unimplemented.New("rollback_error", "savepoint rollback finalized txn")
	}

	// Restore the transaction's state, in case we're rewiding after an error.
	tc.mu.txnState = txnPending

	for _, reqInt := range tc.interceptorStack {
		reqInt.rollbackToSavepoint(ctx, st)
	}

	if st.seqNum == tc.interceptorAlloc.txnSeqNumAllocator.writeSeq {
		// No operations since savepoint was taken. No-op.
		return nil
	}

	tc.mu.txn.IgnoredSeqNums = roachpb.AddIgnoredSeqNumRange(
		tc.mu.txn.IgnoredSeqNums,
		enginepb.IgnoredSeqNumRange{
			Start: st.seqNum + 1, End: tc.interceptorAlloc.txnSeqNumAllocator.writeSeq,
		})

	return nil
}

// ReleaseSavepoint is part of the client.TxnSender interface.
func (tc *TxnCoordSender) ReleaseSavepoint(ctx context.Context, s client.SavepointToken) error {
	if tc.typ != client.RootTxn {
		return errors.AssertionFailedf("cannot release savepoint in non-root txn")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	if tc.mu.txnState != txnPending {
		return ErrSavepointOperationInErrorTxn
	}

	_, err := tc.checkSavepointLocked(s, "release")
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

func (tc *TxnCoordSender) checkSavepointLocked(
	s client.SavepointToken, opName string,
) (*savepoint, error) {
	st, ok := s.(*savepoint)
	if !ok {
		return nil, errors.AssertionFailedf("expected savepointToken, got %T", s)
	}

	if st.seqNum > 0 && st.txnID != tc.mu.txn.ID {
		return nil, errors.Newf("cannot %s savepoint across transaction retries", opName)
	}

	if st.seqNum > 0 && st.epoch != tc.mu.txn.Epoch {
		return nil, errors.Newf("cannot %s savepoint across transaction retries", opName)
	}

	if st.seqNum < 0 || st.seqNum > tc.interceptorAlloc.txnSeqNumAllocator.writeSeq {
		return nil, errors.AssertionFailedf("invalid savepoint: got %d, expected 0-%d",
			st.seqNum, tc.interceptorAlloc.txnSeqNumAllocator.writeSeq)
	}

	return st, nil
}
