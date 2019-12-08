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
)

// savepointToken captures the state in the TxnCoordSender necessary
// to restore that state upon a savepoint rollback.
//
// TODO(knz,andrei): Currently this definition is only sufficient for
// just a few cases of rollbacks. This should be extended to cover
// more ground:
//
// - We also need the current size of txnSpanRefresher.refreshSpans the
//   list of tracked reads, such that upon rollback we tell the
//   refresher interceptor to discard further reads.
// - We also need something about in-flight writes
//   (txnPipeliner.ifWrites). There I guess we need to take some sort of
//   snapshot of the current in-flight writes and, on rollback, discard
//   in-flight writes that are not part of the savepoint. But, on
//   rollback, I don't think we should (nor am I sure that we could)
//   simply overwrite the set of in-flight writes with the ones from the
//   savepoint because writes that have been verified since the snapshot
//   has been taken should continue to be verified. Basically, on
//   rollback I think we need to intersect the savepoint with the
//   current set of in-flight writes.
type savepointToken struct {
	// seqNum is currently the only field that helps to "restore"
	// anything upon a rollback. When used, it does not change anything
	// in the TCS; instead it simply configures the txn to ignore all
	// seqnums from this value until the most recent seqnum emitted by
	// the TCS.
	seqNum enginepb.TxnSeq

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

var _ client.SavepointToken = (*savepointToken)(nil)

// SavepointToken implements the client.SavepointToken interface.
func (s *savepointToken) SavepointToken() {}

// CreateSavepoint is part of the client.TxnSender interface.
func (tc *TxnCoordSender) CreateSavepoint(ctx context.Context) (client.SavepointToken, error) {
	if tc.typ != client.RootTxn {
		return nil, errors.AssertionFailedf("cannot get savepoint in non-root txn")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txnState != txnPending || tc.mu.closed {
		return nil, errors.AssertionFailedf("cannot get savepoint in this txn state (%v, %v)",
			tc.mu.txnState, tc.mu.closed)
	}
	return &savepointToken{
		txnID:  tc.mu.txn.ID,
		epoch:  tc.mu.txn.Epoch,
		seqNum: tc.interceptorAlloc.txnSeqNumAllocator.writeSeq,
	}, nil
}

// RollbackToSavepoint is part of the client.TxnSender interface.
func (tc *TxnCoordSender) RollbackToSavepoint(ctx context.Context, s client.SavepointToken) error {
	if tc.typ != client.RootTxn {
		return errors.AssertionFailedf("cannot rollback savepoint in non-root txn")
	}

	tc.mu.Lock()
	defer tc.mu.Unlock()

	st, err := tc.checkSavepointLocked(true /*allowError*/, s)
	if err != nil {
		return err
	}

	// TODO(knz): handle recoverable errors.
	if tc.mu.txnState == txnError {
		return unimplemented.New("rollback_error", "savepoint rollback after error")
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

	_, err := tc.checkSavepointLocked(false /*allowError*/, s)
	if err != nil {
		return err
	}

	// Release is otherwise a no-op.

	return nil
}

func (tc *TxnCoordSender) checkSavepointLocked(
	allowError bool, s client.SavepointToken,
) (*savepointToken, error) {
	if (tc.mu.txnState != txnPending &&
		(!allowError || tc.mu.txnState != txnError)) ||
		tc.mu.closed {
		return nil, errors.AssertionFailedf("cannot run savepoint operation in this txn state (%v, %v)",
			tc.mu.txnState, tc.mu.closed)
	}

	st, ok := s.(*savepointToken)
	if !ok {
		return nil, errors.AssertionFailedf("expected savepointToken, got %T", s)
	}

	if st.txnID != tc.mu.txn.ID {
		return nil, errors.AssertionFailedf("savepoint from wrong txn ID: got %s, expected %s",
			st.txnID, tc.mu.txn.ID)
	}

	if st.epoch != tc.mu.txn.Epoch {
		return nil, errors.AssertionFailedf("savepoint from wrong txn epoch: got %d, expected %d",
			st.epoch, tc.mu.txn.Epoch)
	}

	if st.seqNum < 0 || st.seqNum > tc.interceptorAlloc.txnSeqNumAllocator.writeSeq {
		return nil, errors.AssertionFailedf("invalid savepoint: got %d, expected 0-%d",
			st.seqNum, tc.interceptorAlloc.txnSeqNumAllocator.writeSeq)
	}

	if enginepb.TxnSeqIsIgnored(st.seqNum+1, tc.mu.txn.IgnoredSeqNums) {
		return nil, errors.AssertionFailedf("savepoint is already rolled back")
	}

	return st, nil
}
