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
	"github.com/cockroachdb/errors"
)

type savepointToken struct {
	epoch  enginepb.TxnEpoch
	seqNum enginepb.TxnSeq
}

var _ client.SavepointToken = (*savepointToken)(nil)

func (s *savepointToken) SavepointToken() {}

// GetSavepoint is part of the client.TxnSender interface.
func (tc *TxnCoordSender) GetSavepoint(ctx context.Context) (client.SavepointToken, error) {
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
