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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// MockTransactionalSender allows a function to be used as a TxnSender.
type MockTransactionalSender struct {
	senderFunc func(
		context.Context, *roachpb.Transaction, roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error)
	txn roachpb.Transaction
}

// NewMockTransactionalSender creates a MockTransactionalSender.
// The passed in txn is cloned.
func NewMockTransactionalSender(
	f func(
		context.Context, *roachpb.Transaction, roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error),
	txn *roachpb.Transaction,
) *MockTransactionalSender {
	return &MockTransactionalSender{senderFunc: f, txn: *txn}
}

// Send is part of the TxnSender interface.
func (m *MockTransactionalSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return m.senderFunc(ctx, &m.txn, ba)
}

// GetLeafTxnInputState is part of the TxnSender interface.
func (m *MockTransactionalSender) GetLeafTxnInputState(
	context.Context, TxnStatusOpt,
) (*roachpb.LeafTxnInputState, error) {
	panic("unimplemented")
}

// GetLeafTxnFinalState is part of the TxnSender interface.
func (m *MockTransactionalSender) GetLeafTxnFinalState(
	context.Context, TxnStatusOpt,
) (*roachpb.LeafTxnFinalState, error) {
	panic("unimplemented")
}

// UpdateRootWithLeafFinalState is part of the TxnSender interface.
func (m *MockTransactionalSender) UpdateRootWithLeafFinalState(
	context.Context, *roachpb.LeafTxnFinalState,
) {
	panic("unimplemented")
}

// AnchorOnSystemConfigRange is part of the TxnSender interface.
func (m *MockTransactionalSender) AnchorOnSystemConfigRange() error {
	return errors.New("unimplemented")
}

// TxnStatus is part of the TxnSender interface.
func (m *MockTransactionalSender) TxnStatus() roachpb.TransactionStatus {
	return m.txn.Status
}

// SetUserPriority is part of the TxnSender interface.
func (m *MockTransactionalSender) SetUserPriority(pri roachpb.UserPriority) error {
	m.txn.Priority = roachpb.MakePriority(pri)
	return nil
}

// SetDebugName is part of the TxnSender interface.
func (m *MockTransactionalSender) SetDebugName(name string) {
	m.txn.Name = name
}

// String is part of the TxnSender interface.
func (m *MockTransactionalSender) String() string {
	return m.txn.String()
}

// ReadTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) ReadTimestamp() hlc.Timestamp {
	return m.txn.ReadTimestamp
}

// ProvisionalCommitTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) ProvisionalCommitTimestamp() hlc.Timestamp {
	return m.txn.WriteTimestamp
}

// CommitTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) CommitTimestamp() hlc.Timestamp {
	return m.txn.ReadTimestamp
}

// CommitTimestampFixed is part of the TxnSender interface.
func (m *MockTransactionalSender) CommitTimestampFixed() bool {
	return m.txn.CommitTimestampFixed
}

// SetFixedTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) SetFixedTimestamp(_ context.Context, ts hlc.Timestamp) error {
	m.txn.WriteTimestamp = ts
	m.txn.ReadTimestamp = ts
	m.txn.GlobalUncertaintyLimit = ts
	m.txn.CommitTimestampFixed = true

	// Set the MinTimestamp to the minimum of the existing MinTimestamp and the fixed
	// timestamp. This ensures that the MinTimestamp is always <= the other timestamps.
	m.txn.MinTimestamp.Backward(ts)
	return nil
}

// RequiredFrontier is part of the TxnSender interface.
func (m *MockTransactionalSender) RequiredFrontier() hlc.Timestamp {
	return m.txn.RequiredFrontier()
}

// ManualRestart is part of the TxnSender interface.
func (m *MockTransactionalSender) ManualRestart(
	ctx context.Context, pri roachpb.UserPriority, ts hlc.Timestamp,
) {
	m.txn.Restart(pri, 0 /* upgradePriority */, ts)
}

// IsSerializablePushAndRefreshNotPossible is part of the TxnSender interface.
func (m *MockTransactionalSender) IsSerializablePushAndRefreshNotPossible() bool {
	return false
}

// CreateSavepoint is part of the client.TxnSender interface.
func (m *MockTransactionalSender) CreateSavepoint(context.Context) (SavepointToken, error) {
	panic("unimplemented")
}

// RollbackToSavepoint is part of the client.TxnSender interface.
func (m *MockTransactionalSender) RollbackToSavepoint(context.Context, SavepointToken) error {
	panic("unimplemented")
}

// ReleaseSavepoint is part of the client.TxnSender interface.
func (m *MockTransactionalSender) ReleaseSavepoint(context.Context, SavepointToken) error {
	panic("unimplemented")
}

// Epoch is part of the TxnSender interface.
func (m *MockTransactionalSender) Epoch() enginepb.TxnEpoch { panic("unimplemented") }

// IsLocking is part of the TxnSender interface.
func (m *MockTransactionalSender) IsLocking() bool { return false }

// TestingCloneTxn is part of the TxnSender interface.
func (m *MockTransactionalSender) TestingCloneTxn() *roachpb.Transaction {
	return m.txn.Clone()
}

// Active is part of the TxnSender interface.
func (m *MockTransactionalSender) Active() bool {
	panic("unimplemented")
}

// UpdateStateOnRemoteRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) UpdateStateOnRemoteRetryableErr(
	ctx context.Context, pErr *roachpb.Error,
) *roachpb.Error {
	panic("unimplemented")
}

// DisablePipelining is part of the client.TxnSender interface.
func (m *MockTransactionalSender) DisablePipelining() error { return nil }

// PrepareRetryableError is part of the client.TxnSender interface.
func (m *MockTransactionalSender) PrepareRetryableError(ctx context.Context, msg string) error {
	return roachpb.NewTransactionRetryWithProtoRefreshError(msg, m.txn.ID, *m.txn.Clone())
}

// Step is part of the TxnSender interface.
func (m *MockTransactionalSender) Step(_ context.Context) error {
	// At least one test (e.g sql/TestPortalsDestroyedOnTxnFinish) requires
	// the ability to run simple statements that do not access storage,
	// and that requires a non-panicky Step().
	return nil
}

// SetReadSeqNum is part of the TxnSender interface.
func (m *MockTransactionalSender) SetReadSeqNum(_ enginepb.TxnSeq) error { return nil }

// ConfigureStepping is part of the TxnSender interface.
func (m *MockTransactionalSender) ConfigureStepping(context.Context, SteppingMode) SteppingMode {
	// See Step() above.
	return SteppingDisabled
}

// GetSteppingMode is part of the TxnSender interface.
func (m *MockTransactionalSender) GetSteppingMode(context.Context) SteppingMode {
	return SteppingDisabled
}

// ManualRefresh is part of the TxnSender interface.
func (m *MockTransactionalSender) ManualRefresh(ctx context.Context) error {
	panic("unimplemented")
}

// DeferCommitWait is part of the TxnSender interface.
func (m *MockTransactionalSender) DeferCommitWait(ctx context.Context) func(context.Context) error {
	panic("unimplemented")
}

// GetTxnRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) GetTxnRetryableErr(
	ctx context.Context,
) *roachpb.TransactionRetryWithProtoRefreshError {
	return nil
}

// ClearTxnRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) ClearTxnRetryableErr(ctx context.Context) {
}

// MockTxnSenderFactory is a TxnSenderFactory producing MockTxnSenders.
type MockTxnSenderFactory struct {
	senderFunc func(context.Context, *roachpb.Transaction, roachpb.BatchRequest) (
		*roachpb.BatchResponse, *roachpb.Error)
	nonTxnSenderFunc Sender
}

var _ TxnSenderFactory = MockTxnSenderFactory{}

// MakeMockTxnSenderFactory creates a MockTxnSenderFactory from a sender
// function that receives the transaction in addition to the request. The
// function is responsible for putting the txn inside the batch, if needed.
func MakeMockTxnSenderFactory(
	senderFunc func(
		context.Context, *roachpb.Transaction, roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error),
) MockTxnSenderFactory {
	return MockTxnSenderFactory{
		senderFunc: senderFunc,
	}
}

// MakeMockTxnSenderFactoryWithNonTxnSender creates a MockTxnSenderFactory from
// two sender functions: one for transactional and one for non-transactional
// requests.
func MakeMockTxnSenderFactoryWithNonTxnSender(
	senderFunc func(
		context.Context, *roachpb.Transaction, roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error),
	nonTxnSenderFunc SenderFunc,
) MockTxnSenderFactory {
	return MockTxnSenderFactory{
		senderFunc:       senderFunc,
		nonTxnSenderFunc: nonTxnSenderFunc,
	}
}

// RootTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) RootTransactionalSender(
	txn *roachpb.Transaction, _ roachpb.UserPriority,
) TxnSender {
	return NewMockTransactionalSender(f.senderFunc, txn)
}

// LeafTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) LeafTransactionalSender(tis *roachpb.LeafTxnInputState) TxnSender {
	return NewMockTransactionalSender(f.senderFunc, &tis.Txn)
}

// NonTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) NonTransactionalSender() Sender {
	return f.nonTxnSenderFunc
}
