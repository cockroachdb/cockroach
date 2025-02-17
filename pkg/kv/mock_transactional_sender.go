// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/redact"
)

// MockTransactionalSender allows a function to be used as a TxnSender.
type MockTransactionalSender struct {
	senderFunc func(
		context.Context, *roachpb.Transaction, *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error)
	txn roachpb.Transaction
	pri roachpb.UserPriority
}

// NewMockTransactionalSender creates a MockTransactionalSender.
// The passed in txn is cloned.
func NewMockTransactionalSender(
	f func(
		context.Context, *roachpb.Transaction, *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error),
	txn *roachpb.Transaction,
	pri roachpb.UserPriority,
) *MockTransactionalSender {
	return &MockTransactionalSender{senderFunc: f, txn: *txn, pri: pri}
}

// Send is part of the TxnSender interface.
func (m *MockTransactionalSender) Send(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	return m.senderFunc(ctx, &m.txn, ba)
}

// GetLeafTxnInputState is part of the TxnSender interface.
func (m *MockTransactionalSender) GetLeafTxnInputState(
	context.Context,
) (*roachpb.LeafTxnInputState, error) {
	panic("unimplemented")
}

// GetLeafTxnFinalState is part of the TxnSender interface.
func (m *MockTransactionalSender) GetLeafTxnFinalState(
	context.Context,
) (*roachpb.LeafTxnFinalState, error) {
	panic("unimplemented")
}

// UpdateRootWithLeafFinalState is part of the TxnSender interface.
func (m *MockTransactionalSender) UpdateRootWithLeafFinalState(
	context.Context, *roachpb.LeafTxnFinalState,
) error {
	panic("unimplemented")
}

// TxnStatus is part of the TxnSender interface.
func (m *MockTransactionalSender) TxnStatus() roachpb.TransactionStatus {
	return m.txn.Status
}

// ClientFinalized is part of the TxnSender interface.
func (m *MockTransactionalSender) ClientFinalized() bool {
	return m.txn.Status.IsFinalized()
}

// SetIsoLevel is part of the TxnSender interface.
func (m *MockTransactionalSender) SetIsoLevel(isoLevel isolation.Level) error {
	m.txn.IsoLevel = isoLevel
	return nil
}

// IsoLevel is part of the TxnSender interface.
func (m *MockTransactionalSender) IsoLevel() isolation.Level {
	return m.txn.IsoLevel
}

// SetUserPriority is part of the TxnSender interface.
func (m *MockTransactionalSender) SetUserPriority(pri roachpb.UserPriority) error {
	m.txn.Priority = roachpb.MakePriority(pri)
	m.pri = pri
	return nil
}

// SetDebugName is part of the TxnSender interface.
func (m *MockTransactionalSender) SetDebugName(name string) {
	m.txn.Name = name
}

// GetOmitInRangefeeds is part of the TxnSender interface.
func (m *MockTransactionalSender) GetOmitInRangefeeds() bool {
	return m.txn.OmitInRangefeeds
}

// SetOmitInRangefeeds is part of the TxnSender interface.
func (m *MockTransactionalSender) SetOmitInRangefeeds() {
	m.txn.OmitInRangefeeds = true
}

// SetBufferedWritesEnabled is part of the TxnSender interface.
func (m *MockTransactionalSender) SetBufferedWritesEnabled(enabled bool) {}

// BufferedWritesEnabled is part of the TxnSender interface.
func (m *MockTransactionalSender) BufferedWritesEnabled() bool {
	return false
}

// String is part of the TxnSender interface.
func (m *MockTransactionalSender) String() string {
	return m.txn.String()
}

// ReadTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) ReadTimestamp() hlc.Timestamp {
	return m.txn.ReadTimestamp
}

// ReadTimestampFixed is part of the TxnSender interface.
func (m *MockTransactionalSender) ReadTimestampFixed() bool {
	return m.txn.ReadTimestampFixed
}

// ProvisionalCommitTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) ProvisionalCommitTimestamp() hlc.Timestamp {
	return m.txn.WriteTimestamp
}

// CommitTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) CommitTimestamp() (hlc.Timestamp, error) {
	return m.txn.ReadTimestamp, nil
}

// SetFixedTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) SetFixedTimestamp(_ context.Context, ts hlc.Timestamp) error {
	m.txn.WriteTimestamp = ts
	m.txn.ReadTimestamp = ts
	m.txn.ReadTimestampFixed = true
	m.txn.GlobalUncertaintyLimit = ts

	// Set the MinTimestamp to the minimum of the existing MinTimestamp and the fixed
	// timestamp. This ensures that the MinTimestamp is always <= the other timestamps.
	m.txn.MinTimestamp.Backward(ts)
	return nil
}

// RequiredFrontier is part of the TxnSender interface.
func (m *MockTransactionalSender) RequiredFrontier() hlc.Timestamp {
	return m.txn.RequiredFrontier()
}

// GenerateForcedRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) GenerateForcedRetryableErr(
	ctx context.Context, ts hlc.Timestamp, mustRestart bool, msg redact.RedactableString,
) error {
	prevTxn := m.txn
	nextTxn := m.txn.Clone()
	nextTxn.WriteTimestamp.Forward(ts)
	if !nextTxn.IsoLevel.PerStatementReadSnapshot() || mustRestart {
		nextTxn.Restart(m.pri, 0 /* upgradePriority */, nextTxn.WriteTimestamp)
	} else {
		nextTxn.BumpReadTimestamp(nextTxn.WriteTimestamp)
	}
	m.txn.Update(nextTxn)
	return kvpb.NewTransactionRetryWithProtoRefreshError(msg, prevTxn.ID, prevTxn.Epoch, *nextTxn)
}

// UpdateStateOnRemoteRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) UpdateStateOnRemoteRetryableErr(
	ctx context.Context, pErr *kvpb.Error,
) *kvpb.Error {
	panic("unimplemented")
}

// GetRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) GetRetryableErr(
	ctx context.Context,
) *kvpb.TransactionRetryWithProtoRefreshError {
	return nil
}

// ClearRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) ClearRetryableErr(ctx context.Context) error {
	return nil
}

// IsSerializablePushAndRefreshNotPossible is part of the TxnSender interface.
func (m *MockTransactionalSender) IsSerializablePushAndRefreshNotPossible() bool {
	return false
}

// CreateSavepoint is part of the kv.TxnSender interface.
func (m *MockTransactionalSender) CreateSavepoint(context.Context) (SavepointToken, error) {
	panic("unimplemented")
}

// RollbackToSavepoint is part of the kv.TxnSender interface.
func (m *MockTransactionalSender) RollbackToSavepoint(context.Context, SavepointToken) error {
	panic("unimplemented")
}

// ReleaseSavepoint is part of the kv.TxnSender interface.
func (m *MockTransactionalSender) ReleaseSavepoint(context.Context, SavepointToken) error {
	panic("unimplemented")
}

// CanUseSavepoint is part of the kv.TxnSender interface.
func (m *MockTransactionalSender) CanUseSavepoint(context.Context, SavepointToken) bool {
	panic("unimplemented")
}

// Key is part of the TxnSender interface.
func (m *MockTransactionalSender) Key() roachpb.Key { panic("unimplemented") }

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

// DisablePipelining is part of the kv.TxnSender interface.
func (m *MockTransactionalSender) DisablePipelining() error { return nil }

// Step is part of the TxnSender interface.
func (m *MockTransactionalSender) Step(context.Context, bool) error {
	// At least one test (e.g sql/TestPortalsDestroyedOnTxnFinish) requires
	// the ability to run simple statements that do not access storage,
	// and that requires a non-panicky Step().
	return nil
}

// GetReadSeqNum is part of the TxnSender interface.
func (m *MockTransactionalSender) GetReadSeqNum() enginepb.TxnSeq { return 0 }

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

// DeferCommitWait is part of the TxnSender interface.
func (m *MockTransactionalSender) DeferCommitWait(ctx context.Context) func(context.Context) error {
	panic("unimplemented")
}

// HasPerformedReads is part of TxnSenderFactory.
func (m *MockTransactionalSender) HasPerformedReads() bool {
	panic("unimplemented")
}

// HasPerformedWrites is part of TxnSenderFactory.
func (m *MockTransactionalSender) HasPerformedWrites() bool {
	panic("unimplemented")
}

// TestingShouldRetry is part of TxnSenderFactory.
func (m *MockTransactionalSender) TestingShouldRetry() bool {
	return false
}

// MockTxnSenderFactory is a TxnSenderFactory producing MockTxnSenders.
type MockTxnSenderFactory struct {
	senderFunc func(context.Context, *roachpb.Transaction, *kvpb.BatchRequest) (
		*kvpb.BatchResponse, *kvpb.Error)
	nonTxnSenderFunc Sender
}

var _ TxnSenderFactory = MockTxnSenderFactory{}

// MakeMockTxnSenderFactory creates a MockTxnSenderFactory from a sender
// function that receives the transaction in addition to the request. The
// function is responsible for putting the txn inside the batch, if needed.
func MakeMockTxnSenderFactory(
	senderFunc func(
		context.Context, *roachpb.Transaction, *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error),
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
		context.Context, *roachpb.Transaction, *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error),
	nonTxnSenderFunc SenderFunc,
) MockTxnSenderFactory {
	return MockTxnSenderFactory{
		senderFunc:       senderFunc,
		nonTxnSenderFunc: nonTxnSenderFunc,
	}
}

// RootTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) RootTransactionalSender(
	txn *roachpb.Transaction, pri roachpb.UserPriority,
) TxnSender {
	return NewMockTransactionalSender(f.senderFunc, txn, pri)
}

// LeafTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) LeafTransactionalSender(tis *roachpb.LeafTxnInputState) TxnSender {
	return NewMockTransactionalSender(f.senderFunc, &tis.Txn, 0 /* pri */)
}

// NonTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) NonTransactionalSender() Sender {
	return f.nonTxnSenderFunc
}
