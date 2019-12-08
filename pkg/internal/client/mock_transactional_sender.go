// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package client

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
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

// GetMeta is part of the TxnSender interface.
func (m *MockTransactionalSender) GetMeta(
	context.Context, TxnStatusOpt,
) (roachpb.TxnCoordMeta, error) {
	panic("unimplemented")
}

// AugmentMeta is part of the TxnSender interface.
func (m *MockTransactionalSender) AugmentMeta(context.Context, roachpb.TxnCoordMeta) {
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

// ReadTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) ReadTimestamp() hlc.Timestamp {
	return m.txn.ReadTimestamp
}

// CommitTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) CommitTimestamp() hlc.Timestamp {
	return m.txn.ReadTimestamp
}

// CommitTimestampFixed is part of the TxnSender interface.
func (m *MockTransactionalSender) CommitTimestampFixed() bool {
	panic("unimplemented")
}

// SetFixedTimestamp is part of the TxnSender interface.
func (m *MockTransactionalSender) SetFixedTimestamp(_ context.Context, ts hlc.Timestamp) {
	m.txn.WriteTimestamp = ts
	m.txn.ReadTimestamp = ts
	m.txn.MaxTimestamp = ts
	m.txn.CommitTimestampFixed = true

	// Set the MinTimestamp to the minimum of the existing MinTimestamp and the fixed
	// timestamp. This ensures that the MinTimestamp is always <= the other timestamps.
	m.txn.MinTimestamp.Backward(ts)

	// For backwards compatibility with 19.2, set the DeprecatedOrigTimestamp too (although
	// not really needed by this Mock sender).
	m.txn.DeprecatedOrigTimestamp = ts
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

// Epoch is part of the TxnSender interface.
func (m *MockTransactionalSender) Epoch() enginepb.TxnEpoch { panic("unimplemented") }

// SerializeTxn is part of the TxnSender interface.
func (m *MockTransactionalSender) SerializeTxn() *roachpb.Transaction {
	return m.txn.Clone()
}

// UpdateStateOnRemoteRetryableErr is part of the TxnSender interface.
func (m *MockTransactionalSender) UpdateStateOnRemoteRetryableErr(
	ctx context.Context, pErr *roachpb.Error,
) *roachpb.Error {
	panic("unimplemented")
}

// DisablePipelining is part of the client.TxnSender interface.
func (m *MockTransactionalSender) DisablePipelining() error { return nil }

// MockTxnSenderFactory is a TxnSenderFactory producing MockTxnSenders.
type MockTxnSenderFactory struct {
	senderFunc func(context.Context, *roachpb.Transaction, roachpb.BatchRequest) (
		*roachpb.BatchResponse, *roachpb.Error)
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

// TransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) TransactionalSender(
	_ TxnType, coordMeta roachpb.TxnCoordMeta, _ roachpb.UserPriority,
) TxnSender {
	return NewMockTransactionalSender(f.senderFunc, &coordMeta.Txn)
}

// NonTransactionalSender is part of TxnSenderFactory.
func (f MockTxnSenderFactory) NonTransactionalSender() Sender {
	return nil
}
