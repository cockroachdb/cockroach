// Copyright 2015 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TxnType specifies whether a transaction is the root (parent)
// transaction, or a leaf (child) in a tree of client.Txns, as
// is used in a DistSQL flow.
type TxnType int

const (
	_ TxnType = iota
	// RootTxn specifies this sender is the root transaction, and is
	// responsible for aggregating all transactional state (see
	// TxnCoordMeta) and finalizing the transaction. The root txn is
	// responsible for heartbeating the transaction record.
	RootTxn
	// LeafTxn specifies this sender is for one of potentially many
	// distributed client transactions. The state from this transaction
	// must be propagated back to the root transaction and used to
	// augment its state before the transaction can be finalized. Leaf
	// transactions do not heartbeat the transaction record.
	//
	// Note: As leaves don't perform heartbeats, the transaction might be cleaned
	// up while this leaf is executing an operation. We rely on the cleanup
	// process poisoning the AbortSpans for all intents so that reads performed
	// through a leaf txn don't miss writes previously performed by the
	// transaction (at least not until the expiration of the GC period / abort
	// span entry timeout).
	LeafTxn
)

// Sender is implemented by modules throughout the crdb stack, on both the
// "client" and the "server", involved in passing along and ultimately
// evaluating requests (batches). The interface is now considered regrettable
// because it's too narrow and at times leaky.
// Notable implementors: client.Txn, kv.TxnCoordSender, storage.Node,
// storage.Store, storage.Replica.
type Sender interface {
	// Send sends a batch for evaluation.
	// The contract about whether both a response and an error can be returned
	// varies between layers.
	//
	// The caller retains ownership of all the memory referenced by the
	// BatchRequest; the callee is not allowed to hold on to any parts of it past
	// after it returns from the call (this is so that the client module can
	// allocate requests from a pool and reuse them). For example, the DistSender
	// makes sure that, if there are concurrent requests, it waits for all of them
	// before returning, even in error cases.
	//
	// Once the request reaches the `transport` module, anothern restriction
	// applies (particularly relevant for the case when the node that the
	// transport is talking to is local, and so there's not gRPC
	// marshaling/unmarshaling):
	// - the callee has to treat everything inside the BatchRequest as
	// read-only. This is so that the client module retains the right to pass
	// pointers into its internals, like for example the Transaction. This
	// wouldn't work if the server would be allowed to change the Transaction
	// willy-nilly.
	// TODO(andrei): The client does not currently use this last guarantee; it
	// clones the txn for every request. Given that a client.Txn can be used
	// concurrently, in order for the client to take advantage of this, it would
	// need to switch to a copy-on-write scheme so that its updates to the txn do
	// not race with the server reading it. We should do this to avoid the cloning
	// allocations. And to be frank, it'd be a good idea for the
	// BatchRequest/Response to generally stop carrying transactions; the requests
	// usually only need a txn id and some timestamp. The responses would ideally
	// contain a list of targeted instructions about what the client should
	// update, as opposed to a full txn that the client is expected to diff with
	// its copy and apply all the updates.
	Send(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
}

// TxnSender is the interface used to call into a CockroachDB instance
// when sending transactional requests. In addition to the usual
// Sender interface, TxnSender facilitates marshaling of transaction
// metadata between the "root" client.Txn and "leaf" instances.
type TxnSender interface {
	Sender

	// AnchorOnSystemConfigRange ensures that the transaction record, if/when it
	// will be created, will be created on the system config range. This is useful
	// because some commit triggers only work when the EndTransaction is evaluated
	// on that range.
	//
	// An error is returned if the transaction's key has already been set by
	// anything other than a previous call to this function (i.e. if the
	// transaction already performed any writes).
	// It is allowed to call this method multiple times.
	AnchorOnSystemConfigRange() error

	// GetMeta retrieves a copy of the TxnCoordMeta, which can be sent from root
	// to leaf transactions or the other way around. Can be combined via
	// AugmentMeta().
	//
	// If AnyTxnStatus is passed, then this function never returns errors.
	GetMeta(context.Context, TxnStatusOpt) (roachpb.TxnCoordMeta, error)

	// AugmentMeta combines the TxnCoordMeta from another distributed
	// TxnSender which is part of the same transaction.
	AugmentMeta(ctx context.Context, meta roachpb.TxnCoordMeta)

	// SetUserPriority sets the txn's priority.
	SetUserPriority(roachpb.UserPriority) error

	// SetDebugName sets the txn's debug name.
	SetDebugName(name string)

	// TxnStatus exports the txn's status.
	TxnStatus() roachpb.TransactionStatus

	// SetFixedTimestamp makes the transaction run in an unusual way, at a "fixed
	// timestamp": Timestamp and ReadTimestamp are set to ts, there's no clock
	// uncertainty, and the txn's deadline is set to ts such that the transaction
	// can't be pushed to a different timestamp.
	//
	// This is used to support historical queries (AS OF SYSTEM TIME queries and
	// backups). This method must be called on every transaction retry (but note
	// that retries should be rare for read-only queries with no clock uncertainty).
	SetFixedTimestamp(ctx context.Context, ts hlc.Timestamp)

	// ManualRestart bumps the transactions epoch, and can upgrade the timestamp
	// and priority.
	// An uninitialized timestamp can be passed to leave the timestamp alone.
	//
	// Used by the SQL layer which sometimes knows that a transaction will not be
	// able to commit and prefers to restart early.
	// It is also used after synchronizing concurrent actors using a txn when a
	// retryable error is seen.
	// TODO(andrei): this second use should go away once we move to a TxnAttempt
	// model.
	ManualRestart(context.Context, roachpb.UserPriority, hlc.Timestamp)

	// UpdateStateOnRemoteRetryableErr updates the txn in response to an error
	// encountered when running a request through the txn.
	UpdateStateOnRemoteRetryableErr(context.Context, *roachpb.Error) *roachpb.Error

	// DisablePipelining instructs the TxnSender not to pipeline requests. It
	// should rarely be necessary to call this method. It is only recommended for
	// transactions that need extremely precise control over the request ordering,
	// like the transaction that merges ranges together.
	DisablePipelining() error

	// ReadTimestamp returns the transaction's current read timestamp.
	// Note a transaction can be internally pushed forward in time before
	// committing so this is not guaranteed to be the commit timestamp.
	// Use CommitTimestamp() when needed.
	ReadTimestamp() hlc.Timestamp

	// CommitTimestamp returns the transaction's start timestamp.
	// The start timestamp can get pushed but the use of this
	// method will guarantee that the caller of this method sees
	// the push and thus calls this method again to receive the new
	// timestamp.
	CommitTimestamp() hlc.Timestamp

	// CommitTimestampFixed returns true if the commit timestamp has
	// been fixed to the start timestamp and cannot be pushed forward.
	CommitTimestampFixed() bool

	// IsSerializablePushAndRefreshNotPossible returns true if the transaction is
	// serializable, its timestamp has been pushed and there's no chance that
	// refreshing the read spans will succeed later (thus allowing the transaction
	// to commit and not be restarted). Used to detect whether the txn is
	// guaranteed to get a retriable error later.
	//
	// Note that this method allows for false negatives: sometimes the client only
	// figures out that it's been pushed when it sends an EndTransaction - i.e.
	// it's possible for the txn to have been pushed asynchoronously by some other
	// operation (usually, but not exclusively, by a high-priority txn with
	// conflicting writes).
	IsSerializablePushAndRefreshNotPossible() bool

	// Epoch returns the txn's epoch.
	Epoch() enginepb.TxnEpoch

	// SerializeTxn returns a clone of the transaction's current proto.
	// This is a nuclear option; generally client code shouldn't deal with protos.
	// However, this is used by DistSQL for sending the transaction over the wire
	// when it creates flows.
	SerializeTxn() *roachpb.Transaction
}

// TxnStatusOpt represents options for TxnSender.GetMeta().
type TxnStatusOpt int

const (
	// AnyTxnStatus means GetMeta() will return the info without checking the
	// txn's status.
	AnyTxnStatus TxnStatusOpt = iota
	// OnlyPending means GetMeta() will return an error if the transaction is not
	// in the pending state.
	// This is used when sending the txn from root to leaves so that we don't
	// create leaves that start up in an aborted state - which is not allowed.
	OnlyPending
)

// TxnSenderFactory is the interface used to create new instances
// of TxnSender.
type TxnSenderFactory interface {
	// TransactionalSender returns a sender to be used for transactional requests.
	// typ specifies whether the sender is the root or one of potentially many
	// child "leaf" nodes in a tree of transaction objects, as is created during a
	// DistSQL flow.
	// coordMeta is the TxnCoordMeta which contains the transaction whose requests
	// this sender will carry.
	TransactionalSender(
		typ TxnType, coordMeta roachpb.TxnCoordMeta, pri roachpb.UserPriority,
	) TxnSender
	// NonTransactionalSender returns a sender to be used for non-transactional
	// requests. Generally this is a sender that TransactionalSender() wraps.
	NonTransactionalSender() Sender
}

// SenderFunc is an adapter to allow the use of ordinary functions
// as Senders.
type SenderFunc func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

// Send calls f(ctx, c).
func (f SenderFunc) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return f(ctx, ba)
}

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
	return fmt.Errorf("unimplemented")
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

// NonTransactionalFactoryFunc is a TxnSenderFactory that cannot, in fact,
// create any transactional senders, only non-transactional ones.
type NonTransactionalFactoryFunc SenderFunc

var _ TxnSenderFactory = NonTransactionalFactoryFunc(nil)

// TransactionalSender is part of the TxnSenderFactory.
func (f NonTransactionalFactoryFunc) TransactionalSender(
	_ TxnType, _ roachpb.TxnCoordMeta, _ roachpb.UserPriority,
) TxnSender {
	panic("not supported")
}

// NonTransactionalSender is part of the TxnSenderFactory.
func (f NonTransactionalFactoryFunc) NonTransactionalSender() Sender {
	return SenderFunc(f)
}

// SendWrappedWith is a convenience function which wraps the request in a batch
// and sends it via the provided Sender and headers. It returns the unwrapped
// response or an error. It's valid to pass a `nil` context; an empty one is
// used in that case.
func SendWrappedWith(
	ctx context.Context, sender Sender, h roachpb.Header, args roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	ba := roachpb.BatchRequest{}
	ba.Header = h
	ba.Add(args)

	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}
	unwrappedReply := br.Responses[0].GetInner()
	header := unwrappedReply.Header()
	header.Txn = br.Txn
	unwrappedReply.SetHeader(header)
	return unwrappedReply, nil
}

// SendWrapped is identical to SendWrappedWith with a zero header.
// TODO(tschottdorf): should move this to testutils and merge with
// other helpers which are used, for example, in `storage`.
func SendWrapped(
	ctx context.Context, sender Sender, args roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	return SendWrappedWith(ctx, sender, roachpb.Header{}, args)
}

// Wrap returns a Sender which applies the given function before delegating to
// the supplied Sender.
func Wrap(sender Sender, f func(roachpb.BatchRequest) roachpb.BatchRequest) Sender {
	return SenderFunc(func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return sender.Send(ctx, f(ba))
	})
}
