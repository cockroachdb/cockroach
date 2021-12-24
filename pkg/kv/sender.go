// Copyright 2015 The Cockroach Authors.
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
)

// TxnType specifies whether a transaction is the root (parent)
// transaction, or a leaf (child) in a tree of client.Txns, as
// is used in a DistSQL flow.
type TxnType int

const (
	_ TxnType = iota
	// RootTxn specifies this sender is the root transaction, and is
	// responsible for aggregating all transactional state and
	// finalizing the transaction. The root txn is responsible for
	// heartbeating the transaction record.
	RootTxn
	// LeafTxn specifies this sender is for one of potentially many
	// distributed client transactions. The state from this transaction
	// must be propagated back to the root transaction and used to
	// augment its state before the transaction can be finalized. Leaf
	// transactions do not heartbeat the transaction record.
	//
	// Note: As leaves don't perform heartbeats, the transaction might
	// be cleaned up while this leaf is executing an operation. We rely
	// on the cleanup process poisoning the AbortSpans for all intents
	// so that reads performed through a leaf txn don't miss writes
	// previously performed by the transaction (at least not until the
	// expiration of the GC period / abort span entry timeout).
	LeafTxn
)

// Sender is implemented by modules throughout the crdb stack, on both
// the "client" and the "server", involved in passing along and
// ultimately evaluating requests (batches). The interface is now
// considered regrettable because it's too narrow and at times leaky.
// Notable implementors: client.Txn, kv.TxnCoordSender, storage.Node,
// storage.Store, storage.Replica.
type Sender interface {
	// Send sends a batch for evaluation. Either a response or an error is
	// returned.
	//
	// The caller retains ownership of all the memory referenced by the
	// BatchRequest; the callee is not allowed to hold on to any parts
	// of it past after it returns from the call (this is so that the
	// client module can allocate requests from a pool and reuse
	// them). For example, the DistSender makes sure that, if there are
	// concurrent requests, it waits for all of them before returning,
	// even in error cases.
	//
	// Once the request reaches the `transport` module, anothern
	// restriction applies (particularly relevant for the case when the
	// node that the transport is talking to is local, and so there's
	// not gRPC marshaling/unmarshaling):
	// - the callee has to treat everything inside the BatchRequest as
	// read-only. This is so that the client module retains the right to
	// pass pointers into its internals, like for example the
	// Transaction. This wouldn't work if the server would be allowed to
	// change the Transaction willy-nilly.
	//
	// TODO(andrei): The client does not currently use this last
	// guarantee; it clones the txn for every request. Given that a
	// client.Txn can be used concurrently, in order for the client to
	// take advantage of this, it would need to switch to a
	// copy-on-write scheme so that its updates to the txn do not race
	// with the server reading it. We should do this to avoid the
	// cloning allocations. And to be frank, it'd be a good idea for the
	// BatchRequest/Response to generally stop carrying transactions;
	// the requests usually only need a txn id and some timestamp. The
	// responses would ideally contain a list of targeted instructions
	// about what the client should update, as opposed to a full txn
	// that the client is expected to diff with its copy and apply all
	// the updates.
	Send(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
}

// TxnSender is the interface used to call into a CockroachDB instance
// when sending transactional requests. In addition to the usual
// Sender interface, TxnSender facilitates marshaling of transaction
// metadata between the "root" client.Txn and "leaf" instances.
type TxnSender interface {
	Sender

	// AnchorOnSystemConfigRange ensures that the transaction record,
	// if/when it will be created, will be created on the system config
	// range. This is useful because some commit triggers only work when
	// the EndTxn is evaluated on that range.
	//
	// An error is returned if the transaction's key has already been
	// set by anything other than a previous call to this function
	// (i.e. if the transaction already performed any writes).
	// It is allowed to call this method multiple times.
	AnchorOnSystemConfigRange() error

	// GetLeafTxnInputState retrieves the input state necessary and
	// sufficient to initialize a LeafTxn from the current RootTxn.
	//
	// If AnyTxnStatus is passed, then this function never returns
	// errors.
	GetLeafTxnInputState(context.Context, TxnStatusOpt) (*roachpb.LeafTxnInputState, error)

	// GetLeafTxnFinalState retrieves the final state of a LeafTxn
	// necessary and sufficient to update a RootTxn with progress made
	// on its behalf by the LeafTxn.
	GetLeafTxnFinalState(context.Context, TxnStatusOpt) (*roachpb.LeafTxnFinalState, error)

	// UpdateRootWithLeafFinalState updates a RootTxn using the final
	// state of a LeafTxn.
	UpdateRootWithLeafFinalState(context.Context, *roachpb.LeafTxnFinalState)

	// SetUserPriority sets the txn's priority.
	SetUserPriority(roachpb.UserPriority) error

	// SetDebugName sets the txn's debug name.
	SetDebugName(name string)

	// String returns a string representation of the txn.
	String() string

	// TxnStatus exports the txn's status.
	TxnStatus() roachpb.TransactionStatus

	// CreateSavepoint establishes a savepoint.
	// This method is only valid when called on RootTxns.
	//
	// Committing (or aborting) the transaction causes every open
	// savepoint to be released (or, respectively, rolled back)
	// implicitly.
	CreateSavepoint(context.Context) (SavepointToken, error)

	// RollbackToSavepoint rolls back to the given savepoint.
	// All savepoints "under" the savepoint being rolled back
	// are also rolled back and their token must not be used any more.
	// The token of the savepoint being rolled back remains valid
	// and can be reused later (e.g. to release or roll back again).
	// Aborting the txn implicitly rolls back all savepoints
	// that are still open.
	//
	// This method is only valid when called on RootTxns.
	RollbackToSavepoint(context.Context, SavepointToken) error

	// ReleaseSavepoint releases the given savepoint.
	// The savepoint must not have been rolled back or released already.
	// All savepoints "under" the savepoint being released
	// are also released and their token must not be used any more.
	// Committing the txn implicitly releases all savepoints
	// that are still open.
	//
	// This method is only valid when called on RootTxns.
	ReleaseSavepoint(context.Context, SavepointToken) error

	// SetFixedTimestamp makes the transaction run in an unusual way, at
	// a "fixed timestamp": Timestamp and ReadTimestamp are set to ts,
	// there's no clock uncertainty, and the txn's deadline is set to ts
	// such that the transaction can't be pushed to a different
	// timestamp.
	//
	// This is used to support historical queries (AS OF SYSTEM TIME queries
	// and backups). This method must be called on every transaction retry
	// (but note that retries should be rare for read-only queries with no
	// clock uncertainty). The method must not be called after the
	// transaction has been used in the current epoch to read or write.
	SetFixedTimestamp(ctx context.Context, ts hlc.Timestamp) error

	// ManualRestart bumps the transactions epoch, and can upgrade the
	// timestamp and priority.
	// An uninitialized timestamp can be passed to leave the timestamp
	// alone.
	//
	// Used by the SQL layer which sometimes knows that a transaction
	// will not be able to commit and prefers to restart early.
	// It is also used after synchronizing concurrent actors using a txn
	// when a retryable error is seen.
	// TODO(andrei): this second use should go away once we move to a
	// TxnAttempt model.
	ManualRestart(context.Context, roachpb.UserPriority, hlc.Timestamp)

	// UpdateStateOnRemoteRetryableErr updates the txn in response to an
	// error encountered when running a request through the txn.
	UpdateStateOnRemoteRetryableErr(context.Context, *roachpb.Error) *roachpb.Error

	// DisablePipelining instructs the TxnSender not to pipeline
	// requests. It should rarely be necessary to call this method. It
	// is only recommended for transactions that need extremely precise
	// control over the request ordering, like the transaction that
	// merges ranges together.
	DisablePipelining() error

	// ReadTimestamp returns the transaction's current read timestamp.
	// Note a transaction can be internally pushed forward in time
	// before committing so this is not guaranteed to be the commit
	// timestamp. Use CommitTimestamp() when needed.
	ReadTimestamp() hlc.Timestamp

	// CommitTimestamp returns the transaction's start timestamp.
	//
	// This method is guaranteed to always return the same value while
	// the transaction is open. To achieve this, the first call to this
	// method also anchors the start timestamp and prevents the sender
	// from automatically pushing transactions forward (i.e. handling
	// certain forms of contention / txn conflicts automatically).
	//
	// In other words, using this method just once increases the
	// likelihood that a retry error will bubble up to a client.
	//
	// See CommitTimestampFixed() below.
	CommitTimestamp() hlc.Timestamp

	// CommitTimestampFixed returns true if the commit timestamp has
	// been fixed to the start timestamp and cannot be pushed forward.
	CommitTimestampFixed() bool

	// ProvisionalCommitTimestamp returns the transaction's provisional
	// commit timestamp. This can move forward throughout the txn's
	// lifetime. See the explanatory comments for the WriteTimestamp
	// field on TxnMeta.
	ProvisionalCommitTimestamp() hlc.Timestamp

	// RequiredFrontier returns the largest timestamp at which the
	// transaction may read values when performing a read-only
	// operation.
	RequiredFrontier() hlc.Timestamp

	// IsSerializablePushAndRefreshNotPossible returns true if the
	// transaction is serializable, its timestamp has been pushed and
	// there's no chance that refreshing the read spans will succeed
	// later (thus allowing the transaction to commit and not be
	// restarted). Used to detect whether the txn is guaranteed to get a
	// retriable error later.
	//
	// Note that this method allows for false negatives: sometimes the
	// client only figures out that it's been pushed when it sends an
	// EndTxn - i.e. it's possible for the txn to have been pushed
	// asynchoronously by some other operation (usually, but not
	// exclusively, by a high-priority txn with conflicting writes).
	IsSerializablePushAndRefreshNotPossible() bool

	// Active returns true iff some commands have been performed with
	// this txn already.
	//
	// TODO(knz): Remove this, see
	// https://github.com/cockroachdb/cockroach/issues/15012
	Active() bool

	// Epoch returns the txn's epoch.
	Epoch() enginepb.TxnEpoch

	// IsLocking returns whether the transaction has begun acquiring locks.
	IsLocking() bool

	// PrepareRetryableError generates a
	// TransactionRetryWithProtoRefreshError with a payload initialized
	// from this txn.
	PrepareRetryableError(ctx context.Context, msg string) error

	// TestingCloneTxn returns a clone of the transaction's current
	// proto. This is for use by tests only. Use
	// GetLeafTxnInitialState() instead when creating leaf transactions.
	TestingCloneTxn() *roachpb.Transaction

	// Step creates a sequencing point in the current transaction. A
	// sequencing point establishes a snapshot baseline for subsequent
	// read-only operations: until the next sequencing point, read-only
	// operations observe the data at the time the snapshot was
	// established and ignore writes performed since.
	//
	// Step() can only be called after stepping mode has been enabled
	// using ConfigureStepping(SteppingEnabled).
	//
	// The method is idempotent.
	Step(context.Context) error

	// SetReadSeqNum sets the read sequence point for the current transaction.
	SetReadSeqNum(seq enginepb.TxnSeq) error

	// ConfigureStepping sets the sequencing point behavior.
	//
	// Note that a Sender is initially in the non-stepping mode,
	// i.e. uses reads-own-writes by default. This makes the step
	// behavior opt-in and backward-compatible with existing code which
	// does not need it.
	//
	// Calling ConfigureStepping(SteppingEnabled) when the stepping mode
	// is currently disabled implies calling Step(), for convenience.
	ConfigureStepping(ctx context.Context, mode SteppingMode) (prevMode SteppingMode)

	// GetSteppingMode accompanies ConfigureStepping. It is provided
	// for use in tests and assertion checks.
	GetSteppingMode(ctx context.Context) (curMode SteppingMode)

	// ManualRefresh attempts to refresh a transactions read timestamp up to its
	// provisional commit timestamp. In the case that the two are already the
	// same, it is a no-op. The reason one might want to do that is to ensure
	// that a transaction can commit without experiencing another push.
	//
	// A transaction which has proven all of its intents and has been fully
	// refreshed and does not perform any additional reads or writes that does not
	// contend with any other transactions will not be pushed further. This
	// method's reason for existence is to ensure that range merge requests can
	// be pushed but then can later commit without the possibility of needing to
	// refresh reads performed on the RHS after the RHS has been subsumed but
	// before the merge transaction completed.
	ManualRefresh(ctx context.Context) error

	// DeferCommitWait defers the transaction's commit-wait operation, passing
	// responsibility of commit-waiting from the TxnSender to the caller of this
	// method. The method returns a function which the caller must eventually
	// run if the transaction completes without error. This function is safe to
	// call multiple times.
	//
	// WARNING: failure to call the returned function could lead to consistency
	// violations where a future, causally dependent transaction may fail to
	// observe the writes performed by this transaction.
	DeferCommitWait(ctx context.Context) func(context.Context) error

	// GetTxnRetryableErr returns an error if the TxnSender had a retryable error,
	// otherwise nil. In this state Send() always fails with the same retryable
	// error. ClearTxnRetryableErr can be called to clear this error and make
	// TxnSender usable again.
	GetTxnRetryableErr(ctx context.Context) *roachpb.TransactionRetryWithProtoRefreshError

	// ClearTxnRetryableErr clears the retryable error, if any.
	ClearTxnRetryableErr(ctx context.Context)
}

// SteppingMode is the argument type to ConfigureStepping.
type SteppingMode bool

const (
	// SteppingDisabled is the default mode, where each read can
	// observe the latest write.
	SteppingDisabled SteppingMode = false

	// SteppingEnabled can be set to indicate that read operations
	// operate on a snapshot taken at the latest Step() invocation.
	SteppingEnabled SteppingMode = true
)

// SavepointToken represents a savepoint.
type SavepointToken interface {
	// Initial returns true if this savepoint has been created before performing
	// any KV operations. If so, it is possible to rollback to it after a
	// retriable error. If not, then rolling back to it after a retriable error
	// will return the retriable error again because reads might have been
	// evaluated before the savepoint and such reads cannot have their timestamp
	// forwarded without a refresh.
	Initial() bool
}

// TxnStatusOpt represents options for TxnSender.GetMeta().
type TxnStatusOpt int

const (
	// AnyTxnStatus means GetMeta() will return the info without
	// checking the txn's status.
	AnyTxnStatus TxnStatusOpt = iota
	// OnlyPending means GetMeta() will return an error if the
	// transaction is not in the pending state.
	// This is used when sending the txn from root to leaves so that we
	// don't create leaves that start up in an aborted state - which is
	// not allowed.
	OnlyPending
)

// TxnSenderFactory is the interface used to create new instances
// of TxnSender.
type TxnSenderFactory interface {
	// RootTransactionalSender returns a root sender to be used for
	// transactional requests. txn contains the transaction whose
	// requests this sender will carry.
	RootTransactionalSender(
		txn *roachpb.Transaction, pri roachpb.UserPriority,
	) TxnSender

	// LeafTransactionalSender returns a leaf sender to be used for
	// transactional requests on behalf of a root.
	LeafTransactionalSender(tis *roachpb.LeafTxnInputState) TxnSender

	// NonTransactionalSender returns a sender to be used for
	// non-transactional requests. Generally this is a sender that
	// TransactionalSender() wraps.
	NonTransactionalSender() Sender
}

// SenderFunc is an adapter to allow the use of ordinary functions as
// Senders.
type SenderFunc func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

// Send calls f(ctx, c).
func (f SenderFunc) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return f(ctx, ba)
}

// NonTransactionalFactoryFunc is a TxnSenderFactory that cannot, in
// fact, create any transactional senders, only non-transactional
// ones.
type NonTransactionalFactoryFunc SenderFunc

var _ TxnSenderFactory = NonTransactionalFactoryFunc(nil)

// RootTransactionalSender is part of the TxnSenderFactory.
func (f NonTransactionalFactoryFunc) RootTransactionalSender(
	_ *roachpb.Transaction, _ roachpb.UserPriority,
) TxnSender {
	panic("not supported")
}

// LeafTransactionalSender is part of the TxnSenderFactory.
func (f NonTransactionalFactoryFunc) LeafTransactionalSender(
	_ *roachpb.LeafTxnInputState,
) TxnSender {
	panic("not supported")
}

// NonTransactionalSender is part of the TxnSenderFactory.
func (f NonTransactionalFactoryFunc) NonTransactionalSender() Sender {
	return SenderFunc(f)
}

// SendWrappedWith is a convenience function which wraps the request
// in a batch and sends it via the provided Sender and headers. It
// returns the unwrapped response or an error. It's valid to pass a
// `nil` context; an empty one is used in that case.
func SendWrappedWith(
	ctx context.Context, sender Sender, h roachpb.Header, args roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	return SendWrappedWithAdmission(ctx, sender, h, roachpb.AdmissionHeader{}, args)
}

// SendWrappedWithAdmission is a convenience function which wraps the request
// in a batch and sends it via the provided Sender and headers. It returns the
// unwrapped response or an error. It's valid to pass a `nil` context; an
// empty one is used in that case.
func SendWrappedWithAdmission(
	ctx context.Context,
	sender Sender,
	h roachpb.Header,
	ah roachpb.AdmissionHeader,
	args roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	ba := roachpb.BatchRequest{}
	ba.Header = h
	ba.AdmissionHeader = ah
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
