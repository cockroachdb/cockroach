// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package concurrency provides a concurrency manager structure that
// encapsulates the details of concurrency control and contention handling for
// serializable key-value transactions.
package concurrency

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Manager is a structure that sequences incoming requests and provides
// isolation between requests that intend to perform conflicting operations.
// During sequencing, conflicts are discovered and any found are resolved
// through a combination of passive queuing and active pushing. Once a request
// has been sequenced, it is free to evaluate without concerns of conflicting
// with other in-flight requests due to the isolation provided by the manager.
// This isolation is guaranteed for the lifetime of the request but terminates
// once the request completes.
//
// Transactions require isolation both within requests and across requests. The
// manager accommodates this by allowing transactional requests to acquire
// locks, which outlive the requests themselves. Locks extend the duration of
// the isolation provided over specific keys to the lifetime of the lock-holder
// transaction itself. They are (typically) only released when the transaction
// commits or aborts. Other requests that find these locks while being sequenced
// wait on them to be released in a queue before proceeding. Because locks are
// checked during sequencing, requests are guaranteed access to all declared
// keys after they have been sequenced. In other words, locks don't need to be
// checked again during evaluation.
//
// However, at the time of writing, not all locks are stored directly under the
// manager's control, so not all locks are discoverable during sequencing.
// Specifically, write intents (replicated, exclusive locks) are stored inline
// in the MVCC keyspace, so they are not detectable until request evaluation
// time. To accommodate this form of lock storage, the manager exposes a
// RetryReqAfterWriteIntentError method, which can be used in conjunction with a
// retry loop around evaluation to integrate external locks with the concurrency
// manager structure. In the future, we intend to pull all locks, including
// those associated with write intents, into the concurrency manager directly
// through a replicated lock table structure.
//
// Fairness is ensured between requests. If any two requests conflict then the
// request that arrived first will be sequenced first. As such, sequencing
// guarantees FIFO semantics. The one exception to this is that a request that
// is part of a transaction which has already acquired a lock does not need to
// wait on that lock during sequencing, and can therefore ignore any queue that
// has formed on the lock.
//
// Internal Components
//
// The concurrency manager is composed of a number of internal synchronization,
// bookkeeping, and queueing structures. Each of these is discussed in more
// detail on their interface definition. The following diagram details how the
// components are tied together:
//
//  +---------------------+-------------------------------------------------+
//  | concurrency.Manager |                                                 |
//  +---------------------+                                                 |
//  |                                                                       |
//  +------------+  acquire   +--------------+        acquire               |
//    Sequence() |--->--->--->| latchManager |<---<---<---<---<---<---<--+  |
//  +------------+            +--------------+                           |  |
//  |                           / check locks + wait queues              |  |
//  |                          v  if conflict, enter q & drop latches    ^  |
//  |             +---------------------------------------------------+  |  |
//  |             | [ lockTable ]                                     |  |  |
//  |             | [    key1   ]    -------------+-----------------+ |  ^  |
//  |             | [    key2   ]  /  MVCCLock:   | lockWaitQueue:  |--<------<---+
//  |             | [    key3   ]-{   - lock type | +-[a]<-[b]<-[c] | |  |  |     |
//  |             | [    key4   ]  \  - txn  meta | |  (no latches) |-->-^  |     |
//  |             | [    key5   ]    -------------+-|---------------+ |     |     |
//  |             | [    ...    ]                   v                 |     |     ^
//  |             +---------------------------------|-----------------+     |     | if lock found during eval
//  |                     |                         |                       |     | - enter lockWaitQueue
//  |                     |       +- may be remote -+--+                    |     | - drop latches
//  |                     |       |                    |                    |     | - wait for lock holder
//  |                     v       v                    ^                    |     |
//  |                     |    +--------------------------+                 |     ^
//  |                     |    | txnWaitQueue:            |                 |     |
//  |                     |    | (located on txn record's |                 |     |
//  |                     v    |  leaseholder replica)    |                 |     |
//  |                     |    |--------------------------|                 |     ^
//  |                     |    | [txn1] [txn2] [txn3] ... |                 |     |
//  |                     |    +--------------------------+                 |     |
//  |                     |                                                 |     |
//  |                     +--> hold latches ---> remain at head of queues -----> evaluate ...
//  |                                                                       |
//  +----------+                                                            |
//    Finish() | ---> exit wait queues ---> drop latches ----------------------> respond  ...
//  +----------+                                                            |
//  |                                                                       |
//  +-----------------------------------------------------------------------+
//
// See the comments on individual components for a more detailed look at their
// interface and inner-workings.
//
// At a high-level, requests enter the concurrency manager and immediately
// acquire latches from the latchManager to serialize access to the keys that
// they intend to touch. This latching takes into account the keys being
// accessed, the MVCC timestamp of accesses, and the access method being used
// (read vs. write) to allow for concurrency where possible. This has the effect
// of queuing on conflicting in-flight operations until their completion.
//
// Once latched, the request consults the lockTable to check for any conflicting
// locks owned by other transactions. If any are found, the request enters the
// corresponding lockWaitQueue and its latches are dropped. The head of the
// lockWaitQueue pushes the owner of the lock through a remote RPC that ends up
// in the pushee's txnWaitQueue. This queue exists on the leaseholder replica of
// the range that contains the pushee's transaction record. Other entries in the
// queue wait for the head of the queue, eventually pushing it to detect
// deadlocks. Once the lock is cleared, the head of the queue reacquires latches
// and attempts to proceed while remains at the head of that lockWaitQueue to
// ensure fairness.
//
// Once a request is latched and observes no conflicting locks in the lockTable
// and no conflicting lockWaitQueues that it is not already the head of, the
// request can proceed to evaluate. During evaluation, the request may insert or
// remove locks from the lockTable for its own transaction. This is performed
// transparently by a lockAwareBatch/lockAwareReadWriter. The request may also
// need to consult its own locks to properly interpret the corresponding intents
// in the MVCC keyspace. This is performed transparently by a lockAwareIter.
//
// When the request completes, it exits any lockWaitQueues that it was at the
// head of and releases its latches. However, any locks that it inserted into
// the lockTable remain.
type Manager interface {
	requestSequencer
	contentionHandler
	transactionManager
	rangeUpdateListener
	metricExporter
}

// requestSequencer is concerned with the sequencing of concurrent requests.
type requestSequencer interface {
	// SequenceReq acquires latches, checks for locks, and queues behind and/or
	// pushes other transactions to resolve any conflicts. Once sequenced, the
	// request is guaranteed sufficient isolation for the duration of its
	// evaluation, until its guard is released.
	// NOTE: this last part will not be true until replicated locks are pulled
	// into the concurrency manager.
	//
	// An optional existing request guard can be provided to SequenceReq. This
	// allows the request's position in lock wait-queues to be retained across
	// sequencing attempts. If provided, the guard should not be holding latches
	// already. This typically means that the guard was acquired through a call
	// to a contentionHandler method.
	//
	// If the method returns a non-nil request guard then the caller must ensure
	// that the guard is eventually released by passing it to FinishReq.
	//
	// Alternatively, the concurrency manager may be able to serve the request
	// directly, in which case it will return a Response for the request. If it
	// does so, it will not return a request guard.
	SequenceReq(context.Context, *Guard, Request) (*Guard, Response, *Error)
	// FinishReq marks the request as complete, releasing any protection
	// the request had against conflicting requests and allowing conflicting
	// requests that are blocked on this one to proceed. The guard should not
	// be used after being released.
	FinishReq(*Guard)
}

// contentionHandler is concerned with handling contention-related errors. This
// typically involves preparing the request to be queued upon a retry.
type contentionHandler interface {
	// HandleWriterIntentError consumes a WriteIntentError by informing the
	// concurrency manager about the replicated write intent that was missing
	// from its lock table. After doing so, it enqueues the request that hit the
	// error in the lock's wait-queue (but does not wait) and releases the
	// guard's latches. It returns an updated guard reflecting this change.
	HandleWriterIntentError(context.Context, *Guard, *roachpb.WriteIntentError) (*Guard, *Error)
	// HandleTransactionPushError consumes a TransactionPushError by informing
	// the concurrency manager about a transaction record that could not be
	// pushes. After doing so, it releases the guard's latches. It returns an
	// updated guard reflecting this change.
	HandleTransactionPushError(context.Context, *Guard, *roachpb.TransactionPushError) *Guard
}

// transactionManager is concerned with tracking transactions that have their
// records stored on the manager's range.
type transactionManager interface {
	// OnTransactionUpdated informs the concurrency manager that a transaction's
	// status was updated by a successful transaction state transition.
	OnTransactionUpdated(context.Context, *roachpb.Transaction)
	// OnLockAcquired informs the concurrency manager that a transaction has
	// acquired a new lock or updated an existing lock that it already held.
	OnLockAcquired(context.Context, roachpb.Intent)
	// OnLockReleased informs the concurrency manager that a transaction has
	// released a lock that it previously held.
	OnLockReleased(context.Context, roachpb.Intent)
	// GetDependents returns a set of transactions waiting on the specified
	// transaction either directly or indirectly. The method is used to perform
	// deadlock detection. See txnWaitQueue for more.
	GetDependents(uuid.UUID) []uuid.UUID
}

// rangeUpdateListener is concerned with observing updates to the concurrency
// manager's range.
type rangeUpdateListener interface {
	// OnDescriptorUpdated informs the manager that its range's descriptor has
	// been updated.
	OnDescriptorUpdated(*roachpb.RangeDescriptor)
	// OnLeaseUpdated informs the concurrency manager that its range's lease has
	// been updated. The argument indicates whether this manager's replica is
	// the leaseholder going forward.
	OnLeaseUpdated(bool)
	// OnSplit informs the concurrency manager that its range has split of a new
	// range to its RHS.
	OnSplit()
	// OnMerge informs the concurrency manager that its range has merged into
	// its LHS neighbor. This is not called on the range being merged into.
	OnMerge()
}

// metricExporter is concerned with providing observability into the state of
// the concurrency manager.
type metricExporter interface {
	// TODO
	// LatchMetrics()
	// LockTableMetrics()
	// TxnWaitQueueMetrics()
}

///////////////////////////////////
// External API Type Definitions //
///////////////////////////////////

// Request is the input to Manager.SequenceReq. The struct contains all of the
// information necessary to sequence a KV request and determine which locks and
// other in-flight requests it conflicts with.
type Request struct {
	// The (optional) transaction that sent the request.
	Txn *roachpb.Transaction
	// The timestamp that the request should evaluate at.
	Timestamp hlc.Timestamp
	// The priority of the request. Only set if Txn is nil.
	Priority roachpb.UserPriority
	// The consistency level of the request. Only set if Txn is nil.
	ReadConsistency roachpb.ReadConsistencyType
	// The individual requests in the batch.
	Requests []roachpb.RequestUnion
	// The maximal set of spans that the request will access.
	Spans *spanset.SpanSet
}

// Guard is returned from Manager.SequenceReq. The guard is passed back in to
// Manager.FinishReq to release the request's resources when it has completed.
type Guard struct {
	req  Request
	lg   latchGuard
	wqgs []lockWaitQueueGuard
}

// Response is a slice of responses to requests in a batch.
type Response = []roachpb.ResponseUnion

// Error is an alias for a roachpb.Error.
type Error = roachpb.Error

///////////////////////////////////
// Internal Structure Interfaces //
///////////////////////////////////

// latchManager serializes access to keys and key ranges.
//
// See additional documentation in pkg/storage/spanlatch.
type latchManager interface {
	// Acquires latches, providing mutual exclusion for conflicting requests.
	Acquire(context.Context, Request) (latchGuard, *Error)
	// Releases latches.
	Release(latchGuard)
}

// latchGuard is a handle to a set of acquired key latches.
type latchGuard interface{}

// lockTable holds a collection of locks acquired by in-progress transactions.
// Each lock in the table has a possibly-empty lock wait-queue associated with
// it, where conflicting transactions can queue while waiting for the lock to be
// released.
//
//  +---------------------------------------------------+
//  | [ lockTable ]                                     |
//  | [    key1   ]    -------------+-----------------+ |
//  | [    key2   ]  /  MVCCLock:   | lockWaitQueue:  | |
//  | [    key3   ]-{   - lock type | <-[a]<-[b]<-[c] | |
//  | [    key4   ]  \  - txn meta  |    (no latches) | |
//  | [    key5   ]    -------------+-----------------+ |
//  | [    ...    ]                                     |
//  +---------------------------------------------------+
//
// TODO(nvanbenschoten): document further.
type lockTable interface {
	// AcquireLock informs the lockTable that a new lock was acquired or an
	// existing lock was updated.
	AcquireLock(roachpb.Intent)
	// AcquireLock informs the lockTable that an existing lock was released.
	ReleaseLock(roachpb.Intent)
	// AddDiscoveredLock informs the lockTable of a lock that was discovered
	// during evaluation that the lockTable wasn't previously tracking.
	AddDiscoveredLock(Request, roachpb.Intent) lockWaitQueueGuard
	// ScanAndEnqueue scans over the spans that the request will access and
	// enqueues the request in the lock wait-queue of any conflicting locks
	// encountered.
	ScanAndEnqueue(Request, *Guard) []lockWaitQueueGuard
	// Dequeue removes the guard from its lock wait-queue.
	Dequeue(lockWaitQueueGuard)
	// SetBounds sets the key bounds of the lockTable.
	SetBounds(start, end roachpb.RKey)
	// Clear removes all locks and lock wait-queues from the lockTable.
	Clear()
}

// lockWaitQueueWaiter is concerned with waiting in a lock wait-queues for
// an individual lock held by a conflicting transaction.
//
// TODO(nvanbenschoten): document further.
type lockWaitQueueWaiter interface {
	MustWaitOnAny([]lockWaitQueueGuard) bool
	WaitOn(context.Context, Request, lockWaitQueueGuard) *Error
	SetBounds(start, end roachpb.RKey)
}

// lockWaitQueueGuard is a handle to an entry in a lock wait-queue.
type lockWaitQueueGuard interface{}

// txnWaitQueue holds a collection of wait-queues for transaction records.
//
// TODO(nvanbenschoten): document further.
// TODO(nvanbenschoten): if we exposed a "queue guard" interface, we could
//   make stronger guarantees around cleaning up enqueued txns when there
//   are no waiters.
type txnWaitQueue interface {
	EnqueueTxn(*roachpb.Transaction)
	UpdateTxn(context.Context, *roachpb.Transaction)
	GetDependents(uuid.UUID) []uuid.UUID
	MaybeWaitForPush(context.Context, *roachpb.PushTxnRequest) (*roachpb.PushTxnResponse, *Error)
	MaybeWaitForQuery(context.Context, *roachpb.QueryTxnRequest) *Error
	Enable()
	Clear(disable bool)
}
