// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency

import (
	"container/list"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Default upper bound on the number of locks in a lockTable.
const defaultLockTableSize = 10000

// The kind of waiting that the request is subject to.
type waitKind int

const (
	_ waitKind = iota

	// waitFor indicates that the request is waiting on another transaction to
	// release its locks or complete its own request. waitingStates with this
	// waitKind will provide information on who the request is waiting on. The
	// request will likely want to eventually push the conflicting transaction.
	waitFor

	// waitForDistinguished is a sub-case of waitFor. It implies everything that
	// waitFor does and additionally indicates that the request is currently the
	// "distinguished waiter". A distinguished waiter is responsible for taking
	// extra actions, e.g. immediately pushing the transaction it is waiting
	// for. If there are multiple requests in the waitFor state waiting on the
	// same transaction, at least one will be a distinguished waiter.
	waitForDistinguished

	// waitElsewhere is used when the lockTable is under memory pressure and is
	// clearing its internal queue state. Like the waitFor* states, it informs
	// the request who it is waiting for so that deadlock detection works.
	// However, sequencing information inside the lockTable is mostly discarded.
	waitElsewhere

	// waitSelf indicates that a different request from the same transaction has
	// claimed the lock already. This request should sit tight and wait for a new
	// notification without pushing anyone.
	//
	// By definition, the lock cannot be held at this point -- if it were, another
	// request from the same transaction would not be in the lock's wait-queues,
	// obviating the need for this state.
	//
	// TODO(arul): this waitSelf state + claimantTxn stuff won't extend well to
	// multiple lock holders. See TODO in informActiveWaiters.
	waitSelf

	// waitQueueMaxLengthExceeded indicates that the request attempted to enter a
	// lock wait-queue as a writer and found that the queue's length was already
	// equal to or exceeding the request's configured maximum. As a result, the
	// request was rejected.
	waitQueueMaxLengthExceeded

	// doneWaiting indicates that the request is done waiting on this pass
	// through the lockTable and should make another call to ScanAndEnqueue.
	doneWaiting
)

// The current waiting state of the request.
//
// See the detailed comment about "Waiting logic" on lockTableGuardImpl.
type waitingState struct {
	kind waitKind

	// Fields below are populated for waitFor* and waitElsewhere kinds.

	// Represents who the request is waiting for. The conflicting
	// transaction may be a lock holder of a conflicting lock or a
	// conflicting request being sequenced through the same lockTable.
	txn           *enginepb.TxnMeta // always non-nil in waitFor{,Distinguished,Self} and waitElsewhere
	key           roachpb.Key       // the key of the conflict
	held          bool              // is the conflict a held lock?
	queuedWriters int               // how many writers are waiting?
	queuedReaders int               // how many readers are waiting?

	// Represents the lock strength of the action that the request was trying to
	// perform when it hit the conflict. E.g. was it trying to perform a (possibly
	// locking) read or write an Intent?
	guardStrength lock.Strength
}

// String implements the fmt.Stringer interface.
func (s waitingState) String() string {
	return redact.StringWithoutMarkers(s)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (s waitingState) SafeFormat(w redact.SafePrinter, _ rune) {
	switch s.kind {
	case waitFor, waitForDistinguished:
		distinguished := redact.SafeString("")
		if s.kind == waitForDistinguished {
			distinguished = " (distinguished)"
		}
		target := redact.SafeString("holding lock")
		if !s.held {
			target = "running request"
		}
		w.Printf("wait for%s txn %s %s @ key %s (queuedWriters: %d, queuedReaders: %d)",
			distinguished, s.txn.Short(), target, s.key, s.queuedWriters, s.queuedReaders)
	case waitSelf:
		w.Printf("wait self @ key %s", s.key)
	case waitElsewhere:
		if !s.held {
			w.SafeString("wait elsewhere by proceeding to evaluation")
		}
		w.Printf("wait elsewhere for txn %s @ key %s", s.txn.Short(), s.key)
	case waitQueueMaxLengthExceeded:
		w.Printf("wait-queue maximum length exceeded @ key %s with length %d",
			s.key, s.queuedWriters)
	case doneWaiting:
		w.SafeString("done waiting")
	default:
		panic("unhandled waitingState.kind")
	}
}

// Implementation
// TODO(sbhola):
// - metrics about lockTable state to export to observability debug pages:
//   number of locks, number of waiting requests, wait time?, ...

// The btree for a particular key.
type treeMu struct {
	mu syncutil.RWMutex // Protects everything in this struct.

	// For assigning sequence numbers to the lockState objects as required by
	// the util/interval/generic type contract.
	lockIDSeqNum uint64

	// Container for lockState structs. Locks that are not held or reserved and
	// have no waiting requests are garbage collected. Additionally, locks that
	// are only held with Replicated durability and have no waiting requests may
	// also be garbage collected since their state can be recovered from
	// persistent storage.
	btree

	// For constraining memory consumption. We need better memory accounting
	// than this.
	// TODO(nvanbenschoten): use an atomic.Int64.
	numLocks int64

	// For dampening the frequency with which we enforce lockTableImpl.maxLocks.
	lockAddMaxLocksCheckInterval uint64
}

// lockTableImpl is an implementation of lockTable.
//
// Concurrency: in addition to holding latches, we require for a particular
// request ScanAndEnqueue() and CurState() must be called by the same
// thread.
//
// Mutex ordering:   lockTableImpl.enabledMu
//
//	> treeMu.mu
//	> lockState.mu
//	> lockTableGuardImpl.mu
type lockTableImpl struct {
	// The ID of the range to which this replica's lock table belongs.
	// Used to populate results when querying the lock table.
	rID roachpb.RangeID

	// Is the lockTable enabled? When enabled, the lockTable tracks locks and
	// allows requests to queue in wait-queues on these locks. When disabled,
	// no locks or wait-queues are maintained.
	//
	// enabledMu is held in read-mode when determining whether the lockTable
	// is enabled and when acting on that information (e.g. adding new locks).
	// It is held in write-mode when enabling or disabling the lockTable.
	//
	// enabledSeq holds the lease sequence for which the lockTable is enabled
	// under. Discovered locks from prior lease sequences are ignored, as they
	// may no longer be accurate.
	enabled    bool
	enabledMu  syncutil.RWMutex
	enabledSeq roachpb.LeaseSequence

	// A sequence number is assigned to each request seen by the lockTable. This
	// is to preserve fairness despite the design choice of allowing
	// out-of-order evaluation of requests with overlapping spans where the
	// latter request does not encounter contention. This out-of-order
	// evaluation happens because requests do not reserve spans that are
	// uncontended while they wait for on contended locks after releasing their
	// latches. Consider the following examples:
	//
	// Example 1:
	// - req1 wants to write to A, B
	// - req2 wants to write to B
	// - lock at A is held by some other txn.
	// - Even though req2 arrives later, req1 will wait only in the queue for A
	//   and allow req2 to proceed to evaluation.
	//
	// Example 2:
	// - Same as example 1 but lock at A is held by txn3 and lock at B is held
	//   by txn4.
	// - Lock at A is released so req1 claims the lock at A and starts waiting at
	//   B.
	// - It is unfair for req1 to wait behind req2 at B. The sequence number
	//   assigned to req1 and req2 will restore the fairness by making req1
	//   wait before req2.
	//
	// Example 3: Deadlock in lock table if it did not use sequence numbers.
	// - Lock at B is acquired by txn0.
	// - req1 (from txn1) arrives at lockTable and wants to write to A and B.
	//   It queues at B.
	// - req2 (from txn2) arrives at lockTable and only wants to write A.
	//   It proceeds to evaluation and acquires the lock at A for txn2 and then
	//   the request is done. The lock is still held.
	// - req3 (from txn3) wants to write to A and B. It queues at A.
	// - txn2 releases A. req3 is in the front of the queue at A so it claims the
	//   lock and starts waiting at B behind req1.
	// - txn0 releases B. req1 gets to claim the lock at B and does another scan
	//   and adds itself to the queue at A, behind req3 which holds the claim for
	//   A.
	// Now in the queues for A and B req1 is behind req3 and vice versa and
	// this deadlock has been created entirely due to the lock table's behavior.
	// TODO(nvanbenschoten): use an atomic.Uint64.
	seqNum uint64

	// locks contains the btree object (wrapped in the treeMu structure) that
	// contains the actual lockState objects. These lockState objects represent
	// the individual locks in the lock table. Locks on both Global and Local keys
	// are stored in the same btree.
	locks treeMu

	// maxLocks is a soft maximum on number of locks. When it is exceeded, and
	// subject to the dampening in lockAddMaxLocksCheckInterval, locks will be
	// cleared.
	maxLocks int64
	// When maxLocks is exceeded, will attempt to clear down to minLocks,
	// instead of clearing everything.
	minLocks int64

	// txnStatusCache is a small LRU cache that tracks the status of
	// transactions that have been successfully pushed.
	//
	// NOTE: it probably makes sense to maintain a single txnStatusCache
	// across all Ranges on a Store instead of an individual cache per
	// Range. For now, we don't do this because we don't share any state
	// between separate concurrency.Manager instances.
	txnStatusCache txnStatusCache

	// clock is used to track the lock hold and lock wait start times.
	clock *hlc.Clock

	// settings provides a handle to cluster settings.
	settings *cluster.Settings
}

var _ lockTable = &lockTableImpl{}

func newLockTable(
	maxLocks int64, rangeID roachpb.RangeID, clock *hlc.Clock, settings *cluster.Settings,
) *lockTableImpl {
	lt := &lockTableImpl{
		rID:      rangeID,
		clock:    clock,
		settings: settings,
	}
	lt.setMaxLocks(maxLocks)
	return lt
}

func (t *lockTableImpl) setMaxLocks(maxLocks int64) {
	// Check at 5% intervals of the max count.
	lockAddMaxLocksCheckInterval := maxLocks / int64(20)
	if lockAddMaxLocksCheckInterval == 0 {
		lockAddMaxLocksCheckInterval = 1
	}
	t.maxLocks = maxLocks
	t.minLocks = maxLocks / 2
	t.locks.lockAddMaxLocksCheckInterval = uint64(lockAddMaxLocksCheckInterval)
}

// lockTableGuardImpl is an implementation of lockTableGuard.
//
// The struct is a guard that is returned to the request the first time it calls
// lockTable.ScanAndEnqueue() and used in later calls to ScanAndEnqueue() and
// done(). After a call to ScanAndEnqueue() (which is made while holding
// latches), the caller must first call lockTableGuard.StartWaiting() and if it
// returns true release the latches and continue interacting with the
// lockTableGuard. If StartWaiting() returns false, the request can proceed to
// evaluation.
//
// Waiting logic: The interface hides the queues that the request is waiting on,
// and the request's position in the queue. One of the reasons for this hiding
// is that queues are not FIFO since a request that did not wait on a queue for
// key k in a preceding call to ScanAndEnqueue() (because k was not locked and
// there was no queue) may need to wait on the queue in a later call to
// ScanAndEnqueue(). So sequencing of requests arriving at the lockTable is
// partially decided by a sequence number assigned to a request when it first
// called ScanAndEnqueue() and queues are ordered by this sequence number.
// However the sequencing is not fully described by the sequence numbers -- a
// request R1 encountering contention over some keys in its span does not
// prevent a request R2 that has a higher sequence number and overlapping span
// to proceed if R2 does not encounter contention. This concurrency (that is not
// completely fair) is deemed desirable.
//
// The interface exposes an abstracted version of the waiting logic in a way
// that the request that starts waiting is considered waiting for at most one
// other request or transaction. This is exposed as a series of state
// transitions where the transitions are notified via newState() and the current
// state can be read using CurState().
//
//   - The waitFor* states provide information on who the request is waiting for.
//     The waitForDistinguished state is a sub-case -- a distinguished waiter is
//     responsible for taking extra actions e.g. immediately pushing the transaction
//     it is waiting for. The implementation ensures that if there are multiple
//     requests in waitFor state waiting on the same transaction at least one will
//     be a distinguished waiter.
//
//     TODO(sbhola): investigate removing the waitForDistinguished state which
//     will simplify the code here. All waitFor requests would wait (currently
//     50ms) before pushing the transaction (for deadlock detection) they are
//     waiting on, say T. Typically T will be done before 50ms which is considered
//     ok: the one exception we will need to make is if T has the min priority or
//     the waiting transaction has max priority -- in both cases it will push
//     immediately. The bad case is if T is ABORTED: the push will succeed after,
//     and if T left N intents, each push would wait for 50ms, incurring a latency
//     of 50*N ms. A cache of recently encountered ABORTED transactions on each
//     Store should mitigate this latency increase. Whenever a transaction sees a
//     waitFor state, it will consult this cache and if T is found, push
//     immediately (if there isn't already a push in-flight) -- even if T is not
//     initially in the cache, the first push will place it in the cache, so the
//     maximum latency increase is 50ms.
//
//   - The waitElsewhere state is a rare state that is used when the lockTable is
//     under memory pressure and is clearing its internal queue state. Like the
//     waitFor* states, it informs the request who it is waiting for so that
//     deadlock detection works. However, sequencing information inside the
//     lockTable is mostly discarded.
//
//   - The waitSelf state is a rare state when a different request from the same
//     transaction has claimed the lock. See the comment about the concept of
//     claiming a lock on claimantTxn().
//
//   - The waitQueueMaxLengthExceeded state is used to indicate that the request
//     was rejected because it attempted to enter a lock wait-queue as a writer
//     and found that the queue's length was already equal to or exceeding the
//     request's configured maximum.
//
//   - The doneWaiting state is used to indicate that the request should make
//     another call to ScanAndEnqueue() (that next call is more likely to return a
//     lockTableGuard that returns false from StartWaiting()).
type lockTableGuardImpl struct {
	seqNum uint64
	lt     *lockTableImpl

	// Information about this request.
	txn                *roachpb.Transaction
	ts                 hlc.Timestamp
	spans              *lockspanset.LockSpanSet
	waitPolicy         lock.WaitPolicy
	maxWaitQueueLength int

	// Snapshot of the tree for which this request has some spans. Note that
	// the lockStates in this snapshot may have been removed from
	// lockTableImpl. Additionally, it is possible that there is a new lockState
	// for the same key. This can result in various harmless anomalies:
	// - the request may hold a claim on a lockState that is no longer
	//   in the tree. When it next does a scan, it will either find a new
	//   lockState where it will compete or none. Both lockStates can be in
	//   the mu.locks map, which is harmless.
	// - the request may wait behind a transaction that has claimed a lock but is
	//   yet to acquire it. This could cause a delay in pushing the lock holder.
	//   This is not a correctness issue (the whole system is not deadlocked) and we
	//   expect will not be a real performance issue.
	//
	// TODO(sbhola): experimentally evaluate the lazy queueing of the current
	// implementation, in comparison with eager queueing. If eager queueing
	// is comparable in system throughput, one can eliminate the above anomalies.
	//
	// TODO(nvanbenschoten): should we be Reset-ing these btree snapshot when we
	// Dequeue a lockTableGuardImpl? In releaseLockTableGuardImpl?
	//
	tableSnapshot btree

	// notRemovableLock points to the lock for which this guard has incremented
	// lockState.notRemovable. It will be set to nil when this guard has decremented
	// lockState.notRemovable. Note that:
	// - notRemovableLock may no longer be the btree in lockTableImpl since it may
	//   have been removed due to the lock being released. This is harmless since
	//   the change in lock state for that lock's key (even if it has meanwhile been
	//   reacquired by a different request) means forward progress for this request,
	//   which guarantees liveness for this request.
	// - Multiple guards can have marked the same lock as notRemovable, which is
	//   why lockState.notRemovable behaves like a reference count.
	notRemovableLock *lockState

	// A request whose startWait is set to true in ScanAndEnqueue is actively
	// waiting at a particular key. This is the first key encountered when
	// iterating through spans that it needs to wait at. A future event (lock
	// release etc.) may cause the request to no longer need to wait at this
	// key. It then needs to continue iterating through spans to find the next
	// key to wait at (we don't want to wastefully start at the beginning since
	// this request probably has a claim at the contended keys there): str, index,
	// and key collectively track the current position to allow it to continue
	// iterating.

	// The key for the lockState.
	key roachpb.Key
	// The key for the lockState is contained in the Span specified by
	// spans[str][index].
	str   lock.Strength // Iterates from strongest to weakest lock strength
	index int

	mu struct {
		syncutil.Mutex
		startWait bool
		// curLockWaitStart represents the timestamp when the request started waiting
		// on the current lock. Multiple consecutive waitingStates might refer to
		// the same lock, in which case the curLockWaitStart is not updated in between
		// them.
		curLockWaitStart time.Time

		state  waitingState
		signal chan struct{}

		// locks for which this request is in the list of queued{Readers,Writers}.
		// For writers, this includes both active and inactive waiters. For readers,
		// there's no such thing as inactive readers, so by definition the request
		// must be an active waiter.
		//
		// TODO(sbhola): investigate whether the logic to maintain this locks map
		// can be simplified so it doesn't need to be adjusted by various lockState
		// methods. It adds additional bookkeeping burden that means it is more
		// prone to inconsistencies. There are two main uses: (a) removing from
		// various lockStates when requestDone() is called, (b) tryActiveWait() uses
		// it as an optimization to know that this request is not known to the
		// lockState. (b) can be handled by other means -- the first scan the
		// request won't be in the lockState and the second scan it likely will. (a)
		// doesn't necessarily require this map to be consistent -- the request
		// could track the places where it is has enqueued as places where it could
		// be present and then do the search.

		locks map[*lockState]struct{}

		// mustComputeWaitingState is set in context of the state change channel
		// being signaled. It denotes whether the signaler has already computed the
		// guard's next waiting state or not.
		//
		// If set to true, a call to CurState() must compute the state from scratch,
		// by resuming its scan. In such cases, the signaler has deferred the
		// computation work on to the callers, which is proportional to the number
		// of waiters.
		//
		// If set to false, the signaler has already computed this request's next
		// waiting state. As such, a call to CurState() can simply return the state
		// without doing any extra work.
		mustComputeWaitingState bool
	}
	// Locks to resolve before scanning again. Doesn't need to be protected by
	// mu since should only be read after the caller has already synced with mu
	// in realizing that it is doneWaiting.
	//
	// toResolve should only include replicated locks; for unreplicated locks,
	// toResolveUnreplicated is used instead.
	toResolve []roachpb.LockUpdate

	// toResolveUnreplicated is a list of locks (only held with durability
	// unreplicated) that are known to belong to finalized transactions. Such
	// locks may be cleared from the lock table (and some requests queueing in the
	// lock's wait queue may be able to proceed). If set, the request should
	// perform these actions on behalf of the lock table, either before proceeding
	// to evaluation, or before waiting on a conflicting lock.
	//
	// TODO(arul): We need to push the responsibility of doing so on to a request
	// because TransactionIsFinalized does not take proactive action. If we
	// addressed the TODO in TransactionIsFinalized, and taught it to take action
	// on locks belonging to finalized transactions, we wouldn't need to bother
	// scanning requests.
	toResolveUnreplicated []roachpb.LockUpdate
}

var _ lockTableGuard = &lockTableGuardImpl{}

// Used to avoid allocations.
var lockTableGuardImplPool = sync.Pool{
	New: func() interface{} {
		g := new(lockTableGuardImpl)
		g.mu.signal = make(chan struct{}, 1)
		g.mu.locks = make(map[*lockState]struct{})
		return g
	},
}

// newLockTableGuardImpl returns a new lockTableGuardImpl. The struct will
// contain pre-allocated mu.signal and mu.locks fields, so it shouldn't be
// overwritten blindly.
func newLockTableGuardImpl() *lockTableGuardImpl {
	return lockTableGuardImplPool.Get().(*lockTableGuardImpl)
}

// releaseLockTableGuardImpl releases the guard back into the object pool.
func releaseLockTableGuardImpl(g *lockTableGuardImpl) {
	// Preserve the signal channel and locks map fields in the pooled
	// object. Drain the signal channel and assert that the map is empty.
	// The map should have been cleared by lockState.requestDone.
	signal, locks := g.mu.signal, g.mu.locks
	select {
	case <-signal:
	default:
	}
	if len(locks) != 0 {
		panic("lockTableGuardImpl.mu.locks not empty after Dequeue")
	}

	*g = lockTableGuardImpl{}
	g.mu.signal = signal
	g.mu.locks = locks
	lockTableGuardImplPool.Put(g)
}

func (g *lockTableGuardImpl) ShouldWait() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	// The request needs to drop latches and wait if:
	// 1. The lock table indicated as such (e.g. the request ran into a
	// conflicting lock).
	// 2. OR the request successfully performed its scan but discovered replicated
	// locks that need to be resolved before it can evaluate.
	return g.mu.startWait || len(g.toResolve) > 0
}

func (g *lockTableGuardImpl) ResolveBeforeScanning() []roachpb.LockUpdate {
	return g.toResolve
}

func (g *lockTableGuardImpl) NewStateChan() chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.signal
}

func (g *lockTableGuardImpl) CurState() waitingState {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.mu.mustComputeWaitingState {
		return g.mu.state
	}
	// Not actively waiting anywhere so no one else can set
	// mustComputeWaitingState to true while this method executes.
	g.mu.mustComputeWaitingState = false
	g.mu.Unlock()
	g.resumeScan(false /* notify */)
	g.mu.Lock() // Unlock deferred
	return g.mu.state
}

// updateStateToDoneWaitingLocked updates the request's waiting state to
// indicate that it is done waiting.
// REQUIRES: g.mu to be locked.
func (g *lockTableGuardImpl) updateStateToDoneWaitingLocked() {
	g.mu.state = waitingState{kind: doneWaiting}
}

// startWaitingWithWaitingState modifies state on the request's guard to let it
// start waiting.
func (g *lockTableGuardImpl) startWaitingWithWaitingState(ws waitingState, notify bool) {
	g.key = ws.key

	g.mu.Lock()
	defer g.mu.Unlock()
	g.mu.startWait = true
	g.mu.curLockWaitStart = g.lt.clock.PhysicalTime()
	g.maybeUpdateWaitingStateLocked(ws, notify)
}

// maybeUpdateWaitingStateLocked updates the request's waiting state if the
// supplied state is meaningfully different[1]. The request's state change
// channel is signaled if the waiting state is updated and the caller has
// dictated such. Eliding updates, and more importantly notifications to the
// state change channel, avoids needlessly nudging a waiting request.
//
// [1] The state is not updated if the lock table waiter does not need to take
// action as a result of the update. In practice, this means updates to
// observability related fields are elided. See updateWaitingStateLocked if this
// behavior is undesirable.
//
// REQUIRES: g.mu to be locked.
func (g *lockTableGuardImpl) maybeUpdateWaitingStateLocked(newState waitingState, notify bool) {
	if g.canElideWaitingStateUpdate(newState) {
		return // the update isn't meaningful; early return
	}
	g.updateWaitingStateLocked(newState)
	if notify {
		g.notify()
	}
}

// updateWaitingStateLocked updates the request's waiting state to indicate
// to the one supplied. The supplied waiting state must imply the request is
// still waiting. Typically, this function is called for the first time when
// the request discovers a conflict while scanning the lock table.
//
// REQUIRES: g.mu to be locked.
func (g *lockTableGuardImpl) updateWaitingStateLocked(newState waitingState) {
	if newState.kind == doneWaiting {
		panic(errors.AssertionFailedf("unexpected waiting state kind: %d", newState.kind))
	}
	newState.guardStrength = g.curStrength() // copy over the strength which caused the conflict
	g.mu.state = newState
}

// canElideWaitingStateUpdate returns true if updating the guard's waiting state
// to the supplied waitingState would not cause the waiter to take a different
// action, such as proceeding with its scan or pushing a different transaction.
// Notably, observability related updates are considered fair game for elision.
//
// REQUIRES: g.mu to be locked.
func (g *lockTableGuardImpl) canElideWaitingStateUpdate(newState waitingState) bool {
	// Note that we don't need to check newState.guardStrength as it's
	// automatically assigned when updating the state.
	return g.mu.state.kind == newState.kind && g.mu.state.txn == newState.txn &&
		g.mu.state.key.Equal(newState.key) && g.mu.state.held == newState.held
}

func (g *lockTableGuardImpl) CheckOptimisticNoConflicts(
	lockSpanSet *lockspanset.LockSpanSet,
) (ok bool) {
	if g.waitPolicy == lock.WaitPolicy_SkipLocked {
		// If the request is using a SkipLocked wait policy, lock conflicts are
		// handled during evaluation.
		return true
	}
	// Temporarily replace the LockSpanSet in the guard.
	originalSpanSet := g.spans
	g.spans = lockSpanSet
	g.str = lock.MaxStrength
	g.index = -1
	defer func() {
		g.spans = originalSpanSet
	}()
	span := stepToNextSpan(g)
	for span != nil {
		startKey := span.Key
		iter := g.tableSnapshot.MakeIter()
		ltRange := &lockState{key: startKey, endKey: span.EndKey}
		for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
			l := iter.Cur()
			if !l.isNonConflictingLock(g, g.curStrength()) {
				return false
			}
		}
		span = stepToNextSpan(g)
	}
	return true
}

func (g *lockTableGuardImpl) IsKeyLockedByConflictingTxn(
	key roachpb.Key, strength lock.Strength,
) (bool, *enginepb.TxnMeta) {
	iter := g.tableSnapshot.MakeIter()
	iter.SeekGE(&lockState{key: key})
	if !iter.Valid() || !iter.Cur().key.Equal(key) {
		// No lock on key.
		return false, nil
	}
	l := iter.Cur()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isEmptyLock() {
		// The lock is empty but has not yet been deleted.
		return false, nil
	}
	conflictingTxn, held := l.claimantTxn()
	assert(conflictingTxn != nil, "non-empty lockState with no claimant transaction")
	if !held {
		if strength == lock.None {
			// Non-locking reads only care about locks that are held.
			return false, nil
		}
		if g.isSameTxn(conflictingTxn) {
			return false, nil
		}
		// If the key is claimed but the lock isn't held (yet), nil is returned.
		return true, nil
	}
	// Key locked.
	txn, ts := l.getLockHolder()
	if strength == lock.None && g.ts.Less(ts) {
		// Non-locking read below lock's timestamp.
		return false, nil
	}
	if g.isSameTxn(txn) {
		// Already locked by this txn.
		return false, nil
	}
	// "If the key is locked, the lock holder is also returned."
	return true, txn
}

func (g *lockTableGuardImpl) notify() {
	select {
	case g.mu.signal <- struct{}{}:
	default:
	}
}

// doneActivelyWaitingAtLock is called when a request, that was previously
// actively waiting at a lock, is no longer doing so. It may have transitioned
// to become an inactive waiter or removed from the lock's wait queues entirely
// -- either way, it should find the next lock (if any) to wait at.
//
// REQUIRES: g.mu to be locked.
func (g *lockTableGuardImpl) doneActivelyWaitingAtLock() {
	g.mu.mustComputeWaitingState = true
	g.notify()
}

func (g *lockTableGuardImpl) txnMeta() *enginepb.TxnMeta {
	if g.txn == nil {
		return nil
	}
	return &g.txn.TxnMeta
}

func (g *lockTableGuardImpl) hasUncertaintyInterval() bool {
	return g.txn != nil && g.txn.ReadTimestamp.Less(g.txn.GlobalUncertaintyLimit)
}

func (g *lockTableGuardImpl) isSameTxn(txn *enginepb.TxnMeta) bool {
	return g.txn != nil && g.txn.ID == txn.ID
}

// curStrength returns the lock strength of the current lock span being scanned
// by the request. Lock spans declared by a request are iterated from strongest
// to weakest, and the return value of this method is mutable as the request's
// scan progresses from lock to lock.
func (g *lockTableGuardImpl) curStrength() lock.Strength {
	return g.str
}

// getCurLockMode returns the lock mode of the current lock being scanned by the
// request. The value returned by this method are mutable as the request's scan
// of the lock table progresses from lock to lock.
func (g *lockTableGuardImpl) curLockMode() lock.Mode {
	var reqMode lock.Mode
	switch g.curStrength() {
	case lock.None:
		iso := isolation.Serializable
		if g.txn != nil {
			iso = g.txn.IsoLevel
		}
		reqMode = lock.MakeModeNone(g.ts, iso)
	case lock.Exclusive:
		assert(g.txn != nil, "only transactional requests can acquire exclusive locks")
		reqMode = lock.MakeModeExclusive(g.ts, g.txn.IsoLevel)
	case lock.Intent:
		reqMode = lock.MakeModeIntent(g.ts)
	default:
		panic(fmt.Sprintf("unhandled request strength: %s", g.curStrength()))
	}
	return reqMode
}

// takeToResolveUnreplicated returns the list of unreplicated locks accumulated
// by the guard for resolution. Ownership, and responsibility to resolve these
// locks, is passed to the caller.
func (g *lockTableGuardImpl) takeToResolveUnreplicated() []roachpb.LockUpdate {
	toResolveUnreplicated := g.toResolveUnreplicated
	g.toResolveUnreplicated = nil
	return toResolveUnreplicated
}

// resumeScan resumes the request's (receiver's) scan of the lock table. The
// scan continues until either all overlapping locks in the lock table have been
// considered and no conflict is found, or until the request encounters a lock
// that it conflicts with. Either way, the receiver's state is mutated such that
// a call to ShouldWait will reflect the termination condition. The same applies
// to the receiver's waitingState; however, if the waitingState does change,
// the state change channel will only be signaled if notify is supplied as true.
//
// Note that the details about scan mechanics are captured on the receiver --
// information such as what lock spans to scan, where to begin the scan from
// etc.
//
// ACQUIRES: g.mu.
func (g *lockTableGuardImpl) resumeScan(notify bool) {
	spans := g.spans.GetSpans(g.curStrength())
	var span *roachpb.Span
	resumingInSameSpan := false
	if g.index == -1 || len(spans[g.index].EndKey) == 0 {
		span = stepToNextSpan(g)
	} else {
		span = &spans[g.index]
		resumingInSameSpan = true
	}
	defer func() {
		// Eagerly update any unreplicated locks that are known to belong to
		// finalized transactions. We do so regardless of whether this request can
		// proceed to evaluation or needs to wait at some conflicting lock.
		//
		// Note that replicated locks are handled differently, using the g.toResolve
		// slice. Additionally, they're only resolved when a request is done
		// waiting and can proceed to evaluation.
		if toResolveUnreplicated := g.takeToResolveUnreplicated(); len(toResolveUnreplicated) > 0 {
			for i := range toResolveUnreplicated {
				g.lt.updateLockInternal(&toResolveUnreplicated[i])
			}
		}
	}()

	for span != nil {
		startKey := span.Key
		if resumingInSameSpan {
			startKey = g.key
		}
		iter := g.tableSnapshot.MakeIter()

		// From here on, the use of resumingInSameSpan is just a performance
		// optimization to deal with the interface limitation of btree that
		// prevents us from specifying an exclusive start key. We need to check
		// that the lock is not the same as our exclusive start key and only need
		// to do that check once -- for the first lock.
		ltRange := &lockState{key: startKey, endKey: span.EndKey}
		for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
			l := iter.Cur()
			if resumingInSameSpan {
				resumingInSameSpan = false
				if l.key.Equal(startKey) {
					// This lock is where it stopped waiting.
					continue
				}
				// Else, past the lock where it stopped waiting. We may not
				// encounter that lock since it may have been garbage collected.
			}
			conflicts := l.scanAndMaybeEnqueue(g, notify)
			if conflicts {
				return
			}
		}
		resumingInSameSpan = false
		span = stepToNextSpan(g)
	}

	if len(g.toResolve) > 0 {
		j := 0
		// Some of the locks in g.toResolve may already have been claimed by
		// another concurrent request and removed, or intent resolution could have
		// happened while this request was waiting (after releasing latches). So
		// we iterate over all the elements of toResolve and only keep the ones
		// where removing the lock via the call to updateLockInternal is not a
		// noop.
		//
		// For pushed transactions that are not finalized, we disable this
		// deduplication and allow all resolution attempts to adjust the lock's
		// timestamp to go through. This is because updating the lock ahead of
		// resolution risks rediscovery loops where the lock is continually
		// rediscovered at a lower timestamp than that in the lock table.
		for i := range g.toResolve {
			var doResolve bool
			if g.toResolve[i].Status.IsFinalized() {
				doResolve = g.lt.updateLockInternal(&g.toResolve[i])
			} else {
				doResolve = true
			}
			if doResolve {
				g.toResolve[j] = g.toResolve[i]
				j++
			}
		}
		g.toResolve = g.toResolve[:j]
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.updateStateToDoneWaitingLocked()
	if notify {
		g.notify()
	}
}

// Waiting writers in a lockState are wrapped in a queuedGuard. A waiting
// writer is typically waiting in an active state, i.e., the
// lockTableGuardImpl.key refers to this lockState. However, there are
// multiple reasons that can cause a writer to be an inactive waiter:
//   - The first transactional writer is able to claim a lock when it is
//     released. Doing so entails the writer being marked inactive.
//   - It is able to claim a lock that was previously claimed by a request with
//     a higher sequence number. In such cases, the writer adds itself to the
//     head of the queue as an inactive waiter and proceeds with its scan.
//   - A discovered lock causes the discoverer to become an inactive waiter
//     (until it scans again).
//   - A lock held by a finalized txn causes the first waiter to be an inactive
//     waiter.
//
// The first two cases above (claiming an unheld lock) only occur for
// transactional requests, but the other cases can happen for both transactional
// and non-transactional requests.
type queuedGuard struct {
	guard  *lockTableGuardImpl
	active bool // protected by lockState.mu
}

// Information about a lock holder for unreplicated locks.
type unreplicatedLockHolderInfo struct {
	// Lock strength is always lock.Exclusive.

	// All the TxnSeqs in the current epoch at which this lock has been acquired,
	// in increasing order. We track these so that if a lock is acquired at both
	// seq 5 and seq 7, rollback of 7 does not cause the lock to be released. This
	// consistent with PostgreSQL semantics; see:
	// https://www.postgresql.org/docs/12/sql-select.html#SQL-FOR-UPDATE-SHARE
	seqs []enginepb.TxnSeq

	// The timestamp at which the unreplicated lock is held. Must not regress.
	ts hlc.Timestamp
}

func (ulh *unreplicatedLockHolderInfo) isEmpty() bool {
	return ulh.seqs == nil && ulh.ts.IsEmpty()
}

func (ulh *unreplicatedLockHolderInfo) safeFormat(sb *redact.StringBuilder) {
	if ulh.isEmpty() {
		return
	}
	sb.SafeString("unrepl ")
	sb.Printf("seqs: [%d", redact.Safe(ulh.seqs[0]))
	for j := 1; j < len(ulh.seqs); j++ {
		sb.Printf(", %d", redact.Safe(ulh.seqs[j]))
	}
	sb.SafeString("]")
}

// Information about a lock holder for replicated locks. Notably, unlike
// unreplicated locks, this does not include any sequence numbers.
type replicatedLockHolderInfo struct {
	// Lock strength is always lock.Intent.

	// The timestamp at which the replicated lock is held. Must not regress.
	ts hlc.Timestamp
}

func (rlh *replicatedLockHolderInfo) isEmpty() bool {
	return rlh.ts.IsEmpty()
}

func (rlh *replicatedLockHolderInfo) safeFormat(sb *redact.StringBuilder) {
	if rlh.isEmpty() {
		return
	}
	sb.SafeString("repl")
}

// Per lock state in lockTableImpl.
//
// NOTE: we can't easily pool lockState objects without some form of reference
// counting because they are used as elements in a copy-on-write btree and may
// still be referenced by clones of the tree even when deleted from the primary.
// However, other objects referenced by lockState can be pooled as long as they
// are removed from all lockStates that reference them first.
type lockState struct {
	id     uint64 // needed for implementing util/interval/generic type contract
	endKey []byte // used in btree iteration and tests

	// The key being locked and the scope of that key. This state is never
	// mutated.
	key roachpb.Key

	mu syncutil.Mutex // Protects everything below.

	// Invariant summary (see detailed comments below):
	// - both holder.locked and waitQ.reservation != nil cannot be true.
	// - if holder.locked and multiple holderInfos have txn != nil: all the
	//   txns must have the same txn.ID.
	// - !holder.locked => waitingReaders.Len() == 0. That is, readers wait
	//   only if the lock is held. They do not wait for a reservation.
	// - If reservation != nil, that request is not in queuedWriters.
	//
	// TODO(arul): These invariants are stale now that we don't have reservations.
	// However, we'll replace this structure soon with what's proposed in the
	// SHARED locks RFC, at which point there will be new invariants altogether.

	// Information about whether the lock is held and the holder. We track
	// information for each durability level separately since a transaction can
	// go through multiple epochs and TxnSeq and may acquire the same lock in
	// replicated and unreplicated mode at different stages.
	holder struct {
		// If held, the TxnMeta of the transaction that holds the lock. For a given
		// transaction, the lock may be acquired/updated using a series of different
		// TxnMetas. While doing so, we provide a few invariants:
		//
		// 1. The epoch of the TxnMeta stored here must be monotonically increasing.
		// 2. The tracking of sequence numbers in unreplicatedLockInfo corresponds
		// to the epoch of the TxnMeta stored below. This allows us to account for
		// savepoint rollbacks within a particular epoch.
		//
		// As a result, the TxnMeta stored here may not correspond to the latest
		// call to acquire/update the lock (if the call was made using a TxnMeta
		// with an older epoch).
		txn *enginepb.TxnMeta

		// INVARIANT: If the lock is held (i.e. the locked boolean is set to true),
		// then atleast one of (and possibly both of) unreplicatedInfo and
		// replicatedInfo must track lock holder information.

		// unreplicatedInfo tracks lock holder information if the lock is held with
		// durability unreplicated.
		unreplicatedInfo unreplicatedLockHolderInfo

		// replicatedInfo tracks lock holder information if the lock is held with
		// durability replicated.
		replicatedInfo replicatedLockHolderInfo

		// The start time of the lockholder being marked as held in the lock table.
		// NB: In the case of a replicated lock that is held by a transaction, if
		// there is no wait-queue, the lock is not tracked by the in-memory lock
		// table; thus for uncontended replicated locks, the startTime may not
		// represent the initial acquisition time of the lock but rather the time
		// of the wait-queue forming and the lock being tracked in the lock table.
		startTime time.Time
	}

	// Information about the requests waiting on the lock.
	lockWaitQueue

	// notRemovable is temporarily incremented when a lock is added using
	// AddDiscoveredLock. This is to ensure liveness by not allowing the lock to
	// be removed until the requester has called ScanAndEnqueue. The *lockState
	// is also remembered in lockTableGuardImpl.notRemovableLock. notRemovable
	// behaves like a reference count since multiple requests may want to mark
	// the same lock as not removable.
	notRemovable int
}

type lockWaitQueue struct {
	// TODO(sbhola): There are a number of places where we iterate over these
	// lists looking for something, as described below. If some of these turn
	// out to be inefficient, consider better data-structures. One idea is that
	// for cases that find a particular guard the lockTableGuardImpl.locks can be
	// a map instead of a set to point directly to the *list.Element.
	//
	// queuedWriters:
	// - to find all active queuedWriters.
	// - to find the first active writer to make it distinguished.
	// - to find a particular guard.
	// - to find the position, based on seqNum, for inserting a particular guard.
	// - to find all waiting writers with a particular txn ID.
	//
	// waitingReaders:
	// - readers with a higher timestamp than some timestamp.
	// - to find a particular guard.

	// Waiters: An active waiter needs to be notified about changes in who it is
	// waiting for.

	// List of *queueGuard. The list is maintained in increasing order of sequence
	// numbers. This helps ensure some degree of fairness as requests are released
	// from the head of the queue. Typically, this happens when the associated
	// lock is released.
	//
	// When a lock is not held, the head of the list should be comprised of an
	// inactive, transactional writer (if the list is non-empty). Keeping its
	// position as an inactive waiter at the head of the queue serves as a claim
	// to prevent other concurrent requests (with higher sequence numbers) from
	// barging in front of it. This is important for two reasons:
	//
	// 1. It helps ensure some degree of fairness, as sequence numbers are a proxy
	// for arrival order.
	// 2. Perhaps more importantly, enforcing this ordering helps prevent
	// range-local lock table deadlocks. This is because all locks aren't known
	// upfront to the lock table (as uncontended, replicated locks are only
	// discovered during evaluation). This means that no total ordering of lock
	// acquisition is enforced by the lock table -- using sequence numbers to
	// break ties allows us to prevent deadlocks that would have arisen otherwise.
	//
	// Conversely, a request with a lower sequence number is allowed to barge in
	// front of an inactive waiter with a higher sequence number if the lock is
	// not held. This can be thought of as "breaking the claim" that the higher
	// sequence numbered request tried to claim. As both these requests sequence
	// through the lock table one of them will win the race. This is fine, as the
	// request that wins the race can only evaluate while holding latches and the
	// two requests must conflict on latches. As a result they're guaranteed to be
	// isolated. We don't concern ourselves with the possible fairness issue if
	// the higher sequence number wins the race.
	//
	// Non-locking readers are held in a separate list to the list of
	// waitingReaders, and they make no claims on unheld locks like writers do.
	// They race with the transactional writer that has made the claim.
	//
	// Similarly, non-transactional requests make no claims either, regardless of
	// their read/write status. Non-transactional writes wait in the queuedWriters
	// list along with transactional writers. The difference is as follows:
	// 1. When a lock transitions from held to released, the head of the queue
	// that is made of non-transactional writes is cleared in one swoop (until we
	// hit the first transactional writer or the queue is entirely drained). This
	// means non-transactional writers race with a transactional writer's claim,
	// like read requests.
	// 2. When deciding whether to wait at an unheld lock or not, a
	// non-transactional writer will check how its sequence number compares to the
	// head of the queuedWriters list. If its lower, it'll proceed; otherwise,
	// it'll wait.
	//
	// Multiple requests from the same transaction wait independently, including
	// the situation where one of the requests is an inactive waiter at the head
	// of the queue. However, if the inactive waiter manages to sequence,
	// evaluate, and acquire the lock, other requests from the same transaction
	// are allowed to be released.
	//
	// The behavior of only one transactional writer being allowed to make a claim
	// by marking itself as inactive when a lock transitions from held to free is
	// subject to change. As we introduce support for multiple locking strengths,
	// and in particular locking strengths that are compatible with each other
	// (read: shared locks), one could imagine a scheme where the head of the
	// queuedWriters (s/queuedWriters/queuedLockers/g) that is compatible with
	// each other is marked as inactive and allowed to proceed. A "joint claim".
	//
	// Once we introduce joint claims, we'll also need to support partially
	// breaking such claims. This means that a request that was previously
	// marked as inactive may have to come back to a lock and actively wait on it.
	// Here's a sketch of what a deadlock could look like if this wasn't
	// supported:
	//
	// - Keys are A, B, C, D.
	// - Key D is locked by some random txn.
	// - req1 from txn1 writes A, B, D. It waits at D.
	// - Some other request from some random txn that writes C arrives,
	//   evaluates, and locks C.
	// - req2 from txn2 that writes A, C. It waits at C.
	// - Some other request from some random txn that writes A arrives,
	//   evaluates, and locks A.
	// - req3 from txn1 that writes A, C. It waits at A. Note that req1 and req3
	//   are from the same txn.
	// - A is unlocked. req3 claims A and waits at C behind req2.
	// - B is locked by some random txn.
	// - D is unlocked. req1 claims D and proceeds to scan again and finds A
	//   is claimed by req3 which is the same txn so becomes a joint
	//   claim holder at A.
	// - Since B is locked, req1 waits at B.
	// - C is unlocked. req2 claims C. It scans and finds req1+req3 holding
	//   the joint claim at A. If it queues behind this joint claim
	//   we have the following situation:
	//           claim      waiter
	//   A     req1+req3     req2
	//   C       req2        req3
	//   This is a deadlock caused by the lock table unless req2 partially
	//   breaks the claim at A.
	queuedWriters list.List

	// List of *lockTableGuardImpl. All of these are actively waiting. If
	// non-empty, the lock must be held. By definition these cannot be in
	// waitSelf state since that requests don't conflict with locks held by their
	// transaction.
	waitingReaders list.List

	// If there is a non-empty set of active waiters that are not waitSelf, then
	// at least one must be distinguished.
	distinguishedWaiter *lockTableGuardImpl
}

//go:generate ../../../util/interval/generic/gen.sh *lockState concurrency

// Methods required by util/interval/generic type contract.
func (l *lockState) ID() uint64         { return l.id }
func (l *lockState) Key() []byte        { return l.key }
func (l *lockState) EndKey() []byte     { return l.endKey }
func (l *lockState) New() *lockState    { return new(lockState) }
func (l *lockState) SetID(v uint64)     { l.id = v }
func (l *lockState) SetKey(v []byte)    { l.key = v }
func (l *lockState) SetEndKey(v []byte) { l.endKey = v }

// REQUIRES: l.mu is locked.
func (l *lockState) String() string {
	var sb redact.StringBuilder
	l.safeFormat(&sb, nil)
	return sb.String()
}

// SafeFormat implements redact.SafeFormatter.
// REQUIRES: l.mu is locked.
func (l *lockState) SafeFormat(w redact.SafePrinter, _ rune) {
	var sb redact.StringBuilder
	l.safeFormat(&sb, nil)
	w.Print(sb)
}

// safeFormat is a helper for SafeFormat and String methods.
// REQUIRES: l.mu is locked. txnStatusCache can be nil.
func (l *lockState) safeFormat(sb *redact.StringBuilder, txnStatusCache *txnStatusCache) {
	sb.Printf(" lock: %s\n", l.key)
	if l.isEmptyLock() {
		sb.SafeString("  empty\n")
		return
	}
	txn, ts := l.getLockHolder()
	if txn != nil { // lock is held
		sb.Printf("  holder: txn: %v epoch: %d, iso: %s, ts: %v, info: ", redact.Safe(txn.ID), redact.Safe(txn.Epoch), redact.Safe(txn.IsoLevel), redact.Safe(ts))
		if !l.holder.replicatedInfo.isEmpty() {
			l.holder.replicatedInfo.safeFormat(sb)
			if !l.holder.unreplicatedInfo.isEmpty() {
				sb.Printf(", ")
			}
		}
		if !l.holder.unreplicatedInfo.isEmpty() {
			l.holder.unreplicatedInfo.safeFormat(sb)
		}
		if txnStatusCache != nil {
			finalizedTxn, ok := txnStatusCache.finalizedTxns.get(txn.ID)
			if ok {
				var statusStr string
				switch finalizedTxn.Status {
				case roachpb.COMMITTED:
					statusStr = "committed"
				case roachpb.ABORTED:
					statusStr = "aborted"
				}
				sb.Printf(" [holder finalized: %s]", redact.Safe(statusStr))
			}
		}
		sb.SafeString("\n")
	}
	// TODO(sumeer): Add an optional `description string` field to Request and
	// lockTableGuardImpl that tests can set to avoid relying on the seqNum to
	// identify requests.
	if l.waitingReaders.Len() > 0 {
		sb.SafeString("   waiting readers:\n")
		for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
			g := e.Value.(*lockTableGuardImpl)
			sb.Printf("    req: %d, txn: ", redact.Safe(g.seqNum))
			if g.txn == nil {
				sb.SafeString("none\n")
			} else {
				sb.Printf("%v\n", redact.Safe(g.txn.ID))
			}
		}
	}
	if l.queuedWriters.Len() > 0 {
		sb.SafeString("   queued writers:\n")
		for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
			qg := e.Value.(*queuedGuard)
			g := qg.guard
			sb.Printf("    active: %t req: %d, txn: ", redact.Safe(qg.active), redact.Safe(qg.guard.seqNum))
			if g.txn == nil {
				sb.SafeString("none\n")
			} else {
				sb.Printf("%v\n", redact.Safe(g.txn.ID))
			}
		}
	}
	if l.distinguishedWaiter != nil {
		sb.Printf("   distinguished req: %d\n", redact.Safe(l.distinguishedWaiter.seqNum))
	}
}

// collectLockStateInfo converts receiver into exportable LockStateInfo metadata
// and returns (true, valid LockStateInfo), or (false, empty LockStateInfo) if
// it was filtered out due to being an empty lock or an uncontended lock (if
// includeUncontended is false).
func (l *lockState) collectLockStateInfo(
	includeUncontended bool, now time.Time,
) (bool, roachpb.LockStateInfo) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Don't include locks that have neither lock holders, nor claims, nor
	// waiting readers/writers.
	if l.isEmptyLock() {
		return false, roachpb.LockStateInfo{}
	}

	// Filter out locks without waiting readers/writers unless explicitly
	// requested.
	//
	// TODO(arul): This should consider the active/inactive status of all queued
	// writers. If all waiting writers are inactive (and there are no waiting
	// readers either), we should consider the lock to be uncontended.
	// See https://github.com/cockroachdb/cockroach/issues/103894.
	if !includeUncontended && l.waitingReaders.Len() == 0 &&
		(l.queuedWriters.Len() == 0 ||
			(l.queuedWriters.Len() == 1 && !l.queuedWriters.Front().Value.(*queuedGuard).active)) {
		return false, roachpb.LockStateInfo{}
	}

	return true, l.lockStateInfo(now)
}

// lockStateInfo converts receiver to the roachpb.LockStateInfo structure.
// REQUIRES: l.mu is locked.
func (l *lockState) lockStateInfo(now time.Time) roachpb.LockStateInfo {
	var txnHolder *enginepb.TxnMeta

	durability := lock.Unreplicated
	if l.isHeld() {
		txnHolder = l.holder.txn
		if l.isHeldReplicated() {
			durability = lock.Replicated
		}
	}

	waiterCount := l.waitingReaders.Len() + l.queuedWriters.Len()
	lockWaiters := make([]lock.Waiter, 0, waiterCount)

	// Add waiting readers before writers as they should run first.
	for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
		readerGuard := e.Value.(*lockTableGuardImpl)
		readerGuard.mu.Lock()
		lockWaiters = append(lockWaiters, lock.Waiter{
			WaitingTxn:   readerGuard.txnMeta(),
			ActiveWaiter: true, // readers always actively wait at a lock
			Strength:     lock.None,
			WaitDuration: now.Sub(readerGuard.mu.curLockWaitStart),
		})
		readerGuard.mu.Unlock()
	}

	// Lastly, add queued writers in order.
	for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		writerGuard := qg.guard
		writerGuard.mu.Lock()
		lockWaiters = append(lockWaiters, lock.Waiter{
			WaitingTxn:   writerGuard.txnMeta(),
			ActiveWaiter: qg.active,
			Strength:     lock.Exclusive,
			WaitDuration: now.Sub(writerGuard.mu.curLockWaitStart),
		})
		writerGuard.mu.Unlock()
	}

	return roachpb.LockStateInfo{
		Key:          l.key,
		LockHolder:   txnHolder,
		Durability:   durability,
		HoldDuration: l.lockHeldDuration(now),
		Waiters:      lockWaiters,
	}
}

// addToMetrics adds the receiver's state to the provided metrics struct.
func (l *lockState) addToMetrics(m *LockTableMetrics, now time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isEmptyLock() {
		return
	}
	totalWaitDuration, maxWaitDuration := l.totalAndMaxWaitDuration(now)
	lm := LockMetrics{
		Key:                  l.key,
		Held:                 l.isHeld(),
		HoldDurationNanos:    l.lockHeldDuration(now).Nanoseconds(),
		WaitingReaders:       int64(l.waitingReaders.Len()),
		WaitingWriters:       int64(l.queuedWriters.Len()),
		WaitDurationNanos:    totalWaitDuration.Nanoseconds(),
		MaxWaitDurationNanos: maxWaitDuration.Nanoseconds(),
	}
	lm.Waiters = lm.WaitingReaders + lm.WaitingWriters
	m.addLockMetrics(lm)
}

// informActiveWaiters informs active waiters about the transaction that has
// claimed the lock. The claimant transaction may have changed, so there may be
// inconsistencies with waitSelf and waitForDistinguished states that need
// changing.
//
// REQUIRES: l.mu is locked.
func (l *lockState) informActiveWaiters() {
	if l.waitingReaders.Len() == 0 && l.queuedWriters.Len() == 0 {
		return // no active waiters to speak of; early return
	}
	waitForState := waitingState{
		kind:          waitFor,
		key:           l.key,
		queuedWriters: l.queuedWriters.Len(),
		queuedReaders: l.waitingReaders.Len(),
	}
	// TODO(arul): This is entirely busted once we have multiple lock holders.
	// In such cases, there may be a request waiting not on the head of the
	// queue, but because there is a waiter with a lower sequence number that it
	// is incompatible with. In such cases, its this guy it should be pushing.
	// However, if we naively plugged things into the current structure, it would
	// either sit tight (because its waiting for itself) or, worse yet, push a
	// transaction it's actually compatible with!
	waitForState.txn, waitForState.held = l.claimantTxn()
	findDistinguished := false
	// We need to find a (possibly new) distinguished waiter if either:
	//   There isn't one for this lock.
	if l.distinguishedWaiter == nil ||
		// OR it belongs to the same transaction that waiters in the lock wait queue
		// are waiting on, because a transaction doesn't push itself (it just sits
		// tight).
		//
		// NB: Note that if the distinguishedWaiter belongs to the same transaction
		// that waiters in the lock wait queue are waiting on, then the lock cannot
		// be held by it. This is because if it were, this request would no longer
		// be waiting in lock wait queues (via a call to releaseWritersFromTxn).
		// This is asserted below.
		l.distinguishedWaiter.isSameTxn(waitForState.txn) {
		// Ensure that if we're trying to find a new distinguished waiter because
		// all waiters on the lock are waiting on the (old) distinguished waiter,
		// the lock is not held.
		assert(
			l.distinguishedWaiter == nil || !l.isHeld(), fmt.Sprintf(
				"distinguished waiter waiting from txn %s waiting on itself with un-held lock",
				waitForState.txn,
			))

		findDistinguished = true
		l.distinguishedWaiter = nil // we'll find a new one
	}

	for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
		state := waitForState
		// Since there are waiting readers, we could not have transitioned out of
		// or into a state where the lock is held. This is because readers only wait
		// for held locks -- they race with other {,non-}transactional writers.
		assert(state.held, "waiting readers should be empty if the lock isn't held")
		g := e.Value.(*lockTableGuardImpl)
		if findDistinguished {
			l.distinguishedWaiter = g
			findDistinguished = false
		}
		if l.distinguishedWaiter == g {
			state.kind = waitForDistinguished
		}
		g.mu.Lock()
		// NB: The waiter is actively waiting on this lock, so it's likely taking
		// some action based on the previous state (e.g. it may be pushing someone).
		// If the state has indeed changed, it must perform a different action -- so
		// we pass notify = true here to nudge it to do so.
		g.maybeUpdateWaitingStateLocked(state, true /* notify */)
		g.mu.Unlock()
	}
	for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		if !qg.active {
			continue
		}
		g := qg.guard
		state := waitForState
		if g.isSameTxn(waitForState.txn) {
			if waitForState.held {
				panic("writer from the lock holder txn should not be waiting in a wait queue")
			}
			state.kind = waitSelf
		} else {
			if findDistinguished {
				l.distinguishedWaiter = g
				findDistinguished = false
			}
			if l.distinguishedWaiter == g {
				state.kind = waitForDistinguished
			}
		}
		g.mu.Lock()
		// NB: The waiter is actively waiting on this lock, so it's likely taking
		// some action based on the previous state (e.g. it may be pushing someone).
		// If the state has indeed changed, it must perform a different action -- so
		// we pass notify = true here to nudge it to do so.
		g.maybeUpdateWaitingStateLocked(state, true /* notify */)
		g.mu.Unlock()
	}
}

// claimantTxn returns the transaction that the lock table deems as having
// claimed the lock. Every lock stored in the lock table must have one and only
// one transaction associated with it that claims the lock. All actively waiting
// requests in this lock's wait queues should use this transaction as the
// transaction to push for {liveness,deadlock,etc.} detection purposes related
// to this key.
//
// The transaction that is considered to have claimed the lock may be the lock
// holder, in which case the return value of held will be true. It may also be
// another request being sequenced through the lock table that other waiters
// conflict with. In such cases, this request is deemed to be the next preferred
// lock holder by the lock table. This preference is based on the lock table's
// notion of fairness.
//
// Locks on a particular key can be acquired by multiple transactions. There may
// also be multiple (compatible) requests being sequenced through the lock table
// concurrently that can acquire locks on a particular key, one of which will
// win the race. Waiting requests should be oblivious to such details; instead,
// they use the concept of the transaction that has claimed a particular lock
// as the transaction to push.
//
// REQUIRES: l.mu to be locked.
func (l *lockState) claimantTxn() (_ *enginepb.TxnMeta, held bool) {
	if lockHolderTxn, _ := l.getLockHolder(); lockHolderTxn != nil {
		return lockHolderTxn, true
	}
	if l.queuedWriters.Len() == 0 {
		panic("no queued writers or lock holder; no one should be waiting on the lock")
	}
	qg := l.queuedWriters.Front().Value.(*queuedGuard)
	return qg.guard.txnMeta(), false
}

// releaseWritersFromTxn removes all waiting writers for the lockState that are
// part of the specified transaction.
// REQUIRES: l.mu is locked.
func (l *lockState) releaseWritersFromTxn(txn *enginepb.TxnMeta) {
	for e := l.queuedWriters.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		curr := e
		e = e.Next()
		g := qg.guard
		if g.isSameTxn(txn) {
			l.removeWriter(curr)
		}
	}
}

// When the active waiters have shrunk and the distinguished waiter has gone,
// try to make a new distinguished waiter if there is at least 1 active
// waiter.
//
// This function should only be called if the claimant transaction has
// not changed. This is asserted below. If the claimant transaction has changed,
// we not only need to find a new distinguished waiter, we also need to update
// the waiting state for other actively waiting requests as well; as such,
// informActiveWaiters is more appropriate.
//
// REQUIRES: l.mu is locked.
func (l *lockState) tryMakeNewDistinguished() {
	var g *lockTableGuardImpl
	claimantTxn, _ := l.claimantTxn()
	if l.waitingReaders.Len() > 0 {
		g = l.waitingReaders.Front().Value.(*lockTableGuardImpl)
	} else if l.queuedWriters.Len() > 0 {
		for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
			qg := e.Value.(*queuedGuard)
			// Only requests actively waiting at this lock should be considered for
			// the distinguished distinction.
			if qg.active && !qg.guard.isSameTxn(claimantTxn) {
				g = qg.guard
				break
			}
		}
	}
	if g != nil {
		l.distinguishedWaiter = g
		g.mu.Lock()
		assert(
			g.mu.state.txn.ID == claimantTxn.ID, "tryMakeNewDistinguished called with new claimant txn",
		)
		g.mu.state.kind = waitForDistinguished
		// The rest of g.state is already up-to-date.
		g.notify()
		g.mu.Unlock()
	}
}

// Returns true iff the lockState is empty, i.e., there is no lock holder and no
// waiters.
//
// REQUIRES: l.mu is locked.
func (l *lockState) isEmptyLock() bool {
	if l.isHeld() {
		return false // lock is held
	}
	// The lock isn't held. Sanity check the lock state is sane:
	// 1. Lock holder information should be zero-ed out.
	// 2. There should be no waiting readers.
	assert(l.holder.unreplicatedInfo.isEmpty(), "lockState !locked but non-zero unreplicatedInfo")
	assert(l.holder.replicatedInfo.isEmpty(), "lockState !locked but non-zero replicatedInfo")
	assert(l.waitingReaders.Len() == 0, "lockState with waiting readers but no holder")
	// Determine if the lock is empty or not by checking the list of queued
	// writers.
	return l.queuedWriters.Len() == 0
}

// assertEmptyLock asserts that the lockState is empty. This condition must hold
// for a lock to be safely removed from the tree. If it does not hold, requests
// with a stale snapshot of the btree will still be able to enter the lock's
// wait-queue, after which point they will never hear of lock updates.
// REQUIRES: l.mu is locked.
func (l *lockState) assertEmptyLock() {
	if !l.isEmptyLock() {
		panic("lockState is not empty")
	}
}

// assertEmptyLockUnlocked is like assertEmptyLock, but it locks the lockState.
// REQUIRES: l.mu is not locked.
func (l *lockState) assertEmptyLockUnlocked() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.assertEmptyLock()
}

// isHeld returns true if the receiver is held. If held, it may be held durably,
// non-durably, or both.
//
// REQUIRES: l.mu is locked.
func (l *lockState) isHeld() bool {
	return l.holder.txn != nil
}

// isHeldReplicated returns true if the receiver is held as a replicated lock.
//
// REQUIRES: l.mu is locked.
func (l *lockState) isHeldReplicated() bool {
	return l.isHeld() && !l.holder.replicatedInfo.ts.IsEmpty()
}

// isheldUnreplicated returns true if the receiver is held as a unreplicated
// lock.
//
// REQUIRES: l.mu is locked.
func (l *lockState) isHeldUnreplicated() bool {
	return l.isHeld() && !l.holder.unreplicatedInfo.ts.IsEmpty()
}

// Returns the duration of time the lock has been tracked as held in the lock table.
// REQUIRES: l.mu is locked.
func (l *lockState) lockHeldDuration(now time.Time) time.Duration {
	if !l.isHeld() {
		return time.Duration(0)
	}

	return now.Sub(l.holder.startTime)
}

// Returns the total amount of time all waiters in the queues of
// readers and writers have been waiting on the lock.
// REQUIRES: l.mu is locked.
func (l *lockState) totalAndMaxWaitDuration(now time.Time) (time.Duration, time.Duration) {
	var totalWaitDuration time.Duration
	var maxWaitDuration time.Duration
	for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
		g := e.Value.(*lockTableGuardImpl)
		g.mu.Lock()
		waitDuration := now.Sub(g.mu.curLockWaitStart)
		totalWaitDuration += waitDuration
		if waitDuration > maxWaitDuration {
			maxWaitDuration = waitDuration
		}
		g.mu.Unlock()
	}
	for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		g := qg.guard
		g.mu.Lock()
		waitDuration := now.Sub(g.mu.curLockWaitStart)
		totalWaitDuration += waitDuration
		if waitDuration > maxWaitDuration {
			maxWaitDuration = waitDuration
		}
		g.mu.Unlock()
	}
	return totalWaitDuration, maxWaitDuration
}

// Returns true iff the lock is currently held by the transaction with the
// given id.
// REQUIRES: l.mu is locked.
func (l *lockState) isLockedBy(id uuid.UUID) bool {
	if l.isHeld() {
		holderTxn, _ := l.getLockHolder()
		return id == holderTxn.ID
	}
	return false
}

// Returns information about the current lock holder if the lock is held, else
// returns nil.
// REQUIRES: l.mu is locked.
func (l *lockState) getLockHolder() (*enginepb.TxnMeta, hlc.Timestamp) {
	if !l.isHeld() {
		return nil, hlc.Timestamp{}
	}

	assert(
		l.isHeldReplicated() || l.isHeldUnreplicated(),
		"lock held, but no replicated or unreplicated lock holder info",
	)

	// If the lock is held as both replicated and unreplicated, we want to prefer
	// the lower of the two timestamps, since the lower timestamp contends with
	// more transactions.
	if l.isHeldReplicated() && l.isHeldUnreplicated() {
		minTS := l.holder.unreplicatedInfo.ts
		minTS.Backward(l.holder.replicatedInfo.ts)
		return l.holder.txn, minTS
	}

	// Else, we return whichever one the lock is held at.
	if l.isHeldUnreplicated() {
		return l.holder.txn, l.holder.unreplicatedInfo.ts
	}
	return l.holder.txn, l.holder.replicatedInfo.ts
}

// getLockMode returns the Mode with which a lock is held. If a lock is held
// by a transaction with multiple locking strengths, the mode corresponding to
// the highest lock strength is returned.
//
// REQUIRES: l.mu is locked.
func (l *lockState) getLockMode() lock.Mode {
	lockHolderTxn, lockHolderTS := l.getLockHolder()
	assert(lockHolderTxn != nil, "cannot get lock mode of an unheld lock")

	if l.isHeldReplicated() {
		return lock.MakeModeIntent(lockHolderTS)
	}
	return lock.MakeModeExclusive(lockHolderTS, lockHolderTxn.IsoLevel)
}

// Removes the current lock holder from the lock.
// REQUIRES: l.mu is locked.
func (l *lockState) clearLockHolder() {
	l.holder.txn = nil
	l.holder.startTime = time.Time{}
	l.holder.replicatedInfo = replicatedLockHolderInfo{}
	l.holder.unreplicatedInfo = unreplicatedLockHolderInfo{}
}

// scanAndMaybeEnqueue scans all locks held on the receiver's key and performs
// conflict resolution with the supplied request. It may[1] enqueue the request
// in the receiver's wait queues. The return value indicates whether the caller
// should suspend its scan of the lock table or not; otherwise, it is free[2]
// to proceed.
//
// [1] To understand when a request is enqueued or not, it's useful to consider
// 3 separate cases:
// 1. Transactional requests of the locking nature are always enqueued.
// 2. Transactional requests that are non-locking are only enqueued if there is a
// conflict.
// 3. Non-transactional {non-locking,write} requests are only enqueued if there
// is a conflict.
//
// [2] Locks belonging to a finalized transaction do not cause the caller to
// wait on them. Likewise, locks belonging to in-progress transactions that are
// known to be pushed to a non-conflicting timestamp (read: higher ts than the
// request's) do not cause the caller to wait on them either. However, in both
// these cases, such locks (may) need to be resolved before the request can
// evaluate. The guard's state is modified to indicate if there are locks that
// need resolution.
//
// REQUIRES: l.mu to be locked.
func (l *lockState) scanAndMaybeEnqueue(g *lockTableGuardImpl, notify bool) (wait bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isEmptyLock() {
		return false /* wait */
	}

	// It is possible that the lock is already held by this request's
	// transaction, and it is held with a lock strength good enough for it.
	if l.alreadyHoldsLockAndIsAllowedToProceed(g) {
		return false /* wait */
	}

	if g.curStrength() == lock.None {
		conflicts := l.maybeEnqueueNonLockingReadRequest(g)
		if conflicts {
			ws := l.constructWaitingState(g)
			g.startWaitingWithWaitingState(ws, notify)
			return true /* wait */
		}
		return false /* wait */
	}

	// We're purely dealing with locking requests from here on out.

	maxQueueLengthExceeded := l.enqueueLockingRequest(g)
	if maxQueueLengthExceeded {
		// NB: Requests that encounter a lock wait-queue that is longer than
		// what they're willing to wait for are rejected by the lock table
		// waiter based on the waiting state we'll construct here.
		ws := l.constructWaitingState(g)
		ws.kind = waitQueueMaxLengthExceeded
		g.startWaitingWithWaitingState(ws, notify)
		// Return true, not because we want to wait, but because we want
		// this request to be rejected in the lock table waiter.
		return true /* wait */
	}

	if l.shouldRequestActivelyWait(g) {
		ws := l.constructWaitingState(g)
		g.startWaitingWithWaitingState(ws, notify)
		// TODO(arul): In the future, when we extend the lock table to consider
		// UPDATE locks as well, we'll need to add a call to informActiveWaiters
		// here. Consider the following construction:
		//
		// keyA: [SHARED, UPDATE]
		// waitQueue: [{r1: UPDATE(seq=10)}]
		// g: {r2: Exclusive(seq=9)}
		//
		// Previously, the r1 was waiting on the UPDATE lock that is held. However,
		// once r2 slots in front of it, r2 is waiting on the SHARED lock. To
		// prevent cases where different waiters are pushing different transactions,
		// we'll need to notify r1 to push the SHARED lock instead. To do so, we
		// need to call informActiveWaiters. Note that informActiveWaiters elides
		// updates if they're not meaningful, so we can get away with being less
		// precise in handling the more general case at this level.
		return true /* wait */
	}

	l.claimBeforeProceeding(g)
	// Inform any active waiters that (may) need to be made aware that this
	// request acquired a claim.
	l.informActiveWaiters()
	return false /* wait */
}

// constructWaitingState constructs the waiting state the supplied request
// should use to wait in the receiver's lock wait-queues.
//
// REQUIRES: l.mu to be locked.
func (l *lockState) constructWaitingState(g *lockTableGuardImpl) waitingState {
	waitForState := waitingState{
		kind:          waitFor,
		key:           l.key,
		queuedWriters: l.queuedWriters.Len(),
		queuedReaders: l.waitingReaders.Len(),
		held:          true,
	}
	txn, held := l.claimantTxn()
	waitForState.held = held
	waitForState.txn = txn
	if g.isSameTxn(waitForState.txn) {
		waitForState.kind = waitSelf
	} else if l.distinguishedWaiter == g {
		waitForState.kind = waitForDistinguished
	}
	return waitForState
}

// alreadyHoldsLockAndIsAllowedToProceed returns true if the request, referenced
// by the supplied lock table guard, is allowed to proceed because its
// transaction already holds the lock with an equal or higher lock strength.
// Otherwise, false is returned.
//
// REQUIRES: l.mu to be locked.
func (l *lockState) alreadyHoldsLockAndIsAllowedToProceed(g *lockTableGuardImpl) bool {
	lockHolderTxn, _ := l.getLockHolder()
	if lockHolderTxn == nil {
		return false // no one holds the lock
	}
	if !g.isSameTxn(lockHolderTxn) {
		return false
	}
	heldMode := l.getLockMode()
	// Check if the lock is already held by the guard's transaction with an equal
	// or higher lock strength. If it is, we're good to go. Otherwise, the request
	// is trying to promote a lock it previously acquired. In such cases, the
	// existence of a lock with weaker strength doesn't do much for this request.
	// It's no different than the case where its trying to acquire a fresh lock.
	return g.curStrength() <= heldMode.Strength ||
		// TODO(arul): We want to allow requests that are writing to keys that they
		// hold exclusive locks on to "jump ahead" of any potential waiters. This
		// prevents deadlocks. The logic here is a bandaid until we implement a
		// solution for the general case of arbitrary lock upgrades
		// (e.g. shared -> exclusive, etc.). We'll do so by prioritizing requests
		// from transaction's that hold locks over transactions that don't when
		// storing them in the list of queuedWriters. Instead of sorting the list
		// of queuedWriters just based on sequence numbers alone, we'll instead use
		// (belongsToALockHolderTxn, sequence number) to construct the sort order.
		(g.curStrength() == lock.Intent && heldMode.Strength == lock.Exclusive)
}

// conflictsWithLockHolder returns true if the request, referenced by the
// supplied lockTableGuardImpl, conflicts with the lock holder. Non-conflicting
// requests are allowed to proceed; conflicting requests must actively wait for
// the lock to be released.
//
// Locks held by transactions that are known to be finalized are considered
// non-conflicting. However, the caller may be responsible for cleaning them up
// before proceeding.
//
// REQUIRES: l.mu is locked.
// REQUIRES: the transaction, to which the request belongs, should not be a lock
// holder.
func (l *lockState) conflictsWithLockHolder(g *lockTableGuardImpl) bool {
	lockHolderTxn, _ := l.getLockHolder()
	if lockHolderTxn == nil {
		return false // the lock isn't held; no conflict to speak of
	}
	// We should never get here if the lock is already held by another request
	// from the same transaction with sufficient strength (read: less than or
	// equal to what this guy wants); this should already be checked in
	// alreadyHoldLockAndIsAllowedToProceed.
	assert(
		!g.isSameTxn(lockHolderTxn) || g.curStrength() > l.getLockMode().Strength,
		"lock already held by the request's transaction with sufficient strength",
	)
	finalizedTxn, ok := g.lt.txnStatusCache.finalizedTxns.get(lockHolderTxn.ID)
	if ok {
		up := roachpb.MakeLockUpdate(finalizedTxn, roachpb.Span{Key: l.key})
		// The lock belongs to a finalized transaction. There's no conflict, but the
		// lock must be resolved -- accumulate it on the appropriate slice.
		if !l.isHeldReplicated() { // only held unreplicated
			g.toResolveUnreplicated = append(g.toResolveUnreplicated, up)
		} else {
			g.toResolve = append(g.toResolve, up)
		}
		return false
	}

	// The lock is held by a different, un-finalized transaction.

	if g.curStrength() == lock.None {
		// If the non-locking reader is reading at a higher timestamp than the lock
		// holder, but it knows that the lock holder has been pushed above its read
		// timestamp, it can proceed after rewriting the lock at its transaction's
		// pushed timestamp. Intent resolution can be deferred to maximize batching
		// opportunities.
		//
		// This fast-path is only enabled for readers without uncertainty intervals,
		// as readers with uncertainty intervals must contend with the possibility
		// of pushing a conflicting intent up into their uncertainty interval and
		// causing more work for themselves, which is avoided with care by the
		// lockTableWaiter but difficult to coordinate through the txnStatusCache.
		// This limitation is acceptable because the most important case here is
		// optimizing the Export requests issued by backup.
		if !g.hasUncertaintyInterval() && g.lt.batchPushedLockResolution() {
			pushedTxn, ok := g.lt.txnStatusCache.pendingTxns.get(lockHolderTxn.ID)
			if ok && g.ts.Less(pushedTxn.WriteTimestamp) {
				up := roachpb.MakeLockUpdate(pushedTxn, roachpb.Span{Key: l.key})
				if !l.isHeldReplicated() {
					// Only held unreplicated. Accumulate it as an unreplicated lock to
					// resolve, in case any other waiting readers can benefit from the
					// pushed timestamp.
					//
					// TODO(arul): this case is only possible while non-locking reads
					// block on Exclusive locks. Once non-locking reads start only
					// blocking on intents, it can be removed and asserted against.
					g.toResolveUnreplicated = append(g.toResolveUnreplicated, up)
				} else {
					// Resolve to push the replicated intent.
					g.toResolve = append(g.toResolve, up)
				}
				return false
			}
		}

		g.mu.Lock()
		_, alsoLocksWithHigherStrength := g.mu.locks[l]
		g.mu.Unlock()
		if alsoLocksWithHigherStrength {
			// If the request already has this lock in its locks map, it must also be
			// trying to acquire this lock at a higher strength. For it to be here, it
			// must have a (possibly joint) claim on this lock. The claim may have been
			// broken since, but that's besides the point -- we defer to the stronger
			// lock strength and continue with our scan.
			//
			// NB: If we were to not defer to the stronger lock strength and start
			// waiting here, we could potentially end up doing so in the wrong wait
			// queue (queuedReaders vs. queuedWriters). There wouldn't be a correctness
			// issue in doing so, but it isn't ideal.
			//
			// NB: Non-transactional requests do not make claims or acquire locks. They
			// can only perform reads or writes, which means they can only have lock
			// spans with strength {None,Intent}. However, because they cannot make
			// claims on locks, we can not detect a key is being accessed with both None
			// and Intent locking strengths, like we can for transactional requests. In
			// some rare cases, the lock may now be held at a timestamp that is not
			// compatible with this request, and it will wait here -- there's no
			// correctness issue in doing so.
			//
			// TODO(arul): I'm not entirely sure I understand why we have the
			// g.str == lock.None condition above. We do need it, because taking it
			// out breaks some tests. Will need to figure this out when trying to
			// extend the lock table to work with multiple lock strengths.
			return false
		}
	}

	// The held lock neither belongs to the request's transaction (which has
	// special handling above) nor to a transaction that has been finalized. Check
	// for conflicts.
	return lock.Conflicts(l.getLockMode(), g.curLockMode(), &g.lt.settings.SV)
}

// maybeEnqueueNonLockingReadRequest enqueues a read request in the receiver's
// wait queue if the reader conflicts with the lock; otherwise, it's a no-op.
// A boolean is returned indicating whether the read request conflicted with
// the lock or not.
//
// REQUIRES: l.mu to be locked.
func (l *lockState) maybeEnqueueNonLockingReadRequest(g *lockTableGuardImpl) (conflicts bool) {
	assert(g.curStrength() == lock.None, "unexpected locking strength; expected read")
	if !l.conflictsWithLockHolder(g) {
		return false // no conflict, no need to enqueue
	}
	l.waitingReaders.PushFront(g)
	// This request may be a candidate to become a distinguished waiter if one
	// doesn't exist yet; try making it such.
	l.maybeMakeDistinguishedWaiter(g)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mu.locks[l] = struct{}{}
	return true
}

// enqueueLockingRequest enqueues the supplied locking request in the receiver's
// lock wait queue. The locking request is wrapped in a queuedGuard which
// denotes it is actively waiting at the receiver. Note that the request may
// already be present in the lock's wait queue, in which case, the queuedGuard
// is modified to reflect its status as an active waiter, if necessary.
//
// Locking requests have a maximum queue length bound configured for them above
// which they refuse to wait. If the receiver's wait queue is longer than this
// configured bound the request is not enqueued; instead, a boolean indicating
// this case is returned to the caller.
//
// REQUIRES: l.mu to be locked.
func (l *lockState) enqueueLockingRequest(g *lockTableGuardImpl) (maxQueueLengthExceeded bool) {
	assert(g.curStrength() != lock.None, "should only be called with a locking request")
	g.mu.Lock()
	defer g.mu.Unlock()

	// First, check if the request is already in the queue. This can happen if
	// this function is called on behalf of a request that was previously was
	// an inactive waiter at this lock and comes back around.
	if _, inQueue := g.mu.locks[l]; inQueue {
		// Find the request; it must already be in the correct position.
		for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
			qqg := e.Value.(*queuedGuard)
			if qqg.guard == g {
				qqg.active = true // set the active status as true, in case it wasn't before
				// Now that this request is actively waiting in the lock's wait queue,
				// it may be a candidate for becoming the distinguished waiter (if one
				// doesn't exist already).
				l.maybeMakeDistinguishedWaiter(g)
				return false /* maxQueueLengthExceeded */
			}
		}
		panic("lock table bug")
	}

	// Check if the lock's wait queue has room for one more request.
	if g.maxWaitQueueLength > 0 && l.queuedWriters.Len() >= g.maxWaitQueueLength {
		// The wait-queue is longer than the request is willing to wait for.
		// Instead of entering the queue, immediately reject the request. For
		// simplicity, we are not finding the position of this writer in the
		// queue and rejecting the tail of the queue above the max length. That
		// would be more fair, but more complicated, and we expect that the
		// common case is that this waiter will be at the end of the queue.
		return true /* maxQueueLengthExceeded */
	}
	qg := &queuedGuard{
		guard:  g,
		active: true,
	}
	// The request isn't in the queue. Add it in the correct position, based on
	// its sequence number.
	var e *list.Element
	for e = l.queuedWriters.Back(); e != nil; e = e.Prev() {
		qqg := e.Value.(*queuedGuard)
		if qqg.guard.seqNum < qg.guard.seqNum {
			break
		}
	}
	if e == nil {
		l.queuedWriters.PushFront(qg)
	} else {
		l.queuedWriters.InsertAfter(qg, e)
	}
	// This request may be a candidate to become a distinguished waiter if one
	// doesn't exist yet; try making it such.
	l.maybeMakeDistinguishedWaiter(g)
	g.mu.locks[l] = struct{}{}
	return false /* maxQueueLengthExceeded */
}

// maybeMakeDistinguishedWaiter designates the supplied request as the
// distinguished waiter if no distinguished waiter. If there is a distinguished
// waiter, or the supplied request is not a candidate for becoming one[1], the
// function is a no-op.
//
// [1] A request that belongs to the lock's claimant transaction is not eligible
// to become a distinguished waiter.
//
// REQUIRES: l.mu to be locked.
func (l *lockState) maybeMakeDistinguishedWaiter(g *lockTableGuardImpl) {
	if l.distinguishedWaiter != nil {
		return
	}
	claimantTxn, _ := l.claimantTxn()
	if !g.isSameTxn(claimantTxn) {
		// We only want to make this request the distinguished waiter if a
		// different request from its transaction isn't the claimant.
		l.distinguishedWaiter = g
	}
}

// shouldRequestActivelyWait returns true iff the supplied request needs to
// actively wait on the receiver.
//
// REQUIRES: l.mu to be locked.
// REQUIRES: g.mu to be locked.
func (l *lockState) shouldRequestActivelyWait(g *lockTableGuardImpl) bool {
	if g.curStrength() == lock.None {
		return true // non-locking read requests always actively wait
	}

	if l.conflictsWithLockHolder(g) {
		return true
	}

	// Start at the head of the queue and iterate backwards. As we iterate, we
	// check for conflicts -- if a conflict is found, the request must actively
	// wait; otherwise, it is free to proceed. Notably, we do not check the
	// active/inactive status of the other requests as we're iterating through the
	// head of the queue[1].
	//
	// [1] This means that there may be an active waiter in front of the request,
	// and it may still decide to not actively wait on the receiver. This can only
	// happen if there are UPDATE strengths in the mix; this is because
	// constructions using just SHARED, EXCLUSIVE, and INTENT lock strengths would
	// result in there either being a conflict, or the head of the queue must
	// entirely be comprised of inactive waiters. The lock table currenlty does
	// not support UPDATE locks. Even if it did, there would be no correctness
	// issue with what we're doing here, as long as the queue is maintained in
	// sequence number order.
	for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
		qqg := e.Value.(*queuedGuard)
		if qqg.guard == g {
			// We found our request while scanning from the front without finding any
			// conflicting waiters; no need to actively wait here.
			return false
		}
		// TODO(arul): Inactive waiters will need to capture the strength at which
		// they're trying to acquire a lock in their queuedGuard. We can't simply
		// use the guard's curStrength (or curLockMode) -- inactive waiters may have
		// mutated these values as they scan. For now, we can just use the intent
		// lock mode as that's the only lock strength supported by the lock table.
		waiterLockMode := lock.MakeModeIntent(qqg.guard.ts)
		if lock.Conflicts(waiterLockMode, g.curLockMode(), &g.lt.settings.SV) {
			return true
		}
	}
	panic("lock table bug: enqueued request not found")
}

// claimBeforeProceeding adjusts datastructures on the receiver such that the
// supplied request lays a claim[1] on it before proceeding[2]. This method
// should only be called by locking requests.
//
// Note that the request acquiring the claim may be a distinguished waiter at
// this lock. In such cases, the distinguished status is cleared to allow the
// request to proceed (inactive waiters cannot be distinguished waiters).
// However, a new distinguished waiter is not chosen -- it's the caller's
// responsibility to detect this case and actually choose one. Typically, this
// is done using a call to informActiveWaiters.
//
// [1] Only transactional, locking requests can lay claims. Non-transactional
// writers cannot.
// [2] While non-transactional writers cannot lay claims, they do need to be
// removed from the receiver's wait queue before proceeding. We do that here.
//
// REQUIRES: l.mu to be locked.
func (l *lockState) claimBeforeProceeding(g *lockTableGuardImpl) {
	assert(g.curStrength() != lock.None, "non-locking requests should not try to grab claims")

	// We're dealing with either a locking, transactional request or a
	// non-transactional writer. We handle these two cases differently, because
	// non-transactional writers are special cased[1]:
	// 1. If the request is a transactional, locking request, it acquires a
	// (possibly joint) claim by marking itself as inactive in the receiver's wait
	// queue.
	// 2. If it's a non-transactional writer, we simply remove it from the queue.
	// As such, it races with other readers and transactional locking requests.
	//
	// [1] Non-transactional are special cased, in that they cannot lay claims on
	// a lock (mark themselves as inactive and proceed with their scan). This is
	// because doing so could result in undetectable deadlocks, as our distributed
	// deadlock detection algorithm relies on {Push,Query}Txn requests.
	// Non-transactional writers, by definition, have no associated transaction a
	// waiter can push.

	// Find the request; iterate from the front, as requests proceeding are more
	// likely to be closer to the front than the back.
	for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
		qqg := e.Value.(*queuedGuard)
		if qqg.guard == g {
			// If the request was previously marked as a distinguished waiter, and is
			// now able to claim the lock and proceed, clear the designation. Note
			// that we're not choosing a new one to replace it; the responsibility of
			// doing so is the caller's.
			if g == l.distinguishedWaiter {
				l.distinguishedWaiter = nil
			}
			if g.txn == nil {
				// Non-transactional writer.
				g.mu.Lock()
				delete(g.mu.locks, l)
				g.mu.Unlock()
				l.queuedWriters.Remove(e)
			} else {
				// Transactional writer.
				qqg.active = false // claim the lock
			}
			return
		}
	}
	panic("lock table bug: did not find enqueued request")
}

func (l *lockState) isNonConflictingLock(g *lockTableGuardImpl, str lock.Strength) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// It is possible that this lock is empty and has not yet been deleted.
	if l.isEmptyLock() {
		return true
	}
	// Lock is not empty.
	lockHolderTxn, lockHolderTS := l.getLockHolder()
	if lockHolderTxn == nil {
		// Transactions that have claimed the lock, but have not acquired it yet,
		// are considered non-conflicting.
		//
		// Optimistic evaluation may call into this function with or without holding
		// latches. It's worth considering both these cases separately:
		//
		// 1. If Optimistic evaluation is holding latches, then there cannot be a
		// conflicting request that has claimed (but not acquired) the lock that is
		// also holding latches. A request could have claimed this lock, discovered
		// a different lock, and dropped its latches before waiting in this second
		// lock's wait queue. In such cases, the request that claimed this lock will
		// have to re-acquire and re-scan the lock table after this optimistic
		// evaluation request drops its latches.
		//
		// 2. If optimistic evaluation does not hold latches, then it will check for
		// conflicting latches before declaring success. A request that claimed this
		// lock, did not discover any other locks, and proceeded to evaluation would
		// thus conflict on latching with our request going through optimistic
		// evaluation. This will be detected, and the request will have to retry
		// pessimistically.
		//
		// All this is to say that if we found a claimed, but not yet acquired lock,
		// we can treat it as non-conflicting. It'll either be detected as a true
		// conflict when we check for conflicting latches, or the request that
		// claimed the lock will know what happened and what to do about it.
		return true
	}
	if g.isSameTxn(lockHolderTxn) {
		// Already locked by this txn.
		return true
	}
	// NB: We do not look at the txnStatusCache in this optimistic evaluation
	// path. A conflict with a finalized txn will be noticed when retrying
	// pessimistically.

	if str == lock.None && g.ts.Less(lockHolderTS) {
		return true
	}
	// Conflicts.
	return false
}

// Acquires this lock. Any requests that are waiting in the lock's wait queues
// from the transaction acquiring the lock are also released.
//
// Acquires l.mu.
func (l *lockState) acquireLock(acq *roachpb.LockAcquisition, clock *hlc.Clock) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isHeld() {
		// Already held.
		beforeTxn, beforeTs := l.getLockHolder()
		if acq.Txn.ID != beforeTxn.ID {
			return errors.AssertionFailedf("existing lock cannot be acquired by different transaction")
		}
		// An unreplicated lock is being re-acquired...
		if acq.Durability == lock.Unreplicated && l.isHeldUnreplicated() {
			switch {
			case l.holder.txn.Epoch < acq.Txn.Epoch: // at a higher epoch
				// Clear sequence numbers from the older epoch.
				l.holder.unreplicatedInfo.seqs = l.holder.unreplicatedInfo.seqs[:0]
			case l.holder.txn.Epoch == acq.Txn.Epoch: // at the same epoch
				// Prune the list of sequence numbers tracked for this lock by removing
				// any sequence numbers that are considered ignored by virtue of a
				// savepoint rollback.
				//
				// Note that the in-memory lock table is the source of truth for just
				// unreplicated locks, so we only do this pruning for unreplicated lock
				// acquisition. On the other hand, for replicated locks, the source of
				// truth is what's written in MVCC. We could try and mimic that logic
				// here, but we choose not to, as doing so is error-prone/difficult to
				// maintain.
				l.holder.unreplicatedInfo.seqs = removeIgnored(
					l.holder.unreplicatedInfo.seqs, acq.IgnoredSeqNums,
				)
			case l.holder.txn.Epoch > acq.Txn.Epoch: // at a prior epoch
				// Reject the request; the logic here parallels how mvccPutInternal
				// handles this case for intents.
				return errors.Errorf(
					"locking request with epoch %d came after lock(unreplicated) had already been acquired at epoch %d in txn %s",
					acq.Txn.Epoch, l.holder.txn.Epoch, acq.Txn.ID,
				)
			default:
				panic("unreachable")
			}
		}
		if l.isIdempotentLockAcquisition(acq) {
			return nil // nothing more to do here.
		}
		// Forward the lock's timestamp instead of assigning to it blindly.
		// While lock acquisition uses monotonically increasing timestamps
		// from the perspective of the transaction's coordinator, this does
		// not guarantee that a lock will never be acquired at a higher
		// epoch and/or sequence number but with a lower timestamp when in
		// the presence of transaction pushes. Consider the following
		// sequence of events:
		//
		//  - txn A acquires lock at sequence 1, ts 10
		//  - txn B pushes txn A to ts 20
		//  - txn B updates lock to ts 20
		//  - txn A's coordinator does not immediately learn of the push
		//  - txn A re-acquires lock at sequence 2, ts 15
		//
		// A lock's timestamp at a given durability level is not allowed to
		// regress, so by forwarding its timestamp during the second acquisition
		// instead of assigning to it blindly, it remains at 20.
		//
		// However, a lock's timestamp as reported by getLockHolder can regress
		// if it is acquired at a lower timestamp and a different durability
		// than it was previously held with. This is necessary to support
		// because the hard constraint which we must uphold here that the
		// lockHolderInfo for a replicated lock cannot diverge from the
		// replicated state machine in such a way that its timestamp in the
		// lockTable exceeds that in the replicated keyspace. If this invariant
		// were to be violated, we'd risk infinite lock-discovery loops for
		// requests that conflict with the lock as is written in the replicated
		// state machine but not as is reflected in the lockTable.
		//
		// Lock timestamp regressions are safe from the perspective of other
		// transactions because the request which re-acquired the lock at the
		// lower timestamp must have been holding a write latch at or below the
		// new lock's timestamp. This means that no conflicting requests could
		// be evaluating concurrently. Instead, all will need to re-scan the
		// lockTable once they acquire latches and will notice the reduced
		// timestamp at that point, which may cause them to conflict with the
		// lock even if they had not conflicted before. In a sense, it is no
		// different than the first time a lock is added to the lockTable.
		switch acq.Durability {
		case lock.Unreplicated:
			l.holder.unreplicatedInfo.ts.Forward(acq.Txn.WriteTimestamp)
			l.holder.unreplicatedInfo.seqs = append(l.holder.unreplicatedInfo.seqs, acq.Txn.Sequence)
		case lock.Replicated:
			l.holder.replicatedInfo.ts.Forward(acq.Txn.WriteTimestamp)
		default:
			panic(fmt.Sprintf("unknown lock durability: %s", acq.Durability))
		}

		// Selectively update the txn meta.
		switch {
		case l.holder.txn.Epoch > acq.Txn.Epoch: // lock is being acquired at a prior epoch
			// We do not update the txn meta here -- the epoch is not allowed to
			// regress.
			//
			// NB: We can get here if the lock acquisition here corresponds to an
			// operation from a prior epoch. We've already handled the case for
			// unreplicated lock acquisition above, so this can only happen if the
			// lock acquisition corresponds to a replicated lock.
			//
			// If mvccPutInternal is aware of the newer epoch, it'll simply reject
			// this operation and we'll never get here. However, it's not guaranteed
			// that mvccPutInternal knows about this newer epoch. The in-memory lock
			// table may know about the newer epoch though, if there's been a
			// different unreplicated lock acquisition on this key by the transaction.
			// So if we were to blindly update the TxnMeta here, we'd be regressing
			// the epoch, which messes with our sequence number tracking inside of
			// unreplicatedLockInfo.
			assert(acq.Durability == lock.Replicated, "the unreplicated case should have been handled above")
		case l.holder.txn.Epoch == acq.Txn.Epoch: // lock is being acquired at the same epoch
			l.holder.txn = &acq.Txn
		case l.holder.txn.Epoch < acq.Txn.Epoch: // lock is being acquired at a newer epoch
			l.holder.txn = &acq.Txn
			// The txn meta tracked here corresponds to unreplicated locks. When we
			// learn about a newer epoch during lock acquisition of a replicated lock,
			// we clear out the unreplicatedLockInfo state being tracked from the
			// older epoch.
			//
			// Note that we don't clear out replicatedLockInfo when an unreplicated
			// lock is being acquired at a newer epoch. This is because replicated
			// locks are held across epochs, and there is no per-epoch tracking in the
			// lock table for them.
			if acq.Durability == lock.Replicated {
				l.holder.unreplicatedInfo = unreplicatedLockHolderInfo{}
			}
		}

		_, afterTs := l.getLockHolder()
		if beforeTs.Less(afterTs) {
			l.increasedLockTs(afterTs)
		}
		return nil
	}

	// NB: The lock isn't held, so the request trying to acquire the lock must be
	// an (inactive) queued writer in the lock's wait queues. Typically, we expect
	// this to be the first queued writer; the list of queued writers is
	// maintained in lock table arrival order. When a lock transitions from held
	// to released, the first of these writers is marked as inactive and allowed
	// to proceed. This is done to uphold fairness between concurrent lock
	// acquirers. However, in some rare cases[1], this may not be true -- i.e.,
	// the request trying to acquire the lock here may not be the first queued
	// writer. This does not violate any correctness properties. This is because
	// the request must be holding latches, as it has proceeded to evaluation for
	// it to be calling into this method. As such, it is isolated from the first
	// inactive queued writer.
	//
	// [1] Requests that run into conflicting locks drop their latches and enter
	// its wait queues. Once the lock is released, and they can proceed with their
	// scan, they do so without re-acquiring latches. In such cases, latches are
	// acquired before evaluation. So they may insert themselves in front of
	// another inactive waiting writer (which may or may not hold latches) if
	// their arrival order dictates as such. The rare cases being talked about
	// above are when the inactive waiting writer (in front of which the request
	// inserted itself) was evaluating while holding latches and calls into this
	// function once it finishes evaluation to actually acquire the lock.

	l.releaseWritersFromTxn(&acq.Txn)

	// Sanity check that there aren't any waiting readers on this lock. There
	// shouldn't be any, as the lock wasn't held.
	if l.waitingReaders.Len() > 0 {
		panic("lockTable bug")
	}

	l.holder.txn = &acq.Txn // first acquisition, blindly assign
	l.holder.startTime = clock.PhysicalTime()
	switch acq.Durability {
	case lock.Unreplicated:
		l.holder.unreplicatedInfo.ts = acq.Txn.WriteTimestamp
		l.holder.unreplicatedInfo.seqs = append([]enginepb.TxnSeq(nil), acq.Txn.Sequence)
	case lock.Replicated:
		l.holder.replicatedInfo.ts = acq.Txn.WriteTimestamp
	default:
		panic(fmt.Sprintf("unknown lock durability: %s", acq.Durability))
	}

	// Inform active waiters since lock has transitioned to held.
	l.informActiveWaiters()
	return nil
}

// isIdempotentLockAcquisition returns true if the lock acquisition is
// idempotent. Idempotent lock acquisitions do not require any changes to what
// is being tracked in the lock's state.
//
// REQUIRES: l.mu to be locked.
func (l *lockState) isIdempotentLockAcquisition(acq *roachpb.LockAcquisition) bool {
	txn, _ := l.getLockHolder()
	assert(txn.ID == acq.Txn.ID, "existing lock transaction is different from the acquisition")
	switch acq.Durability {
	case lock.Unreplicated:
		seqs := l.holder.unreplicatedInfo.seqs
		// Cheaply check if this could be an idempotent lock acquisition.
		if len(seqs) > 0 && seqs[len(seqs)-1] >= acq.Txn.Sequence {
			// Idempotent lock acquisition. In this case, we simply ignore the lock
			// acquisition as long as it corresponds to an existing sequence number.
			// If the sequence number is not being tracked yet, insert it into the
			// sequence history. The validity of such a lock re-acquisition should
			// have already been determined at the MVCC level.

			if i := sort.Search(len(seqs), func(i int) bool {
				return seqs[i] >= acq.Txn.Sequence
			}); i == len(seqs) {
				panic("lockTable bug - search value <= last element")
			} else if seqs[i] != acq.Txn.Sequence {
				// TODO(arul): Once we change the lockState datastructure to only track
				// the highest sequence number, we should remove all mutations happening
				// inside this function.
				seqs = append(seqs, 0)
				copy(seqs[i+1:], seqs[i:])
				seqs[i] = acq.Txn.Sequence
				l.holder.unreplicatedInfo.seqs = seqs
			}
			// NB: Lock re-acquisitions at different timestamps are not considered
			// idempotent. Strictly speaking, we could tighten this condition to
			// consider lock re-acquisition at lower timestamps idempotent, as a
			// lock's timestamp at a given durability never regresses.
			return l.holder.unreplicatedInfo.ts.Equal(acq.Txn.WriteTimestamp)
		}
		return false
	case lock.Replicated:
		// NB: Lock re-acquisitions at different timestamps are not considered
		// idempotent. Strictly speaking, we could tighten this condition to
		// consider lock re-acquisition at lower timestamps idempotent, as a
		// lock's timestamp at a given durability never regresses.
		return l.isHeldReplicated() && l.holder.replicatedInfo.ts.Equal(acq.Txn.WriteTimestamp)
	default:
		panic(fmt.Sprintf("unknown lock durability: %s", acq.Durability))
	}
}

// A replicated lock held by txn with timestamp ts was discovered by guard g
// where g is trying to access this key with strength accessStrength.
// Acquires l.mu.
func (l *lockState) discoveredLock(
	txn *enginepb.TxnMeta,
	ts hlc.Timestamp,
	g *lockTableGuardImpl,
	accessStrength lock.Strength,
	notRemovable bool,
	clock *hlc.Clock,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if notRemovable {
		l.notRemovable++
	}
	if l.isHeld() {
		if !l.isLockedBy(txn.ID) {
			return errors.AssertionFailedf(
				"discovered lock by different transaction (%s) than existing lock (see issue #63592): %s",
				txn, l)
		}
		// TODO(arul): If the discovered lock indicates a newer epoch than what's
		// being tracked, should we clear out unreplicatedLockInfo here?
	} else {
		l.holder.txn = txn
		l.holder.startTime = clock.PhysicalTime()
	}

	if l.holder.replicatedInfo.isEmpty() {
		l.holder.replicatedInfo.ts = ts
	}

	switch accessStrength {
	case lock.None:
		// Don't enter the lock's queuedReaders list, because all queued readers
		// are expected to be active. Instead, wait until the next scan.

		// Confirm that the guard will wait on the lock the next time it scans
		// the lock table. If not then it shouldn't have discovered the lock in
		// the first place. Bugs here would cause infinite loops where the same
		// lock is repeatedly re-discovered.
		if g.ts.Less(ts) {
			return errors.AssertionFailedf("discovered non-conflicting lock")
		}

	case lock.Intent, lock.Exclusive:
		// Immediately enter the lock's queuedWriters list.
		// NB: this inactive waiter can be non-transactional.
		g.mu.Lock()
		_, presentHere := g.mu.locks[l]
		if !presentHere {
			// Since g will place itself in queue as inactive waiter below.
			g.mu.locks[l] = struct{}{}
		}
		g.mu.Unlock()

		if !presentHere {
			// Put self in queue as inactive waiter.
			qg := &queuedGuard{
				guard:  g,
				active: false,
			}
			// g is not necessarily first in the queue in the (rare) case (a) above.
			var e *list.Element
			for e = l.queuedWriters.Front(); e != nil; e = e.Next() {
				qqg := e.Value.(*queuedGuard)
				if qqg.guard.seqNum > g.seqNum {
					break
				}
			}
			if e == nil {
				l.queuedWriters.PushBack(qg)
			} else {
				l.queuedWriters.InsertBefore(qg, e)
			}
		}
	default:
		panic(errors.AssertionFailedf("unhandled lock strength %s", accessStrength))
	}

	// If there are waiting requests from the same txn, they no longer need to wait.
	l.releaseWritersFromTxn(txn)

	// Active waiters need to be told about who they are waiting for.
	l.informActiveWaiters()
	return nil
}

func (l *lockState) decrementNotRemovable() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.notRemovable--
	if l.notRemovable < 0 {
		panic(fmt.Sprintf("lockState.notRemovable is negative: %d", l.notRemovable))
	}
}

// Acquires l.mu.
func (l *lockState) tryClearLock(force bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.notRemovable > 0 && !force {
		return false
	}

	// Clear lock holder. While doing so, construct the closure used to transition
	// waiters.
	lockHolderTxn, _ := l.getLockHolder() // only needed if this is a replicated lock
	replicatedHeld := l.isHeldReplicated()
	transitionWaiter := func(g *lockTableGuardImpl) {
		if replicatedHeld && !force {
			// Note that none of the current waiters can be requests from
			// lockHolderTxn, so they will never be told to waitElsewhere on
			// themselves.
			waitState := waitingState{
				kind: waitElsewhere,
				txn:  lockHolderTxn,
				key:  l.key,
				held: true,
			}
			g.updateWaitingStateLocked(waitState)
		} else {
			// !replicatedHeld || force. Both are handled as doneWaiting since the
			// system is no longer tracking the lock that was possibly held.
			g.updateStateToDoneWaitingLocked()
		}
	}
	l.clearLockHolder()

	// Clear waitingReaders.
	for e := l.waitingReaders.Front(); e != nil; {
		g := e.Value.(*lockTableGuardImpl)

		curr := e
		e = e.Next()
		l.waitingReaders.Remove(curr)

		g.mu.Lock()
		transitionWaiter(g)
		g.notify()
		delete(g.mu.locks, l)
		g.mu.Unlock()
	}

	// Clear queuedWriters.
	for e := l.queuedWriters.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)

		curr := e
		e = e.Next()
		l.queuedWriters.Remove(curr)

		g := qg.guard
		g.mu.Lock()
		if qg.active {
			transitionWaiter(g)
			g.notify()
		}
		delete(g.mu.locks, l)
		g.mu.Unlock()
	}

	// Clear distinguishedWaiter.
	l.distinguishedWaiter = nil

	// The lockState must now be empty.
	l.assertEmptyLock()
	return true
}

// Removes the TxnSeqs in heldSeqNums that are contained in ignoredSeqNums.
// REQUIRES: ignoredSeqNums contains non-overlapping ranges and sorted in
// increasing seq order.
func removeIgnored(
	heldSeqNums []enginepb.TxnSeq, ignoredSeqNums []enginepb.IgnoredSeqNumRange,
) []enginepb.TxnSeq {
	if len(ignoredSeqNums) == 0 {
		return heldSeqNums
	}
	held := heldSeqNums[:0]
	for _, n := range heldSeqNums {
		i := sort.Search(len(ignoredSeqNums), func(i int) bool { return ignoredSeqNums[i].End >= n })
		if i == len(ignoredSeqNums) || ignoredSeqNums[i].Start > n {
			held = append(held, n)
		}
	}
	return held
}

// Tries to update the lock: noop if this lock is held by a different
// transaction, else the lock is updated. Returns whether the lockState can be
// garbage collected, and whether it was held by the txn.
// Acquires l.mu.
func (l *lockState) tryUpdateLock(up *roachpb.LockUpdate) (heldByTxn, gc bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.tryUpdateLockLocked(*up)
}

// REQUIRES: l.mu is locked.
func (l *lockState) tryUpdateLockLocked(up roachpb.LockUpdate) (heldByTxn, gc bool) {
	if l.isEmptyLock() {
		// Already free. This can happen when an unreplicated lock is removed in
		// tryActiveWait due to the txn being in the txnStatusCache.
		return false, true
	}
	if !l.isLockedBy(up.Txn.ID) {
		return false, false
	}
	if up.Status.IsFinalized() {
		l.clearLockHolder()
		gc = l.lockIsFree()
		return true, gc
	}

	txn := &up.Txn
	ts := up.Txn.WriteTimestamp
	_, beforeTs := l.getLockHolder()
	advancedTs := beforeTs.Less(ts)
	isLocked := false
	// The MVCC keyspace is the source of truth about the disposition of a
	// replicated lock. Replicated locks are updated using
	// mvccResolveWriteIntent(). Trying to be consistent[1] with the handling in
	// mvccResolveWriteIntent() is error-prone, especially as things evolve.
	// Instead of attempting to do so, we simply forget the replicated lock.
	//
	// [1] For a little flavour of the complexity here, mvccResolveWriteIntent()
	// has special handling of the case where a pusher is using an epoch lower
	// than the epoch of the intent. The intent is written at the newer epoch (the
	// one not known to the pusher) but at a higher timestamp. The pusher will
	// then call into this function with that lower epoch.
	if !l.holder.replicatedInfo.isEmpty() {
		l.holder.replicatedInfo = replicatedLockHolderInfo{}
	}
	// However, for unreplicated locks, the lock table is the source of truth.
	// As such, we best-effort mirror the behavior of mvccResolveWriteIntent().
	if l.isHeldUnreplicated() {
		switch {
		//...update corresponds to a higher epoch.
		case txn.Epoch > l.holder.txn.Epoch:
			// Forget what was tracked previously.
			l.holder.unreplicatedInfo = unreplicatedLockHolderInfo{}

			// ...update corresponds to the current epoch.
		case txn.Epoch == l.holder.txn.Epoch:
			l.holder.unreplicatedInfo.seqs = removeIgnored(l.holder.unreplicatedInfo.seqs, up.IgnoredSeqNums)
			if len(l.holder.unreplicatedInfo.seqs) == 0 {
				l.holder.unreplicatedInfo = unreplicatedLockHolderInfo{}
				isLocked = false
				break
			}
			if advancedTs {
				// NB: Unlike the case below, where we can't update the txn object
				// because of the desire to best-effort mirror mvccResolveWriteIntent
				// internal, we can do so here because the epochs are the same. Note
				// that doing so doesn't get us too much -- other than the epoch and
				// transaction ID, we don't make use of the fields on the TxnMeta
				// anyway (and we know both of those haven't changed).
				l.holder.txn = txn
				l.holder.unreplicatedInfo.ts = ts
			}
			isLocked = true

			// ...update corresponds to an older epoch of the transaction.
		case txn.Epoch < l.holder.txn.Epoch:
			if advancedTs {
				// We may advance ts here but not update the holder.txn object below for
				// the reason stated in the comment about mvccResolveWriteIntent(). The
				// {unreplicated,replicated}LockHolderInfo.ts is the source of truth
				// regarding the timestamp of the lock, and not TxnMeta.WriteTimestamp.
				l.holder.unreplicatedInfo.ts = ts
			}
			isLocked = true
		default:
			panic("unreachable")
		}
	}

	if !isLocked {
		l.clearLockHolder()
		gc = l.lockIsFree()
		return true, gc
	}

	if advancedTs {
		l.increasedLockTs(ts)
	}
	// Else no change for waiters. This can happen due to a race between different
	// callers of UpdateLocks().

	return true, false
}

// The lock holder timestamp has increased. Some of the waiters may no longer
// need to wait.
// REQUIRES: l.mu is locked.
func (l *lockState) increasedLockTs(newTs hlc.Timestamp) {
	distinguishedRemoved := false
	for e := l.waitingReaders.Front(); e != nil; {
		g := e.Value.(*lockTableGuardImpl)
		curr := e
		e = e.Next()
		if g.ts.Less(newTs) {
			distinguishedRemoved = distinguishedRemoved || l.removeReader(curr)
		}
		// Else don't inform an active waiter which continues to be an active waiter
		// despite the timestamp increase.
	}
	if distinguishedRemoved {
		l.tryMakeNewDistinguished()
	}
}

// removeWriter removes the writer, referenced by the supplied list.Element,
// from the lock's queuedWriters list. Returns whether the writer was the
// distinguished waiter.
func (l *lockState) removeWriter(e *list.Element) bool {
	qg := e.Value.(*queuedGuard)
	g := qg.guard
	l.queuedWriters.Remove(e)
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.mu.locks, l)
	if qg.active {
		g.doneActivelyWaitingAtLock()
	}
	if g == l.distinguishedWaiter {
		assert(qg.active, "distinguished waiter should be active")
		l.distinguishedWaiter = nil
		return true
	}
	return false
}

// removeReader removes the reader, referenced by the supplied list.Element,
// from the lock's queuedReaders list. Returns whether the reader was the
// distinguished waiter or not.
func (l *lockState) removeReader(e *list.Element) bool {
	g := e.Value.(*lockTableGuardImpl)
	l.waitingReaders.Remove(e)
	g.mu.Lock()
	delete(g.mu.locks, l)
	g.doneActivelyWaitingAtLock()
	g.mu.Unlock()
	if g == l.distinguishedWaiter {
		l.distinguishedWaiter = nil
		return true
	}
	return false
}

// A request known to this lockState is done. The request could be a waiting
// reader or writer. Note that there is the possibility of a race and the g may
// no longer be known to l, which we treat as a noop (this race is allowed since
// we order l.mu > g.mu). Returns whether the lockState can be garbage
// collected.
//
// Acquires l.mu.
func (l *lockState) requestDone(g *lockTableGuardImpl) (gc bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	g.mu.Lock()
	if _, present := g.mu.locks[l]; !present {
		g.mu.Unlock()
		return false
	}
	delete(g.mu.locks, l)
	g.mu.Unlock()

	// May be in queuedWriters or waitingReaders.
	distinguishedRemoved := false
	doneRemoval := false
	for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		if qg.guard == g {
			l.queuedWriters.Remove(e)
			if qg.guard == l.distinguishedWaiter {
				distinguishedRemoved = true
				l.distinguishedWaiter = nil
			}
			doneRemoval = true
			break
		}
	}

	if !l.isHeld() && doneRemoval {
		// The head of the list of waiting writers should always be an inactive,
		// transactional writer if the lock isn't held. That may no longer be true
		// if the guy we removed above was serving this purpose; the call to
		// maybeReleaseFirstTransactionalWriter should fix that. And if it wasn't,
		// it'll be a no-op.
		l.maybeReleaseFirstTransactionalWriter()
	}

	if !doneRemoval {
		for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
			gg := e.Value.(*lockTableGuardImpl)
			if gg == g {
				l.waitingReaders.Remove(e)
				if g == l.distinguishedWaiter {
					distinguishedRemoved = true
					l.distinguishedWaiter = nil
				}
				doneRemoval = true
				break
			}
		}
	}
	if !doneRemoval {
		panic("lockTable bug")
	}
	if distinguishedRemoved {
		l.tryMakeNewDistinguished()
	}
	return l.isEmptyLock()
}

// tryFreeLockOnReplicatedAcquire attempts to free a write-uncontended lock
// during the state transition from the Unreplicated durability to the
// Replicated durability. This is possible because a Replicated lock is also
// stored as an MVCC intent, so it does not need to also be stored in the
// lockTable if writers are not queuing on it. This is beneficial because it
// serves as a mitigation for #49973. Since we aren't currently great at
// avoiding excessive contention on limited scans when locks are in the
// lockTable, it's better the keep locks out of the lockTable when possible.
//
// If any of the readers do truly contend with this lock even after their limit
// has been applied, they will notice during their MVCC scan and re-enter the
// queue (possibly recreating the lock through AddDiscoveredLock). Still, in
// practice this seems to work well in avoiding most of the artificial
// concurrency discussed in #49973.
//
// Acquires l.mu.
func (l *lockState) tryFreeLockOnReplicatedAcquire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Bail if not locked with only the Unreplicated durability.
	if !l.isHeld() || l.isHeldReplicated() {
		return false
	}

	// Bail if the lock has waiting writers. It is not uncontended.
	if l.queuedWriters.Len() != 0 {
		return false
	}

	// The lock is uncontended by other writers, so we're safe to drop it.
	// This may release readers who were waiting on the lock.
	l.clearLockHolder()
	gc := l.lockIsFree()
	if !gc {
		panic("expected lockIsFree to return true")
	}
	return true
}

// The lock has transitioned from locked to unlocked. There could be waiters.
//
// REQUIRES: l.mu is locked.
// TODO(arul): rename this + improve comment here to better reflect the state
// transitions this function performs.
func (l *lockState) lockIsFree() (gc bool) {
	if l.isHeld() {
		panic("called lockIsFree on lock with holder")
	}

	// All waiting readers don't need to wait here anymore.
	// NB: all waiting readers are by definition active waiters.
	for e := l.waitingReaders.Front(); e != nil; {
		curr := e
		e = e.Next()
		l.removeReader(curr)
	}

	l.maybeReleaseFirstTransactionalWriter()

	// We've already cleared waiting readers above. The lock can be released if
	// there are no waiting writers, active or otherwise.
	if l.queuedWriters.Len() == 0 {
		l.assertEmptyLock()
		return true
	}
	return false
}

// maybeReleaseFirstTransactionalWriter goes through the list of writers waiting
// in the receiver's lock wait queues and, if present and actively waiting,
// releases the first transactional writer it finds. Releasing a transactional
// writer entails marking it as inactive and nudging it via a call to notify().
//
// Any non-transactional writers at the head of the queue are also released. If
// no transactional writers are to be found, the list of queued writers will be
// empty when this function returns[1]. The function will no-op if the first
// transactional writer is already marked inactive (i.e. there's no releasing to
// do).
//
// [1] As if there are any requests in the list, they must be non-transactional,
// and all of them will be released.
//
// REQUIRES: l.mu is locked.
// REQUIRES: the (receiver) lock must not be held.
// REQUIRES: there should not be any waitingReaders in the lock's wait queues.
func (l *lockState) maybeReleaseFirstTransactionalWriter() {
	if l.isHeld() {
		panic("maybeReleaseFirstTransactionalWriter called when lock is held")
	}
	if l.waitingReaders.Len() != 0 {
		panic("there cannot be waiting readers")
	}

	// The prefix of the queue that is non-transactional writers is done
	// waiting.
	for e := l.queuedWriters.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		g := qg.guard
		if g.txn != nil { // transactional writer
			break
		}
		curr := e
		e = e.Next()
		l.removeWriter(curr)
	}

	if l.queuedWriters.Len() == 0 {
		return // no transactional writer
	}

	// Check if the first (transactional) writer is active, and if it is, mark
	// it as inactive. The call to doneActivelyWaitingAtLock should nudge it to
	// pick up its scan from where it left off.
	e := l.queuedWriters.Front()
	qg := e.Value.(*queuedGuard)
	g := qg.guard
	if qg.active {
		qg.active = false // mark as inactive
		if g == l.distinguishedWaiter {
			// We're only clearing the distinguishedWaiter for now; a new one will be
			// selected below in the call to informActiveWaiters.
			l.distinguishedWaiter = nil
		}
		g.mu.Lock()
		g.doneActivelyWaitingAtLock()
		g.mu.Unlock()
	}
	// Else the waiter is already inactive.

	// Tell the active waiters who they are waiting for.
	l.informActiveWaiters()
}

// Delete removes the specified lock from the tree.
// REQUIRES: t.mu is locked.
func (t *treeMu) Delete(l *lockState) {
	if buildutil.CrdbTestBuild {
		l.assertEmptyLockUnlocked()
	}
	t.btree.Delete(l)
}

// Reset removes all locks from the tree.
// REQUIRES: t.mu is locked.
func (t *treeMu) Reset() {
	if buildutil.CrdbTestBuild {
		iter := t.MakeIter()
		for iter.First(); iter.Valid(); iter.Next() {
			iter.Cur().assertEmptyLockUnlocked()
		}
	}
	t.btree.Reset()
}

func (t *treeMu) nextLockSeqNum() (seqNum uint64, checkMaxLocks bool) {
	t.lockIDSeqNum++
	checkMaxLocks = t.lockIDSeqNum%t.lockAddMaxLocksCheckInterval == 0
	return t.lockIDSeqNum, checkMaxLocks
}

func (t *lockTableImpl) ScanOptimistic(req Request) lockTableGuard {
	g := t.newGuardForReq(req)
	t.doSnapshotForGuard(g)
	return g
}

// ScanAndEnqueue implements the lockTable interface.
func (t *lockTableImpl) ScanAndEnqueue(req Request, guard lockTableGuard) lockTableGuard {
	// NOTE: there is no need to synchronize with enabledMu here. ScanAndEnqueue
	// scans the lockTable and enters any conflicting lock wait-queues, but a
	// disabled lockTable will be empty. If the scan's btree snapshot races with
	// a concurrent call to clear/disable then it might enter some wait-queues,
	// but it will quickly be released from them.

	var g *lockTableGuardImpl
	if guard == nil {
		g = t.newGuardForReq(req)
	} else {
		g = guard.(*lockTableGuardImpl)
		g.key = nil
		g.str = lock.MaxStrength
		g.index = -1
		g.mu.Lock()
		g.mu.startWait = false
		g.mu.state = waitingState{}
		g.mu.mustComputeWaitingState = false
		g.mu.Unlock()
		g.toResolve = g.toResolve[:0]
	}
	t.doSnapshotForGuard(g)

	if g.waitPolicy == lock.WaitPolicy_SkipLocked {
		// If the request is using a SkipLocked wait policy, it captures a lockTable
		// snapshot but does not scan the lock table when sequencing. Instead, it
		// calls into IsKeyLockedByConflictingTxn before adding keys to its result
		// set to determine which keys it should skip.
		return g
	}

	g.resumeScan(true /* notify */)
	if g.notRemovableLock != nil {
		// Either waiting at the notRemovableLock, or elsewhere. Either way we are
		// making forward progress, which ensures liveness.
		g.notRemovableLock.decrementNotRemovable()
		g.notRemovableLock = nil
	}
	return g
}

func (t *lockTableImpl) newGuardForReq(req Request) *lockTableGuardImpl {
	g := newLockTableGuardImpl()
	g.seqNum = atomic.AddUint64(&t.seqNum, 1)
	g.lt = t
	g.txn = req.Txn
	g.ts = req.Timestamp
	g.spans = req.LockSpans
	g.waitPolicy = req.WaitPolicy
	g.maxWaitQueueLength = req.MaxLockWaitQueueLength
	g.str = lock.MaxStrength
	g.index = -1
	return g
}

func (t *lockTableImpl) doSnapshotForGuard(g *lockTableGuardImpl) {
	if g.spans.Empty() {
		// A request with no lock spans has an empty snapshot as it doesn't need
		// one.
		return
	}
	t.locks.mu.RLock()
	g.tableSnapshot.Reset()
	g.tableSnapshot = t.locks.Clone()
	t.locks.mu.RUnlock()
}

// Dequeue implements the lockTable interface.
func (t *lockTableImpl) Dequeue(guard lockTableGuard) {
	// NOTE: there is no need to synchronize with enabledMu here. Dequeue only
	// accesses state already held by the guard and does not add anything to the
	// lockTable.

	g := guard.(*lockTableGuardImpl)
	defer releaseLockTableGuardImpl(g)
	if g.notRemovableLock != nil {
		g.notRemovableLock.decrementNotRemovable()
		g.notRemovableLock = nil
	}
	var candidateLocks []*lockState
	g.mu.Lock()
	for l := range g.mu.locks {
		candidateLocks = append(candidateLocks, l)
	}
	g.mu.Unlock()
	var locksToGC []*lockState
	for _, l := range candidateLocks {
		if gc := l.requestDone(g); gc {
			locksToGC = append(locksToGC, l)
		}
	}

	t.tryGCLocks(&t.locks, locksToGC)
}

// AddDiscoveredLock implements the lockTable interface.
//
// We discussed in
// https://github.com/cockroachdb/cockroach/issues/62470#issuecomment-818374388
// the possibility of consulting the txnStatusCache in AddDiscoveredLock,
// and not adding the lock if the txn is already finalized, and instead
// telling the caller to do batched intent resolution before calling
// ScanAndEnqueue.
// This reduces memory pressure on the lockTableImpl in the extreme case of
// huge numbers of discovered locks. Note that when there isn't memory
// pressure, the consultation of the txnStatusCache in the ScanAndEnqueue
// achieves the same batched intent resolution. Additionally, adding the lock
// to the lock table allows it to coordinate the population of
// lockTableGuardImpl.toResolve for different requests that encounter the same
// lock, to reduce the likelihood of duplicated intent resolution. This
// coordination could be improved further, as outlined in the comment in
// tryActiveWait, but it hinges on the lock table having the state of the
// discovered lock.
//
// For now we adopt the following heuristic: the caller calls DiscoveredLocks
// with the count of locks discovered, prior to calling AddDiscoveredLock for
// each of the locks. At that point a decision is made whether to consult the
// txnStatusCache eagerly when adding discovered locks.
func (t *lockTableImpl) AddDiscoveredLock(
	intent *roachpb.Intent,
	seq roachpb.LeaseSequence,
	consultTxnStatusCache bool,
	guard lockTableGuard,
) (added bool, _ error) {
	t.enabledMu.RLock()
	defer t.enabledMu.RUnlock()
	if !t.enabled {
		// If not enabled, don't track any locks.
		return false, nil
	}
	if seq < t.enabledSeq {
		// If the lease sequence is too low, this discovered lock may no longer
		// be accurate, so we ignore it.
		return false, nil
	} else if seq > t.enabledSeq {
		// The enableSeq is set synchronously with the application of a new
		// lease, so it should not be possible for a request to evaluate at a
		// higher lease sequence than the current value of enabledSeq.
		return false, errors.AssertionFailedf("unexpected lease sequence: %d > %d", seq, t.enabledSeq)
	}
	g := guard.(*lockTableGuardImpl)
	key := intent.Key
	str, err := findHighestLockStrengthInSpans(key, g.spans)
	if err != nil {
		return false, err
	}
	if consultTxnStatusCache {
		finalizedTxn, ok := t.txnStatusCache.finalizedTxns.get(intent.Txn.ID)
		if ok {
			g.toResolve = append(
				g.toResolve, roachpb.MakeLockUpdate(finalizedTxn, roachpb.Span{Key: key}))
			return true, nil
		}

		// If the discoverer is a non-locking read, also check whether the lock's
		// holder is known to have been pushed above the reader's timestamp. See the
		// comment in scanAndMaybeEnqueue for more details, including why we include
		// the hasUncertaintyInterval condition.
		if str == lock.None && !g.hasUncertaintyInterval() && t.batchPushedLockResolution() {
			pushedTxn, ok := g.lt.txnStatusCache.pendingTxns.get(intent.Txn.ID)
			if ok && g.ts.Less(pushedTxn.WriteTimestamp) {
				g.toResolve = append(
					g.toResolve, roachpb.MakeLockUpdate(pushedTxn, roachpb.Span{Key: key}))
				return true, nil
			}
		}
	}
	var l *lockState
	t.locks.mu.Lock()
	iter := t.locks.MakeIter()
	iter.FirstOverlap(&lockState{key: key})
	checkMaxLocks := false
	if !iter.Valid() {
		var lockSeqNum uint64
		lockSeqNum, checkMaxLocks = t.locks.nextLockSeqNum()
		l = &lockState{id: lockSeqNum, key: key}
		l.queuedWriters.Init()
		l.waitingReaders.Init()
		t.locks.Set(l)
		atomic.AddInt64(&t.locks.numLocks, 1)
	} else {
		l = iter.Cur()
	}
	notRemovableLock := false
	if g.notRemovableLock == nil {
		// Only one discovered lock needs to be marked notRemovable to ensure
		// liveness, since we only need to prevent all the discovered locks from
		// being garbage collected. We arbitrarily pick the first one that the
		// requester adds after evaluation.
		g.notRemovableLock = l
		notRemovableLock = true
	}
	err = l.discoveredLock(&intent.Txn, intent.Txn.WriteTimestamp, g, str, notRemovableLock, g.lt.clock)
	// Can't release tree.mu until call l.discoveredLock() since someone may
	// find an empty lock and remove it from the tree.
	t.locks.mu.Unlock()
	if checkMaxLocks {
		t.checkMaxLocksAndTryClear()
	}
	return true, err
}

// AcquireLock implements the lockTable interface.
func (t *lockTableImpl) AcquireLock(acq *roachpb.LockAcquisition) error {
	t.enabledMu.RLock()
	defer t.enabledMu.RUnlock()
	if !t.enabled {
		// If not enabled, don't track any locks.
		return nil
	}
	switch acq.Strength {
	case lock.Intent:
		assert(acq.Durability == lock.Replicated, "incorrect durability")
	case lock.Exclusive:
		assert(acq.Durability == lock.Unreplicated, "incorrect durability")
	default:
		return errors.AssertionFailedf("unsupported lock strength %s", acq.Strength)
	}
	var l *lockState
	t.locks.mu.Lock()
	// Can't release tree.mu until call l.acquireLock() since someone may find
	// an empty lock and remove it from the tree. If we expect that lockState
	// will already be in tree we can optimize this by first trying with a
	// tree.mu.RLock().
	iter := t.locks.MakeIter()
	iter.FirstOverlap(&lockState{key: acq.Key})
	checkMaxLocks := false
	if !iter.Valid() {
		if acq.Durability == lock.Replicated {
			// Don't remember uncontended replicated locks. The downside is that
			// sometimes contention won't be noticed until when the request
			// evaluates. Remembering here would be better, but our behavior when
			// running into the maxLocks limit is somewhat crude. Treating the
			// data-structure as a bounded cache with eviction guided by contention
			// would be better.
			t.locks.mu.Unlock()
			return nil
		}
		var lockSeqNum uint64
		lockSeqNum, checkMaxLocks = t.locks.nextLockSeqNum()
		l = &lockState{id: lockSeqNum, key: acq.Key}
		l.queuedWriters.Init()
		l.waitingReaders.Init()
		t.locks.Set(l)
		atomic.AddInt64(&t.locks.numLocks, 1)
	} else {
		l = iter.Cur()
		if acq.Durability == lock.Replicated && l.tryFreeLockOnReplicatedAcquire() {
			// Don't remember uncontended replicated locks. Just like in the
			// case where the lock is initially added as replicated, we drop
			// replicated locks from the lockTable when being upgraded from
			// Unreplicated to Replicated, whenever possible.
			// TODO(sumeer): now that limited scans evaluate optimistically, we
			// should consider removing this hack. But see the comment in the
			// preceding block about maxLocks.
			t.locks.Delete(l)
			t.locks.mu.Unlock()
			atomic.AddInt64(&t.locks.numLocks, -1)
			return nil
		}
	}
	err := l.acquireLock(acq, t.clock)
	t.locks.mu.Unlock()

	if checkMaxLocks {
		t.checkMaxLocksAndTryClear()
	}
	return err
}

func (t *lockTableImpl) checkMaxLocksAndTryClear() {
	totalLocks := atomic.LoadInt64(&t.locks.numLocks)
	if totalLocks > t.maxLocks {
		numToClear := totalLocks - t.minLocks
		t.tryClearLocks(false /* force */, int(numToClear))
	}
}

func (t *lockTableImpl) lockCountForTesting() int64 {
	return atomic.LoadInt64(&t.locks.numLocks)
}

// tryClearLocks attempts to clear locks.
//   - force=false: removes locks until it has removed numToClear locks. It does
//     not remove locks marked as notRemovable.
//   - force=true: removes all locks.
//
// Waiters of removed locks are told to wait elsewhere or that they are done
// waiting.
func (t *lockTableImpl) tryClearLocks(force bool, numToClear int) {
	clearCount := 0
	t.locks.mu.Lock()
	var locksToClear []*lockState
	iter := t.locks.MakeIter()
	for iter.First(); iter.Valid(); iter.Next() {
		l := iter.Cur()
		if l.tryClearLock(force) {
			locksToClear = append(locksToClear, l)
			clearCount++
			if !force && clearCount >= numToClear {
				break
			}
		}
	}
	atomic.AddInt64(&t.locks.numLocks, int64(-len(locksToClear)))
	if t.locks.Len() == len(locksToClear) {
		// Fast-path full clear.
		t.locks.Reset()
	} else {
		for _, l := range locksToClear {
			t.locks.Delete(l)
		}
	}
	t.locks.mu.Unlock()
}

// findHighestLockStrengthInSpans returns the highest lock strength specified
// for the given key in the supplied spans. It is expected for the key to be
// present in the spans; an assertion failed error is returned otherwise.
func findHighestLockStrengthInSpans(
	key roachpb.Key, spans *lockspanset.LockSpanSet,
) (lock.Strength, error) {
	for str := lock.MaxStrength; str >= 0; str-- {
		s := spans.GetSpans(str)
		// First span that starts after key
		i := sort.Search(len(s), func(i int) bool {
			return key.Compare(s[i].Key) < 0
		})
		if i > 0 &&
			((len(s[i-1].EndKey) > 0 && key.Compare(s[i-1].EndKey) < 0) || key.Equal(s[i-1].Key)) {
			return str, nil
		}
	}
	return 0, errors.AssertionFailedf("could not find access in spans")
}

// Tries to GC locks that were previously known to have become empty.
func (t *lockTableImpl) tryGCLocks(tree *treeMu, locks []*lockState) {
	if len(locks) == 0 {
		return // bail early
	}
	tree.mu.Lock()
	defer tree.mu.Unlock()
	for _, l := range locks {
		iter := tree.MakeIter()
		iter.FirstOverlap(l)
		// Since the same lockState can go from non-empty to empty multiple times
		// it is possible that multiple threads are racing to delete it and
		// multiple find it empty and one wins. If a concurrent thread made the
		// lockState non-empty we do not want to delete it accidentally.
		if !iter.Valid() {
			continue
		}
		l = iter.Cur()
		l.mu.Lock()
		empty := l.isEmptyLock()
		l.mu.Unlock()
		if empty {
			tree.Delete(l)
			atomic.AddInt64(&tree.numLocks, -1)
		}
	}
}

// UpdateLocks implements the lockTable interface.
func (t *lockTableImpl) UpdateLocks(up *roachpb.LockUpdate) error {
	_ = t.updateLockInternal(up)
	return nil
}

// updateLockInternal is where the work for UpdateLocks is done. It
// returns whether there was a lock held by this txn.
func (t *lockTableImpl) updateLockInternal(up *roachpb.LockUpdate) (heldByTxn bool) {
	// NOTE: there is no need to synchronize with enabledMu here. Update only
	// accesses locks already in the lockTable, but a disabled lockTable will be
	// empty. If the lock-table scan below races with a concurrent call to clear
	// then it might update a few locks, but they will quickly be cleared.

	span := up.Span
	var locksToGC []*lockState
	heldByTxn = false
	changeFunc := func(l *lockState) {
		held, gc := l.tryUpdateLock(up)
		heldByTxn = heldByTxn || held
		if gc {
			locksToGC = append(locksToGC, l)
		}
	}
	t.locks.mu.RLock()
	iter := t.locks.MakeIter()
	ltRange := &lockState{key: span.Key, endKey: span.EndKey}
	for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
		changeFunc(iter.Cur())
		// Optimization to avoid a second key comparison (not for correctness).
		if len(span.EndKey) == 0 {
			break
		}
	}
	t.locks.mu.RUnlock()

	t.tryGCLocks(&t.locks, locksToGC)
	return heldByTxn
}

// Iteration helper for resumeScan. Returns the next span to search over, or nil
// if the iteration is done.
//
// REQUIRES: g.mu is locked.
func stepToNextSpan(g *lockTableGuardImpl) *roachpb.Span {
	g.index++
	for ; g.str >= 0; g.str-- {
		spans := g.spans.GetSpans(g.str)
		if g.index < len(spans) {
			span := &spans[g.index]
			g.key = span.Key
			return span
		}
		g.index = 0
	}
	g.str = lock.MaxStrength
	return nil
}

// batchPushedLockResolution returns whether non-locking readers can defer and
// batch resolution of conflicting locks whose holder is known to be pending and
// have been pushed above the reader's timestamp.
func (t *lockTableImpl) batchPushedLockResolution() bool {
	return BatchPushedLockResolution.Get(&t.settings.SV)
}

// PushedTransactionUpdated implements the lockTable interface.
func (t *lockTableImpl) PushedTransactionUpdated(txn *roachpb.Transaction) {
	// TODO(sumeer): We don't take any action for requests that are already
	// waiting on locks held by txn. They need to take some action, like
	// pushing, and resume their scan, to notice the change to this txn. We
	// could be more proactive if we knew which locks in lockTableImpl were held
	// by txn.
	t.txnStatusCache.add(txn)
}

// Enable implements the lockTable interface.
func (t *lockTableImpl) Enable(seq roachpb.LeaseSequence) {
	// Avoid disrupting other requests if the lockTable is already enabled.
	// NOTE: This may be a premature optimization, but it can't hurt.
	t.enabledMu.RLock()
	enabled, enabledSeq := t.enabled, t.enabledSeq
	t.enabledMu.RUnlock()
	if enabled && enabledSeq == seq {
		return
	}
	t.enabledMu.Lock()
	t.enabled = true
	t.enabledSeq = seq
	t.enabledMu.Unlock()
}

// Clear implements the lockTable interface.
func (t *lockTableImpl) Clear(disable bool) {
	// If disabling, lock the entire table to prevent concurrent accesses
	// from adding state to the table as we clear it. If not, there's no
	// need to synchronize with enabledMu because we're only removing state.
	if disable {
		t.enabledMu.Lock()
		defer t.enabledMu.Unlock()
		t.enabled = false
	}
	// The numToClear=0 is arbitrary since it is unused when force=true.
	t.tryClearLocks(true /* force */, 0)
	// Also clear the txn status cache, since it won't be needed any time
	// soon and consumes memory.
	t.txnStatusCache.clear()
}

// QueryLockTableState implements the lockTable interface.
func (t *lockTableImpl) QueryLockTableState(
	span roachpb.Span, opts QueryLockTableOptions,
) ([]roachpb.LockStateInfo, QueryLockTableResumeState) {
	t.enabledMu.RLock()
	defer t.enabledMu.RUnlock()
	if !t.enabled {
		// If not enabled, don't return any locks from the query.
		return []roachpb.LockStateInfo{}, QueryLockTableResumeState{}
	}

	// Grab tree snapshot to avoid holding read lock during iteration.
	t.locks.mu.RLock()
	snap := t.locks.Clone()
	t.locks.mu.RUnlock()
	// Reset snapshot to free resources.
	defer snap.Reset()

	now := t.clock.PhysicalTime()

	lockTableState := make([]roachpb.LockStateInfo, 0, snap.Len())
	resumeState := QueryLockTableResumeState{}
	var numLocks int64
	var numBytes int64
	var nextKey roachpb.Key
	var nextByteSize int64

	// Iterate over locks and gather metadata.
	iter := snap.MakeIter()
	ltRange := &lockState{key: span.Key, endKey: span.EndKey}
	for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
		l := iter.Cur()

		if ok, lInfo := l.collectLockStateInfo(opts.IncludeUncontended, now); ok {
			nextKey = l.key
			nextByteSize = int64(lInfo.Size())
			lInfo.RangeID = t.rID

			// Check if adding the lock would exceed our byte or count limits,
			// though we must ensure we return at least one lock.
			if len(lockTableState) > 0 && opts.TargetBytes > 0 && (numBytes+nextByteSize) > opts.TargetBytes {
				resumeState.ResumeReason = kvpb.RESUME_BYTE_LIMIT
				break
			} else if len(lockTableState) > 0 && opts.MaxLocks > 0 && numLocks >= opts.MaxLocks {
				resumeState.ResumeReason = kvpb.RESUME_KEY_LIMIT
				break
			}

			lockTableState = append(lockTableState, lInfo)
			numLocks++
			numBytes += nextByteSize
		}
	}

	// If we need to paginate results, set the continuation key in the ResumeSpan.
	if resumeState.ResumeReason != 0 {
		resumeState.ResumeNextBytes = nextByteSize
		resumeState.ResumeSpan = &roachpb.Span{Key: nextKey, EndKey: span.EndKey}
	}
	resumeState.TotalBytes = numBytes

	return lockTableState, resumeState
}

// Metrics implements the lockTable interface.
func (t *lockTableImpl) Metrics() LockTableMetrics {
	var m LockTableMetrics
	// Grab tree snapshot to avoid holding read lock during iteration.
	t.locks.mu.RLock()
	snap := t.locks.Clone()
	t.locks.mu.RUnlock()
	// Reset snapshot to free resources.
	defer snap.Reset()

	// Iterate and compute metrics.
	now := t.clock.PhysicalTime()
	iter := snap.MakeIter()
	for iter.First(); iter.Valid(); iter.Next() {
		iter.Cur().addToMetrics(&m, now)
	}
	return m
}

// String implements the lockTable interface.
func (t *lockTableImpl) String() string {
	var sb redact.StringBuilder
	t.locks.mu.RLock()
	sb.Printf("num=%d\n", atomic.LoadInt64(&t.locks.numLocks))
	iter := t.locks.MakeIter()
	for iter.First(); iter.Valid(); iter.Next() {
		l := iter.Cur()
		l.mu.Lock()
		l.safeFormat(&sb, &t.txnStatusCache)
		l.mu.Unlock()
	}
	t.locks.mu.RUnlock()
	return sb.String()
}

// assert panics with the supplied message if the condition does not hold true.
func assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}
