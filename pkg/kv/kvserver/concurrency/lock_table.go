// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency

import (
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/util/container/list"
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

	// waitElsewhere is used when the lockTable is under memory pressure and is
	// clearing its internal queue state. Like the waitFor* states, it informs
	// the request who it is waiting for so that deadlock detection works.
	// However, sequencing information inside the lockTable is mostly discarded.
	waitElsewhere

	// waitSelf indicates that a different request from the same transaction has
	// claimed (but not acquired) the lock already. This request should sit tight
	// and wait for a new notification without pushing anyone.
	//
	// It must be the case that there are no lock holders for the associated key
	// if a request is actively waiting in the waitSelf state. That's because if
	// the key is locked by another transaction, the request must be in the
	// waitFor state. A request does not need to actively wait if the only lock
	// that's held on the key belongs to its own transaction, thus making the
	// waitSelf state impossible for held locks.
	//
	// NB: The main motivation of the waitSelf state is to avoid certain
	// dependency cycles by potentially re-ordering the lock's wait queue.
	// Typically, the waitSelf state arises when there are >= 1 locking requests
	// from other transactions between two requests from the same transaction
	// waiting in a lock's wait queue. So something like:
	//
	// key a: unlocked
	// wait-queue: [(r1,txn1,inactive), (r2,txn2,active), ... (r3,txn1,active)]
	//
	// There is a dependency cycle between the requests from txn1 and txn2 here.
	// Instead of forcing one of the two transaction's to abort, we're able to
	// break the dependency cycle by having r3 wait and sit tight in the waitSelf
	// state[1]. Now, if r1 acquires the lock, r3 will be re-ordered[2] to proceed
	// before r2 (thus breaking the dependency cycle); if it doesn't, r2 will no
	// longer depend on txn1 and can thus proceed (i.e. no more dependency cycle).
	//
	// [1] A transaction can't push itself, but if it finds the request at the
	// head of the queue everyone is pushing belongs to its own transaction, it
	// knows there is a local dependency cycle.
	//
	// [2] Note that re-ordering happens only when r1 acquires the lock, so the
	// waitSelf state is a mechanism by which r3 is able to sit tight without
	// pushing anyone.
	waitSelf

	// waitQueueMaxLengthExceeded indicates that the request attempted to enter a
	// lock wait-queue as a locking request and found that the queue's length was
	// already equal to or exceeding the request's configured maximum. As a
	// result, the request was rejected.
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

	// Fields below are only populated for waitFor*, waitElsewhere and
	// waitQueueMaxLengthExceeded kinds.

	// Represents who the request is waiting for. The conflicting
	// transaction may be a lock holder of a conflicting lock or a
	// conflicting request being sequenced through the same lockTable.
	txn                   *enginepb.TxnMeta // always non-nil
	key                   roachpb.Key       // the key of the conflict
	held                  bool              // is the conflict a held lock?
	queuedLockingRequests int               // how many locking requests are waiting?
	queuedReaders         int               // how many readers are waiting?

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
	case waitFor:
		target := redact.SafeString("holding lock")
		if !s.held {
			target = "running request"
		}
		w.Printf("wait for txn %s %s @ key %s (queuedLockingRequests: %d, queuedReaders: %d)",
			s.txn.Short(), target, s.key, s.queuedLockingRequests, s.queuedReaders)
	case waitSelf:
		w.Printf("wait self @ key %s", s.key)
	case waitElsewhere:
		if !s.held {
			w.SafeString("wait elsewhere by proceeding to evaluation")
		}
		w.Printf("wait elsewhere for txn %s @ key %s", s.txn.Short(), s.key)
	case waitQueueMaxLengthExceeded:
		w.Printf("wait-queue maximum length exceeded @ key %s with length %d",
			s.key, s.queuedLockingRequests)
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

// The btree for keys that have locks on them.
type treeMu struct {
	mu syncutil.RWMutex // Protects everything in this struct.

	// For assigning sequence numbers to the keyLocks objects as required by
	// the util/interval/generic type contract.
	lockIDSeqNum uint64

	// Container for all keyLocks structs, inside which per-key lock tracking
	// is done. This includes information about the lock holder(s), requests being
	// sequenced through the lock table, and any queues that form on the key
	// because of read-write or write-write conflicts. Empty[1] keyLocks structs
	// are garbage collected. Additionally, keys which only have replicated locks
	// on them and no contending requests may also be garbage collected, as their
	// state can be recovered from persistent storage.
	//
	// [1] Keys that are not locked and have an empty wait-queue.
	btree

	// numKeysLocked tracks the number of keyLocks structs in the b-tree. It is
	// primarily used for constraining memory consumption. Ideally, we should be
	// doing better memory accounting than this.
	numKeysLocked atomic.Int64

	// For dampening the frequency with which we enforce
	// lockTableImpl.maxKeysLocked.
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
//	> keyLocks.mu
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
	seqNum atomic.Uint64

	// locks contains the btree object (wrapped in the treeMu structure) that
	// contains the actual keyLocks objects. These keyLocks objects represent the
	// individual keys (that may have one or more locks on them) in the lock
	// table. Locks on both Global and Local keys are stored in the same btree.
	locks treeMu

	// maxKeysLocked is a soft maximum on amount of per-key lock information
	// tracking[1]. When it is exceeded, and subject to the dampening in
	// lockAddMaxLocksCheckInterval, locks will be cleared.
	//
	// [1] Simply put, the number of keyLocks objects in the lockTable btree.
	maxKeysLocked int64
	// When maxKeysLocked is exceeded, will attempt to clear down to minKeysLocked,
	// instead of clearing everything.
	minKeysLocked int64

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
	lt.setMaxKeysLocked(maxLocks)
	return lt
}

func (t *lockTableImpl) setMaxKeysLocked(maxKeysLocked int64) {
	// Check at 5% intervals of the max count.
	lockAddMaxLocksCheckInterval := maxKeysLocked / int64(20)
	if lockAddMaxLocksCheckInterval == 0 {
		lockAddMaxLocksCheckInterval = 1
	}
	t.maxKeysLocked = maxKeysLocked
	t.minKeysLocked = maxKeysLocked / 2
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
//   - The waitFor* states provide information on who the request is waiting
//     for.
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
//     was rejected because it attempted to enter a lock wait-queue as a locking
//     request and found that the queue's length was already equal to or
//     exceeding the request's configured maximum.
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
	// lockTableImpl. Additionally, it is possible that there is a new keyLocks
	// struct for the same key. This can result in various harmless anomalies:
	// - the request may hold a claim on a keyLocks struct that is no longer
	//   in the tree. When it next does a scan, it will either find a new
	//   keyLocks struct where it will compete or none. Both lockStates can be in
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
	// keyLocks.notRemovable. It will be set to nil when this guard has decremented
	// keyLocks.notRemovable. Note that:
	// - notRemovableLock may no longer be the btree in lockTableImpl since it may
	//   have been removed due to the lock being released. This is harmless since
	//   the change in lock state for that lock's key (even if it has meanwhile been
	//   reacquired by a different request) means forward progress for this request,
	//   which guarantees liveness for this request.
	// - Multiple guards can have marked the same lock as notRemovable, which is
	//   why keyLocks.notRemovable behaves like a reference count.
	notRemovableLock *keyLocks

	// A request whose startWait is set to true in ScanAndEnqueue is actively
	// waiting at a particular key. This is the first key encountered when
	// iterating through spans that it needs to wait at. A future event (lock
	// release etc.) may cause the request to no longer need to wait at this
	// key. It then needs to continue iterating through spans to find the next
	// key to wait at (we don't want to wastefully start at the beginning since
	// this request probably has a claim at the contended keys there): str, index,
	// and key collectively track the current position to allow it to continue
	// iterating.

	// The key for the keyLocks.
	key roachpb.Key
	// The key for the keyLocks is contained in the Span specified by
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

		// locks for which this request is in the list of
		// queued{Readers,LockingRequests}. For locking requests, this includes both
		// active and inactive waiters. For readers, there's no such thing as an
		// inactive reader, so by definition the request must be an active waiter.
		// The highest lock strength with which the request is trying to lock the
		// key is also stored.
		//
		// TODO(sbhola): investigate whether the logic to maintain this locks map
		// can be simplified so it doesn't need to be adjusted by various keyLocks
		// methods. It adds additional bookkeeping burden that means it is more
		// prone to inconsistencies. There are two main uses: (a) removing from
		// various lockStates when requestDone() is called, (b) tryActiveWait() uses
		// it as an optimization to know that this request is not known to the
		// keyLocks struct. (b) can be handled by other means -- the first scan the
		// request won't be in the keyLocks struct and the second scan it likely
		// will. (a) doesn't necessarily require this map to be consistent -- the
		// request could track the places where it is has enqueued as places where
		// it could be present and then do the search.
		locks map[*keyLocks]lock.Strength

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
		g.mu.locks = make(map[*keyLocks]lock.Strength)
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
	// The map should have been cleared by keyLocks.requestDone.
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

func (g *lockTableGuardImpl) CurState() (waitingState, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.mu.mustComputeWaitingState {
		return g.mu.state, nil
	}
	// Not actively waiting anywhere so no one else can set
	// mustComputeWaitingState to true while this method executes.
	g.mu.mustComputeWaitingState = false
	g.mu.Unlock()
	err := g.resumeScan(false /* notify */)
	g.mu.Lock() // Unlock deferred
	if err != nil {
		return waitingState{}, err
	}
	return g.mu.state, nil
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
// The supplied waiting state must imply the request is still waiting.
//
// REQUIRES: g.mu to be locked.
func (g *lockTableGuardImpl) canElideWaitingStateUpdate(newState waitingState) bool {
	// NB: The txn field on waitingState is expected to be non-nil for all
	// waitKinds bar doneWaiting. Moreover, we don't expect this function to be
	// called when a request transitions to doneWaiting. Sanity check both these
	// invariants hold by asserting them.
	assert(newState.kind != doneWaiting, "unexpected waiting state kind")
	assert(newState.txn != nil, "txn cannot be unset if waiting state isn't doneWaiting")
	// Note that we don't need to check newState.guardStrength as it's
	// automatically assigned when updating the state.
	return g.mu.state.kind == newState.kind && g.mu.state.txn.ID == newState.txn.ID &&
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
		ltRange := &keyLocks{key: startKey, endKey: span.EndKey}
		for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
			l := iter.Cur()
			if !l.isNonConflictingLock(g) {
				return false
			}
		}
		span = stepToNextSpan(g)
	}
	return true
}

func (g *lockTableGuardImpl) IsKeyLockedByConflictingTxn(
	_ context.Context, key roachpb.Key, str lock.Strength,
) (bool, *enginepb.TxnMeta, error) {
	iter := g.tableSnapshot.MakeIter()
	iter.SeekGE(&keyLocks{key: key})
	if !iter.Valid() || !iter.Cur().key.Equal(key) {
		// No lock on key.
		return false, nil, nil
	}
	l := iter.Cur()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.isEmptyLock() {
		// The lock is empty but has not yet been deleted.
		return false, nil, nil
	}

	isAllowedToProceed, err := l.alreadyHoldsLockAndIsAllowedToProceed(g, str)
	if err != nil {
		return false, nil, err
	}
	if isAllowedToProceed {
		// If another request from this transaction has already locked this key with
		// sufficient locking strength then there's no conflict; we can proceed.
		return false, nil, nil
	}

	if l.isLocked() {
		for e := l.holders.Front(); e != nil; e = e.Next() {
			tl := e.Value
			if !g.isSameTxn(tl.getLockHolderTxn()) &&
				lock.Conflicts(tl.getLockMode(), makeLockMode(str, g.txnMeta(), g.ts), &g.lt.settings.SV) {
				return true, tl.txn, nil // the key is locked by some other transaction; return it
			}
		}
		// We can be in either of 2 cases at this point:
		// 1. All locks held on this key are at non-conflicting strengths (i.e,
		// they're compatible with the supplied strength).
		// 2. OR one of the locks held on this key is by the transaction itself, and
		// that lock is held with a lower strength. Simply put, the request is
		// trying to upgrade its lock.
	}

	// There's no conflict with the lock holder itself. However, there may be
	// other locking requests that came before us that we may conflict with.
	// Checking for conflicts with the list of queuedLockingRequests ensures
	// fairness by preventing a stream of locking[1] SKIP LOCKED requests from
	// starving out regular locking requests.
	if str == lock.None { // [1] we only need to do this checking for locking requests
		return false, nil, nil
	}

	for e := l.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		qqg := e.Value
		qo := makeQueueOrder(g, l)
		if qqg.order.after(qo) {
			// We only need to check for conflicts with requests that sort before us.
			// Note that the list of queuedLockingRequests is already sorted.
			break
		}
		if qqg.guard.txnMeta() != nil && g.isSameTxn(qqg.guard.txnMeta()) {
			// Normally, in the way SQL uses KV, a SKIP LOCKED request should never
			// find another request from its own transaction in a lock's wait queue.
			// However, this is possible in case there is a replay involved. We handle
			// this by treating the request as non-conflicting.
			continue
		}
		if lock.Conflicts(qqg.mode, makeLockMode(str, g.txnMeta(), g.ts), &g.lt.settings.SV) {
			return true, nil, nil // the conflict isn't with a lock holder, nil is returned
		}
	}
	return false, nil, nil // no conflict
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
	return makeLockMode(g.curStrength(), g.txnMeta(), g.ts)
}

// makeLockMode constructs and returns a lock mode.
func makeLockMode(str lock.Strength, txn *enginepb.TxnMeta, ts hlc.Timestamp) lock.Mode {
	iso := isolation.Serializable
	if txn != nil {
		iso = txn.IsoLevel
	}
	switch str {
	case lock.None:
		return lock.MakeModeNone(ts, iso)
	case lock.Shared:
		return lock.MakeModeShared()
	case lock.Exclusive:
		return lock.MakeModeExclusive(ts, iso)
	case lock.Intent:
		return lock.MakeModeIntent(ts)
	default:
		panic(fmt.Sprintf("unhandled request strength: %s", str))
	}
}

// maybeAddToLocksMap adds the supplied lock to the guard's locks map if:
// 1. It's not already present.
// 2. OR it's being accessed with a higher lock strength than the one currently
// associated.
// The return value indicates whether the lock was added or not.
//
// REQUIRES: g.mu to be locked.
func (g *lockTableGuardImpl) maybeAddToLocksMap(
	kl *keyLocks, accessStr lock.Strength,
) (added bool) {
	str, present := g.mu.locks[kl]
	if !present || str < accessStr {
		g.mu.locks[kl] = accessStr
		return true // added
	}
	return false // added
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
func (g *lockTableGuardImpl) resumeScan(notify bool) error {
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
		ltRange := &keyLocks{key: startKey, endKey: span.EndKey}
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
			conflicts, err := l.scanAndMaybeEnqueue(g, notify)
			if err != nil {
				return err
			}
			if conflicts {
				return nil
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
	return nil
}

// queuedGuard is used to wrap waiting locking requests in the keyLocks struct.
// Waiting requests typically wait in an active state, i.e., the
// lockTableGuardImpl.key refers to the same key inside this keyLock struct.
// However, there are multiple reasons that can cause a locking request to wait
// inactively at a keyLock:
//   - The first locking request is able to claim a lock when it is released.
//     Doing so entails the request being marked inactive.
//   - It is able to claim a lock that was previously claimed by a request with
//     a higher sequence number. In such cases, the request adds itself to the
//     head of the queue as an inactive waiter and proceeds with its scan.
//   - A discovered lock causes the discoverer to become an inactive waiter
//     (until it scans again).
//   - A lock held by a finalized txn causes the first waiter to be an inactive
//     waiter.
//
// The first two cases above (claiming an unheld lock) only occur for
// (transactional) locking requests, but the other cases can happen for
// both locking requests and non-transactional writers.
type queuedGuard struct {
	guard  *lockTableGuardImpl
	mode   lock.Mode // protected by keyLocks.mu
	active bool      // protected by keyLocks.mu
	order  queueOrder
}

// queueOrder encapsulates fields that are used to determine the order in which
// locking requests are stored in a lock's wait queue.
type queueOrder struct {
	reqSeqNum   uint64
	isPromoting bool
}

// makeQueueOrder constructs a queueOrder.
//
// REQUIRES: kl.mu to be locked.
func makeQueueOrder(g *lockTableGuardImpl, kl *keyLocks) queueOrder {
	isPromoting := g.txn != nil && kl.isLockedBy(g.txn.ID)
	return queueOrder{
		reqSeqNum:   g.seqNum,
		isPromoting: isPromoting,
	}
}

// after returns true if the receiver should be ordered after the supplied
// queueOrder.
//
// Comparison is based on sequence numbers, which correspond to a request's
// arrival time -- requests that arrive later are ordered after requests that
// arrive earlier, and vice-versa. However, requests that are trying to promote
// locks already held by their transaction are ordered before ones that are not.
func (o1 queueOrder) after(o2 queueOrder) bool {
	if o1.reqSeqNum == o2.reqSeqNum {
		return false // same request; doesn't sort after
	}
	if o1.isPromoting != o2.isPromoting {
		return o2.isPromoting
	}
	// If both requests are trying to promote their locks, or neither are, then
	// use the sequence number dictates the order.
	return o1.reqSeqNum > o2.reqSeqNum
}

// Information about a lock holder for unreplicated locks.
type unreplicatedLockHolderInfo struct {
	// strengths tracks whether the lock is held with a particular strength; if it
	// is, the lowest sequence number (that hasn't been rolled back) that it was
	// acquired with is stored. If the lock isn't held with a particular strength,
	// a sentinel value (-1) is stored.
	//
	// NB: Intents cannot be held/acquired in unreplicated fashion; thus the
	// highest lock strength for unreplicated locks is Exclusive.
	strengths [len(unreplicatedHolderStrengths)]enginepb.TxnSeq

	// The ignoredSeqNums slice is the last-seen ignoredSeqNums
	// from the acquiring transaction. At a given point in time,
	// the transaction may have a larger set of ignoredSeqNums.
	//
	// The intent is to use this for potential flushing of unreplicated
	// locks as replicated locks. When doing this flush, we want to avoid
	// clobbering existing replicated locks that may be on disk and should
	// be preserved to ensure correct rollback behaviour.
	//
	// *Motivating example*
	//
	// First consider the following state:
	//
	//    replicated lock @ seq=1 (on disk, not in memory)
	//           rollback @ seq=1
	//  unreplicated lock @ seq=2 (seq 1 ignored)
	//
	// The on-disk replicated lock will be ignored by future scans of the
	// lock table. When we go to flush our unreplicated lock as a
	// replicated lock, we want to overwrite the replicated lock at seq=1
	// with our new lock at seq=2.
	//
	// Now consider a slightly different scenario:
	//
	//    replicated lock @ seq=1 (on disk, not in memory)
	//  unreplicated lock @ seq=2
	//
	// In this case, the in-memory lock table will have an unreplicated lock
	// at seq=2. When we go to flush our unreplicated lock as a replicated
	// lock, we DO NOT want to overwrite the replicated lock at seq=1.
	//
	// Here we can see that in both cases we have an unreplicated lock held
	// at seq=2, but what to do when flushing depends on what sequence
	// numbers have been rolled back. Storing the set of ignored sequence
	// numbers from the point of acquisition allows us to do this.
	//
	// Note that in the second case if seq=2 has been rolled back since we
	// stored the ignoredSeqNums, that is OK, we have a lock at seq=1
	// still. If seq=1 has been rolled back since we stored the
	// ingoredSeqNums, that is also OK, because future requests will include
	// it in the ignored seq number list and we also know there is no way to
	// rollback 1 without rolling back 2.
	//
	// We may ask whether the min sequence number in strengths above is
	// sufficient. It isn't since in the cases above, one cannot tell you
	// whether or not you need to preserve the replicated lock at seq=1
	// based on the sequence of the held unreplicated lock.
	ignoredSeqNums []enginepb.IgnoredSeqNumRange

	// The timestamp at which the unreplicated lock is held. Must not regress.
	ts hlc.Timestamp
}

const notHeldSentinel = -1

// Fixed length slice for all supported lock strengths for unreplicated locks.
// May be used to iterate supported lock strengths in strength order (strongest
// to weakest).
var unreplicatedHolderStrengths = [...]lock.Strength{lock.Exclusive, lock.Shared}

// unreplicatedLockHolderStrengthToIndexMap returns a mapping between
// (strength, index) pairs that can be used to index into the
// unreplicatedLockHolderInfo.strengths array.
//
// Trying to use a lock strength that isn't supported with unreplicated locks to
// index into the unreplicatedLockHolderInfo.strengths array will cause a
// runtime error.
var unreplicatedLockHolderStrengthToIndexMap = func() [lock.MaxStrength + 1]int {
	var m [lock.MaxStrength + 1]int
	// Initialize all to -1.
	for str := range m {
		m[str] = notHeldSentinel
	}
	// Set the indices of the valid strengths.
	for i, str := range unreplicatedHolderStrengths {
		m[str] = i
	}
	return m
}()

// init initializes an unreplicatedLockHolderInfo struct.
func (ulh *unreplicatedLockHolderInfo) init() {
	ulh.resetStrengths()
}

// clear removes previously tracked unreplicated lock holder information.
func (ulh *unreplicatedLockHolderInfo) clear() {
	ulh.resetStrengths()
	ulh.ignoredSeqNums = nil
	ulh.ts = hlc.Timestamp{}
}

// epochBumped is called when a transaction is known to have its epoch bumped.
// State specific to the previous epoch is cleared out but the timestamp is
// left unchanged.
func (ulh *unreplicatedLockHolderInfo) epochBumped() {
	ulh.resetStrengths()
	ulh.ignoredSeqNums = nil
}

func (ulh *unreplicatedLockHolderInfo) resetStrengths() {
	for strIdx := range ulh.strengths {
		ulh.strengths[strIdx] = notHeldSentinel
	}
}

// minSeqNumber returns the minimum sequence number the lock is held with given
// the supplied lock strength. -1 is returned if the lock is not held with the
// supplied lock strength.
func (ulh *unreplicatedLockHolderInfo) minSeqNumber(str lock.Strength) enginepb.TxnSeq {
	return ulh.strengths[unreplicatedLockHolderStrengthToIndexMap[str]]
}

func (ulh *unreplicatedLockHolderInfo) ignoredSequenceNumbersEqual(
	acq *roachpb.LockAcquisition,
) bool {
	// TODO(ssd): This uses structural equality rather than semantic equality. We
	// should update this to make sure that the lists are semantically equivalent
	// and add an assertion that the inbound set is a superset of the existing
	// set.
	if len(ulh.ignoredSeqNums) != len(acq.IgnoredSeqNums) {
		return false
	}
	for i := range ulh.ignoredSeqNums {
		if ulh.ignoredSeqNums[i] != acq.IgnoredSeqNums[i] {
			return false
		}
	}
	return true
}

func (ulh *unreplicatedLockHolderInfo) setIgnoredSequenceNumbers(
	ign []enginepb.IgnoredSeqNumRange,
) {
	if enginepb.TxnSeqListExtends(ign, ulh.ignoredSeqNums) {
		ulh.ignoredSeqNums = ign
	}
}

// acquire updates tracking on the receiver, if necessary[1], to denote the lock
// is held with the supplied lock strength at the supplied sequence number.
//
// [1] We only track the lowest (non-rolled back) sequence number with which a
// lock is held, as doing so is sufficient.
func (ulh *unreplicatedLockHolderInfo) acquire(
	str lock.Strength, seqNum enginepb.TxnSeq, ignoredSeqNums []enginepb.IgnoredSeqNumRange,
) error {
	if ulh.held(str) && seqNum < ulh.minSeqNumber(str) {
		// If the lock is already held at the given strength, with a given sequence
		// number, that sequence number is not allowed to regress. This invariant
		// is relied upon by how savepoint rollbacks work, where the lock table must
		// learn
		return errors.Newf(
			"cannot acquire lock with strength %s at seq number %d, already tracked at higher seq number %d",
			str, seqNum, ulh.minSeqNumber(str))
	}
	if !ulh.held(str) {
		ulh.strengths[unreplicatedLockHolderStrengthToIndexMap[str]] = seqNum
	}
	ulh.setIgnoredSequenceNumbers(ignoredSeqNums)

	return nil
}

// held returns true if the receiver is held with the supplied lock strength.
func (ulh *unreplicatedLockHolderInfo) held(str lock.Strength) bool {
	return ulh.minSeqNumber(str) != notHeldSentinel
}

// rollbackIgnoredSeqNumbers mutates the receiver to rollback any locks that are
// known to be held at sequence numbers that are known to be rolled back.
func (ulh *unreplicatedLockHolderInfo) rollbackIgnoredSeqNumbers(
	ignoredSeqNums []enginepb.IgnoredSeqNumRange,
) {
	if len(ignoredSeqNums) == 0 {
		return
	}
	// NOTE: this logic differs slightly from replicated lock acquisition, where
	// we don't rollback locks at ignored sequence numbers unless they are the
	// same strength as the lock acquisition. See the comment in MVCCAcquireLock.
	for strIdx, minSeqNumber := range ulh.strengths {
		if minSeqNumber == -1 {
			continue
		}
		if enginepb.TxnSeqIsIgnored(minSeqNumber, ignoredSeqNums) {
			ulh.strengths[strIdx] = -1
		}
	}
	ulh.setIgnoredSequenceNumbers(ignoredSeqNums)
}

func (ulh *unreplicatedLockHolderInfo) isEmpty() bool {
	for _, str := range unreplicatedHolderStrengths {
		if ulh.held(str) { // lock is held
			return false
		}
	}
	assert(ulh.ts.IsEmpty(), "lock not held, timestamp should be empty")
	return true
}

func (ulh *unreplicatedLockHolderInfo) safeFormat(sb *redact.StringBuilder) {
	if ulh.isEmpty() {
		return
	}
	sb.SafeString("unrepl [")
	first := true
	for _, str := range unreplicatedHolderStrengths {
		if !ulh.held(str) {
			continue
		}
		if !first {
			sb.Printf(", ")
		}
		first = false
		sb.Printf(
			"(str: %s seq: %d)",
			redact.Safe(str),
			redact.Safe(ulh.minSeqNumber(str)),
		)
	}
	sb.SafeString("]")
	if len(ulh.ignoredSeqNums) > 0 {
		sb.Printf(" ign seq: %v", ulh.ignoredSeqNums)
	}
}

// Fixed length slice for all supported lock strengths for replicated locks. May
// be used to iterate supported lock strengths in strength order (strongest to
// weakest).
var replicatedHolderStrengths = [...]lock.Strength{lock.Intent, lock.Exclusive, lock.Shared}

// replicatedLockHolderStrengthToIndexMap returns a mapping between (strength,
// index) pairs that can be used to index into the
// replicatedLockHolderInfo.strengths array.
//
// Trying to use a lock strength that isn't supported with replicated locks to
// index into the replicatedLockHolderInfo.strengths array will cause a runtime
// error.
var replicatedLockHolderStrengthToIndexMap = func() [lock.MaxStrength + 1]int {
	var m [lock.MaxStrength + 1]int
	// Initialize all to -1.
	for str := range m {
		m[str] = -1
	}
	// Set the indices of the valid strengths.
	for i, str := range replicatedHolderStrengths {
		m[str] = i
	}
	return m
}()

// Information about a lock holder for replicated locks.
type replicatedLockHolderInfo struct {
	// strengths tracks whether the lock is held with a particular strength or not.
	// Notably, unlike unreplicated locks, we do not track the sequence number at
	// which a lock was acquired with a particular strength. This way, we don't
	// need to worry about keeping what's in the replicated lock table keyspace in
	// sync with the in-memory lock table.
	strengths [len(replicatedHolderStrengths)]bool

	// ts is set iff the replicated lock is held with lock strength lock.Intent.
	// The timestamp here corresponds to the timestamp at which the intent is
	// written[*]. It must not regress.
	//
	// [*] As opposed to the timestamp of the transaction that wrote the intent,
	// as known to the lock table. This is important because intents aren't
	// rewritten when their transaction is pushed; they need to be resolved. As
	// such, for the purposes of sequencing, non-locking readers must use the
	// timestamp at which the intent is written when sequencing.
	ts hlc.Timestamp
}

// clear removes previously tracked replicated lock holder information.
func (rlh *replicatedLockHolderInfo) clear() {
	rlh.resetStrengths()
	rlh.ts = hlc.Timestamp{}
}

func (rlh *replicatedLockHolderInfo) isEmpty() bool {
	for _, str := range replicatedHolderStrengths {
		if rlh.held(str) { // lock is held
			return false
		}
	}
	assert(rlh.ts.IsEmpty(), "lock not held, timestamp should be empty")
	return true
}

func (rlh *replicatedLockHolderInfo) resetStrengths() {
	for strIdx := range rlh.strengths {
		rlh.strengths[strIdx] = false
	}
}

// acquire updates the tracking on the receiver to indicate a lock is held with
// the supplied lock strength.
func (rlh *replicatedLockHolderInfo) acquire(str lock.Strength, ts hlc.Timestamp) {
	if str == lock.Intent { // only set (or update) ts tracking for intents
		rlh.ts.Forward(ts)
	}
	if rlh.held(str) {
		return
	}
	rlh.strengths[replicatedLockHolderStrengthToIndexMap[str]] = true
}

// held returns true if the receiver is held with the supplied lock strength.
func (rlh *replicatedLockHolderInfo) held(str lock.Strength) bool {
	return rlh.strengths[replicatedLockHolderStrengthToIndexMap[str]]
}

func (rlh *replicatedLockHolderInfo) safeFormat(sb *redact.StringBuilder) {
	if rlh.isEmpty() {
		return
	}
	sb.SafeString("repl [")
	first := true
	for _, str := range replicatedHolderStrengths {
		if !rlh.held(str) {
			continue
		}
		if !first {
			sb.Printf(", ")
		}
		first = false
		sb.Printf(
			"%s",
			redact.Safe(str),
		)
	}
	sb.SafeString("]")
}

// Per-key locks state in lockTableImpl.
//
// NOTE: we can't easily pool keyLocks objects without some form of reference
// counting because they are used as elements in a copy-on-write btree and may
// still be referenced by clones of the tree even when deleted from the primary.
// However, other objects referenced by keyLocks can be pooled as long as they
// are removed from all keyLocks that reference them first.
type keyLocks struct {
	id     uint64 // needed for implementing util/interval/generic type contract
	endKey []byte // used in btree iteration and tests

	// The key being locked and the scope of that key. This state is never
	// mutated.
	key roachpb.Key

	mu syncutil.Mutex // Protects everything below.

	// Some Invariants:
	// - holders.Len() == len(heldByMap). That is, every transaction that holds a
	// lock on this key must also have an entry in the heldByMap (and vice versa).
	// - !holder.isLocked() => waitingReaders.Len() == 0. That is, readers only
	// wait if the lock is held.

	// holders is a list of *txnLocks. Lock information on a particular key is
	// tracked per-transaction; every element in this list represents a lock
	// acquired by a different transaction.
	//
	// This list may be empty, in which case the current key is unlocked.
	holders list.List[*txnLock]
	// heldByMap is used to lookup a txnLock in the holders list using transaction
	// IDs. This obviates the need to iterate over the entire list in various
	// places where we index by transaction ID.
	heldBy map[uuid.UUID]*list.Element[*txnLock]

	// Information about the requests waiting on the lock.
	lockWaitQueue

	// notRemovable is temporarily incremented when a lock is added using
	// AddDiscoveredLock. This is to ensure liveness by not allowing the lock to
	// be removed until the requester has called ScanAndEnqueue. The *keyLocks
	// is also remembered in lockTableGuardImpl.notRemovableLock. notRemovable
	// behaves like a reference count since multiple requests may want to mark
	// the same lock as not removable.
	notRemovable int
}

// txnLock tracks information about locks held by a specific transaction on a
// single key.
type txnLock struct {
	// The TxnMeta of the transaction that holds the lock. For a given
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
	//
	// TODO(arul): Now that we expect this to always be set, let's store the
	// entire txnMeta here instead of storing it by reference.
	txn *enginepb.TxnMeta

	// INVARIANT: At least one of (and possibly both of) unreplicatedInfo and
	// replicatedInfo must be set to track lock holder information.

	// unreplicatedInfo tracks lock holder information if the lock is held with
	// durability unreplicated.
	unreplicatedInfo unreplicatedLockHolderInfo

	// replicatedInfo tracks lock holder information if the lock is held with
	// durability replicated.
	replicatedInfo replicatedLockHolderInfo

	// The time at which the lock table started tracking this transaction's lock.
	// Note that if the lock is replicated in nature, the lock would not be
	// tracked by the lock table when it was acquired as we do not track
	// uncontended replicated locks. Instead, this field would be initialized when
	// a contending request discovered this lock.
	startTime time.Time
}

// newTxnLock constructs and returns a new txnLock.
func newTxnLock(txn *enginepb.TxnMeta, clock *hlc.Clock) *txnLock {
	tl := &txnLock{}
	tl.txn = txn
	tl.unreplicatedInfo.init()
	tl.startTime = clock.PhysicalTime()
	return tl
}

// getLockHolderTxn returns the transaction that holds the lock.
//
// REQUIRES: kl.mu is locked.
func (tl *txnLock) getLockHolderTxn() *enginepb.TxnMeta {
	return tl.txn
}

// writeTS returns a timestamp at which a provisional write has been
// performed[*]. Any non-locking readers at or above this timestamp should block
// on this lock. Note that this timestamp may be different from the
// transaction's provisional commit timestamp; however, non-locking readers
// cannot proceed until the intent has been moved to a higher timestamp.
//
// [*] Unreplicated exclusive block non-locking readers even though they aren't
// writes. If only an unreplicated exclusive lock exists, we return its
// timestamp. If both an unreplicatd exclusive lock and an intent is present,
// we defer to the intent.
//
// REQUIRES: kl.mu to be locked.
func (tl *txnLock) writeTS() hlc.Timestamp {
	// Replicated locks only block non-locking readers if they're held with
	// lock strength == lock.Intent. Note that replicated exclusive locks do not
	// block non-locking readers.
	if tl.isHeldReplicated() && tl.replicatedInfo.held(lock.Intent) {
		return tl.replicatedInfo.ts
	}
	// Unreplicated locks only block non-locking readers if they're held with
	// lock strength == lock.Exclusive. Note that unreplicated locks can't be held
	// with lock strength lock.Intent.
	if tl.isHeldUnreplicated() && tl.unreplicatedInfo.held(lock.Exclusive) {
		// If there's both a write intent and an unreplicated exclusive lock, we want
		// to prefer the lower of the two timestamps, since the lower timestamp
		// blocks more non-locking readers.
		return tl.unreplicatedInfo.ts
	}
	return hlc.MaxTimestamp
}

// isHeldReplicated returns true if the receiver is held as a replicated lock.
//
// REQUIRES: kl.mu is locked.
func (tl *txnLock) isHeldReplicated() bool {
	return !tl.replicatedInfo.isEmpty()
}

// isHeldUnreplicated returns true if the receiver is held as a unreplicated
// lock.
//
// REQUIRES: kl.mu is locked.
func (tl *txnLock) isHeldUnreplicated() bool {
	return !tl.unreplicatedInfo.ts.IsEmpty()
}

// getLockMode returns the Mode with which a lock is held. If a lock is held
// by a transaction with multiple locking strengths, the mode corresponding to
// the highest lock strength is returned.
//
// REQUIRES: kl.mu is locked.
func (tl *txnLock) getLockMode() lock.Mode {
	lockHolderTxn := tl.getLockHolderTxn()
	ts := tl.writeTS()

	// Only replicated locks can be held with strength lock.Intent. It's also the
	// strongest locking strength, so if held, it's the one we prefer.
	if tl.replicatedInfo.held(lock.Intent) {
		return lock.MakeModeIntent(ts)
	}
	// Other than lock.Intent, which we've already handled above,
	// replicatedHolderStrengths == unreplicatedHolderStrengths.
	for _, str := range unreplicatedHolderStrengths {
		if !tl.unreplicatedInfo.held(str) && !tl.replicatedInfo.held(str) {
			continue
		}
		switch str {
		case lock.Exclusive:
			if tl.unreplicatedInfo.held(str) {
				return lock.MakeModeExclusive(ts, lockHolderTxn.IsoLevel)
			}
			// Using hlc.MaxTimestamp as the timestamp for the exclusive lock ensures
			// that non-locking reads do not conflict with replicated exclusive locks.
			return lock.MakeModeExclusive(hlc.MaxTimestamp, lockHolderTxn.IsoLevel)
		case lock.Shared:
			return lock.MakeModeShared()
		default:
			panic(fmt.Sprintf("unexpected lock strength %s", str))
		}
	}
	panic("unreachable")
}

// isIdempotentLockAcquisition returns true if the lock acquisition is
// idempotent. Idempotent lock acquisitions do not require any changes to what
// is being tracked in the lock's state.
//
// REQUIRES: kl.mu to be locked.
func (tl *txnLock) isIdempotentLockAcquisition(acq *roachpb.LockAcquisition) bool {
	assert(tl.getLockHolderTxn().ID == acq.Txn.ID,
		"existing lock transaction is different from the acquisition")

	switch acq.Durability {
	case lock.Unreplicated:
		if !tl.unreplicatedInfo.held(acq.Strength) { // unheld lock
			return false
		}
		// Lock is being re-acquired at a higher sequence number when it's already
		// held at a lower sequence number.
		return tl.unreplicatedInfo.minSeqNumber(acq.Strength) <= acq.Txn.Sequence &&
			// NB: Lock re-acquisitions at different timestamps are not considered
			// idempotent. Strictly speaking, we could tighten this condition to
			// consider lock re-acquisition at lower timestamps idempotent, as a
			// lock's timestamp at a given durability never regresses.
			tl.unreplicatedInfo.ts.Equal(acq.Txn.WriteTimestamp) && tl.unreplicatedInfo.ignoredSequenceNumbersEqual(acq)
	case lock.Replicated:
		// Lock is being re-acquired with the same strength.
		return tl.replicatedInfo.held(acq.Strength) &&
			// NB: Lock re-acquisitions at different timestamps are not considered
			// idempotent. Strictly speaking, we could tighten this condition in a
			// few ways:
			// 1. We could consider lock re-acquisition at a lower timestamp idempotent,
			// as a lock's timestamp at a given durability can never regress.
			// 2. We could only check the timestamp if the lock is being acquired with
			// strength lock.Intent, as we disregard the timestamp for all other lock
			// strengths when dealing with replicated locks.
			tl.replicatedInfo.ts.Equal(acq.Txn.WriteTimestamp)
	default:
		panic(fmt.Sprintf("unknown lock durability: %s", acq.Durability))
	}
}

// reacquireLock is called in response to a lock re-acquisition. In such cases,
// a lock is already held on the receiver's key by the transaction referenced in
// the supplied lock acquisition.
//
// REQUIRES: kl.mu to be locked.
func (tl *txnLock) reacquireLock(acq *roachpb.LockAcquisition) error {
	// An unreplicated lock is being re-acquired...
	if acq.Durability == lock.Unreplicated && tl.isHeldUnreplicated() {
		switch {
		case tl.txn.Epoch < acq.Txn.Epoch: // at a higher epoch
			tl.unreplicatedInfo.epochBumped()
		case tl.txn.Epoch == acq.Txn.Epoch: // at the same epoch
			// Prune the list of sequence numbers tracked for this lock by removing
			// any sequence numbers that are considered ignored by virtue of a
			// savepoint rollback.
			//
			// Note that the in-memory lock table is the source of truth for just
			// unreplicated locks, and as such, sequence numbers are only tracked
			// for them.
			tl.unreplicatedInfo.rollbackIgnoredSeqNumbers(acq.IgnoredSeqNums)
		case tl.txn.Epoch > acq.Txn.Epoch: // at a prior epoch
			// Reject the request; the logic here parallels how mvccPutInternal
			// handles this case for intents.
			return errors.Errorf(
				"locking request with epoch %d came after lock(unreplicated) had already been acquired at epoch %d in txn %s",
				acq.Txn.Epoch, tl.txn.Epoch, acq.Txn.ID,
			)
		default:
			panic("unreachable")
		}
	}
	if tl.isIdempotentLockAcquisition(acq) {
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
	// However, a lock's timestamp as reported by writeTS can regress if it is
	// acquired at a lower timestamp and a different durability than it was
	// previously held with. This is necessary to support because the hard
	// constraint which we must uphold here that the lockHolderInfo for a
	// replicated lock cannot diverge from the replicated state machine in such a
	// way that its timestamp in the lockTable exceeds that in the replicated
	// keyspace. If this invariant were to be violated, we'd risk infinite
	// lock-discovery loops for requests that conflict with the lock as is written
	// in the replicated state machine but not as is reflected in the lockTable.
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
		tl.unreplicatedInfo.ts.Forward(acq.Txn.WriteTimestamp)
		if err := tl.unreplicatedInfo.acquire(acq.Strength, acq.Txn.Sequence, acq.IgnoredSeqNums); err != nil {
			return err
		}
	case lock.Replicated:
		tl.replicatedInfo.acquire(acq.Strength, acq.Txn.WriteTimestamp)
	default:
		panic(fmt.Sprintf("unknown lock durability: %s", acq.Durability))
	}

	// Selectively update the txn meta.
	switch {
	case tl.txn.Epoch > acq.Txn.Epoch: // lock is being acquired at a prior epoch
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
	case tl.txn.Epoch == acq.Txn.Epoch: // lock is being acquired at the same epoch
		tl.txn = &acq.Txn
	case tl.txn.Epoch < acq.Txn.Epoch: // lock is being acquired at a newer epoch
		tl.txn = &acq.Txn
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
			tl.unreplicatedInfo.clear()
		}
	}

	return nil
}

type lockWaitQueue struct {
	// TODO(sbhola): There are a number of places where we iterate over these
	// lists looking for something, as described below. If some of these turn
	// out to be inefficient, consider better data-structures. One idea is that
	// for cases that find a particular guard the lockTableGuardImpl.locks can be
	// a map instead of a set to point directly to the *list.Element.
	//
	// queuedLockingRequests:
	// - to find all active queued locking requests.
	// - to find a particular guard.
	// - to find the position, based on seqNum, for inserting a particular guard.
	// - to find all waiting locking requests with a particular txn ID.
	//
	// waitingReaders:
	// - readers with a higher timestamp than some timestamp.
	// - to find a particular guard.

	// Waiters: An active waiter needs to be notified about changes in who it is
	// waiting for.

	// queuedLockingRequests is a list of requests queued at a key. They may be
	// waiting actively or inactively. The list is maintained in increasing order
	// of sequence numbers. This helps ensure some degree of fairness as requests
	// are released from the head of the queue. Typically, this happens when all
	// locks on the associated key are released.
	//
	// When a lock is not held, the head of the list should be comprised of an
	// inactive, transactional locking request (if the list is non-empty). Keeping
	// its position as an inactive waiter at the head of the queue serves as a
	// claim to prevent other concurrent locking requests (with higher sequence
	// numbers) from barging in front of it. This is important for two reasons:
	//
	// 1. It helps ensure some degree of fairness, as sequence numbers are a proxy
	// for arrival order.
	// 2. Perhaps more importantly, enforcing this ordering helps prevent
	// range-local lock table deadlocks. This is because all locks aren't known
	// upfront to the lock table (as uncontended, replicated locks are only
	// discovered during evaluation). This means that no total ordering of lock
	// acquisition is enforced by the lock table -- using sequence numbers to
	// break ties allows us to prevent deadlocks that could have arisen otherwise.
	//
	// Conversely, a request with a lower sequence number is allowed to barge in
	// front of an inactive waiter with a higher sequence number if the lock is
	// not held. This can be thought of as "breaking the claim" that the higher
	// sequence numbered request tried to establish. As both these requests
	// sequence through the lock table one of them will win the race. This is
	// fine, as the request that wins the race can only evaluate while holding
	// latches and the two requests must conflict on latches. As a result they're
	// guaranteed to be isolated. We don't concern ourselves with the possible
	// fairness issue if the higher sequence number wins the race.
	//
	// Non-locking readers are held separately, in the waitingReaders list. Unlike
	// locking requests, they make no claims on unheld locks. Instead, they race
	// with other locking request(s) that have made a claim.
	//
	// Similarly, non-transactional requests make no claims either, regardless of
	// their read/write status. Non-transactional writers wait in the
	// queuedLockingRequests list along with transactional locking requests. The
	// difference between non-transactional writers and transactional locking
	// requests is as follows:
	//
	// 1. When a lock transitions from held to released, the head of the queue
	// that is made up of non-transactional writes is cleared in one swoop (until
	// we hit the first transactional writer or the queue is entirely drained).
	// This means non-transactional writers race with transactional locking
	// requests' claim(s), like read requests.
	// 2. When deciding whether to wait at an unheld lock or not, a
	// non-transactional writer will check how its sequence number compares to the
	// head of the queuedLockingRequests list. If it's lower, it'll proceed
	// without inserting itself into the queuedLockingRequests list; otherwise,
	// it'll wait.
	//
	// Multiple requests from the same transaction wait independently, including
	// the situation where one of the requests is an inactive waiter at the head
	// of the queue. However, if the inactive waiter manages to sequence,
	// evaluate, and acquire the lock, other requests from the same transaction
	// with lock strength <= the strength with which the lock was acquired are
	// allowed to proceed.
	//
	// The behavior of only one transactional locking request being allowed to
	// make a claim by marking itself as inactive when a lock transitions from
	// held to free is subject to change. As we introduce support for multiple
	// locking strengths, and in particular locking strengths that are compatible
	// with each other (read: shared locks), one could imagine a scheme where the
	// head of the queuedLockingRequests that is compatible with each other is
	// marked as inactive and allowed to proceed. A "joint claim".
	// TODO(arul): update the comment above, this change is coming soon!
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
	queuedLockingRequests list.List[*queuedGuard]

	// waitingReaders is a list of non-locking reads that are actively waiting at
	// a key. If this list is non-empty, the key must be locked, as non-locking
	// reads do not wait otherwise.
	//
	// NB: Non-locking readers can never wait in the waitSelf state, because if
	// another request from their transaction already holds a lock on the key,
	// they are allowed to proceed.
	waitingReaders list.List[*lockTableGuardImpl]
}

//go:generate ../../../util/interval/generic/gen.sh *keyLocks concurrency

// Methods required by util/interval/generic type contract.
func (kl *keyLocks) ID() uint64         { return kl.id }
func (kl *keyLocks) Key() []byte        { return kl.key }
func (kl *keyLocks) EndKey() []byte     { return kl.endKey }
func (kl *keyLocks) New() *keyLocks     { return new(keyLocks) }
func (kl *keyLocks) SetID(v uint64)     { kl.id = v }
func (kl *keyLocks) SetKey(v []byte)    { kl.key = v }
func (kl *keyLocks) SetEndKey(v []byte) { kl.endKey = v }

// REQUIRES: kl.mu is locked.
func (kl *keyLocks) String() string {
	var sb redact.StringBuilder
	kl.safeFormat(&sb, nil)
	return sb.String()
}

// SafeFormat implements redact.SafeFormatter.
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) SafeFormat(w redact.SafePrinter, _ rune) {
	var sb redact.StringBuilder
	kl.safeFormat(&sb, nil)
	w.Print(sb)
}

// safeFormat is a helper for SafeFormat and String methods.
// REQUIRES: kl.mu is locked. txnStatusCache can be nil.
func (kl *keyLocks) safeFormat(sb *redact.StringBuilder, txnStatusCache *txnStatusCache) {
	sb.Printf(" lock: %s\n", kl.key)
	if kl.isEmptyLock() {
		sb.SafeString("  empty\n")
		return
	}
	if kl.isLocked() {

		first := true
		for e := kl.holders.Front(); e != nil; e = e.Next() {
			tl := e.Value
			txn := tl.getLockHolderTxn()
			// We use the lowest timestamp at which a lock is held at in its
			// formatting. The lowest timestamp contends with more transactions
			// (assuming the strength dictates as such).
			var ts = tl.writeTS()

			var prefix string
			if first {
				first = false
				optPlural := ""
				if kl.holders.Len() > 1 {
					optPlural = "s"
				}
				prefix = fmt.Sprintf("  holder%s: ", optPlural)
			} else {
				prefix = "\n           "
			}
			var optTS string
			if !ts.Equal(hlc.MaxTimestamp) {
				optTS = fmt.Sprintf(" ts: %v,", ts)
			}
			sb.Printf("%stxn: %v epoch: %d, iso: %s,%s info: ",
				redact.Safe(prefix), redact.Safe(txn.ID), redact.Safe(txn.Epoch),
				redact.Safe(txn.IsoLevel), redact.Safe(optTS),
			)
			if !tl.replicatedInfo.isEmpty() {
				tl.replicatedInfo.safeFormat(sb)
				if !tl.unreplicatedInfo.isEmpty() {
					sb.Printf(", ")
				}
			}
			if !tl.unreplicatedInfo.isEmpty() {
				tl.unreplicatedInfo.safeFormat(sb)
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
		}
		sb.SafeString("\n")
	}
	// TODO(sumeer): Add an optional `description string` field to Request and
	// lockTableGuardImpl that tests can set to avoid relying on the seqNum to
	// identify requests.
	if kl.waitingReaders.Len() > 0 {
		sb.SafeString("   waiting readers:\n")
		for e := kl.waitingReaders.Front(); e != nil; e = e.Next() {
			g := e.Value
			sb.Printf("    req: %d, txn: ", redact.Safe(g.seqNum))
			if g.txn == nil {
				sb.SafeString("none\n")
			} else {
				sb.Printf("%v\n", redact.Safe(g.txn.ID))
			}
		}
	}
	if kl.queuedLockingRequests.Len() > 0 {
		sb.SafeString("   queued locking requests:\n")
		for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
			qg := e.Value
			g := qg.guard
			optPromotingMsg := redact.SafeString("")
			if qg.order.isPromoting {
				optPromotingMsg = " promoting: true"
			}
			sb.Printf("    active: %t req: %d%s, strength: %s, txn: ",
				qg.active, qg.order.reqSeqNum, optPromotingMsg, qg.mode.Strength,
			)
			if g.txn == nil {
				sb.SafeString("none\n")
			} else {
				sb.Printf("%v\n", redact.Safe(g.txn.ID))
			}
		}
	}
}

// collectLockStateInfo exports all locks held on the receiver's key as a list
// of roachpb.LockStateInfos. If no locks are held, or the lock is uncontended
// and includeUncontended is false, nothing is returned.
func (kl *keyLocks) collectLockStateInfo(
	includeUncontended bool, now time.Time, rangeID roachpb.RangeID,
) []roachpb.LockStateInfo {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	// Don't include locks that have neither lock holders, nor claims, nor
	// waiting readers/locking requests.
	if kl.isEmptyLock() {
		return nil
	}

	// Filter out locks without waiting readers/active locking requests unless explicitly
	// requested.
	if !includeUncontended && kl.waitingReaders.Len() == 0 &&
		!kl.hasActivelyWaitingLockingRequest() {
		return nil
	}

	return kl.lockStateInfo(now, rangeID)
}

// lockStateInfo converts receiver to the roachpb.LockStateInfo structure.
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) lockStateInfo(now time.Time, rangeID roachpb.RangeID) []roachpb.LockStateInfo {
	waiterCount := kl.waitingReaders.Len() + kl.queuedLockingRequests.Len()
	lockWaiters := make([]lock.Waiter, 0, waiterCount)

	// Add waiting readers before locking requests as they should run first.
	for e := kl.waitingReaders.Front(); e != nil; e = e.Next() {
		readerGuard := e.Value
		readerGuard.mu.Lock()
		lockWaiters = append(lockWaiters, lock.Waiter{
			WaitingTxn:   readerGuard.txnMeta(),
			ActiveWaiter: true, // readers always actively wait at a lock
			Strength:     lock.None,
			WaitDuration: now.Sub(readerGuard.mu.curLockWaitStart),
		})
		readerGuard.mu.Unlock()
	}

	// Lastly, add queued locking requests, in order.
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		qg := e.Value
		g := qg.guard
		g.mu.Lock()
		lockWaiters = append(lockWaiters, lock.Waiter{
			WaitingTxn:   g.txnMeta(),
			ActiveWaiter: qg.active,
			Strength:     lock.Exclusive,
			WaitDuration: now.Sub(g.mu.curLockWaitStart),
		})
		g.mu.Unlock()
	}

	if !kl.isLocked() {
		return []roachpb.LockStateInfo{
			{
				RangeID:      rangeID,
				Key:          kl.key,
				LockHolder:   nil,
				Durability:   lock.Unreplicated,
				HoldDuration: kl.lockHeldDuration(now),
				Waiters:      lockWaiters,
				LockStrength: lock.None,
			},
		}
	}

	var lockStateInfos []roachpb.LockStateInfo
	for e := kl.holders.Front(); e != nil; e = e.Next() {
		tl := e.Value
		durability := lock.Unreplicated
		if tl.isHeldReplicated() {
			durability = lock.Replicated
		}
		lsi := roachpb.LockStateInfo{
			RangeID:      rangeID,
			Key:          kl.key,
			LockHolder:   tl.txn,
			Durability:   durability,
			HoldDuration: now.Sub(tl.startTime),
			Waiters:      lockWaiters,
			LockStrength: tl.getLockMode().Strength,
		}
		lockStateInfos = append(lockStateInfos, lsi)
	}
	return lockStateInfos
}

// addToMetrics adds the receiver's state to the provided metrics struct.
func (kl *keyLocks) addToMetrics(m *LockTableMetrics, now time.Time) {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	if kl.isEmptyLock() {
		return
	}
	totalWaitDuration, maxWaitDuration := kl.totalAndMaxWaitDuration(now)
	lm := LockMetrics{
		Key:                  kl.key,
		Held:                 kl.isLocked(),
		HoldDurationNanos:    kl.lockHeldDuration(now).Nanoseconds(),
		WaitingReaders:       int64(kl.waitingReaders.Len()),
		WaitingWriters:       int64(kl.queuedLockingRequests.Len()),
		WaitDurationNanos:    totalWaitDuration.Nanoseconds(),
		MaxWaitDurationNanos: maxWaitDuration.Nanoseconds(),
	}
	lm.Waiters = lm.WaitingReaders + lm.WaitingWriters
	m.addLockMetrics(lm)
}

// informActiveWaiters informs active waiters about the transaction that has
// claimed the lock. The claimant transaction may have changed, so there may be
// inconsistencies with waitFor and waitSelf states that need fixing.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) informActiveWaiters() {
	if kl.waitingReaders.Len() == 0 && kl.queuedLockingRequests.Len() == 0 {
		return // no active waiters to speak of; early return
	}
	waitForState := waitingState{
		kind:                  waitFor,
		key:                   kl.key,
		queuedLockingRequests: kl.queuedLockingRequests.Len(),
		queuedReaders:         kl.waitingReaders.Len(),
	}
	// TODO(arul): This is entirely busted once we have multiple lock holders.
	// In such cases, there may be a request waiting not on the head of the
	// queue, but because there is a waiter with a lower sequence number that it
	// is incompatible with. In such cases, its this guy it should be pushing.
	// However, if we naively plugged things into the current structure, it would
	// either sit tight (because its waiting for itself) or, worse yet, push a
	// transaction it's actually compatible with!

	for e := kl.waitingReaders.Front(); e != nil; e = e.Next() {
		state := waitForState
		g := e.Value
		state.txn, state.held = kl.claimantTxnFor(g)
		// Since there are waiting readers, we could not have transitioned out of
		// or into a state where the lock is held. This is because readers only wait
		// for held locks -- they race with other non-transactional writers and
		// locking requests.
		assert(state.held, "waiting readers should be empty if the lock isn't held")
		g.mu.Lock()
		// NB: The waiter is actively waiting on this lock, so it's likely taking
		// some action based on the previous state (e.g. it may be pushing someone).
		// If the state has indeed changed, it must perform a different action -- so
		// we pass notify = true here to nudge it to do so.
		g.maybeUpdateWaitingStateLocked(state, true /* notify */)
		g.mu.Unlock()
	}
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		qg := e.Value
		if !qg.active {
			continue
		}
		g := qg.guard
		state := waitForState
		state.txn, state.held = kl.claimantTxnFor(g)
		if g.isSameTxn(state.txn) {
			// The waitSelf state is only possible if the lock isn't held. See the
			// comment at the enum declaration for an explanation of why.
			if state.held {
				panic("requests should not be in the waitSelf state if locks are held")
			}
			state.kind = waitSelf
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

// claimantTxnFor returns the transaction that the lock table deems to have
// claimed the key, which in turn causes the supplied request to actively wait.
// Transactions that have acquired locks on keys are deemed to have claimed
// them. If a key is not locked, the lock table will deem a request being
// sequenced through the lock table as its next preferred[1] lock holder, and
// coronate it the claimant.
//
// There can be multiple locks on a particular key. There may also be multiple
// compatible (in the common case, compatible with each other) requests being
// sequenced through the lock table. Actively waiting requests are oblivious to
// such details; instead, they use the concept of the claimant transaction as
// the transaction to push for liveness and deadlock detection. In some sense,
// we're using the concept of claimant transactions to collapse the local
// dependency graph represented by the lock's wait queue, but abstracting the
// specifics from the lock table waiter (which is responsible for pushing).
//
// There is only a single claimant transaction for a key from a request's
// perspective. As such, an actively waiting request only needs to push a single
// transaction at any given time. However, actively waiting requests from
// different transactions may observe different claimants. This is only possible
// if there is an unresolvable[2], local deadlock in the lock's wait queues.
// We're currently not smart enough to recognize this case without engaging
// distributed deadlock detection. Otherwise, all actively waiting requests
// will observe the same claimant on a particular key.
//
// [1] This preference is based on the lock table's notion of fairness, with
// some re-ordering provisions to avoid deadlocks. See queueOrder for more
// details.
//
// [2] A deadlock is unresolvable if we can't locally re-order the wait queue to
// prevent it. Typically, this happens when 2 shared locks are held on a key,
// and both the shared lock holding transactions are trying to promote their
// locks to either Exclusive or Intent lock strength.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) claimantTxnFor(g *lockTableGuardImpl) (_ *enginepb.TxnMeta, held bool) {
	if kl.isLocked() {
		// We want the claimant transaction to be the same for a particular request
		// every time this function is called, unless there has been a state
		// transition (e.g. the claimant released the lock) that necessitates the
		// claimant to change. In cases where there are multiple lock holders on a
		// key, we ensure this by always iterating from the start of the lock
		// holders list.
		for e := kl.holders.Front(); e != nil; e = e.Next() {
			// claimantTxnFor is only called by actively waiting requests. Requests
			// don't conflict with locks held by their transaction. So, if the lock is
			// already held by the supplied request's transaction, then there must be
			// another conflicting lock held by a different transaction that's causing
			// it to actively wait on this key -- in such cases, this other
			// transaction is the claimant from the supplied request's perspective.
			// This prompts the check below.
			if !g.isSameTxn(e.Value.txn) {
				return e.Value.txn, true
			}
		}
	}
	if kl.queuedLockingRequests.Len() == 0 {
		panic("no queued locking request or lock holder; no one should be waiting on the lock")
	}
	qg := kl.queuedLockingRequests.Front().Value
	// NB: the returned transaction can be the same transaction to which the
	// supplied request belongs, and that's okay. See the waitSelf state for more
	// details.
	return qg.guard.txnMeta(), false
}

// releaseLockingRequestsFromTxn removes all locking requests waiting on the
// key, referenced in the receiver, that are part of the specified transaction.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) releaseLockingRequestsFromTxn(txn *enginepb.TxnMeta) {
	for e := kl.queuedLockingRequests.Front(); e != nil; {
		qg := e.Value
		curr := e
		e = e.Next()
		g := qg.guard
		if g.isSameTxn(txn) {
			kl.removeLockingRequest(curr)
		}
	}
}

// Returns true iff the keyLocks is empty, i.e., there is no lock holder and no
// waiters.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) isEmptyLock() bool {
	if kl.isLocked() {
		return false // lock is held
	}
	// The lock isn't held. Sanity check the lock holder state is sane.
	// 1. The heldBy map and the holders list should be empty.
	// 2. There should be no waiting readers.
	assert(kl.holders.Len() == 0, "non-empty list of holders for an unlocked key")
	assert(len(kl.heldBy) == 0, "non-empty heldBy map for an unlocked key")
	assert(kl.waitingReaders.Len() == 0, "keyLocks with waiting readers for unlocked key")
	// Determine if the lock is empty or not by checking the list of queued
	// locking requests.
	return kl.queuedLockingRequests.Len() == 0
}

// assertEmptyLock asserts that the keyLocks is empty. This condition must hold
// for a lock to be safely removed from the tree. If it does not hold, requests
// with a stale snapshot of the btree will still be able to enter the lock's
// wait-queue, after which point they will never hear of lock updates.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) assertEmptyLock() {
	if !kl.isEmptyLock() {
		panic("keyLocks is not empty")
	}
}

// assertEmptyLockUnlocked is like assertEmptyLock, but it locks the keyLocks.
//
// REQUIRES: kl.mu is not locked.
func (kl *keyLocks) assertEmptyLockUnlocked() {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	kl.assertEmptyLock()
}

// isAnyLockHeldReplicated returns true if any of the locks held on the key are
// held with durability replicated. If so, the first transaction that holds a
// replicated lock is also returned.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) isAnyLockHeldReplicated() (bool, *enginepb.TxnMeta) {
	if !kl.isLocked() {
		return false, nil
	}
	for e := kl.holders.Front(); e != nil; e = e.Next() {
		tl := e.Value
		if tl.isHeldReplicated() {
			return true, tl.txn
		}
	}
	return false, nil
}

// Returns the duration of time the lock has been tracked as held in the lock table.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) lockHeldDuration(now time.Time) time.Duration {
	if !kl.isLocked() {
		return time.Duration(0)
	}

	var minStartTS time.Time // we'll find the lowest timestamp across all locks held on this key
	for e := kl.holders.Front(); e != nil; e = e.Next() {
		tl := e.Value
		if minStartTS.IsZero() || tl.startTime.Before(minStartTS) {
			minStartTS = tl.startTime
		}
	}
	return now.Sub(minStartTS)
}

// Returns the total amount of time all active waiters in the queues of
// readers and locking requests have been waiting on the key referenced in the
// receiver.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) totalAndMaxWaitDuration(now time.Time) (time.Duration, time.Duration) {
	var totalWaitDuration time.Duration
	var maxWaitDuration time.Duration
	for e := kl.waitingReaders.Front(); e != nil; e = e.Next() {
		g := e.Value
		g.mu.Lock()
		waitDuration := now.Sub(g.mu.curLockWaitStart)
		totalWaitDuration += waitDuration
		if waitDuration > maxWaitDuration {
			maxWaitDuration = waitDuration
		}
		g.mu.Unlock()
	}
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		qg := e.Value
		if qg.active {
			g := qg.guard
			g.mu.Lock()
			waitDuration := now.Sub(g.mu.curLockWaitStart)
			totalWaitDuration += waitDuration
			if waitDuration > maxWaitDuration {
				maxWaitDuration = waitDuration
			}
			g.mu.Unlock()
		}
	}
	return totalWaitDuration, maxWaitDuration
}

// Returns true iff the lock is currently held by the transaction with the
// given id.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) isLockedBy(id uuid.UUID) bool {
	if kl.isLocked() {
		_, ok := kl.heldBy[id]
		return ok
	}
	return false
}

// isLocked returns true if key is locked. If locked, the key be locked by one
// or more transactions. Each transaction's locks may be held durably,
// non-durably, or both.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) isLocked() bool {
	return kl.holders.Len() != 0
}

func (kl *keyLocks) clearAllLockHolders() {
	kl.holders.Init()
	kl.heldBy = nil
}

// releaseLock releases the lock, if held, by the supplied transaction.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) releaseLock(txn *enginepb.TxnMeta) {
	e, held := kl.heldBy[txn.ID]
	if !held {
		return // nothing to do
	}
	kl.holders.Remove(e)
	delete(kl.heldBy, txn.ID)
	// There may be requests in the wait queue that belong to the supplied
	// transaction. They were considered promoters while the lock was held, but
	// that's no longer the case. As such, their queueOrder.isPromoting flag is
	// incorrect, and so is their spot in the lock's wait queue. We can fix this
	// in two different ways:
	// 1. Recompute queueOrder.isPromoting for waiting requests that belong to the
	// transaction that just released its lock and then reorder the wait queue (if
	// needed).
	// 2. Release any locking requests that belong to the transaction that just
	// released its lock. They'll re-scan, re-determine whether they're promoting
	// or not, and be inserted in the correct spot.
	//
	// We choose option 2.
	//
	// TODO(arul): Option 1, where we push this complexity into
	// recomputeWaitQueues, is better. We should switch to that, and in doing
	// so, get rid of all calls to releaseLockingRequestsFromTxn and replace
	// them with recomputeWaitQueues. Notably, this includes the call to that
	// method in the lock acquisition path as well.
	kl.releaseLockingRequestsFromTxn(txn)
}

// lockAcquiredOrDiscovered is called when the supplied lock is successfully
// acquired or discovered on the key referenced in the receiver for the first
// time.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) lockAcquiredOrDiscovered(tl *txnLock) {
	_, found := kl.heldBy[tl.txn.ID]
	assert(!found, "lock was already being tracked for this key")
	kl.heldBy[tl.txn.ID] = kl.holders.PushBack(tl)
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
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) scanAndMaybeEnqueue(g *lockTableGuardImpl, notify bool) (wait bool, _ error) {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	if kl.isEmptyLock() {
		return false /* wait */, nil
	}

	// It is possible that the lock is already held by this request's
	// transaction, and it is held with a lock strength good enough for it.
	isAllowedToProceed, err := kl.alreadyHoldsLockAndIsAllowedToProceed(g, g.curStrength())
	if err != nil {
		return false, err
	}
	if isAllowedToProceed {
		return false /* wait */, nil
	}

	if g.curStrength() == lock.None {
		conflicts := kl.maybeEnqueueNonLockingReadRequest(g)
		if conflicts {
			ws := kl.constructWaitingState(g)
			g.startWaitingWithWaitingState(ws, notify)
			return true /* wait */, nil
		}
		return false /* wait */, nil
	}

	// We're purely dealing with locking requests from here on out.

	maxQueueLengthExceeded, err := kl.enqueueLockingRequest(g)
	if err != nil {
		return false, err
	}
	if maxQueueLengthExceeded {
		// NB: Requests that encounter a lock wait-queue that is longer than
		// what they're willing to wait for are rejected by the lock table
		// waiter based on the waiting state we'll construct here.
		ws := kl.constructWaitingState(g)
		ws.kind = waitQueueMaxLengthExceeded
		g.startWaitingWithWaitingState(ws, notify)
		// Return true, not because we want to wait, but because we want
		// this request to be rejected in the lock table waiter.
		return true /* wait */, nil
	}

	if kl.shouldRequestActivelyWait(g) {
		ws := kl.constructWaitingState(g)
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
		return true /* wait */, nil
	}

	kl.claimBeforeProceeding(g)
	// Now that this request has acquired a claim, requests actively waiting
	// behind this request may be able to proceed by establishing a joint claim if
	// they are compatible. recomputeWaitQueues will also inform active waiters
	// that may need to be made aware that this request has acquired a claim.
	kl.recomputeWaitQueues(g.lt.settings)
	return false /* wait */, nil
}

// constructWaitingState constructs the waiting state the supplied request
// should use to wait in the receiver's lock wait-queues.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) constructWaitingState(g *lockTableGuardImpl) waitingState {
	waitForState := waitingState{
		kind:                  waitFor,
		key:                   kl.key,
		queuedLockingRequests: kl.queuedLockingRequests.Len(),
		queuedReaders:         kl.waitingReaders.Len(),
		held:                  true,
	}
	txn, held := kl.claimantTxnFor(g)
	waitForState.held = held
	waitForState.txn = txn
	if g.isSameTxn(waitForState.txn) {
		waitForState.kind = waitSelf
	}
	return waitForState
}

// alreadyHoldsLockAndIsAllowedToProceed returns true if the request, referenced
// by the supplied lock table guard, is allowed to proceed because its
// transaction already holds the lock with an equal or higher lock strength
// compared to the one supplied. Otherwise, false is returned.
//
// REQUIRES: kl.mu to be locked.
// TODO(arul): We can get rid of the error path here.
func (kl *keyLocks) alreadyHoldsLockAndIsAllowedToProceed(
	g *lockTableGuardImpl, str lock.Strength,
) (bool, error) {
	if !kl.isLocked() {
		return false, nil // no one holds the lock
	}
	if g.txn == nil {
		return false, nil // non-transactional requests do not hold locks
	}
	e, found := kl.heldBy[g.txn.ID]
	if !found {
		return false, nil
	}
	tl := e.Value
	heldMode := tl.getLockMode()
	// Check if the lock is already held by the guard's transaction with an equal
	// or higher lock strength. If it is, we're good to go. Otherwise, the request
	// is trying to promote a lock it previously acquired. In such cases, the
	// existence of a lock with weaker strength doesn't do much for this request
	// apart from its ordering in the wait queue. For the most part, it's no
	// different from the case where it's trying to acquire a fresh lock.
	return str <= heldMode.Strength, nil
}

// conflictsWithLockHolders returns true if the request, referenced by the
// supplied lockTableGuardImpl, conflicts with any of the locks held on this
// key. Non-conflicting requests are allowed to proceed; conflicting requests
// must actively wait for the lock to be released.
//
// Locks held by transactions that are known to be finalized are considered
// non-conflicting. However, the caller may be responsible for cleaning them up
// before proceeding.
//
// REQUIRES: kl.mu is locked.
// REQUIRES: the transaction, to which the request belongs, should not hold the
// lock with the same or stronger lock strength.
func (kl *keyLocks) conflictsWithLockHolders(g *lockTableGuardImpl) bool {
	if !kl.isLocked() {
		return false // the lock isn't held; no conflict to speak of
	}

	g.mu.Lock()
	str, present := g.mu.locks[kl]
	g.mu.Unlock()
	alsoLocksWithHigherStrength := present && str > g.curStrength()
	if alsoLocksWithHigherStrength {
		// If the request already has this lock in its locks map, it must also be
		// trying to acquire this lock at a higher strength. For it to be here, it
		// must have a (possibly joint) claim on this lock. The claim may have
		// been broken since, but that's besides the point -- we defer to the
		// stronger lock strength and continue with our scan.
		//
		// NB: If we were to not defer to the stronger lock strength and start
		// waiting here, we could potentially end up doing so in the wrong wait
		// queue (queuedReaders vs. queuedLockingRequests). There wouldn't be a
		// correctness issue in doing so, but it isn't ideal.
		//
		// NB: Non-transactional requests do not make claims or acquire locks.
		// They can only perform reads or writes, which means they can only have
		// lock spans with strength {None,Intent}. However, because they cannot
		// make claims on locks, we can not detect a key is being accessed with
		// both None and Intent locking strengths, like we can for transactional
		// requests. In some rare cases, the lock may now be held at a timestamp
		// that is not compatible with this request, and it will wait here --
		// there's no correctness issue in doing so.
		return false
	}

	for e := kl.holders.Front(); e != nil; e = e.Next() {
		tl := e.Value
		lockHolderTxn := tl.getLockHolderTxn()

		if g.isSameTxn(lockHolderTxn) {
			// We should never get here if the lock is already held by another request
			// from the same transaction with sufficient strength (read: greater than
			// or equal to what this guy wants); this should already be checked in
			// alreadyHoldsLockAndIsAllowedToProceed.
			assert(g.curStrength() > tl.getLockMode().Strength,
				"lock already held by the request's transaction with sufficient strength")
			continue // no conflict with a lock held by our own transaction
		}

		finalizedTxn, ok := g.lt.txnStatusCache.finalizedTxns.get(lockHolderTxn.ID)
		if ok {
			up := roachpb.MakeLockUpdate(finalizedTxn, roachpb.Span{Key: kl.key})
			// The lock belongs to a finalized transaction. There's no conflict, but
			// the lock must be resolved -- accumulate it on the appropriate slice.
			if !tl.isHeldReplicated() { // only held unreplicated
				g.toResolveUnreplicated = append(g.toResolveUnreplicated, up)
			} else {
				g.toResolve = append(g.toResolve, up)
			}
			continue // check next lock
		}

		// The lock is held by a different, un-finalized transaction.

		if g.curStrength() == lock.None {
			// If the non-locking reader is reading at a higher timestamp than the
			// lock holder, but it knows that the lock holder has been pushed above
			// its read timestamp, it can proceed after rewriting the lock at its
			// transaction's pushed timestamp. Intent resolution can be deferred to
			// maximize batching opportunities.
			//
			// This fast-path is only enabled for readers without uncertainty
			// intervals, as readers with uncertainty intervals must contend with the
			// possibility of pushing a conflicting intent up into their uncertainty
			// interval and causing more work for themselves, which is avoided with
			// care by the lockTableWaiter but difficult to coordinate through the
			// txnStatusCache. This limitation is acceptable because the most
			// important case here is optimizing the Export requests issued by backup.
			if !g.hasUncertaintyInterval() && g.lt.batchPushedLockResolution() {
				pushedTxn, ok := g.lt.txnStatusCache.pendingTxns.get(lockHolderTxn.ID)
				if ok && g.ts.Less(pushedTxn.WriteTimestamp) {
					up := roachpb.MakeLockUpdate(pushedTxn, roachpb.Span{Key: kl.key})
					if !tl.isHeldReplicated() {
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
					continue // check next lock
				}
			}
		}

		// The held lock neither belongs to the request's transaction (which has
		// special handling above) nor to a transaction that has been finalized.
		// Check for conflicts.
		if lock.Conflicts(tl.getLockMode(), g.curLockMode(), &g.lt.settings.SV) {
			return true
		}
	}
	return false
}

// maybeEnqueueNonLockingReadRequest enqueues a read request in the receiver's
// wait queue if the reader conflicts with the lock; otherwise, it's a no-op.
// A boolean is returned indicating whether the read request conflicted with
// the lock or not.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) maybeEnqueueNonLockingReadRequest(g *lockTableGuardImpl) (conflicts bool) {
	assert(g.curStrength() == lock.None, "unexpected locking strength; expected read")
	if !kl.conflictsWithLockHolders(g) {
		return false // no conflict, no need to enqueue
	}
	kl.waitingReaders.PushFront(g)
	g.mu.Lock()
	defer g.mu.Unlock()
	g.maybeAddToLocksMap(kl, lock.None)
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
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) enqueueLockingRequest(
	g *lockTableGuardImpl,
) (maxQueueLengthExceeded bool, _ error) {
	assert(g.curStrength() != lock.None, "should only be called with a locking request")
	g.mu.Lock()
	defer g.mu.Unlock()

	// First, check if the request is already in the queue. This can happen if
	// this function is called on behalf of a request that was previously was
	// an inactive waiter at this lock and comes back around.
	if _, inQueue := g.mu.locks[kl]; inQueue {
		// Find the request; it must already be in the correct position.
		for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
			qqg := e.Value
			if qqg.guard == g {
				qqg.active = true // set the active status as true, in case it wasn't before
				return false /* maxQueueLengthExceeded */, nil
			}
		}
		panic("lock table bug")
	}

	// Check if the lock's wait queue has room for one more request.
	if g.maxWaitQueueLength > 0 && kl.queuedLockingRequests.Len() >= g.maxWaitQueueLength {
		// The wait-queue is longer than the request is willing to wait for.
		// Instead of entering the queue, immediately reject the request. For
		// simplicity, we are not finding the position of this request in the
		// queue and rejecting the tail of the queue above the max length. That
		// would be more fair, but more complicated, and we expect that the
		// common case is that this waiter will be at the end of the queue.
		return true /* maxQueueLengthExceeded */, nil
	}

	if _, err := kl.insertLockingRequest(g, g.curStrength()); err != nil {
		return false, err
	}
	return false /* maxQueueLengthExceeded */, nil
}

// insertLockingRequest inserts the locking request, trying to access the lock
// with the supplied strength, at the correct position in the lock's wait queue.
// The request is wrapped in a queuedGuard to insert it into the queue,
// the reference for which is returned to the caller.
//
// Note that the request should not already be present in the lock's wait queue.
//
// REQUIRES: kl.mu to be locked.
// REQUIRES: g.mu to be locked.
//
// TODO(arul): we can change this function to no longer return an error once
// lock promotion is allowed.
func (kl *keyLocks) insertLockingRequest(
	g *lockTableGuardImpl, accessStrength lock.Strength,
) (*queuedGuard, error) {
	assert(accessStrength != lock.None, "should only be called with a locking request")
	_, inQueue := g.mu.locks[kl]
	assert(!inQueue, "should not be called with a locking request already in the queue")

	qg := &queuedGuard{
		guard:  g,
		mode:   makeLockMode(accessStrength, g.txnMeta(), g.ts),
		active: true,
		order:  makeQueueOrder(g, kl),
	}
	// The request isn't in the queue. Add it in the correct position, based on
	// its queueOrder.
	var e *list.Element[*queuedGuard]
	// TODO(arul): Once we've addressed lifted the limitation on lock promotion
	// (https://github.com/cockroachdb/cockroach/issues/110435), we should switch
	// this loop to iterate from the back of the queue -- it's more likely that
	// new requests are added to the back of the queue.
	for e = kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		qqg := e.Value
		if qqg.order.after(qg.order) {
			break
		}
	}
	if e == nil {
		kl.queuedLockingRequests.PushBack(qg)
	} else {
		kl.queuedLockingRequests.InsertBefore(qg, e)
	}
	added := g.maybeAddToLocksMap(kl, accessStrength)
	assert(added, "should not have been present before")
	return qg, nil
}

// shouldRequestActivelyWait returns true iff the supplied request needs to
// actively wait on the receiver.
//
// REQUIRES: kl.mu to be locked.
// REQUIRES: g.mu to be locked.
func (kl *keyLocks) shouldRequestActivelyWait(g *lockTableGuardImpl) bool {
	if g.curStrength() == lock.None {
		return true // non-locking read requests always actively wait
	}

	if kl.conflictsWithLockHolders(g) {
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
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		qqg := e.Value
		if qqg.guard == g {
			// We found our request while scanning from the front without finding any
			// conflicting waiters; no need to actively wait here.
			return false
		}
		if g.txn != nil && qqg.guard.isSameTxn(g.txnMeta()) {
			continue // no need to check for conflicts with requests that belong to our own transaction
		}
		if lock.Conflicts(qqg.mode, g.curLockMode(), &g.lt.settings.SV) {
			return true
		}
	}
	panic("lock table bug: enqueued request not found")
}

// claimBeforeProceeding adjusts datastructures on the receiver such that the
// supplied request lays a claim[1] on it before proceeding[2]. This method
// should only be called by locking requests.
//
// [1] Only transactional, locking requests can establish claims.
// Non-transactional writers cannot.
// [2] While non-transactional writers cannot establish claims, they do need to
// be removed from the receiver's wait queue before proceeding. We do that here.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) claimBeforeProceeding(g *lockTableGuardImpl) {
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
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		qqg := e.Value
		if qqg.guard == g {
			if g.txn == nil {
				// Non-transactional writer.
				g.mu.Lock()
				delete(g.mu.locks, kl)
				g.mu.Unlock()
				kl.queuedLockingRequests.Remove(e)
			} else {
				// Transactional locking request.
				qqg.active = false // claim the lock
			}
			return
		}
	}
	panic("lock table bug: did not find enqueued request")
}

func (kl *keyLocks) isNonConflictingLock(g *lockTableGuardImpl) bool {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	// It is possible that this lock is empty and has not yet been deleted.
	if kl.isEmptyLock() {
		return true
	}
	// Lock is not empty.
	if !kl.isLocked() {
		// If the lock is neither empty nor held it must be the case that
		// another transaction has claimed the lock. Locks that have been
		// claimed, but have not been acquired yet, are considered
		// non-conflicting. Optimistic evaluation may call into this function
		// with or without holding latches. It's worth considering both these
		// cases separately:
		//
		// 1. If Optimistic evaluation is holding latches, then there cannot be
		// a conflicting request that has claimed (but not acquired) the lock
		// that is also holding latches. A request could have claimed this lock,
		// discovered a different lock, and dropped its latches before waiting
		// in this second lock's wait queue. In such cases, the request that
		// claimed this lock will have to re-acquire and re-scan the lock table
		// after this optimistic evaluation request drops its latches.
		//
		// 2. If optimistic evaluation does not hold latches, then it will check
		// for conflicting latches before declaring success. A request that
		// claimed this lock, did not discover any other locks, and proceeded to
		// evaluation would thus conflict on latching with our request going
		// through optimistic evaluation. This will be detected, and the request
		// will have to retry pessimistically.
		// All this is to say that if we found a claimed, but not yet acquired
		// lock, we can treat it as non-conflicting. It'll either be detected as
		// a true conflict when we check for conflicting latches, or the request
		// that claimed the lock will know what happened and what to do about
		// it.
		return true
	}

	for e := kl.holders.Front(); e != nil; e = e.Next() {
		tl := e.Value
		if g.isSameTxn(tl.txn) {
			// NB: Unlike the pessimistic (normal) evaluation code path, we do
			// not need to check the lock's strength if it is already held by
			// this transaction -- it's non-conflicting. There's two cases to
			// consider:
			//
			// 1. If the lock is held with the same/higher lock strength on this
			// key then this optimistic evaluation attempt already has all the
			// protection it needs.
			//
			// 2. If the lock is held with a weaker lock strength other
			// transactions may be able to acquire a lock on this key that
			// conflicts with this optimistic evaluation attempt. This is okay,
			// as we'll detect such cases as we continue our iteration here --
			// however, the weaker lock in itself is not conflicting with the
			// optimistic evaluation attempt.
			continue
		}
		if lock.Conflicts(tl.getLockMode(), g.curLockMode(), &g.lt.settings.SV) {
			return false // is not non-conflicting
		}
	}
	return true // non-conflicting
}

// Acquires this lock. Any requests that are waiting in the lock's wait queues
// from the transaction acquiring the lock are also released.
//
// Acquires l.mu.
func (kl *keyLocks) acquireLock(
	acq *roachpb.LockAcquisition, clock *hlc.Clock, st *cluster.Settings,
) error {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	if kl.isLockedBy(acq.Txn.ID) {
		// Already held.
		e, found := kl.heldBy[acq.Txn.ID]
		assert(found, "expected to find lock held by the transaction")
		tl := e.Value
		beforeTs := tl.writeTS()
		err := tl.reacquireLock(acq)
		if err != nil {
			return err
		}
		afterTs := tl.writeTS()
		if beforeTs.Less(afterTs) {
			// If the lock's timestamp has increased as a result of this lock
			// acquisition, the queue of waiting readers might need to be recomputed.
			kl.recomputeWaitQueues(st)
		}
		return nil
	}

	// NB: The key isn't locked, so the request trying to acquire the lock must
	// be an (inactive) request in the receiver's queuedLockingRequests wait
	// queue. Typically, the request has a (possibly joint) claim on the key.
	// However, in some rare cases[1], this may not be true -- i.e. the request
	// may have had its claim broken by a different request being sequenced
	// through the lock table. This does not violate any correctness properties,
	// because the request must be holding latches to have proceeded to evaluation
	// and finally ended up here. That means this request is isolated from the
	// request that broke its claim by latching.
	//
	// [1] Requests that run into locks drop their latches and enter its wait
	// queues. Once the lock is released, and they can proceed with their scan,
	// they do so without re-acquiring latches. So it is possible for requests to
	// break claims of requests that hold latches without holding latches
	// themselves.

	// NB: The lock may have been acquired with a lower strength than the strength
	// with which other requests from the (now) lock holder transaction are trying
	// to access this key. Put another way, some requests may now lock promoters
	// as a result of this lock acquisition. Instead of marking them as inactive,
	// we simply release them from the lock's wait queue and have them re-scan.
	// This ensures they enqueue in the correct place (promoters get preference)
	// and correctly determine whether they need to wait or not at this key.
	//
	// TODO(arul): Replace the call to releaseLockingRequestsFromTxn with
	// recomputeWaitQueue, and push the responsibility of re-ordering the wait
	// queue into there. Ditto for the call in addDiscoveredLock.
	kl.releaseLockingRequestsFromTxn(&acq.Txn)

	// Sanity check that there aren't any waiting readers on this lock. There
	// shouldn't be any, as the lock wasn't held.
	if kl.waitingReaders.Len() > 0 {
		panic("lockTable bug")
	}

	// TODO(arul): add a test for unreplicated locks as well where this assertion
	// is triggered.
	if buildutil.CrdbTestBuild && kl.isLocked() {
		// If lock(s) are already held on this key by other transactions, sanity
		// check that the lock acquisition is compatible.
		m := makeLockMode(acq.Strength, &acq.Txn, acq.Txn.WriteTimestamp)
		if err := kl.testingAssertCompatibleLockMode(m, &acq.Txn, st); err != nil {
			return err
		}
	}

	tl := newTxnLock(&acq.Txn, clock)
	switch acq.Durability {
	case lock.Unreplicated:
		tl.unreplicatedInfo.ts = acq.Txn.WriteTimestamp
		if err := tl.unreplicatedInfo.acquire(acq.Strength, acq.Txn.Sequence, acq.IgnoredSeqNums); err != nil {
			return err
		}
	case lock.Replicated:
		tl.replicatedInfo.acquire(acq.Strength, acq.Txn.WriteTimestamp)
	default:
		panic(fmt.Sprintf("unknown lock durability: %s", acq.Durability))
	}
	// Update the tracking to include this transaction's lock.
	kl.lockAcquiredOrDiscovered(tl)
	// We may need to recompute wait queues after removing locking requests that
	// belong to the transaction that acquired the lock
	// (releaseLockingRequestsFromTxn), as doing so could allow requests that were
	// actively waiting previously to now proceed. Such cases are rare while lock
	// promotion is disallowed, but still possible.
	//
	// recomputeWaitQueues will also call informActiveWaiters, letting them know
	// that the lock has transitioned to held. In cases where the lock is acquired
	// by a different transaction than the one which holds the claim, this call to
	// informActiveWaiters will also cause them to push the new lock holder.
	kl.recomputeWaitQueues(st)
	return nil
}

// discoveredLock is called with a lock that is discovered by guard g when trying
// to access this key with strength accessStrength.
//
// Acquires kl.mu.
func (kl *keyLocks) discoveredLock(
	foundLock *roachpb.Lock,
	g *lockTableGuardImpl,
	accessStrength lock.Strength,
	notRemovable bool,
	clock *hlc.Clock,
) error {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	if notRemovable {
		kl.notRemovable++
	}

	var tl *txnLock
	if kl.isLockedBy(foundLock.Txn.ID) {
		e := kl.heldBy[foundLock.Txn.ID]
		tl = e.Value
		tl.replicatedInfo.acquire(foundLock.Strength, foundLock.Txn.WriteTimestamp)
		// TODO(arul): If the discovered lock indicates a newer epoch than what's
		// being tracked, should we clear out unreplicatedLockInfo here?
	} else {
		if buildutil.CrdbTestBuild {
			// If lock(s) are already held on this key by other transactions, sanity
			// check that the discovered lock is compatible with them.
			m := makeLockMode(foundLock.Strength, &foundLock.Txn, foundLock.Txn.WriteTimestamp)
			if err := kl.testingAssertCompatibleLockMode(m, &foundLock.Txn, g.lt.settings); err != nil {
				return err
			}
		}
		// The lock is compatible with any locks that may already be held on this
		// key. We can start tracking it.
		tl = newTxnLock(&foundLock.Txn, clock)
		kl.lockAcquiredOrDiscovered(tl)
	}

	if tl.replicatedInfo.isEmpty() {
		tl.replicatedInfo.acquire(foundLock.Strength, foundLock.Txn.WriteTimestamp)
	}

	if accessStrength == lock.None {
		// Don't enter the lock's queuedReaders list, because all queued readers
		// are expected to be active. Instead, wait until the next scan.

		// Confirm that the guard will wait on the lock the next time it scans
		// the lock table. If not then it shouldn't have discovered the lock in
		// the first place. Bugs here would cause infinite loops where the same
		// lock is repeatedly re-discovered.
		if foundLock.Strength != lock.Intent || g.ts.Less(foundLock.Txn.WriteTimestamp) {
			return errors.AssertionFailedf("discovered non-conflicting lock")
		}
	} else {
		// Immediately enter the lock's queuedLockingRequests list.
		// NB: this inactive waiter can be non-transactional.
		g.mu.Lock()
		_, inQueue := g.mu.locks[kl]
		if !inQueue {
			// We weren't previously waiting in this lock's wait queue, so we need to
			// add the request to the list of queuedLockingRequests as an inactive
			// waiter.
			qg, err := kl.insertLockingRequest(g, accessStrength)
			if err != nil {
				g.mu.Unlock()
				return err
			}
			qg.active = false
		}
		g.mu.Unlock()
	}

	// If there are waiting requests from the same txn, they no longer need to wait.
	kl.releaseLockingRequestsFromTxn(&foundLock.Txn)

	// Active waiters need to be told about who they are waiting for.
	kl.informActiveWaiters()
	return nil
}

func (kl *keyLocks) decrementNotRemovable() {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	kl.notRemovable--
	if kl.notRemovable < 0 {
		panic(fmt.Sprintf("keyLocks.notRemovable is negative: %d", kl.notRemovable))
	}
}

// Acquires l.mu.
func (kl *keyLocks) tryClearLock(force bool) bool {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	if kl.notRemovable > 0 && !force {
		return false
	}

	// Clear lock holder. While doing so, construct the closure used to transition
	// waiters.
	replicatedHeld, replicatedLockHolderTxn := kl.isAnyLockHeldReplicated()
	transitionWaiter := func(g *lockTableGuardImpl) {
		if replicatedHeld && !force {
			// Note that none of the current waiters can be requests from
			// lockHolderTxn, so they will never be told to waitElsewhere on
			// themselves.
			waitState := waitingState{
				kind: waitElsewhere,
				txn:  replicatedLockHolderTxn,
				key:  kl.key,
				held: true,
			}
			g.updateWaitingStateLocked(waitState)
		} else {
			// !replicatedHeld || force. Both are handled as doneWaiting since the
			// system is no longer tracking the lock that was possibly held.
			g.updateStateToDoneWaitingLocked()
		}
	}
	kl.clearAllLockHolders()

	// Clear waitingReaders.
	for e := kl.waitingReaders.Front(); e != nil; {
		g := e.Value
		curr := e
		e = e.Next()
		kl.waitingReaders.Remove(curr)

		g.mu.Lock()
		transitionWaiter(g)
		g.notify()
		delete(g.mu.locks, kl)
		g.mu.Unlock()
	}

	// Clear queuedLockingRequests.
	for e := kl.queuedLockingRequests.Front(); e != nil; {
		qg := e.Value
		curr := e
		e = e.Next()
		kl.queuedLockingRequests.Remove(curr)

		g := qg.guard
		g.mu.Lock()
		if qg.active {
			transitionWaiter(g)
			g.notify()
		}
		delete(g.mu.locks, kl)
		g.mu.Unlock()
	}

	// The keyLocks struct must now be empty.
	kl.assertEmptyLock()
	return true
}

// Tries to update the lock: noop if this lock is held by a different
// transaction, else the lock is updated. Returns whether the keyLocks struct
// can be garbage collected, and whether it was held by the txn.
// Acquires l.mu.
func (kl *keyLocks) tryUpdateLock(
	up *roachpb.LockUpdate, st *cluster.Settings,
) (heldByTxn, gc bool) {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	return kl.tryUpdateLockLocked(*up, st)
}

// REQUIRES: kl.mu is locked.
func (kl *keyLocks) tryUpdateLockLocked(
	up roachpb.LockUpdate, st *cluster.Settings,
) (heldByTxn, gc bool) {
	if kl.isEmptyLock() {
		// Already free. This can happen when an unreplicated lock is removed in
		// tryActiveWait due to the txn being in the txnStatusCache.
		return false, true
	}
	if !kl.isLockedBy(up.Txn.ID) {
		return false, false
	}
	if up.Status.IsFinalized() {
		kl.releaseLock(&up.Txn)
		if !kl.isLocked() {
			// The lock transitioned from held to unheld as a result of this lock
			// update.
			gc = kl.releaseWaitersOnKeyUnlocked()
		} else {
			// If we're in this branch, it must be the case that there were multiple
			// shared locks held on this key.
			//
			// [1] Releasing one of the locks hasn't transitioned the key to unlocked.
			// However, the lock we just released might have been the one some of the
			// waiters were pushing, in which case they need to push a new guy.
			//
			// [2] Moreover, any requests that belong to a transaction that holds a
			// shared lock that were waiting to promote their lock to something
			// stronger might be able to do so now.
			//
			// As such, we need to recompute the wait queue for the lock. However, [2]
			// is only possible if there's only one shared lock left on this key.
			// Otherwise, the promoting request (if present) will conflict with the
			// other shared lock. Moreover, for [1], simply calling
			// informActiveWaiters will do. As recomputeWaitQueues is O(number of lock
			// holder), we can optimize this to the if condition below:
			if kl.holders.Len() == 1 {
				kl.recomputeWaitQueues(st)
			} else {
				kl.informActiveWaiters()
			}
		}
		return true, gc
	}

	e := kl.heldBy[up.Txn.ID]
	tl := e.Value
	txn := &up.Txn
	ts := up.Txn.WriteTimestamp
	beforeTs := tl.writeTS()
	beforeStr := tl.getLockMode().Strength
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
	if !tl.replicatedInfo.isEmpty() {
		tl.replicatedInfo.clear()
	}
	// However, for unreplicated locks, the lock table is the source of truth.
	// As such, we best-effort mirror the behavior of mvccResolveWriteIntent().
	if tl.isHeldUnreplicated() {
		switch {
		//...update corresponds to a higher epoch.
		case txn.Epoch > tl.txn.Epoch:
			// Forget what was tracked previously.
			tl.unreplicatedInfo.clear()

			// ...update corresponds to the current epoch.
		case txn.Epoch == tl.txn.Epoch:
			tl.unreplicatedInfo.rollbackIgnoredSeqNumbers(up.IgnoredSeqNums)
			// Check if the lock is still held after rolling back ignored sequence
			// numbers.
			held := false
			for _, str := range unreplicatedHolderStrengths {
				if tl.unreplicatedInfo.held(str) {
					held = true
					break
				}
			}
			if !held {
				tl.unreplicatedInfo.clear()
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
				tl.txn = txn
				tl.unreplicatedInfo.ts = ts
			}
			isLocked = true

			// ...update corresponds to an older epoch of the transaction.
		case txn.Epoch < tl.txn.Epoch:
			if advancedTs {
				// We may advance ts here but not update the holder.txn object below for
				// the reason stated in the comment about mvccResolveWriteIntent(). The
				// {unreplicated,replicated}LockHolderInfo.ts is the source of truth
				// regarding the timestamp of the lock, and not TxnMeta.WriteTimestamp.
				tl.unreplicatedInfo.ts = ts
			}
			isLocked = true
		default:
			panic("unreachable")
		}
	}

	if !isLocked {
		kl.releaseLock(txn)
		if !kl.isLocked() {
			gc = kl.releaseWaitersOnKeyUnlocked()
		} else {
			// See the handling of finalized transactions that release a shared lock
			// above for an explanation. Releasing a lock due to ignored sequence
			// numbers is no different than releasing a lock because the holder's
			// transaction has been finalized.
			if kl.holders.Len() == 1 {
				kl.recomputeWaitQueues(st)
			} else {
				kl.informActiveWaiters()
			}
		}
		return true, gc
	}

	afterStr := tl.getLockMode().Strength

	if beforeStr != afterStr || advancedTs {
		kl.recomputeWaitQueues(st)
	}
	// Else no change for waiters. This can happen due to a race between different
	// callers of UpdateLocks().

	return true, false
}

// recomputeWaitQueues goes through the receiver's wait queues and recomputes
// whether actively waiting requests should continue to do so, given the key's
// locks holders and other waiting requests. A non-exhaustive list scenarios where
// such computation is necessary is:
// - A lock's strength has decreased[1]
// - Or locking requests have dropped out of wait queue's[2] without actually
// acquiring the lock
// - Or the lock's timestamp has advanced
// - Or one of the locks on a key has been released[3]
//
// [1] This can happen as a result of savepoint rollback or when the lock table
// stops tracking a replicated lock because of a PUSH_TIMESTAMP that
// successfully bumps the pushee's timestamp.
// [2] A locking request that doesn't conflict with any held lock(s) may still
// have to actively wait if it conflicts with a lower sequence numbered request
// already in the lock's wait queue. Locking requests dropping out of a lock's
// wait queue can therefore result in other requests no longer needing to
// actively wait.
// [3] If there are multiple shared locks on a key and on of the transactions
// that holds the shared lock is trying to promote its strength to something
// stronger, then the release of a shared lock may allow it to do so. Note that
// this can only happen if there's one shared lock (the one belonging to the
// promoting transaction) left.
//
// TODO(arul): We could optimize this function if we had information about the
// context it was being called in.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) recomputeWaitQueues(st *cluster.Settings) {
	// Determine and maintain strongest mode for this key. Note that this logic is
	// generalized for Update locks already -- things could be tightened if we
	// only considered None, Shared, Exclusive, and Intent locking strengths
	// (which are the only ones that exist at the time of writing).
	var strongestMode lock.Mode
	var lockHolderTxn *enginepb.TxnMeta // nil if {no,more than one} locks are held
	// Go through the list of lock holders.
	// TODO(arul): We should annotate each of the holders to reflect if the lock
	// belongs to a finalized transaction or not. If it does, we should exclude
	// it when computing strongestLockMode.
	for e := kl.holders.Front(); e != nil; e = e.Next() {
		mode := e.Value.getLockMode()
		if strongestMode.Empty() {
			lockHolderTxn = e.Value.txn
		} else {
			lockHolderTxn = nil
		}
		if strongestMode.Weaker(mode) {
			strongestMode = mode
		}
	}

	// Go through the list of non-locking readers (all which are actively waiting,
	// by definition) and check if any of them no longer conflict with the lock
	// holder(s).
	for e := kl.waitingReaders.Front(); e != nil; {
		reader := e.Value
		curr := e
		e = e.Next()
		if !lock.Conflicts(reader.curLockMode(), strongestMode, &st.SV) {
			kl.removeReader(curr)
		}
	}

	// Go through the list of locking requests and check if any that are actively
	// waiting no longer need to do so. We start the iteration from the front of
	// the linked list as requests are stored in increasing sequence number order.
	// Moreover, as locking requests conflict with both a lock's holder(s) and
	// lower sequence numbered requests waiting in the queue, we must update
	// strongestMode as we go along.
	for e := kl.queuedLockingRequests.Front(); e != nil; {
		qlr := e.Value
		curr := e
		e = e.Next()
		belongsToOnlyLockHolder := lockHolderTxn != nil && qlr.guard.isSameTxn(lockHolderTxn)
		conflictsWithEarlier := lock.Conflicts(qlr.mode, strongestMode, &st.SV) &&
			!belongsToOnlyLockHolder
		if conflictsWithEarlier {
			break
		}
		removed := false
		if qlr.guard.txn == nil {
			// We're dealing with a queued non-transactional locking request that no
			// longer conflicts with locks/claims on this key. As non-transactional
			// requests do not acquire claims, the request can be removed from the
			// wait queue.
			kl.removeLockingRequest(curr)
			removed = true
		} else if qlr.active {
			// A transactional locking request that no longer conflicts with
			// locks/claims on this key but is actively waiting. It no longer needs to
			// do so, but before it can proceed, it must acquire a (possibly joint)
			// claim. It does so by marking itself as inactive.
			qlr.active = false // mark as inactive
			qlr.guard.mu.Lock()
			qlr.guard.doneActivelyWaitingAtLock()
			qlr.guard.mu.Unlock()
		}

		// Locking requests conflict with both the lock holder(s) and other lower
		// sequence numbered locking requests in the lock's wait queue, so we may
		// need to update strongestMode before moving on with our iteration.
		if !removed && strongestMode.Weaker(qlr.mode) {
			strongestMode = qlr.mode
		}
	}
	kl.informActiveWaiters()
}

// removeLockingRequest removes the locking request (or non-transactional
// writer), referenced by the supplied list.Element, from the lock's
// queuedLockingRequests list.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) removeLockingRequest(e *list.Element[*queuedGuard]) {
	qg := e.Value
	g := qg.guard
	kl.queuedLockingRequests.Remove(e)
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.mu.locks, kl)
	if qg.active {
		g.doneActivelyWaitingAtLock()
	}
}

// removeReader removes the reader, referenced by the supplied list.Element,
// from the lock's queuedReaders list.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) removeReader(e *list.Element[*lockTableGuardImpl]) {
	g := e.Value
	kl.waitingReaders.Remove(e)
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.mu.locks, kl)
	g.doneActivelyWaitingAtLock()
}

// A request known to the receiver is done. The request could be a locking or
// non-locking request. Note that there is the possibility of a race and the g
// may no longer be known to l, which we treat as a noop (this race is allowed
// since we order l.mu > g.mu). Returns whether the keyLocks struct can be
// garbage collected.
//
// Acquires l.mu.
func (kl *keyLocks) requestDone(g *lockTableGuardImpl) (gc bool) {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	g.mu.Lock()
	// TODO(arul): Now that we're storing the strength in g.mu.locks, we don't
	// have to look at both queuedLockingRequests and waitingReaders below -- we
	// should decide based off the strength instead.
	if _, present := g.mu.locks[kl]; !present {
		g.mu.Unlock()
		return false
	}
	delete(g.mu.locks, kl)
	g.mu.Unlock()

	// May be in queuedLockingRequests or waitingReaders.
	doneRemoval := false
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		qg := e.Value
		if qg.guard == g {
			kl.queuedLockingRequests.Remove(e)
			doneRemoval = true
			break
		}
	}

	if doneRemoval {
		// If a locking request is removed from the receiver's wait queues without
		// actually acquiring a lock other locking requests may be able to proceed.
		// This is because locking requests must actively wait if they conflict with
		// either the lock holder or any lower sequence numbered locking requests.
		// In cases where request(s) were waiting just because they conflicted with
		// the request we just removed, and they are compatible with all lock
		// holders (if any), we need to let them go. A call to recomputeWaitQueues
		// will do exactly that, and will be a no-op otherwise.
		kl.recomputeWaitQueues(g.lt.settings)
	}

	if !doneRemoval {
		for e := kl.waitingReaders.Front(); e != nil; e = e.Next() {
			gg := e.Value
			if gg == g {
				kl.waitingReaders.Remove(e)
				doneRemoval = true
				break
			}
		}
	}
	if !doneRemoval {
		panic("lockTable bug")
	}
	return kl.isEmptyLock()
}

// tryFreeLockOnReplicatedAcquire attempts to free a lock on a write-uncontended
// key during a re-acquisition. The lock is free-ed only if the replicated lock
// provides sufficient protection, whereby tracking the replicated lock is
// pointless.
//
// A boolean indicating whether the lock was free-ed or not is returned to the
// caller. If it was free-ed, the caller should not re-add the replicate lock
// back to the lock table. Additionally, if the last lock on a key was cleared,
// it's the caller's responsibility to remove the receiver from the lock table's
// tree. A boolean, mustGC, is returned true in this case.
//
// Historically, this served as a mitigation for #49973, prior to the
// introduction of optimistic evaluation. The benefits of keeping this scheme
// around today are less pronounced, and may be re-evaluated.
//
// Acquires kl.mu.
func (kl *keyLocks) tryFreeLockOnReplicatedAcquire(
	acq *roachpb.LockAcquisition,
) (freed bool, mustGC bool) {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	if !kl.isLockedBy(acq.Txn.ID) {
		// The lock isn't held by the transaction that's acquiring the replicated
		// lock. There's no opportunity to free anything up.
		return false /* freed */, false /* mustGC */
	}

	// Bail if the lock has waiting locking requests from other transactions[*].
	// It isn't uncontended, so we can't free it, otherwise:
	//
	// 1. EITHER the request would proceed to evaluation and re-discover the
	// (newly acquired) replicated lock and re-add it to the lock table,
	// performing wasted work.
	// 2. OR it would wait on a different lock and give up a (potential) claim on
	// this one, thereby allowing other requests to potentially barge in front of
	// it once this lock is actually released.
	//
	// [*] It's possible there are queued locking requests belonging to the lock
	// holder's transaction if it is trying to promote its lock.
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		if e.Value.guard.isSameTxn(&acq.Txn) {
			continue
		}
		return false /* freed */, false /* mustGC */
	}

	tl := kl.heldBy[acq.Txn.ID].Value

	// Bail if the transaction doesn't hold just an unreplicated lock -- there's
	// no potential to forget anything here.
	if !tl.isHeldUnreplicated() || tl.isHeldReplicated() {
		return false /* freed */, false /* mustGC */
	}

	// Bail if there's an epoch number mismatch, as we can't compare sequence
	// numbers across epochs. Note that we're not making any effort to forget
	// unreplicated locks if the replicated lock acquisition corresponds to a
	// newer epoch -- we defer to acquireLock to handle epcoh bumps instead.
	if tl.txn.Epoch != acq.Txn.Epoch {
		return false /* freed */, false /* mustGC */
	}

	// The transaction is trying to re-acquire a replicated lock on this key, and
	// it holds an unreplicated lock that we may be able to forget as the key is
	// uncontended. However, to do so, the replicated lock must provide enough
	// protection to subsume the unreplicated lock(s). Check that.
	canFree := true
	for _, str := range unreplicatedHolderStrengths {
		if str > acq.Strength && tl.unreplicatedInfo.held(str) {
			// The replicated lock isn't of sufficient lock strength.
			canFree = false
			break
		}
		// NB: Strictly speaking, we should also be checking if the sequence number
		// of the unreplicated lock we're about to forget here is less than or equal
		// to the replicated lock acquisition. Otherwise, a savepoint rollback could
		// cause us to roll back the replicated lock (in favor of which we're
		// deciding to no longer track this unreplicated lock). However, this would
		// mean that the common case of unreplicated locks being re-acquired as
		// replicated ones won't be able to take advantage of this optimization
		// (think SELECT FOR UPDATE or implicit exclusive locks acquired as part of
		// UPDATE statements). We thus decide to ignore sequence numbers in our
		// determination here -- savepoint rollbacks are rare enough. It's also
		// not like unreplicated locks can't be lost for other reasons.
		//
		// TODO(ssd): Any change to this behaviour must consider IgnoredSeqNumbers
		// and the case of the unreplicated locks being flushed. See the
		// discussion on #140113.
	}
	if !canFree {
		// The replicated lock acquisition doesn't offer sufficient protection
		return false /* freed */, false /* mustGC */
	}

	kl.releaseLock(&acq.Txn)
	if kl.isLocked() {
		return true /* freed */, false /* mustGC */
	}

	// If this was the only transaction that held the lock, the lock is now
	// forgotten, and the key is considered unlocked. All waiting readers[*] will
	// be released. However, at this point, there may still be
	// queuedLockingRequests on this lock that belong to the lock holder
	// transaction. This is only possible when the transaction is trying to
	// promote its lock, as we don't add the request in the lock's wait queue in
	// non lock promotion cases.
	//
	// As a consequence of the above, we won't GC locks if this replicated lock
	// acquisition (and freeing) corresponds to lock promotion. That's fine, as
	// the lock will be GC-ed once the promoting request finishes evaluation and
	// calls Dequeue().
	//
	// [*] Waiting readers are only possible if exclusive locks block non-locking
	// reads.
	mustGC = kl.releaseWaitersOnKeyUnlocked()
	return true /* freed */, mustGC
}

// releaseWaitersOnKeyUnlocked is called when the key, referenced in the
// receiver, transitions from locked to unlocked to handle state transitions for
// waitingReaders and queuedLockingRequests; if this results in there no longer
// being waiters on this key (read: the receiver is empty), a boolean gc=true is
// returned, indicating the receiver can be GC-ed by the caller.
//
// REQUIRES: kl.mu is locked.
func (kl *keyLocks) releaseWaitersOnKeyUnlocked() (gc bool) {
	assert(!kl.isLocked(), "releaseWaitersOnKeyUnlocked should only be called on unheld locks")

	// All waiting readers don't need to wait here anymore.
	// NB: all waiting readers are by definition active waiters.
	for e := kl.waitingReaders.Front(); e != nil; {
		curr := e
		e = e.Next()
		kl.removeReader(curr)
	}

	kl.maybeReleaseCompatibleLockingRequests()

	// We've already cleared waiting readers above. The lock can be released if
	// there are no waiting locking requests, active or otherwise.
	if kl.queuedLockingRequests.Len() == 0 {
		kl.assertEmptyLock()
		return true
	}
	return false
}

// maybeReleaseCompatibleLockingRequests goes through the list of locking
// requests waiting in the receiver's wait queue and releases all requests from
// the head of the queue that are compatible with each other. Releasing[1] a
// locking request entails marking it as inactive and nudging it by calling
// notify(). The released request(s) are said to have established a (possibly
// joint) claim.
//
// Any non-transactional writers at the head of the queue are also released.
//
// [1] If the request is not actively waiting in the lock wait queue, it's a
// noop for the request.
//
// REQUIRES: kl.mu is locked.
// REQUIRES: the (receiver) lock must not be held.
// REQUIRES: there should not be any waitingReaders in the lock's wait queues.
//
// TODO(arul): There's a lot of overlap between this method and
// recomputeWaitQueues. We should simplify things by trying to replace all
// usages of this method with recomputeWaitQueues.
func (kl *keyLocks) maybeReleaseCompatibleLockingRequests() {
	if kl.isLocked() {
		panic("maybeReleaseCompatibleLockingRequests called when lock is held")
	}
	if kl.waitingReaders.Len() != 0 {
		panic("there cannot be waiting readers")
	}

	// The prefix of the queue that is non-transactional writers is done waiting.
	// As non-transactional writers have lock.Intent locking strength, they will
	// be incompatible with any (possibly joint) claim that transactional
	// request(s) will establish by the time we're done with this method. This
	// means we only need special case handling for non-transactional requests
	// just once -- for the ones that are at the head of the queue.
	for e := kl.queuedLockingRequests.Front(); e != nil; {
		qg := e.Value
		g := qg.guard
		if g.txn != nil { // (transactional) locking request
			break
		}
		curr := e
		e = e.Next()
		kl.removeLockingRequest(curr)
	}

	if kl.queuedLockingRequests.Len() == 0 {
		return // no locking requests
	}

	// Mark all compatible locking requests as inactive. While doing so, we check
	// if they were actively waiting at this key -- if they were, and we
	// transitioned them to inactive, a call to doneActivelyWaitingAtLock should
	// nudge the request to pick up its scan from where it left off.

	var mode lock.Mode
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		qg := e.Value
		g := qg.guard

		if mode.Empty() {
			mode = qg.mode
		} else {
			if lock.Conflicts(mode, qg.mode, &qg.guard.lt.settings.SV) {
				break
			}
			// NB: Once we add support for UPDATE locking strength, this logic
			// will need to accumulate the strongest lock mode seen so far. For
			// example, consider the following:
			// waitQueue: [Shared, Update, Shared, Update, Exclusive]
			//
			// We want to release the first 3 requests (as they're compatible with
			// each other), not the first 4.
		}

		if qg.active {
			qg.active = false // mark as inactive
			g.mu.Lock()
			g.doneActivelyWaitingAtLock()
			g.mu.Unlock()
		}
		// Else the waiter is already inactive.
	}

	// Tell the active waiters who they are waiting for.
	kl.informActiveWaiters()
}

// testingAssertCompatibleLockMode ensures the supplied lock mode is compatible
// with all locks held on the receiver. Any locks held by the transaction itself
// are considered compatible; the supplied transaction meta is used for this
// determination.
//
// An error is returned if the supplied lock mode is incompatible. This check is
// expensive when there are multiple shared locks on a single key; we only
// expect it to be triggered by test builds.
//
// REQUIRES: kl.mu to be locked.
//
// TODO(arul): Once https://github.com/cockroachdb/cockroach/issues/108843 is
// addressed, we should pull this verification into the more general purpose
// verification.
func (kl *keyLocks) testingAssertCompatibleLockMode(
	m lock.Mode, txn *enginepb.TxnMeta, st *cluster.Settings,
) error {
	if buildutil.CrdbTestBuild {
		for e := kl.holders.Front(); e != nil; e = e.Next() {
			holder := e.Value
			holderMode := holder.getLockMode()
			// Holder belongs to a different transaction ...
			if holder.getLockHolderTxn().ID != txn.ID &&
				// ... which conflicts with the supplied lock mode.
				lock.Conflicts(holderMode, m, &st.SV) {
				return errors.AssertionFailedf(
					"incompatibility detected; lock by transaction %s with strength %s incompatible with an "+
						"already held lock by %s with strength %s",
					txn.ID, m.Strength, holder.txn.ID, holderMode.Strength,
				)
			}
		}
	}
	return nil
}

// verify asserts properties about all locks held on and waiters waiting on a
// given key. Verification is expensive, so it should only be done in testing
// builds.
//
// REQUIRES: kl.mu to be locked.
func (kl *keyLocks) verify(st *cluster.Settings) error {
	// 1. Ensure all lock holders are compatible with each other.
	for e1 := kl.holders.Front(); e1 != nil; e1 = e1.Next() {
		h1 := e1.Value
		if h1.getLockHolderTxn() == nil {
			return errors.AssertionFailedf("lock cannot be held by non-transactional request")
		}
		for e2 := kl.holders.Front(); e2 != nil; e2 = e2.Next() {
			h2 := e2.Value
			if h2.getLockHolderTxn() == nil {
				return errors.AssertionFailedf("lock cannot be held by non-transactional request")
			}
			if h1.getLockHolderTxn().ID == h2.getLockHolderTxn().ID {
				if h1 != h2 {
					return errors.AssertionFailedf(
						"same transaction should not be present twice in the holders list",
					)
				}
				continue // locks are compatible with themselves; nothing to check
			}
			if lock.Conflicts(h1.getLockMode(), h2.getLockMode(), &st.SV) {
				return errors.AssertionFailedf(
					"lock holders incompatible with each other; %s holds lock with "+
						"strength %s and %s holds lock with strength %s; lock: %s",
					h1.getLockHolderTxn(), h1.getLockMode().Strength,
					h2.getLockHolderTxn(), h2.getLockMode().Strength,
					kl,
				)
			}
		}
	}

	// 2. Ensure queued locking requests are stored in the correct order, as
	// dictated by the queuedGuard.order field.
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		if e.Prev() != nil && e.Prev().Value.order.after(e.Value.order) {
			return errors.AssertionFailedf(
				"queued locking requests should be stored in sorted order %s", kl,
			)
		}
	}

	// 3. Ensure all (active) waiters conflict with at least one of the lock
	// holders.
	// 3a. Waiting readers:

	// NB: Non-locking reads:
	// 1. Only conflict with intents (and exclusive locks, if dictated by the
	// lock.ExclusiveLocksBlockNonLockingReads cluster setting). Both of these
	// strengths are incompatible with shared locks, and as such, there can be
	// <= 1 lock holders for this key.
	// 2. Do not conflict with any claimants. So there must be >= 1 lock holders
	// on this key.
	//
	// This gives us the following condition if the list of waiting readers is
	// non-empty:
	if kl.waitingReaders.Len() != 0 && kl.holders.Len() != 1 {
		return errors.AssertionFailedf(
			"unexpected number of lock holders when waiting readers are present: %s", kl,
		)
	}
	for e := kl.waitingReaders.Front(); e != nil; e = e.Next() {
		// Ensure each of the readers does indeed conflict with the lock holder.
		reader := e.Value
		if reader.txn != nil && kl.holders.Front().Value.getLockHolderTxn().ID == reader.txn.ID {
			return errors.AssertionFailedf(
				"waiting non-locking reader belongs to the lock holder txn %s", kl,
			)
		}
		if !lock.Conflicts(kl.holders.Front().Value.getLockMode(), reader.curLockMode(), &st.SV) {
			return errors.AssertionFailedf("non locking reader %v does not conflict with lock holder %s",
				reader, kl,
			)
		}
	}
	// 3b. Queued locking requests.
	for e1 := kl.queuedLockingRequests.Front(); e1 != nil; e1 = e1.Next() {
		qlr := e1.Value

		if !qlr.active { // not actively waiting; nothing more to check for inactive waiters
			continue
		}
		// If a locking request is actively waiting at a key, it must either:
		// 1. Conflict with one of the lock holders.
		// 2. OR Conflict with one of the queued locking requests in front of it.
		conflicts := false
		for e2 := kl.holders.Front(); e2 != nil; e2 = e2.Next() {
			holder := e2.Value
			if qlr.guard.txn != nil && holder.getLockHolderTxn().ID == qlr.guard.txn.ID {
				continue // requests can't conflict with locks held by their own transaction
			}

			if lock.Conflicts(holder.getLockMode(), qlr.mode, &st.SV) {
				conflicts = true
				break
			}
		}
		if !conflicts {
			// No conflict found with lock holder(s); check other queued locking
			// requests waiting in front of the request.
			for e2 := kl.queuedLockingRequests.Front(); ; e2 = e2.Next() {
				qg := e2.Value
				if qg.guard == qlr.guard {
					break // we've found our request
				}
				if lock.Conflicts(qg.mode, qlr.mode, &st.SV) {
					conflicts = true
					break
				}
			}
		}
		// TODO(arul): uncomment once 115694 is resolved. This validation has been
		// regularly failing on KVNemesis -- for now, we disable it to ensure legit
		// bugs aren't drowned out by the noise.
		// validation
		//if !conflicts {
		//	return errors.AssertionFailedf(
		//		"queued locking request %d does not conflict with holder/waiting requests %s",
		//		qlr.guard.seqNum, kl,
		//	)
		//}
	}

	// 4. Assert some invariants around the queuedLockingRequests wait queue if
	// the lock isn't held.
	if !kl.isLocked() {
		if kl.queuedLockingRequests.Len() > 0 {
			// 5a. The first request should have a (possibly joint) claim. As such, it
			// should be inactive.
			if kl.queuedLockingRequests.Front().Value.active {
				return errors.AssertionFailedf("first request should be an inactive waiter for unheld lock")
			}
			// 5b. It should also be a transactional request, as non-transactional
			// requests cannot establish claims.
			if kl.queuedLockingRequests.Front().Value.guard.txn == nil {
				return errors.AssertionFailedf("first request should be transactional for unheld lock")
			}
			// Note that we can't make any assertions about (what looks like) joint
			// claims, because claims can be broken.
		}
	}

	// 5. Verify the waiting state on each of the waiters.
	for e := kl.waitingReaders.Front(); e != nil; e = e.Next() {
		claimantTxn, _ := kl.claimantTxnFor(e.Value)
		e.Value.mu.Lock()
		if e.Value.mu.state.kind == waitSelf {
			return errors.AssertionFailedf("readers should never wait for themselves")
		}
		if e.Value.mu.state.txn != nil && e.Value.mu.state.txn.ID != claimantTxn.ID {
			return errors.AssertionFailedf("mismatch between claimant txn ID and waiting state txn ID")
		}
		e.Value.mu.Unlock()
	}
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		if !e.Value.active {
			// Waiting state, in the context of this lock, is only meaningful for
			// actively waiting requests.
			continue
		}
		claimantTxn, _ := kl.claimantTxnFor(e.Value.guard)
		e.Value.guard.mu.Lock()
		if e.Value.guard.isSameTxn(claimantTxn) && e.Value.guard.mu.state.kind != waitSelf {
			return errors.AssertionFailedf("locking request should be in waitSelf")
		} else if e.Value.guard.mu.state.kind == waitSelf && !e.Value.guard.isSameTxn(claimantTxn) {
			return errors.AssertionFailedf("locking request should not be in waitSelf state")
		}
		if e.Value.guard.mu.state.txn != nil && e.Value.guard.mu.state.txn.ID != claimantTxn.ID {
			return errors.AssertionFailedf("mismatch between claimant txn ID and waiting state txn ID")
		}
		e.Value.guard.mu.Unlock()
	}

	// 6. Verify the lock promotion state for each of the waiters is copacetic.
	for e := kl.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		// Handle non-transactional requests first. They should never consider
		// themselves promoting.
		if e.Value.guard.txn == nil {
			if e.Value.order.isPromoting {
				return errors.AssertionFailedf("non-locking transactions can't promote their locks")
			}
			continue // we're good
		}
		// Otherwise, requests that consider themselves promoters should hold locks,
		// and requests that don't shouldn't.
		if _, held := kl.heldBy[e.Value.guard.txnMeta().ID]; held != e.Value.order.isPromoting {
			return errors.AssertionFailedf("mismatched notion of promoting and lock held status")
		}
	}

	return nil
}

func (kl *keyLocks) hasActivelyWaitingLockingRequest() bool {
	for e := kl.lockWaitQueue.queuedLockingRequests.Front(); e != nil; e = e.Next() {
		if e.Value.active {
			return true
		}
	}
	return false
}

// Delete removes the specified lock from the tree.
// REQUIRES: t.mu is locked.
func (t *treeMu) Delete(l *keyLocks) {
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
func (t *lockTableImpl) ScanAndEnqueue(req Request, guard lockTableGuard) (lockTableGuard, *Error) {
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
		return g, nil
	}

	err := g.resumeScan(true /* notify */)
	if err != nil {
		// We're not returning the guard on this error path, so we need to
		// release the guard in case it has already entered any wait-queues.
		t.Dequeue(g)
		return nil, kvpb.NewError(err)
	}
	if g.notRemovableLock != nil {
		// Either waiting at the notRemovableLock, or elsewhere. Either way we are
		// making forward progress, which ensures liveness.
		g.notRemovableLock.decrementNotRemovable()
		g.notRemovableLock = nil
	}
	return g, nil
}

func (t *lockTableImpl) newGuardForReq(req Request) *lockTableGuardImpl {
	g := newLockTableGuardImpl()
	g.seqNum = t.seqNum.Add(1)
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
	var candidateLocks []*keyLocks
	g.mu.Lock()
	for l := range g.mu.locks {
		candidateLocks = append(candidateLocks, l)
	}
	g.mu.Unlock()
	var locksToGC []*keyLocks
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
	foundLock *roachpb.Lock,
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
	key := foundLock.Key
	str, err := findHighestLockStrengthInSpans(key, g.spans)
	if err != nil {
		return false, err
	}
	if consultTxnStatusCache {
		finalizedTxn, ok := t.txnStatusCache.finalizedTxns.get(foundLock.Txn.ID)
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
			pushedTxn, ok := g.lt.txnStatusCache.pendingTxns.get(foundLock.Txn.ID)
			if ok && g.ts.Less(pushedTxn.WriteTimestamp) {
				g.toResolve = append(
					g.toResolve, roachpb.MakeLockUpdate(pushedTxn, roachpb.Span{Key: key}))
				return true, nil
			}
		}
	}
	var l *keyLocks
	t.locks.mu.Lock()
	iter := t.locks.MakeIter()
	iter.FirstOverlap(&keyLocks{key: key})
	checkMaxLocks := false
	if !iter.Valid() {
		var lockSeqNum uint64
		lockSeqNum, checkMaxLocks = t.locks.nextLockSeqNum()
		l = &keyLocks{id: lockSeqNum, key: key}
		l.queuedLockingRequests.Init()
		l.waitingReaders.Init()
		l.holders.Init()
		l.heldBy = make(map[uuid.UUID]*list.Element[*txnLock])
		t.locks.Set(l)
		t.locks.numKeysLocked.Add(1)
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
	err = l.discoveredLock(foundLock, g, str, notRemovableLock, g.lt.clock)
	// Can't release tree.mu until call l.discoveredLock() since someone may
	// find an empty lock and remove it from the tree.
	t.locks.mu.Unlock()
	if checkMaxLocks {
		t.checkMaxKeysLockedAndTryClear()
	}
	if err != nil {
		return true, err
	}
	return true, nil
}

// AcquireLock implements the lockTable interface.
func (t *lockTableImpl) AcquireLock(acq *roachpb.LockAcquisition) error {
	t.enabledMu.RLock()
	defer t.enabledMu.RUnlock()
	if !t.enabled {
		// If not enabled, don't track any locks.
		return nil
	}
	if acq.Empty() {
		return errors.AssertionFailedf("unexpected empty lock acquisition %s", acq)
	}
	switch acq.Strength {
	case lock.Intent:
		assert(acq.Durability == lock.Replicated, "incorrect durability")
	case lock.Exclusive, lock.Shared:
		// Both shared and exclusive locks can have either replicated or
		// unreplicated durability.
	default:
		return errors.AssertionFailedf("unsupported lock strength %s", acq.Strength)
	}
	var kl *keyLocks
	t.locks.mu.Lock()
	// Can't release tree.mu until call kl.acquireLock() since someone may find
	// an empty lock and remove it from the tree. If we expect that keyLocks
	// will already be in tree we can optimize this by first trying with a
	// tree.mu.RLock().
	iter := t.locks.MakeIter()
	iter.FirstOverlap(&keyLocks{key: acq.Key})
	checkMaxLocks := false
	if !iter.Valid() {
		if acq.Durability == lock.Replicated {
			// Don't remember uncontended replicated locks. The downside is that
			// sometimes contention won't be noticed until when the request
			// evaluates. Remembering here would be better, but our behavior when
			// running into the maxKeysLocked limit is somewhat crude. Treating the
			// data-structure as a bounded cache with eviction guided by contention
			// would be better.
			t.locks.mu.Unlock()
			return nil
		}
		var lockSeqNum uint64
		lockSeqNum, checkMaxLocks = t.locks.nextLockSeqNum()
		kl = &keyLocks{id: lockSeqNum, key: acq.Key}
		kl.queuedLockingRequests.Init()
		kl.waitingReaders.Init()
		kl.holders.Init()
		kl.heldBy = make(map[uuid.UUID]*list.Element[*txnLock])
		t.locks.Set(kl)
		t.locks.numKeysLocked.Add(1)
	} else {
		kl = iter.Cur()
		if acq.Durability == lock.Replicated {
			if freed, mustGC := kl.tryFreeLockOnReplicatedAcquire(acq); freed {
				// Don't remember uncontended replicated locks. Just like in the case
				// where the lock is initially added as replicated, we drop replicated
				// locks from the lockTable when being upgraded from Unreplicated to
				// Replicated, whenever possible.
				//
				// TODO(sumeer): now that limited scans evaluate optimistically, we should
				// consider removing this hack. But see the comment in the preceding block
				// about maxKeysLocked.
				if mustGC {
					// The key is no longer locked; it can be removed from the tree.
					t.locks.Delete(kl)
					t.locks.numKeysLocked.Add(-1)
				}
				t.locks.mu.Unlock()
				return nil // don't remember the replicated lock
			}
		}
	}
	err := kl.acquireLock(acq, t.clock, t.settings)
	t.locks.mu.Unlock()

	if checkMaxLocks {
		t.checkMaxKeysLockedAndTryClear()
	}
	if err != nil {
		return err
	}
	return nil
}

// checkMaxKeysLockedAndTryClear checks if the request is tracking more lock
// information on keys in its lock table snapshot than it should. If it is, this
// method relieves memory pressure by clearing as much per-key tracking as it
// can to bring things under budget.
func (t *lockTableImpl) checkMaxKeysLockedAndTryClear() {
	totalLocks := t.locks.numKeysLocked.Load()
	if totalLocks > t.maxKeysLocked {
		numToClear := totalLocks - t.minKeysLocked
		t.tryClearLocks(false /* force */, int(numToClear))
	}
}

func (t *lockTableImpl) lockCountForTesting() int64 {
	return t.locks.numKeysLocked.Load()
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
	var locksToClear []*keyLocks
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
	t.clearLocksMuLocked(locksToClear)
	t.locks.mu.Unlock()
}

// tryClearLockGE attempts to clear all locks greater or equal to given key.
//
// Waiters of removed locks are told to wait elsewhere or that they are done
// waiting.
func (t *lockTableImpl) tryClearLocksGE(key roachpb.Key) []roachpb.LockAcquisition {
	t.locks.mu.Lock()
	defer t.locks.mu.Unlock()

	var locksToClear []*keyLocks
	var lockAcquisitionsToReturn []roachpb.LockAcquisition
	iter := t.locks.MakeIter()
	for iter.SeekGE(&keyLocks{key: key}); iter.Valid(); iter.Next() {
		l := iter.Cur()

		// Add any held locks to the slice of returned LockAcquisitions that the RHS
		// should attempt.
		for hl := l.holders.Front(); hl != nil; hl = hl.Next() {
			tl := hl.Value
			// The returned locks are used to populate a new
			// in-memory lock table. We only return unreplicated
			// locks because replicated locks already exist on disk.
			if tl == nil || tl.unreplicatedInfo.isEmpty() {
				continue
			}

			for _, str := range unreplicatedHolderStrengths {
				if tl.unreplicatedInfo.held(str) {
					lockAcquisitionsToReturn = append(lockAcquisitionsToReturn, roachpb.LockAcquisition{
						Span: roachpb.Span{
							Key: l.key,
						},
						Txn:        *hl.Value.txn,
						Durability: lock.Unreplicated,
						Strength:   str,
					})
				}
			}
		}

		if l.tryClearLock(true) {
			locksToClear = append(locksToClear, l)
		}
	}
	t.clearLocksMuLocked(locksToClear)
	return lockAcquisitionsToReturn
}

func (t *lockTableImpl) clearLocksMuLocked(locksToClear []*keyLocks) {
	t.locks.numKeysLocked.Add(int64(-len(locksToClear)))
	if t.locks.Len() == len(locksToClear) {
		// Fast-path full clear.
		t.locks.Reset()
	} else {
		for _, l := range locksToClear {
			t.locks.Delete(l)
		}
	}
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
func (t *lockTableImpl) tryGCLocks(tree *treeMu, locks []*keyLocks) {
	if len(locks) == 0 {
		return // bail early
	}
	tree.mu.Lock()
	defer tree.mu.Unlock()
	for _, l := range locks {
		iter := tree.MakeIter()
		iter.FirstOverlap(l)
		// Since the same keyLocks can go from non-empty to empty multiple times
		// it is possible that multiple threads are racing to delete it and
		// multiple find it empty and one wins. If a concurrent thread made the
		// keyLocks non-empty we do not want to delete it accidentally.
		if !iter.Valid() {
			continue
		}
		l = iter.Cur()
		l.mu.Lock()
		empty := l.isEmptyLock()
		l.mu.Unlock()
		if empty {
			tree.Delete(l)
			tree.numKeysLocked.Add(-1)
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
	var locksToGC []*keyLocks
	heldByTxn = false
	changeFunc := func(l *keyLocks) {
		held, gc := l.tryUpdateLock(up, t.settings)
		heldByTxn = heldByTxn || held
		if gc {
			locksToGC = append(locksToGC, l)
		}
	}
	t.locks.mu.RLock()
	iter := t.locks.MakeIter()
	ltRange := &keyLocks{key: span.Key, endKey: span.EndKey}
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

// ClearGE implements the lockTable interface.
func (t *lockTableImpl) ClearGE(key roachpb.Key) []roachpb.LockAcquisition {
	return t.tryClearLocksGE(key)
}

// ExportUnreplicatedLocks implements the lockTable interface.
//
// TODO(ssd): Once we have the full set of functions we need for lock flushing, we
// should do a refactoring pass to reduce some of the duplication.
func (t *lockTableImpl) ExportUnreplicatedLocks(
	span roachpb.Span, exporter func(*roachpb.LockAcquisition),
) {
	t.enabledMu.RLock()
	defer t.enabledMu.RUnlock()
	if !t.enabled {
		return
	}

	t.locks.mu.RLock()
	defer t.locks.mu.RUnlock()

	iter := t.locks.MakeIter()
	for iter.SeekGE(&keyLocks{key: span.Key}); iter.Valid(); iter.Next() {
		l := iter.Cur()
		if !l.key.Less(span.EndKey) {
			return
		}

		for hl := l.holders.Front(); hl != nil; hl = hl.Next() {
			tl := hl.Value
			if tl == nil || tl.unreplicatedInfo.isEmpty() {
				continue
			}

			for _, str := range unreplicatedHolderStrengths {
				if tl.unreplicatedInfo.held(str) {
					exporter(&roachpb.LockAcquisition{
						Span: roachpb.Span{
							Key: l.key,
						},
						Txn:            *hl.Value.txn,
						Durability:     lock.Unreplicated,
						Strength:       str,
						IgnoredSeqNums: tl.unreplicatedInfo.ignoredSeqNums,
					})
				}
			}
		}
	}
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
	var nextNumBytes int64

	// Iterate over locks and gather metadata.
	iter := snap.MakeIter()
	ltRange := &keyLocks{key: span.Key, endKey: span.EndKey}
	for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
		l := iter.Cur()
		nextKey = l.key

		lInfos := l.collectLockStateInfo(opts.IncludeUncontended, now, t.rID)
		nextNumBytes = 0
		nextNumLocks := int64(len(lInfos))
		for _, lInfo := range lInfos {
			nextNumBytes += int64(lInfo.Size())
		}

		// We always return locks on at least one key, regardless of the byte or
		// count limits.
		if len(lockTableState) > 0 {
			// Check if accumulating the result will cause byte limits to be exceeded.
			if opts.TargetBytes > 0 && (numBytes+nextNumBytes) > opts.TargetBytes {
				resumeState.ResumeReason = kvpb.RESUME_BYTE_LIMIT
				break
			}
			// Check if accumulating the result will cause lock count limits to be
			// exceeded.
			if opts.MaxLocks > 0 && (numLocks+nextNumLocks) > opts.MaxLocks {
				resumeState.ResumeReason = kvpb.RESUME_KEY_LIMIT
				break
			}
		}

		// Adding all locks on this key won't cause us to go over byte/count limits.
		lockTableState = append(lockTableState, lInfos...)
		numBytes += nextNumBytes
		numLocks += nextNumLocks
	}

	// If we need to paginate results, set the continuation key in the ResumeSpan.
	if resumeState.ResumeReason != 0 {
		resumeState.ResumeNextBytes = nextNumBytes
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
	t.locks.mu.RLock()
	defer t.locks.mu.RUnlock()
	return t.stringRLocked()
}

// stringRLocked is like String but the caller is responsible for acquiring a
// read lock on t.locks.mu.
//
// REQUIRES: t.locks.mu to be RLocked.
func (t *lockTableImpl) stringRLocked() string {
	var sb redact.StringBuilder
	sb.Printf("num=%d\n", t.locks.numKeysLocked.Load())
	iter := t.locks.MakeIter()
	for iter.First(); iter.Valid(); iter.Next() {
		l := iter.Cur()
		l.mu.Lock()
		l.safeFormat(&sb, &t.txnStatusCache)
		l.mu.Unlock()
	}
	return sb.String()
}

// TestingSetMaxLocks implements the lockTable interface.
func (t *lockTableImpl) TestingSetMaxLocks(maxKeysLocked int64) {
	t.setMaxKeysLocked(maxKeysLocked)
}

// verify implements the verifiableLockTable interface.
//
// ACQUIRES: t.mu
func (t *lockTableImpl) verify() {
	t.locks.mu.RLock()
	defer t.locks.mu.RUnlock()
	iter := t.locks.MakeIter()
	for iter.First(); iter.Valid(); iter.Next() {
		l := iter.Cur()
		err := func() error {
			l.mu.Lock()
			defer l.mu.Unlock()
			return l.verify(t.settings)
		}()
		if err != nil {
			panic(fmt.Sprintf("lock table %s\nerror: %v", t.stringRLocked(), err))
		}
	}
}

// verifyKey implements the verifiableLockTable interface.
//
// ACQUIRES: t.mu
func (t *lockTableImpl) verifyKey(key roachpb.Key) {
	t.locks.mu.RLock()
	defer t.locks.mu.RUnlock()
	iter := t.locks.MakeIter()
	iter.FirstOverlap(&keyLocks{key: key})
	if !iter.Valid() {
		return // no locks exist on this key
	}
	l := iter.Cur()
	err := func() error {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.verify(t.settings)
	}()
	if err != nil {
		panic(fmt.Sprintf(
			"error verifying key %s; lock table %s\nerror: %v", key, t.stringRLocked(), err,
		))
	}
}

var _ verifiableLockTable = &lockTableImpl{}

// assert panics with the supplied message if the condition does not hold true.
func assert(condition bool, msg string) {
	if !condition {
		panic(msg)
	}
}
