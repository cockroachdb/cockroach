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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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
	// to release its locks or complete its own request. waitingStates with this
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

	// waitSelf indicates that a different requests from the same transaction
	// has a conflicting reservation. See the comment about "Reservations" in
	// lockState. This request should sit tight and wait for a new notification
	// without pushing anyone.
	waitSelf

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
	txn           *enginepb.TxnMeta // always non-nil in waitFor{,Distinguished,Self}
	key           roachpb.Key       // the key of the conflict
	held          bool              // is the conflict a held lock?
	queuedWriters int               // how many writers are waiting?
	queuedReaders int               // how many readers are waiting?

	// Represents the action that the request was trying to perform when
	// it hit the conflict. E.g. was it trying to read or write?
	guardAccess spanset.SpanAccess
}

// String implements the fmt.Stringer interface.
func (s waitingState) String() string {
	switch s.kind {
	case waitFor, waitForDistinguished:
		distinguished := ""
		if s.kind == waitForDistinguished {
			distinguished = " (distinguished)"
		}
		target := "holding lock"
		if !s.held {
			target = "running request"
		}
		return fmt.Sprintf("wait for%s txn %s %s @ key %s (queuedWriters: %d, queuedReaders: %d)",
			distinguished, s.txn.ID.Short(), target, s.key, s.queuedWriters, s.queuedReaders)
	case waitSelf:
		return fmt.Sprintf("wait self @ key %s", s.key)
	case waitElsewhere:
		if !s.held {
			return "wait elsewhere by proceeding to evaluation"
		}
		return fmt.Sprintf("wait elsewhere for txn %s @ key %s", s.txn.ID.Short(), s.key)
	case doneWaiting:
		return "done waiting"
	default:
		panic("unhandled waitingState.kind")
	}
}

// Implementation
// TODO(sbhola):
// - metrics about lockTable state to export to observability debug pages:
//   number of locks, number of waiting requests, wait time?, ...

// The btree for a particular SpanScope.
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
//                 > treeMu.mu
//                 > lockState.mu
//                 > lockTableGuardImpl.mu
type lockTableImpl struct {
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
	// - Lock at A is released so req1 acquires the reservation at A and starts
	//   waiting at B.
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
	// - txn2 releases A. req3 is in the front of the queue at A and gets the
	//   reservation and starts waiting at B behind req1.
	// - txn0 releases B. req1 gets the reservation at B and does another scan
	//   and adds itself to the queue at A, behind req3 which holds the
	//   reservation at A.
	// Now in the queues for A and B req1 is behind req3 and vice versa and
	// this deadlock has been created entirely due to the lock table's behavior.
	seqNum uint64

	locks [spanset.NumSpanScope]treeMu

	// maxLocks is a soft maximum on number of locks. When it is exceeded, and
	// subject to the dampening in lockAddMaxLocksCheckInterval, locks will be
	// cleared.
	maxLocks int64
	// When maxLocks is exceeded, will attempt to clear down to minLocks,
	// instead of clearing everything.
	minLocks int64

	// finalizedTxnCache is a small LRU cache that tracks transactions that
	// were pushed and found to be finalized (COMMITTED or ABORTED). It is
	// used as an optimization to avoid repeatedly pushing the transaction
	// record when cleaning up the intents of an abandoned transaction.
	//
	// NOTE: it probably makes sense to maintain a single finalizedTxnCache
	// across all Ranges on a Store instead of an individual cache per
	// Range. For now, we don't do this because we don't share any state
	// between separate concurrency.Manager instances.
	finalizedTxnCache txnCache
}

var _ lockTable = &lockTableImpl{}

func newLockTable(maxLocks int64) *lockTableImpl {
	// Check at 5% intervals of the max count.
	lockAddMaxLocksCheckInterval := maxLocks / (int64(spanset.NumSpanScope) * 20)
	if lockAddMaxLocksCheckInterval == 0 {
		lockAddMaxLocksCheckInterval = 1
	}
	lt := &lockTableImpl{
		maxLocks: maxLocks,
		minLocks: maxLocks / 2,
	}
	for i := 0; i < int(spanset.NumSpanScope); i++ {
		lt.locks[i].lockAddMaxLocksCheckInterval = uint64(lockAddMaxLocksCheckInterval)
	}
	return lt
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
// - The waitFor* states provide information on who the request is waiting for.
//   The waitForDistinguished state is a sub-case -- a distinguished waiter is
//   responsible for taking extra actions e.g. immediately pushing the transaction
//   it is waiting for. The implementation ensures that if there are multiple
//   requests in waitFor state waiting on the same transaction at least one will
//   be a distinguished waiter.
//
//   TODO(sbhola): investigate removing the waitForDistinguished state which
//   will simplify the code here. All waitFor requests would wait (currently
//   50ms) before pushing the transaction (for deadlock detection) they are
//   waiting on, say T. Typically T will be done before 50ms which is considered
//   ok: the one exception we will need to make is if T has the min priority or
//   the waiting transaction has max priority -- in both cases it will push
//   immediately. The bad case is if T is ABORTED: the push will succeed after,
//   and if T left N intents, each push would wait for 50ms, incurring a latency
//   of 50*N ms. A cache of recently encountered ABORTED transactions on each
//   Store should mitigate this latency increase. Whenever a transaction sees a
//   waitFor state, it will consult this cache and if T is found, push
//   immediately (if there isn't already a push in-flight) -- even if T is not
//   initially in the cache, the first push will place it in the cache, so the
//   maximum latency increase is 50ms.
//
// - The waitElsewhere state is a rare state that is used when the lockTable is
//   under memory pressure and is clearing its internal queue state. Like the
//   waitFor* states, it informs the request who it is waiting for so that
//   deadlock detection works. However, sequencing information inside the
//   lockTable is mostly discarded.
//
// - The waitSelf state is a rare state when a different request from the same
//   transaction has a reservation. See the comment about "Reservations" in
//   lockState.
//
// - The doneWaiting state is used to indicate that the request should make
//   another call to ScanAndEnqueue() (that next call is more likely to return a
//   lockTableGuard that returns false from StartWaiting()).
type lockTableGuardImpl struct {
	seqNum uint64
	lt     *lockTableImpl

	// Information about this request.
	txn   *enginepb.TxnMeta
	ts    hlc.Timestamp
	spans *spanset.SpanSet

	// Snapshots of the trees for which this request has some spans. Note that
	// the lockStates in these snapshots may have been removed from
	// lockTableImpl. Additionally, it is possible that there is a new lockState
	// for the same key. This can result in various harmless anomalies:
	// - the request may hold a reservation on a lockState that is no longer
	//   in the tree. When it next does a scan, it will either find a new
	//   lockState where it will compete or none. Both lockStates can be in
	//   the mu.locks map, which is harmless.
	// - the request may wait behind a reservation holder that is not the
	//   lock holder. This could cause a delay in pushing the lock holder.
	//   This is not a correctness issue (the whole system is not deadlocked)
	//   and we expect will not be a real performance issue.
	//
	// TODO(sbhola): experimentally evaluate the lazy queueing of the current
	// implementation, in comparison with eager queueing. If eager queueing
	// is comparable in system throughput, one can eliminate the above anomalies.
	//
	tableSnapshot [spanset.NumSpanScope]btree

	// notRemovableLock points to the lock for which this guard has incremented
	// lockState.notRemovable. It will be set to nil when this guard has decremented
	// lockState.notRemovable. Note that:
	// - notRemovableLock may no longer be in one of the btrees in lockTableImpl
	//   since it may have been removed due to the lock being released. This is
	//   harmless since the change in lock state for that lock's key (even if it
	//   has meanwhile been reacquired by a different request) means forward
	//   progress for this request, which guarantees liveness for this request.
	// - Multiple guards can have marked the same lock as notRemovable, which is
	//   why lockState.notRemovable behaves like a reference count.
	notRemovableLock *lockState

	// A request whose startWait is set to true in ScanAndEnqueue is actively
	// waiting at a particular key. This is the first key encountered when
	// iterating through spans that it needs to wait at. A future event (lock
	// release etc.) may cause the request to no longer need to wait at this
	// key. It then needs to continue iterating through spans to find the next
	// key to wait at (we don't want to wastefully start at the beginning since
	// this request probably has a reservation at the contended keys there): sa,
	// ss, index, key collectively track the current position to allow it to
	// continue iterating.

	// The key for the lockState.
	key roachpb.Key
	// The key for the lockState is contained in the Span specified by
	// spans[sa][ss][index].
	ss    spanset.SpanScope
	sa    spanset.SpanAccess // Iterates from stronger to weaker strength
	index int

	mu struct {
		syncutil.Mutex
		startWait bool

		state  waitingState
		signal chan struct{}

		// locks for which this request has a reservation or is in the queue of
		// writers (active or inactive) or actively waiting as a reader.
		//
		// TODO(sbhola): investigate whether the logic to maintain this locks map
		// can be simplified so it doesn't need to be adjusted by various
		// lockState methods. It adds additional bookkeeping burden that means it
		// is more prone to inconsistencies. There are two main uses: (a) removing
		// from various lockStates when done() is called, (b) tryActiveWait() uses
		// it as an optimization to know that this request is not known to the
		// lockState. (b) can be handled by other means -- the first scan the
		// request won't be in the lockState and the second scan it likely will.
		// (a) doesn't necessarily require this map to be consistent -- the
		// request could track the places where it is has enqueued as places where
		// it could be present and then do the search.

		locks map[*lockState]struct{}

		// If this is true, the state has changed and the channel has been
		// signaled, but what the state should be has not been computed. The call
		// to CurState() needs to compute that current state. Deferring the
		// computation makes the waiters do this work themselves instead of making
		// the call to release/update locks or release reservations do this work
		// (proportional to number of waiters).
		mustFindNextLockAfter bool
	}
	// Locks to resolve before scanning again. Doesn't need to be protected by
	// mu since should only be read after the caller has already synced with mu
	// in realizing that it is doneWaiting.
	toResolve []roachpb.LockUpdate
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
	return g.mu.startWait
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
	if !g.mu.mustFindNextLockAfter {
		return g.mu.state
	}
	// Not actively waiting anywhere so no one else can set
	// mustFindNextLockAfter to true while this method executes.
	g.mu.mustFindNextLockAfter = false
	g.mu.Unlock()
	g.findNextLockAfter(false /* notify */)
	g.mu.Lock() // Unlock deferred
	return g.mu.state
}

func (g *lockTableGuardImpl) CheckOptimisticNoConflicts(spanSet *spanset.SpanSet) (ok bool) {
	// Temporarily replace the SpanSet in the guard.
	originalSpanSet := g.spans
	g.spans = spanSet
	g.sa = spanset.NumSpanAccess - 1
	g.ss = spanset.SpanScope(0)
	g.index = -1
	defer func() {
		g.spans = originalSpanSet
	}()
	span := stepToNextSpan(g)
	for span != nil {
		startKey := span.Key
		tree := g.tableSnapshot[g.ss]
		iter := tree.MakeIter()
		ltRange := &lockState{key: startKey, endKey: span.EndKey}
		for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
			l := iter.Cur()
			if !l.isNonConflictingLock(g, g.sa) {
				return false
			}
		}
		span = stepToNextSpan(g)
	}
	return true
}

func (g *lockTableGuardImpl) notify() {
	select {
	case g.mu.signal <- struct{}{}:
	default:
	}
}

// Called when the request is no longer actively waiting at lock l, and should
// look for the next lock to wait at. hasReservation is true iff the request
// acquired the reservation at l. Note that it will be false for requests that
// were doing a read at the key, or non-transactional writes at the key.
func (g *lockTableGuardImpl) doneWaitingAtLock(hasReservation bool, l *lockState) {
	g.mu.Lock()
	if !hasReservation {
		delete(g.mu.locks, l)
	}
	g.mu.mustFindNextLockAfter = true
	g.notify()
	g.mu.Unlock()
}

func (g *lockTableGuardImpl) isSameTxn(txn *enginepb.TxnMeta) bool {
	return g.txn != nil && g.txn.ID == txn.ID
}

func (g *lockTableGuardImpl) isSameTxnAsReservation(ws waitingState) bool {
	return !ws.held && g.isSameTxn(ws.txn)
}

// Finds the next lock, after the current one, to actively wait at. If it
// finds the next lock the request starts actively waiting there, else it is
// told that it is done waiting. lockTableImpl.finalizedTxnCache is used to
// accumulate intents to resolve.
// Acquires g.mu.
func (g *lockTableGuardImpl) findNextLockAfter(notify bool) {
	spans := g.spans.GetSpans(g.sa, g.ss)
	var span *spanset.Span
	resumingInSameSpan := false
	if g.index == -1 || len(spans[g.index].EndKey) == 0 {
		span = stepToNextSpan(g)
	} else {
		span = &spans[g.index]
		resumingInSameSpan = true
	}
	// Locks that transition to free because of the finalizedTxnCache are GC'd
	// before returning. Note that these are only unreplicated locks. Replicated
	// locks are handled via the g.toResolve.
	var locksToGC [spanset.NumSpanScope][]*lockState
	defer func() {
		for i := 0; i < len(locksToGC); i++ {
			if len(locksToGC[i]) > 0 {
				g.lt.tryGCLocks(&g.lt.locks[i], locksToGC[i])
			}
		}
	}()

	for span != nil {
		startKey := span.Key
		if resumingInSameSpan {
			startKey = g.key
		}
		tree := g.tableSnapshot[g.ss]
		iter := tree.MakeIter()

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
			wait, transitionedToFree := l.tryActiveWait(g, g.sa, notify)
			if transitionedToFree {
				locksToGC[g.ss] = append(locksToGC[g.ss], l)
			}
			if wait {
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
		for i := range g.toResolve {
			if heldByTxn := g.lt.updateLockInternal(&g.toResolve[i]); heldByTxn {
				g.toResolve[j] = g.toResolve[i]
				j++
			}
		}
		g.toResolve = g.toResolve[:j]
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.mu.state = waitingState{kind: doneWaiting}
	// We are doneWaiting but may have some locks to resolve. There are
	// two cases:
	// - notify=false: the caller was already waiting and will look at this list
	//   of locks.
	// - notify=true: this is a scan initiated by the caller, and it is holding
	//   latches. We need to tell it to "wait", so that it does this resolution
	//   first. startWait is currently false. This is the case handled below.
	if notify {
		if len(g.toResolve) > 0 {
			// Force caller to release latches and resolve intents. The first
			// state it will see after releasing latches is doneWaiting, which
			// will cause it to resolve intents.
			g.mu.startWait = true
		}
		g.notify()
	}
}

// Waiting writers in a lockState are wrapped in a queuedGuard. A waiting
// writer is typically waiting in an active state, i.e., the
// lockTableGuardImpl.key refers to this lockState. However, there are
// multiple reasons that can cause a writer to be an inactive waiter:
// - Breaking of reservations (see the comment on reservations below, in
//   lockState) can cause a writer to be an inactive waiter.
// - A discovered lock causes the discoverer to become an inactive waiter
//   (until it scans again).
// - A lock held by a finalized txn causes the first waiter to be an inactive
//   waiter.
// The first case above (breaking reservations) only occurs for transactional
// requests, but the other cases can happen for both transactional and
// non-transactional requests.
type queuedGuard struct {
	guard  *lockTableGuardImpl
	active bool // protected by lockState.mu
}

// Information about a lock holder.
type lockHolderInfo struct {
	// nil if there is no holder. Else this is the TxnMeta of the latest call to
	// acquire/update the lock by this transaction. For a given transaction if
	// the lock is continuously held by a succession of different TxnMetas, the
	// epoch must be monotonic and the ts (derived from txn.WriteTimestamp for
	// some calls, and request.ts for other calls) must be monotonic. After ts
	// is intialized, the timestamps inside txn are not used.
	txn *enginepb.TxnMeta

	// All the TxnSeqs in the current epoch at which this lock has been
	// acquired. In increasing order. We track these so that if a lock is
	// acquired at both seq 5 and seq 7, rollback of 7 does not cause the lock
	// to be released. This is also consistent with PostgreSQL semantics
	// https://www.postgresql.org/docs/12/sql-select.html#SQL-FOR-UPDATE-SHARE
	seqs []enginepb.TxnSeq

	// The timestamp at which the lock is held.
	ts hlc.Timestamp
}

func (lh *lockHolderInfo) isEmpty() bool {
	return lh.txn == nil && lh.seqs == nil && lh.ts.IsEmpty()
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
	ss  spanset.SpanScope

	mu syncutil.Mutex // Protects everything below.

	// Invariant summary (see detailed comments below):
	// - both holder.locked and waitQ.reservation != nil cannot be true.
	// - if holder.locked and multiple holderInfos have txn != nil: all the
	//   txns must have the same txn.ID.
	// - !holder.locked => waitingReaders.Len() == 0. That is, readers wait
	//   only if the lock is held. They do not wait for a reservation.
	// - If reservation != nil, that request is not in queuedWriters.

	// Information about whether the lock is held and the holder. We track
	// information for each durability level separately since a transaction can
	// go through multiple epochs and TxnSeq and may acquire the same lock in
	// replicated and unreplicated mode at different stages.
	holder struct {
		locked bool
		// LockStrength is always Exclusive
		holder [lock.MaxDurability + 1]lockHolderInfo
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
	// Reservations:
	//
	// A not-held lock can be "reserved". A reservation is just a claim that
	// prevents multiple requests from racing when the lock is released. A
	// reservation by req2 can be broken by req1 is req1 has a smaller seqNum
	// than req2. Only requests that specify SpanReadWrite for a key can make
	// reservations. This means a reservation can only be made when the lock is
	// not held, since the reservation (which can acquire an Exclusive lock) and
	// the lock holder (which is an Exclusive lock) conflict.
	//
	// Read reservations are not permitted due to the complexities discussed in
	// the review for #43740. Additionally, reads do not queue for their turn at
	// all -- they are held in the waitingReaders list while the lock is held
	// and removed when the lock is not released, so they race with
	// reservations. Let us consider scenarios where reads did wait in the same
	// queue: the lock could be held or reserved by a write at ts=20, followed
	// by a waiting writer at ts=18, writer at ts=10, reader at ts=12. That
	// reader is waiting not because of a conflict with the holder, or reserver,
	// or the first waiter, but because there is a waiter ahead of it which it
	// conflicts with. This introduces more complexity in tracking who this
	// reader should push. Also consider a scenario where a reader did not wait
	// in the queue and waited on the side like in waitingReaders but acquired a
	// read reservation (together with other readers) when the lock was
	// released. Ignoring the unfairness of this, we can construct a deadlock
	// scenario with request req1 with seqnum 1 and req2 with seqnum 2 where
	// req1 and req2 both want to write at one key and so get ordered by their
	// seqnums but at another key req2 wants to read and req1 wants to write and
	// since req2 does not wait in the queue it acquires a read reservation
	// before req1. See the discussion at the end of this comment section on how
	// the behavior will extend when we start supporting Shared and Upgrade
	// locks.
	//
	// Non-transactional requests can do both reads and writes but cannot be
	// depended on since they don't have a transaction that can be pushed.
	// Therefore they not only do not acquire locks, but cannot make reservations.
	// The non-reservation for reads is already covered in the previous
	// paragraph. For non-transactional writes, the request waits in the queue
	// with other writers. The difference occurs:
	// - when it gets to the front of the queue and there is no lock holder
	//   or reservation: instead of acquiring the reservation it removes
	//   itself from the lockState and proceeds to the next lock. If it
	//   does not need to wait for any more locks and manages to acquire
	//   latches before those locks are acquired by some other request, it
	//   will evaluate.
	// - when deciding to wait at a lock: if the lock has a reservation with
	//   a sequence num higher than this non-transactional request it will
	//   ignore that reservation. Note that ignoring such reservations is
	//   safe since when this non-transactional request is holding latches
	//   those reservation holders cannot be holding latches, so they cannot
	//   conflict.
	//
	// Multiple requests from the same transaction wait independently, including
	// the situation where one of the requests has a reservation and the other
	// is waiting (currently this can only happen if both requests are doing
	// SpanReadWrite). Making multiple requests from the same transaction
	// jointly hold the reservation introduces code complexity since joint
	// reservations can be partially broken (see deadlock example below), and is
	// not necessarily fair to other requests. Additionally, if req1 from txn1
	// is holding a a reservation and req2 from txn1 is waiting, they must
	// conflict wrt latches and cannot evaluate concurrently so there isn't a
	// benefit to joint reservations. However, if one of the requests acquires
	// the lock the other request no longer needs to wait on this lock. This
	// situation motivates the waitSelf state.
	//
	// Deadlock example if joint reservations were supported and we did not
	// allow partial breaking of such reservations:
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
	// - A is unlocked. req3 reserves A and waits at C behind req2.
	// - B is locked by some random txn.
	// - D is unlocked. req1 reserves D and proceeds to scan again and finds A
	//   is reserved by req3 which is the same txn so becomes a joint
	//   reservation holder at A.
	// - Since B is locked, req1 waits at B.
	// - C is unlocked. req2 reserves C. It scans and finds req1+req3 holding
	//   the joint reservation at A. If it queues behind this joint reservation
	//   we have the following situation:
	//        reservation   waiter
	//   A     req1+req3     req2
	//   C       req2        req3
	//   This is a deadlock caused by the lock table unless req2 partially
	//   breaks the reservation at A.
	//
	// Extension for Shared and Upgrade locks:
	// There are 3 aspects to consider: holders; reservers; the dependencies
	// that need to be captured when waiting.
	//
	// - Holders: only shared locks are compatible with themselves, so there can
	//   be one of (a) no holder (b) multiple shared lock holders, (c) one
	//   exclusive holder, (d) one upgrade holder. Non-locking reads will
	//   wait in waitingReaders for only an incompatible exclusive holder.
	//
	// - Reservers: This follows the same pattern as holders. Non-locking reads
	//   do not wait on reservers.
	//
	// - Queueing and dependencies: All potential lockers and non-transactional
	//   writers will wait in the same queue. A sequence of consecutive requests
	//   that have the potential to acquire a shared lock will jointly reserve
	//   that shared lock. Such requests cannot jump ahead of requests with a
	//   lower seqnum just because there is currently a shared lock reservation
	//   (this can cause lockTable induced deadlocks). Such joint reservations
	//   can be partially broken by a waiter desiring an exclusive or upgrade
	//   lock. Like the current code, non-transactional writes will wait for
	//   reservations that have a lower sequence num, but not make their own
	//   reservation. Additionally, they can partially break joint reservations.
	//
	//   Reservations that are (partially or fully) broken cause requests to
	//   reenter the queue as inactive waiters. This is no different than the
	//   current behavior. Each request can specify the same key in spans for
	//   ReadOnly, ReadShared, ReadUpgrade, ReadWrite. The spans will be
	//   iterated over in decreasing order of strength, to only wait at a lock
	//   at the highest strength (this is similar to the current behavior using
	//   accessDecreasingStrength).
	//
	//   For dependencies, a waiter desiring an exclusive or upgrade lock always
	//   conflicts with the holder(s) or reserver(s) so that is the dependency
	//   that will be captured. A waiter desiring a shared lock may encounter a
	//   situation where it does not conflict with the holder(s) or reserver(s)
	//   since those are also shared lockers. In that case it will depend on the
	//   first waiter since that waiter must be desiring a lock that is
	//   incompatible with a shared lock.

	reservation *lockTableGuardImpl

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

	// List of *queuedGuard. A subset of these are actively waiting. If
	// non-empty, either the lock is held or there is a reservation.
	queuedWriters list.List

	// List of *lockTableGuardImpl. All of these are actively waiting. If
	// non-empty, the lock must be held. By definition these cannot be in
	// waitSelf state since that state is only used when there is a reservation.
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
// REQUIRES: l.mu is locked. finalizedTxnCache can be nil.
func (l *lockState) safeFormat(sb *redact.StringBuilder, finalizedTxnCache *txnCache) {
	sb.Printf(" lock: %s\n", l.key)
	if l.isEmptyLock() {
		sb.SafeString("  empty\n")
		return
	}
	writeResInfo := func(sb *redact.StringBuilder, txn *enginepb.TxnMeta, ts hlc.Timestamp) {
		// TODO(sbhola): strip the leading 0 bytes from the UUID string since tests are assigning
		// UUIDs using a counter and makes this output more readable.
		sb.Printf("txn: %v, ts: %v, seq: %v\n",
			redact.Safe(txn.ID), redact.Safe(ts), redact.Safe(txn.Sequence))
	}
	writeHolderInfo := func(sb *redact.StringBuilder, txn *enginepb.TxnMeta, ts hlc.Timestamp) {
		sb.Printf("  holder: txn: %v, ts: %v, info: ", redact.Safe(txn.ID), redact.Safe(ts))
		first := true
		for i := range l.holder.holder {
			h := &l.holder.holder[i]
			if h.txn == nil {
				continue
			}
			if !first {
				sb.SafeString(", ")
			}
			first = false
			if lock.Durability(i) == lock.Replicated {
				sb.SafeString("repl ")
			} else {
				sb.SafeString("unrepl ")
			}
			if finalizedTxnCache != nil {
				finalizedTxn, ok := finalizedTxnCache.get(h.txn.ID)
				if ok {
					var statusStr string
					switch finalizedTxn.Status {
					case roachpb.COMMITTED:
						statusStr = "committed"
					case roachpb.ABORTED:
						statusStr = "aborted"
					}
					sb.Printf("[holder finalized: %s] ", redact.Safe(statusStr))
				}
			}
			sb.Printf("epoch: %d, seqs: [%d", redact.Safe(h.txn.Epoch), redact.Safe(h.seqs[0]))
			for j := 1; j < len(h.seqs); j++ {
				sb.Printf(", %d", redact.Safe(h.seqs[j]))
			}
			sb.SafeString("]")
		}
		sb.SafeString("\n")
	}
	txn, ts := l.getLockHolder()
	if txn == nil {
		sb.Printf("  res: req: %d, ", l.reservation.seqNum)
		writeResInfo(sb, l.reservation.txn, l.reservation.ts)
	} else {
		writeHolderInfo(sb, txn, ts)
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

// Called for a write request when there is a reservation. Returns true iff it
// succeeds.
// REQUIRES: l.mu is locked.
func (l *lockState) tryBreakReservation(seqNum uint64) bool {
	if l.reservation.seqNum > seqNum {
		qg := &queuedGuard{
			guard:  l.reservation,
			active: false,
		}
		l.queuedWriters.PushFront(qg)
		l.reservation = nil
		return true
	}
	return false
}

// Informs active waiters about reservation or lock holder. The reservation
// may have changed so this needs to fix any inconsistencies wrt waitSelf and
// waitForDistinguished states.
// REQUIRES: l.mu is locked.
func (l *lockState) informActiveWaiters() {
	waitForState := waitingState{
		kind:          waitFor,
		key:           l.key,
		queuedWriters: l.queuedWriters.Len(),
		queuedReaders: l.waitingReaders.Len(),
	}
	findDistinguished := l.distinguishedWaiter == nil
	if lockHolderTxn, _ := l.getLockHolder(); lockHolderTxn != nil {
		waitForState.txn = lockHolderTxn
		waitForState.held = true
	} else {
		waitForState.txn = l.reservation.txn
		if !findDistinguished && l.distinguishedWaiter.isSameTxnAsReservation(waitForState) {
			findDistinguished = true
			l.distinguishedWaiter = nil
		}
	}

	for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
		state := waitForState
		state.guardAccess = spanset.SpanReadOnly
		// Since there are waiting readers we could not have transitioned out of
		// or into a state with a reservation, since readers do not wait for
		// reservations.
		g := e.Value.(*lockTableGuardImpl)
		if findDistinguished {
			l.distinguishedWaiter = g
			findDistinguished = false
		}
		g.mu.Lock()
		g.mu.state = state
		if l.distinguishedWaiter == g {
			g.mu.state.kind = waitForDistinguished
		}
		g.notify()
		g.mu.Unlock()
	}
	for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		if !qg.active {
			continue
		}
		g := qg.guard
		state := waitForState
		if g.isSameTxnAsReservation(state) {
			state.kind = waitSelf
		} else {
			state.guardAccess = spanset.SpanReadWrite
			if findDistinguished {
				l.distinguishedWaiter = g
				findDistinguished = false
			}
			if l.distinguishedWaiter == g {
				state.kind = waitForDistinguished
			}
		}
		g.mu.Lock()
		g.mu.state = state
		g.notify()
		g.mu.Unlock()
	}
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
			if qg.active {
				if g == l.distinguishedWaiter {
					l.distinguishedWaiter = nil
				}
				g.doneWaitingAtLock(false, l)
			} else {
				g.mu.Lock()
				delete(g.mu.locks, l)
				g.mu.Unlock()
			}
			l.queuedWriters.Remove(curr)
		}
	}
}

// When the active waiters have shrunk and the distinguished waiter has gone,
// try to make a new distinguished waiter if there is at least 1 active
// waiter.
// REQUIRES: l.mu is locked.
func (l *lockState) tryMakeNewDistinguished() {
	var g *lockTableGuardImpl
	if l.waitingReaders.Len() > 0 {
		g = l.waitingReaders.Front().Value.(*lockTableGuardImpl)
	} else if l.queuedWriters.Len() > 0 {
		for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
			qg := e.Value.(*queuedGuard)
			if qg.active && (l.reservation == nil || !qg.guard.isSameTxn(l.reservation.txn)) {
				g = qg.guard
				break
			}
		}
	}
	if g != nil {
		l.distinguishedWaiter = g
		g.mu.Lock()
		g.mu.state.kind = waitForDistinguished
		// The rest of g.state is already up-to-date.
		g.notify()
		g.mu.Unlock()
	}
}

// Returns true iff the lockState is empty, i.e., there is no lock holder or
// reservation.
// REQUIRES: l.mu is locked.
func (l *lockState) isEmptyLock() bool {
	if !l.holder.locked && l.reservation == nil {
		for i := range l.holder.holder {
			if !l.holder.holder[i].isEmpty() {
				panic("lockState with !locked but non-zero lockHolderInfo")
			}
		}
		if l.waitingReaders.Len() > 0 || l.queuedWriters.Len() > 0 {
			panic("lockState with waiters but no holder or reservation")
		}
		return true
	}
	return false
}

// Returns true iff the lock is currently held by the transaction with the
// given id.
// REQUIRES: l.mu is locked.
func (l *lockState) isLockedBy(id uuid.UUID) bool {
	if l.holder.locked {
		var holderID uuid.UUID
		if l.holder.holder[lock.Unreplicated].txn != nil {
			holderID = l.holder.holder[lock.Unreplicated].txn.ID
		} else {
			holderID = l.holder.holder[lock.Replicated].txn.ID
		}
		return id == holderID
	}
	return false
}

// Returns information about the current lock holder if the lock is held, else
// returns nil.
// REQUIRES: l.mu is locked.
func (l *lockState) getLockHolder() (*enginepb.TxnMeta, hlc.Timestamp) {
	if !l.holder.locked {
		return nil, hlc.Timestamp{}
	}

	// If the lock is held as both replicated and unreplicated we want to
	// provide the lower of the two timestamps, since the lower timestamp
	// contends with more transactions. Else we provide whichever one it is held
	// at.

	// Start with the assumption that it is held as replicated.
	index := lock.Replicated
	// Condition under which we prefer the unreplicated holder.
	if l.holder.holder[index].txn == nil || (l.holder.holder[lock.Unreplicated].txn != nil &&
		// If we are evaluating the following clause we are sure that it is held
		// as both replicated and unreplicated.
		l.holder.holder[lock.Unreplicated].ts.Less(l.holder.holder[lock.Replicated].ts)) {
		index = lock.Unreplicated
	}
	return l.holder.holder[index].txn, l.holder.holder[index].ts
}

// Removes the current lock holder from the lock.
// REQUIRES: l.mu is locked.
func (l *lockState) clearLockHolder() {
	l.holder.locked = false
	for i := range l.holder.holder {
		l.holder.holder[i] = lockHolderInfo{}
	}
}

// Decides whether the request g with access sa should actively wait at this
// lock and if yes, adjusts the data-structures appropriately. The notify
// parameter is true iff the request's new state channel should be notified --
// it is set to false when the call to tryActiveWait is happening due to an
// event for a different request or transaction (like a lock release) since in
// that case the channel is notified first and the call to tryActiveWait()
// happens later in lockTableGuard.CurState().
//
// It uses the finalizedTxnCache to decide that the caller does not need to
// wait on a lock of a transaction that is already finalized.
//
// - For unreplicated locks, this method will silently remove the lock and
//   proceed as normal.
// - For replicated locks the behavior is more complicated since we need to
//   resolve the intent. We desire:
//   A. batching of intent resolution.
//   B. minimize races where intent resolution is being performed by multiple
//      requests.
//   C. minimize races where the intent has not yet been resolved but has been
//      removed from the lock table, thereby causing some other request to
//      evaluate wastefully and discover the intent.
//
//  For A, the caller of tryActiveWait will accumulate the LockUpdates. For B,
//  we only generate a LockUpdate here if this request is either a reader, or
//  the first writer in the queue, i.e., it is only blocked by the lock
//  holder. This prevents races between multiple writers in doing resolution
//  but not between multiple readers and between readers and writers. We could
//  be more conservative in only doing the intent resolution if the waiter was
//  equivalent to a distinguished-waiter, but there it no guarantee that that
//  distinguished waiter will do intent resolution in a timely manner (since
//  it could block waiting on some other lock). Instead, the caller of
//  tryActiveWait makes a best-effort to reduce racing (explained below). For
//  C, the caller of tryActiveWait removes the lock from the in-memory
//  data-structure only if the request does not need to wait anywhere, which
//  means it will immediately proceed to intent resolution. Additionally, if
//  the lock has already been removed, it suggests that some other request has
//  already claimed intent resolution (or done it), so this request does not
//  need to do the resolution.
//
//  Ideally, we would strengthen B and C -- a request should make a claim on
//  intent resolution for a set of keys, and will either resolve the intent,
//  or due to an error will return that claim so others can do so. A
//  replicated lock (intent) would not be removed from the in-memory
//  data-structure until it was actually gone.
//  TODO(sumeer): do this cleaner solution for batched intent resolution.
//
//  In the future we'd like to augment the lockTable with an understanding of
//  finalized but not yet resolved locks. These locks will allow conflicting
//  transactions to proceed with evaluation without the need to first remove
//  all traces of them via a round of replication. This is discussed in more
//  detail in #41720. Specifically, see mention of "contention footprint" and
//  COMMITTED_BUT_NOT_REMOVABLE.
//  Also, resolving these locks/intents would proceed without latching, so we
//  would not rely on MVCC scanning to add discovered locks to the lock table,
//  since the discovered locks may be stale.
//
// The return value is true iff it is actively waiting.
// Acquires l.mu, g.mu.
func (l *lockState) tryActiveWait(
	g *lockTableGuardImpl, sa spanset.SpanAccess, notify bool,
) (wait bool, transitionedToFree bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// It is possible that this lock is empty and has not yet been deleted.
	if l.isEmptyLock() {
		return false, false
	}

	// Lock is not empty.
	lockHolderTxn, lockHolderTS := l.getLockHolder()
	if lockHolderTxn != nil && g.isSameTxn(lockHolderTxn) {
		// Already locked by this txn.
		return false, false
	}

	var replicatedLockFinalizedTxn *roachpb.Transaction
	if lockHolderTxn != nil {
		finalizedTxn, ok := g.lt.finalizedTxnCache.get(lockHolderTxn.ID)
		if ok {
			if l.holder.holder[lock.Replicated].txn == nil {
				// Only held unreplicated. Release immediately.
				l.clearLockHolder()
				if l.lockIsFree() {
					// Empty lock.
					return false, true
				}
				lockHolderTxn = nil
				// There is a reservation holder, which may be the caller itself,
				// so fall through to the processing below.
			} else {
				replicatedLockFinalizedTxn = finalizedTxn
			}
		}
	}

	if sa == spanset.SpanReadOnly {
		if lockHolderTxn == nil {
			// Reads only care about locker, not a reservation.
			return false, false
		}
		// Locked by some other txn.
		if g.ts.Less(lockHolderTS) {
			return false, false
		}
		g.mu.Lock()
		_, alsoHasStrongerAccess := g.mu.locks[l]
		g.mu.Unlock()

		// If the request already has this lock in its locks map, it must also be
		// writing to this key and must be either a reservation holder or inactive
		// waiter at this lock. The former has already been handled above. For the
		// latter, it must have had its reservation broken. Since this is a weaker
		// access we defer to the stronger access and don't wait here.
		//
		// For non-transactional requests that have the key specified as both
		// SpanReadOnly and SpanReadWrite, the request never acquires a
		// reservation, so using the locks map to detect this duplication of the
		// key is not possible. In the rare case, the lock is now held at a
		// timestamp that is not compatible with this request and it will wait
		// here -- there is no correctness issue with doing that.
		if alsoHasStrongerAccess {
			return false, false
		}
	}

	waitForState := waitingState{kind: waitFor, key: l.key}
	if lockHolderTxn != nil {
		waitForState.txn = lockHolderTxn
		waitForState.held = true
	} else {
		if l.reservation == g {
			// Already reserved by this request.
			return false, false
		}
		// A non-transactional write request never makes or breaks reservations,
		// and only waits for a reservation if the reservation has a lower
		// seqNum. Note that `sa == spanset.SpanRead && lockHolderTxn == nil`
		// was already checked above.
		if g.txn == nil && l.reservation.seqNum > g.seqNum {
			// Reservation is held by a request with a higher seqNum and g is a
			// non-transactional request. Ignore the reservation.
			return false, false
		}
		waitForState.txn = l.reservation.txn
	}

	// Incompatible with whoever is holding lock or reservation.

	if l.reservation != nil && sa == spanset.SpanReadWrite && l.tryBreakReservation(g.seqNum) {
		l.reservation = g
		g.mu.Lock()
		g.mu.locks[l] = struct{}{}
		g.mu.Unlock()
		// There cannot be waitingReaders, since they do not wait for
		// reservations. And the set of active queuedWriters has not changed, but
		// they do need to be told about the change in who they are waiting for.
		l.informActiveWaiters()
		return false, false
	}

	// May need to wait.
	wait = true
	g.mu.Lock()
	defer g.mu.Unlock()
	if sa == spanset.SpanReadWrite {
		var qg *queuedGuard
		if _, inQueue := g.mu.locks[l]; inQueue {
			// Already in queue and must be in the right position, so mark as active
			// waiter there. We expect this to be rare.
			for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
				qqg := e.Value.(*queuedGuard)
				if qqg.guard == g {
					qg = qqg
					break
				}
			}
			if qg == nil {
				panic("lockTable bug")
			}
			// Tentative. See below.
			qg.active = true
		} else {
			// Not in queue so insert as active waiter. The active waiter
			// designation is tentative (see below).
			qg = &queuedGuard{
				guard:  g,
				active: true,
			}
			if l.queuedWriters.Len() == 0 {
				l.queuedWriters.PushFront(qg)
			} else {
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
			}
			g.mu.locks[l] = struct{}{}
		}
		if replicatedLockFinalizedTxn != nil && l.queuedWriters.Front().Value.(*queuedGuard) == qg {
			// First waiter, so should not wait. NB: this inactive waiter can be
			// non-transactional.
			qg.active = false
			wait = false
		}
	} else {
		if replicatedLockFinalizedTxn != nil {
			// Don't add to waitingReaders since all readers in waitingReaders are
			// active waiters, and this request is not an active waiter here.
			wait = false
		} else {
			l.waitingReaders.PushFront(g)
			g.mu.locks[l] = struct{}{}
		}
	}
	if !wait {
		g.toResolve = append(
			g.toResolve, roachpb.MakeLockUpdate(replicatedLockFinalizedTxn, roachpb.Span{Key: l.key}))
		return false, false
	}
	// Make it an active waiter.
	g.key = l.key
	g.mu.startWait = true
	waitForState.queuedWriters = l.queuedWriters.Len()
	waitForState.queuedReaders = l.waitingReaders.Len()
	if g.isSameTxnAsReservation(waitForState) {
		state := waitForState
		state.kind = waitSelf
		g.mu.state = state
	} else {
		state := waitForState
		state.guardAccess = sa
		if l.distinguishedWaiter == nil {
			l.distinguishedWaiter = g
			state.kind = waitForDistinguished
		}
		g.mu.state = state
	}
	if notify {
		g.notify()
	}
	return true, false
}

func (l *lockState) isNonConflictingLock(g *lockTableGuardImpl, sa spanset.SpanAccess) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// It is possible that this lock is empty and has not yet been deleted.
	if l.isEmptyLock() {
		return true
	}
	// Lock is not empty.
	lockHolderTxn, lockHolderTS := l.getLockHolder()
	if lockHolderTxn == nil {
		// Reservation holders are non-conflicting.
		//
		// When optimistic evaluation holds latches, there cannot be a conflicting
		// reservation holder that is also holding latches (reservation holder
		// without latches can happen due to lock discovery). So after this
		// optimistic evaluation succeeds and releases latches, the reservation
		// holder will acquire latches and scan the lock table again. When
		// optimistic evaluation does not hold latches, it will check for
		// conflicting latches before declaring success and a reservation holder
		// that holds latches will be discovered, and the optimistic evaluation
		// will retry as pessimistic.
		return true
	}
	if g.isSameTxn(lockHolderTxn) {
		// Already locked by this txn.
		return true
	}
	// NB: We do not look at the finalizedTxnCache in this optimistic evaluation
	// path. A conflict with a finalized txn will be noticed when retrying
	// pessimistically.

	if sa == spanset.SpanReadOnly && g.ts.Less(lockHolderTS) {
		return true
	}
	// Conflicts.
	return false
}

// Acquires this lock. Returns the list of guards that are done actively
// waiting at this key -- these will be requests from the same transaction
// that is acquiring the lock.
// Acquires l.mu.
func (l *lockState) acquireLock(
	_ lock.Strength, durability lock.Durability, txn *enginepb.TxnMeta, ts hlc.Timestamp,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder.locked {
		// Already held.
		beforeTxn, beforeTs := l.getLockHolder()
		if txn.ID != beforeTxn.ID {
			return errors.AssertionFailedf("existing lock cannot be acquired by different transaction")
		}
		seqs := l.holder.holder[durability].seqs
		if l.holder.holder[durability].txn != nil && l.holder.holder[durability].txn.Epoch < txn.Epoch {
			// Clear the sequences for the older epoch.
			seqs = seqs[:0]
		}
		if len(seqs) > 0 && seqs[len(seqs)-1] >= txn.Sequence {
			// Idempotent lock acquisition. In this case, we simply ignore the lock
			// acquisition as long as it corresponds to an existing sequence number.
			// If the sequence number is not being tracked yet, insert it into the
			// sequence history. The validity of such a lock re-acquisition should
			// have already been determined at the MVCC level.
			if i := sort.Search(len(seqs), func(i int) bool {
				return seqs[i] >= txn.Sequence
			}); i == len(seqs) {
				panic("lockTable bug - search value <= last element")
			} else if seqs[i] != txn.Sequence {
				seqs = append(seqs, 0)
				copy(seqs[i+1:], seqs[i:])
				seqs[i] = txn.Sequence
				l.holder.holder[durability].seqs = seqs
			}
			return nil
		}
		l.holder.holder[durability].txn = txn
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
		// instead if assigning to it blindly, it remains at 20.
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
		l.holder.holder[durability].ts.Forward(ts)
		l.holder.holder[durability].seqs = append(seqs, txn.Sequence)

		_, afterTs := l.getLockHolder()
		if beforeTs.Less(afterTs) {
			l.increasedLockTs(afterTs)
		}
		return nil
	}
	// Not already held, so may be reserved by this request. There is also the
	// possibility that some other request has broken this reservation because
	// of a concurrent release but that is harmless since this request is
	// holding latches and has proceeded to evaluation.
	if l.reservation != nil {
		if l.reservation.txn.ID != txn.ID {
			// Reservation is broken.
			qg := &queuedGuard{
				guard:  l.reservation,
				active: false,
			}
			l.queuedWriters.PushFront(qg)
		} else {
			// Else, reservation is not broken, or broken by a different request
			// from the same transaction. In the latter case, both requests are not
			// actively waiting at this lock. We don't know which is in the queue
			// and which is holding the reservation but it does not matter. Both
			// will have their requestGuardImpl.mu.locks updated and neither will be
			// in the queue at the end of this method.
			l.reservation.mu.Lock()
			delete(l.reservation.mu.locks, l)
			l.reservation.mu.Unlock()
		}
		if l.waitingReaders.Len() > 0 {
			panic("lockTable bug")
		}
	} else {
		if l.queuedWriters.Len() > 0 || l.waitingReaders.Len() > 0 {
			panic("lockTable bug")
		}
	}
	l.reservation = nil
	l.holder.locked = true
	l.holder.holder[durability].txn = txn
	l.holder.holder[durability].ts = ts
	l.holder.holder[durability].seqs = append([]enginepb.TxnSeq(nil), txn.Sequence)

	// If there are waiting requests from the same txn, they no longer need to wait.
	l.releaseWritersFromTxn(txn)

	// Inform active waiters since lock has transitioned to held.
	l.informActiveWaiters()
	return nil
}

// A replicated lock held by txn with timestamp ts was discovered by guard g
// where g is trying to access this key with access sa.
// Acquires l.mu.
func (l *lockState) discoveredLock(
	txn *enginepb.TxnMeta,
	ts hlc.Timestamp,
	g *lockTableGuardImpl,
	sa spanset.SpanAccess,
	notRemovable bool,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if notRemovable {
		l.notRemovable++
	}
	if l.holder.locked {
		if !l.isLockedBy(txn.ID) {
			return errors.AssertionFailedf(
				"discovered lock by different transaction (%s) than existing lock (see issue #63592): %s",
				txn, l)
		}
	} else {
		l.holder.locked = true
	}
	holder := &l.holder.holder[lock.Replicated]
	if holder.txn == nil {
		holder.txn = txn
		holder.ts = ts
		holder.seqs = append(holder.seqs, txn.Sequence)
	}

	// Queue the existing reservation holder. Note that this reservation
	// holder may not be equal to g due to two reasons (a) the reservation
	// of g could have been broken even though g is holding latches (see
	// the comment in acquireLock()), (b) g may be a non-transactional
	// request (read or write) that can ignore the reservation.
	if l.reservation != nil {
		qg := &queuedGuard{
			guard:  l.reservation,
			active: false,
		}
		l.queuedWriters.PushFront(qg)
		l.reservation = nil
	}

	switch sa {
	case spanset.SpanReadOnly:
		// Don't enter the lock's queuedReaders list, because all queued readers
		// are expected to be active. Instead, wait until the next scan.

		// Confirm that the guard will wait on the lock the next time it scans
		// the lock table. If not then it shouldn't have discovered the lock in
		// the first place. Bugs here would cause infinite loops where the same
		// lock is repeatedly re-discovered.
		if g.ts.Less(ts) {
			return errors.AssertionFailedf("discovered non-conflicting lock")
		}

	case spanset.SpanReadWrite:
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
	replicatedHeld := l.holder.locked && l.holder.holder[lock.Replicated].txn != nil

	// Remove unreplicated holder.
	l.holder.holder[lock.Unreplicated] = lockHolderInfo{}
	var waitState waitingState
	if replicatedHeld && !force {
		lockHolderTxn, _ := l.getLockHolder()
		// Note that none of the current waiters can be requests
		// from lockHolderTxn.
		waitState = waitingState{
			kind:        waitElsewhere,
			txn:         lockHolderTxn,
			key:         l.key,
			held:        true,
			guardAccess: spanset.SpanReadOnly,
		}
	} else {
		// !replicatedHeld || force. Both are handled as doneWaiting since the
		// system is no longer tracking the lock that was possibly held.
		l.clearLockHolder()
		waitState = waitingState{kind: doneWaiting}
	}

	l.distinguishedWaiter = nil
	if l.reservation != nil {
		g := l.reservation
		g.mu.Lock()
		delete(g.mu.locks, l)
		g.mu.Unlock()
		l.reservation = nil
	}
	for e := l.waitingReaders.Front(); e != nil; {
		g := e.Value.(*lockTableGuardImpl)
		curr := e
		e = e.Next()
		l.waitingReaders.Remove(curr)

		g.mu.Lock()
		g.mu.state = waitState
		g.notify()
		delete(g.mu.locks, l)
		g.mu.Unlock()
	}

	waitState.guardAccess = spanset.SpanReadWrite
	for e := l.queuedWriters.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		curr := e
		e = e.Next()
		l.queuedWriters.Remove(curr)

		g := qg.guard
		g.mu.Lock()
		if qg.active {
			g.mu.state = waitState
			g.notify()
		}
		delete(g.mu.locks, l)
		g.mu.Unlock()
	}
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
	if l.isEmptyLock() {
		// Already free. This can happen when an unreplicated lock is removed in
		// tryActiveWait due to the txn being in the finalizedTxnCache.
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
	for i := range l.holder.holder {
		holder := &l.holder.holder[i]
		if holder.txn == nil {
			continue
		}
		// Note that mvccResolveWriteIntent() has special handling of the case
		// where the pusher is using an epoch lower than the epoch of the intent
		// (replicated lock), but is trying to push to a higher timestamp. The
		// replicated lock gets written with the newer epoch (not the epoch known
		// to the pusher) but a higher timestamp. Then the pusher will call into
		// this function with that lower epoch. Instead of trying to be consistent
		// with mvccResolveWriteIntent() in the current state of the replicated
		// lock we simply forget the replicated lock since it is no longer in the
		// way of this request. Eventually, once we have segregated locks, the
		// lock table will be the source of truth for replicated locks too, and
		// this forgetting behavior will go away.
		//
		// For unreplicated locks the lock table is the source of truth, so we
		// best-effort mirror the behavior of mvccResolveWriteIntent() by updating
		// the timestamp.
		if lock.Durability(i) == lock.Replicated || txn.Epoch > holder.txn.Epoch {
			*holder = lockHolderInfo{}
			continue
		}
		// Unreplicated lock held in same epoch or a higher epoch.
		if advancedTs {
			// We may advance ts here but not update the holder.txn object below
			// for the reason stated in the comment about mvccResolveWriteIntent().
			// The lockHolderInfo.ts is the source of truth regarding the timestamp
			// of the lock, and not TxnMeta.WriteTimestamp.
			holder.ts = ts
		}
		if txn.Epoch == holder.txn.Epoch {
			holder.seqs = removeIgnored(holder.seqs, up.IgnoredSeqNums)
			if len(holder.seqs) == 0 {
				*holder = lockHolderInfo{}
				continue
			}
			if advancedTs {
				holder.txn = txn
			}
		}
		// Else txn.Epoch < lockHolderTxn.Epoch, so only the timestamp has been
		// potentially updated.
		isLocked = true
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
			// Stop waiting.
			l.waitingReaders.Remove(curr)
			if g == l.distinguishedWaiter {
				distinguishedRemoved = true
				l.distinguishedWaiter = nil
			}
			g.doneWaitingAtLock(false, l)
		}
		// Else don't inform an active waiter which continues to be an active waiter
		// despite the timestamp increase.
	}
	if distinguishedRemoved {
		l.tryMakeNewDistinguished()
	}
}

// A request known to this lockState is done. The request could be a reserver,
// or waiting reader or writer. Acquires l.mu. Note that there is the
// possibility of a race and the g may no longer be known to l, which we treat
// as a noop (this race is allowed since we order l.mu > g.mu). Returns whether
// the lockState can be garbage collected.
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

	if l.reservation == g {
		l.reservation = nil
		return l.lockIsFree()
	}
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
	return false
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
	if !l.holder.locked || l.holder.holder[lock.Replicated].txn != nil {
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

// The lock has transitioned from locked/reserved to unlocked. There could be
// waiters, but there cannot be a reservation.
// REQUIRES: l.mu is locked.
func (l *lockState) lockIsFree() (gc bool) {
	if l.holder.locked {
		panic("called lockIsFree on lock with holder")
	}
	if l.reservation != nil {
		panic("called lockIsFree on lock with reservation")
	}

	// All waiting readers don't need to wait here anymore.
	// NB: all waiting readers are by definition active waiters.
	for e := l.waitingReaders.Front(); e != nil; {
		g := e.Value.(*lockTableGuardImpl)
		curr := e
		e = e.Next()
		l.waitingReaders.Remove(curr)
		if g == l.distinguishedWaiter {
			l.distinguishedWaiter = nil
		}
		g.doneWaitingAtLock(false, l)
	}

	// The prefix of the queue that is non-transactional writers is done
	// waiting.
	for e := l.queuedWriters.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		g := qg.guard
		if g.txn == nil {
			curr := e
			e = e.Next()
			l.queuedWriters.Remove(curr)
			if qg.active {
				if g == l.distinguishedWaiter {
					l.distinguishedWaiter = nil
				}
				g.doneWaitingAtLock(false, l)
			} else {
				g.mu.Lock()
				delete(g.mu.locks, l)
				g.mu.Unlock()
			}
		} else {
			break
		}
	}

	if l.queuedWriters.Len() == 0 {
		return true
	}

	// First waiting writer (it must be transactional) gets the reservation.
	e := l.queuedWriters.Front()
	qg := e.Value.(*queuedGuard)
	g := qg.guard
	l.reservation = g
	l.queuedWriters.Remove(e)
	if qg.active {
		if g == l.distinguishedWaiter {
			l.distinguishedWaiter = nil
		}
		g.doneWaitingAtLock(true, l)
	}
	// Else inactive waiter and is waiting elsewhere.

	// Tell the active waiters who they are waiting for.
	l.informActiveWaiters()
	return false
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
		g.sa = spanset.NumSpanAccess - 1
		g.ss = spanset.SpanScope(0)
		g.index = -1
		g.mu.Lock()
		g.mu.startWait = false
		g.mu.mustFindNextLockAfter = false
		g.mu.Unlock()
		g.toResolve = g.toResolve[:0]
	}
	t.doSnapshotForGuard(g)
	g.findNextLockAfter(true /* notify */)
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
	g.txn = req.txnMeta()
	g.ts = req.Timestamp
	g.spans = req.LockSpans
	g.sa = spanset.NumSpanAccess - 1
	g.index = -1
	return g
}

func (t *lockTableImpl) doSnapshotForGuard(g *lockTableGuardImpl) {
	for ss := spanset.SpanScope(0); ss < spanset.NumSpanScope; ss++ {
		for sa := spanset.SpanAccess(0); sa < spanset.NumSpanAccess; sa++ {
			if len(g.spans.GetSpans(sa, ss)) > 0 {
				// Since the spans are constant for a request, every call to
				// ScanAndEnqueue for that request will execute the following code
				// for the same SpanScope(s). Any SpanScope for which this code does
				// not execute will always have an empty snapshot.
				t.locks[ss].mu.RLock()
				g.tableSnapshot[ss].Reset()
				g.tableSnapshot[ss] = t.locks[ss].Clone()
				t.locks[ss].mu.RUnlock()
				break
			}
		}
	}
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
	var locksToGC [spanset.NumSpanScope][]*lockState
	for _, l := range candidateLocks {
		if gc := l.requestDone(g); gc {
			locksToGC[l.ss] = append(locksToGC[l.ss], l)
		}
	}

	for i := 0; i < len(locksToGC); i++ {
		if len(locksToGC[i]) > 0 {
			t.tryGCLocks(&t.locks[i], locksToGC[i])
		}
	}
}

// AddDiscoveredLock implements the lockTable interface.
//
// We discussed in
// https://github.com/cockroachdb/cockroach/issues/62470#issuecomment-818374388
// the possibility of consulting the finalizedTxnCache in AddDiscoveredLock,
// and not adding the lock if the txn is already finalized, and instead
// telling the caller to do batched intent resolution before calling
// ScanAndEnqueue.
// This reduces memory pressure on the lockTableImpl in the extreme case of
// huge numbers of discovered locks. Note that when there isn't memory
// pressure, the consultation of the finalizedTxnCache in the ScanAndEnqueue
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
// finalizedTxnCache eagerly when adding discovered locks.
func (t *lockTableImpl) AddDiscoveredLock(
	intent *roachpb.Intent,
	seq roachpb.LeaseSequence,
	consultFinalizedTxnCache bool,
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
		// be accurate, so we ignore it. However, we still return true so that
		// the request immediately retries, this time under a newer lease.
		return true, nil
	} else if seq > t.enabledSeq {
		// The enableSeq is set synchronously with the application of a new
		// lease, so it should not be possible for a request to evaluate at a
		// higher lease sequence than the current value of enabledSeq.
		return false, errors.AssertionFailedf("unexpected lease sequence: %d > %d", seq, t.enabledSeq)
	}
	g := guard.(*lockTableGuardImpl)
	key := intent.Key
	sa, ss, err := findAccessInSpans(key, g.spans)
	if err != nil {
		return false, err
	}
	if consultFinalizedTxnCache {
		finalizedTxn, ok := t.finalizedTxnCache.get(intent.Txn.ID)
		if ok {
			g.toResolve = append(
				g.toResolve, roachpb.MakeLockUpdate(finalizedTxn, roachpb.Span{Key: key}))
			return true, nil
		}
	}
	var l *lockState
	tree := &t.locks[ss]
	tree.mu.Lock()
	iter := tree.MakeIter()
	iter.FirstOverlap(&lockState{key: key})
	checkMaxLocks := false
	if !iter.Valid() {
		var lockSeqNum uint64
		lockSeqNum, checkMaxLocks = tree.nextLockSeqNum()
		l = &lockState{id: lockSeqNum, key: key, ss: ss}
		l.queuedWriters.Init()
		l.waitingReaders.Init()
		tree.Set(l)
		atomic.AddInt64(&tree.numLocks, 1)
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
	err = l.discoveredLock(&intent.Txn, intent.Txn.WriteTimestamp, g, sa, notRemovableLock)
	// Can't release tree.mu until call l.discoveredLock() since someone may
	// find an empty lock and remove it from the tree.
	tree.mu.Unlock()
	if checkMaxLocks {
		t.checkMaxLocksAndTryClear()
	}
	return true, err
}

// AcquireLock implements the lockTable interface.
func (t *lockTableImpl) AcquireLock(
	txn *enginepb.TxnMeta, key roachpb.Key, strength lock.Strength, durability lock.Durability,
) error {
	t.enabledMu.RLock()
	defer t.enabledMu.RUnlock()
	if !t.enabled {
		// If not enabled, don't track any locks.
		return nil
	}
	if strength != lock.Exclusive {
		return errors.AssertionFailedf("lock strength not Exclusive")
	}
	ss := spanset.SpanGlobal
	if keys.IsLocal(key) {
		ss = spanset.SpanLocal
	}
	var l *lockState
	tree := &t.locks[ss]
	tree.mu.Lock()
	// Can't release tree.mu until call l.acquireLock() since someone may find
	// an empty lock and remove it from the tree. If we expect that lockState
	// will already be in tree we can optimize this by first trying with a
	// tree.mu.RLock().
	iter := tree.MakeIter()
	iter.FirstOverlap(&lockState{key: key})
	checkMaxLocks := false
	if !iter.Valid() {
		if durability == lock.Replicated {
			// Don't remember uncontended replicated locks. The downside is that
			// sometimes contention won't be noticed until when the request
			// evaluates. Remembering here would be better, but our behavior when
			// running into the maxLocks limit is somewhat crude. Treating the
			// data-structure as a bounded cache with eviction guided by contention
			// would be better.
			tree.mu.Unlock()
			return nil
		}
		var lockSeqNum uint64
		lockSeqNum, checkMaxLocks = tree.nextLockSeqNum()
		l = &lockState{id: lockSeqNum, key: key, ss: ss}
		l.queuedWriters.Init()
		l.waitingReaders.Init()
		tree.Set(l)
		atomic.AddInt64(&tree.numLocks, 1)
	} else {
		l = iter.Cur()
		if durability == lock.Replicated && l.tryFreeLockOnReplicatedAcquire() {
			// Don't remember uncontended replicated locks. Just like in the
			// case where the lock is initially added as replicated, we drop
			// replicated locks from the lockTable when being upgraded from
			// Unreplicated to Replicated, whenever possible.
			// TODO(sumeer): now that limited scans evaluate optimistically, we
			// should consider removing this hack. But see the comment in the
			// preceding block about maxLocks.
			tree.Delete(l)
			tree.mu.Unlock()
			atomic.AddInt64(&tree.numLocks, -1)
			return nil
		}
	}
	err := l.acquireLock(strength, durability, txn, txn.WriteTimestamp)
	tree.mu.Unlock()

	if checkMaxLocks {
		t.checkMaxLocksAndTryClear()
	}
	return err
}

func (t *lockTableImpl) checkMaxLocksAndTryClear() {
	var totalLocks int64
	for i := 0; i < len(t.locks); i++ {
		totalLocks += atomic.LoadInt64(&t.locks[i].numLocks)
	}
	if totalLocks > t.maxLocks {
		numToClear := totalLocks - t.minLocks
		t.tryClearLocks(false /* force */, int(numToClear))
	}
}

func (t *lockTableImpl) lockCountForTesting() int64 {
	var totalLocks int64
	for i := 0; i < len(t.locks); i++ {
		totalLocks += atomic.LoadInt64(&t.locks[i].numLocks)
	}
	return totalLocks
}

// tryClearLocks attempts to clear locks.
// - force=false: removes locks until it has removed numToClear locks. It does
//   not remove locks marked as notRemovable.
// - force=true: removes all locks.
// Waiters of removed locks are told to wait elsewhere or that they are done
// waiting.
func (t *lockTableImpl) tryClearLocks(force bool, numToClear int) {
	done := false
	clearCount := 0
	for i := 0; i < int(spanset.NumSpanScope) && !done; i++ {
		tree := &t.locks[i]
		tree.mu.Lock()
		var locksToClear []*lockState
		iter := tree.MakeIter()
		for iter.First(); iter.Valid(); iter.Next() {
			l := iter.Cur()
			if l.tryClearLock(force) {
				locksToClear = append(locksToClear, l)
				clearCount++
				if !force && clearCount >= numToClear {
					done = true
					break
				}
			}
		}
		atomic.AddInt64(&tree.numLocks, int64(-len(locksToClear)))
		if tree.Len() == len(locksToClear) {
			// Fast-path full clear.
			tree.Reset()
		} else {
			for _, l := range locksToClear {
				tree.Delete(l)
			}
		}
		tree.mu.Unlock()
	}
}

// Given the key must be in spans, returns the strongest access
// specified in the spans, along with the scope of the key.
func findAccessInSpans(
	key roachpb.Key, spans *spanset.SpanSet,
) (spanset.SpanAccess, spanset.SpanScope, error) {
	ss := spanset.SpanGlobal
	if keys.IsLocal(key) {
		ss = spanset.SpanLocal
	}
	for sa := spanset.NumSpanAccess - 1; sa >= 0; sa-- {
		s := spans.GetSpans(sa, ss)
		// First span that starts after key
		i := sort.Search(len(s), func(i int) bool {
			return key.Compare(s[i].Key) < 0
		})
		if i > 0 &&
			((len(s[i-1].EndKey) > 0 && key.Compare(s[i-1].EndKey) < 0) || key.Equal(s[i-1].Key)) {
			return sa, ss, nil
		}
	}
	return 0, 0, errors.AssertionFailedf("could not find access in spans")
}

// Tries to GC locks that were previously known to have become empty.
func (t *lockTableImpl) tryGCLocks(tree *treeMu, locks []*lockState) {
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
	ss := spanset.SpanGlobal
	if keys.IsLocal(span.Key) {
		ss = spanset.SpanLocal
	}
	tree := &t.locks[ss]
	var locksToGC []*lockState
	heldByTxn = false
	changeFunc := func(l *lockState) {
		held, gc := l.tryUpdateLock(up)
		heldByTxn = heldByTxn || held
		if gc {
			locksToGC = append(locksToGC, l)
		}
	}
	tree.mu.RLock()
	iter := tree.MakeIter()
	ltRange := &lockState{key: span.Key, endKey: span.EndKey}
	for iter.FirstOverlap(ltRange); iter.Valid(); iter.NextOverlap(ltRange) {
		changeFunc(iter.Cur())
		// Optimization to avoid a second key comparison (not for correctness).
		if len(span.EndKey) == 0 {
			break
		}
	}
	tree.mu.RUnlock()

	if len(locksToGC) > 0 {
		t.tryGCLocks(tree, locksToGC)
	}
	return heldByTxn
}

// Iteration helper for findNextLockAfter. Returns the next span to search
// over, or nil if the iteration is done.
// REQUIRES: g.mu is locked.
func stepToNextSpan(g *lockTableGuardImpl) *spanset.Span {
	g.index++
	for ; g.ss < spanset.NumSpanScope; g.ss++ {
		for ; g.sa >= 0; g.sa-- {
			spans := g.spans.GetSpans(g.sa, g.ss)
			if g.index < len(spans) {
				span := &spans[g.index]
				g.key = span.Key
				return span
			}
			g.index = 0
		}
		g.sa = spanset.NumSpanAccess - 1
	}
	return nil
}

// TransactionIsFinalized implements the lockTable interface.
func (t *lockTableImpl) TransactionIsFinalized(txn *roachpb.Transaction) {
	// TODO(sumeer): We don't take any action for requests that are already
	// waiting on locks held by txn. They need to take some action, like
	// pushing, and resume their scan, to notice the change to this txn. We
	// could be more proactive if we knew which locks in lockTableImpl were held
	// by txn.
	t.finalizedTxnCache.add(txn)
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
	// Also clear the finalized txn cache, since it won't be needed any time
	// soon and consumes memory.
	t.finalizedTxnCache.clear()
}

// For tests.
func (t *lockTableImpl) String() string {
	var sb redact.StringBuilder
	for i := 0; i < len(t.locks); i++ {
		tree := &t.locks[i]
		scope := spanset.SpanScope(i).String()
		tree.mu.RLock()
		sb.Printf("%s: num=%d\n", scope, atomic.LoadInt64(&tree.numLocks))
		iter := tree.MakeIter()
		for iter.First(); iter.Valid(); iter.Next() {
			l := iter.Cur()
			l.mu.Lock()
			l.safeFormat(&sb, &t.finalizedTxnCache)
			l.mu.Unlock()
		}
		tree.mu.RUnlock()
	}
	return sb.String()
}
