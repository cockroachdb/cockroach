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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/google/btree"
	"sync"
	"sync/atomic"
)

type Request interface {
	// nil when not a transactional request -- such requests can only have SpanReadOnly spans and not
	// acquire any locks, but are sequenced through lockTable.
	txnMeta() *enginepb.TxnMeta

	// A SpanAccess of SpanReadWrite allows the requester to acquire an Exclusive lock for a key
	// contained in the corresponding Span (when evaluating). SpanReadOnly spans do not permit any
	// lock acquisition for their contained keys but are sequenced by the lockTable.
	spans() *spanset.SpanSet

	// The timestamp of the request. It is assumed that this is equal to the Span.Timestamp in all of
	// the spans in the SpanSet.
	ts() hlc.Timestamp
}

type LockDurability uint32

const (
	_ LockDurability = 1 << iota
	Replicated
	Unreplicated
	NumDurability
)

type LockStrength uint32

const (
	_ LockStrength = 1 << iota
	Exclusive
)

// A guard that is returned to the request the first time it calls lockTable.scanAndEnqueue() and
// used in later calls to scanAndEnqueue() and done().
// After a call to scanAndEnqueue() (which is made while holding latches), the caller must first
// call requestGuard.startWaiting() and if it returns true release the latches and continue
// interacting with the requestGuard. If startWaiting() returns false, the request can proceed
// to evaluation.
//
// Waiting logic: The interface hides the queues that the request is waiting on, and the request's
// position in the queue. One of the reasons for this hiding is that queues are not FIFO since a
// request that did not wait on a queue for key k in a preceding call to scanAndEnqueue() (because
// k was not locked and there was no queue) may need to wait on the queue in a later call to
// scanAndEnqueue(). So sequencing of requests arriving at the lockTable is partially decided
// by a sequence number assigned to a request when it first called scanAndEnqueue() and queues are
// ordered by this sequence number.
// However the sequencing is not fully described by the sequence numbers -- a request R1 encountering
// contention over some keys in its span does not prevent a request R2 that has a
// higher sequence number and overlapping span to proceed if R2 does not encounter contention. This
// concurrency (that is not quite fair) is deemed desirable.
//
// The interface exposes an abstracted version of the waiting logic in a way that the request that
// starts waiting is considered waiting for at most one other request or transaction. This is exposed
// as a series of state transitions where the transitions are notified via newState() and the current
// state can be read using currentState().
// - The waitFor* states provide information on who the request is waiting for. The
//   waitForDistinguished state is a sub-case -- a distinguished waiter is responsible for taking extra
//   actions e.g. pushing the transaction it is waiting for. The implementation ensures that if
//   there are multiple requests in waitFor state waiting on the same transaction at least one will
//   be a distinguished waiter.
// - The doneWaiting state is used to indicate that the request should make another call to
//   scanAndEnqueue() (that next call is more likely to return a requestGuard that returns false from
//   startWaiting()).
// - The waitElsewhere state is a rare state that is used when the lockTable is under memory
//   pressure and is clearing its internal queue state. Like the waitFor* states, it informs the
//   request who it is waiting for so that deadlock detection works. However, sequencing information
//   inside the lockTable is discarded and a later call to scanAndEnqueue() should use a nil
//   requestGuard.
type requestGuard interface {
	startWaiting() bool
	newState() <-chan struct{}
	currentState() queueState
}

type stateType int

const (
	waitForDistinguished stateType = iota
	waitFor
	doneWaiting
	waitElsewhere
)

type queueState struct {
	stateType stateType

	// Populated for waitFor* and waitElsewhere type, and represents who the request is waiting for.
	txn    *enginepb.TxnMeta // always non-nil
	ts     hlc.Timestamp
	access spanset.SpanAccess // Currently only SpanReadWrite.
}

// Usage:
//
// Request evaluation:
//
// g = nil
// // Outer for loop that repeatedly calls scanAndEnqueue() until request can "evalutate".
// for {
//   acquire all latches for req.spans()
//   // Discovers "all" locks and queues in req.spans() and queues itself where necessary.
//   g, err := lockTable.scanAndEnqueue(..., g)
//   if !g.startWaiting() {
//     // "Evaluate" request while holding latches
//     ...
//     if found an exclusive-replicated lock {
//        lockTable.addDiscoveredLock(...)
//        release latches
//        continue
//     }
//     // May call lockTable.acquireLock() if wants to lock something for later requests in this
//     // transaction, or if placed a write intent and it has been applied to the state machine.
//
//
//     lockTable.done(handle)  // Does not release locks.
//     break
//   }
//   // Either there is a lock held by some other txn for which this request has queued, or
//   // there isn't a lock but this request is not at the front of the queue so needs to wait
//   // its turn.
//
//   release all span latches
//   var timer *time.Timer
//   // Inner for loop that repeats until it is time to call scanAndEnqueue() again, or to return
//   // without "evaluation".
//   for {
//     select {
//     case c <- g.newState():
//       state := g.currentState();
//       // Act on state: deadlock detection, pushing other txns etc.
//       if event.eventType == doneWaiting {
//         break
//       }
//       if event.eventType == waitFor {
//          if timer == nil {
//            // create timer for placing oneself in txn wait queue
//            timer = NewTimer(...)
//          }
//          continue
//       }
//       if event.eventType == waitForDistinguished {
//          Do blocking push // when returns will likely be doneWaiting but not guaranteed.
//          continue
//       }
//       if event.eventType == waitElsewhere {
//         g = nil
//         Do blocking call to put oneself in txn wait queue for txn mentioned in state
//         break
//       }
//     case <- timer.C: // need to put oneself in txn wait queue
//       timer = nil
//       Do blocking call to put oneself in txn wait queue for txn mentioned in state
//       // When return continue waiting on this handle.
//       continue
//     case deadline or cancellation:
//       lockTable.done(g)
//       return
//     }
//   }  // end inner for loop
// }  // end outer for loop
//
// Transaction is done:
//   call lockTable.releaseLocks()
//
// Transaction is pushed and holds some locks:
//   call lockTable.changeLocksTs()
type lockTable interface {
	// Used to find locks and queues to add the request to. If !requestGuard.startWaiting() on the
	// returned requestGuard, proceed to evaluation without releasing latches. Else release the latches and
	// continue interacting with requestGuard. When done waiting on requestGuard, latches need
	// to be reacquired, the next call scanAndEnqueue() should reuse the requestGuard so that the
	// lockTable can fairly order this request that has already waited in case it needs to be added
	// to new queues.
	scanAndEnqueue(req Request, guard requestGuard) (requestGuard, error)

	// Request is done with all the queues it is in, whether it evaluated or not. This causes it
	// to be removed from all the queues. Does not release any locks. This method must be called on
	// the last guard returned from scanAndEnqueue() even if one of the other lockTable calls that
	// used a requestGuard parameter returned an error.
	done(guard requestGuard) error

	// Only permitted for requests that have a non-nil TxnMeta.
	// Must be called in evaluation phase before calling done(). Must be holding latches. This contract
	// ensures that the lock is not held in a conflicting manner by a different transaction. Acquiring a
	// lock that is already held is a noop. The value of strength must be Exclusive.
	//
	// For replicated locks, this must be called after the intent has been committed to the
	// replicated state machine.
	acquireLock(key roachpb.Key, strength LockStrength, durability LockDurability, guard requestGuard) error

	// Can be called during request evaluation or after. If during evaluation do not try to reacquire
	// the lock during the same evaluation.
	//
	// Note that spans can be wider than the actual keys on which locks were acquired, and it is ok
	// if no locks are found or locks held by other transactions are found.
	//
	// For replicated locks, this must be called after the intent removal has been applied to the
	// replicated state machine.
	//
	// TODO(sbhola): should we infer the SpanScope using span.Key?
	releaseLocks(txnID *uuid.UUID, span roachpb.Span, ss spanset.SpanScope) error

	// An exclusive replicated lock held by a different transaction was discovered when reading the
	// MVCC keys during evaluation of this request. Adds the lock and enqueues this requester. It is
	// assumed that request evaluation will discover such locks before acquiring its own locks. The
	// parameter sa is the access needed by guard (it can be computed using key and the SpanSet
	// contained in guard, but that would be expensive).
	addDiscoveredLock(
		key roachpb.Key, txn *enginepb.TxnMeta, ts hlc.Timestamp, guard requestGuard, sa spanset.SpanAccess) error

	// Updates the locks held by txn for span and scope ss, to the current timestamp ts of the
	// transaction. It removes any locks held by older transaction epochs of this transaction.
	//
	// For spans containing Replicated locks, this must be called after the intent timestamp change
	// has been applied to the replicated state machine.
	updateLocks(txn *enginepb.TxnMeta, ts hlc.Timestamp, span roachpb.Span, ss spanset.SpanScope) error
}

// Implementation
// TODO(sbhola):
// - use the cow btree.
// - proper fmt.Errorf strings

type treeMu struct {
	mu sync.RWMutex  // Protects everything in this struct.
	*btree.BTree

	// For constraining memory consumption. We need better memory accounting than this.
	numLocks int64
}

type lockTableImpl struct {
	seqNum uint64

	// Containers for lockState structs. Locks that are not held and have no waiting requests are
	// garbage collected. Additionally, locks that are only held with Replicated durability and have
	// no waiting requests may also be garbage collected since their state can be recovered from
	// persistent storage.
	locks [spanset.NumSpanScope]treeMu

	maxLocks int64
}

func newLockTable(maxLocks int64) lockTable {
	lt := &lockTableImpl{maxLocks: maxLocks}
	for i := 0; i < len(lt.locks); i++ {
		lt.locks[i].BTree = btree.New(16)
	}
	return lt
}

var _ lockTable = &lockTableImpl{}

// Synchronization lock ordering:
// treeMu.mu > lockState.mu > requestGuardImpl.mu
//
// requestGuardImpl.mu could be broken into muScan and muState where muScan > lockState.mu,
// so don't have to release and reacquire it when scanning, but this issue will go away with the
// cow tree so don't bother.
//
// queuedGuard.active is protected by lockState.mu

// Implementation of requestGuard.
type requestGuardImpl struct {
	seqNum uint64
	table  *lockTableImpl

	// Information about this request.
	txn   *enginepb.TxnMeta
	ts    hlc.Timestamp
	spans *spanset.SpanSet

	mu struct {
		sync.Mutex
		startWait bool
		// Information about the key where the request is actively waiting. The key for the lockState
		// is contained in the Span specified by spans[sa][ss][index].
		sa    spanset.SpanAccess
		ss    spanset.SpanScope
		index int
		// The key for the lockState.
		key roachpb.Key

		state  queueState
		signal chan struct{}

		// locks for which this request has a reservation or is in the queue or actively waiting as
		// a reader.
		locks map[*lockState]struct{}

		done bool
	}
}

var _ requestGuard = &requestGuardImpl{}

func (g *requestGuardImpl) startWaiting() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.startWait
}

func (g *requestGuardImpl) newState() <-chan struct{} {
	// TODO(sbhola): make newState() do the work for moving things forward.
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.signal
}
func (g *requestGuardImpl) currentState() queueState {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.state
}

func (g *requestGuardImpl) notify() {
	select {
	case g.mu.signal <- struct{}{}:
	default:
	}
}

type queuedGuard struct {
	guard  *requestGuardImpl
	active bool
}

type holderInfo struct {
	txn *enginepb.TxnMeta
	ts  hlc.Timestamp
}

// Per lock state in lockTableImpl.
type lockState struct {
	key roachpb.Key
	ss  spanset.SpanScope

	mu sync.Mutex // Protects everything below.

	// Invariant summary (see detailed comments below):
	// - both holder.locked and reservation != nil cannot be true.
	// - if holder.locked and multiple holderInfos have txn != nil: all the txns must have the same
	//   txn.ID.
	// - !holder.locked => waitingReaders.Len() == 0. That is, readers wait only if the lock is held.
	//   They do not wait for a reservation.
	// - If reservation != nil, that request is not in queuedWriters.
	holder struct {
		locked bool
		// LockStrength is always Exclusive
		holder [NumDurability]holderInfo
	}

	// A not-held lock can be "reserved". A reservation is just a claim that prevents multiple
	// requests from racing when the lock is released. A reservation by req2 can be broken by req1
	// is req1 has a smaller seqNum than req2. Only requests that specify SpanReadWrite for a key
	// can make reservations. This means a reservation can only be made when the lock is not held,
	// since the reservation (which can acquire an Exclusive lock) and the lock holder (which is
	// an Exclusive lock) conflict.
	//
	// Read reservations are not permitted due to the complexities discussed in the review for
	// #43740. Additionally, for reasons discussed there, reads do not queue for their turn at all --
	// they are held in the waitingReaders list while the lock is held and removed when the lock is
	// not released, so they race with reservations.
	reservation *requestGuardImpl

	// If there is a non-empty set of active waiters, at least one must be distinguished.

	// List of *queuedGuard. A subset of these are actively waiting. If non-empty, either the lock is
	// held or there is a reservation.
	queuedWriters *list.List

	// List of *requestGuardImpl. All of these are actively waiting. If non-empty, the lock must be
	// held.
	waitingReaders *list.List

	distinguishedWaiter *requestGuardImpl
}

func (l *lockState) Less(i btree.Item) bool {
	return l.key.Compare(i.(*lockState).key) < 0
}

// Called for a write request when there is a reservation. Returns true iff it succeeds.
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

// Informs them about reservation or lock holder.
// REQUIRES: l.mu is locked.
func (l *lockState) informActiveWaiters() {
	waitForTxn, waitForTs := l.getLockerInfo()
	if waitForTxn == nil {
		waitForTxn = l.reservation.txn
		waitForTs = l.reservation.ts
	}
	state := queueState{
		stateType: waitFor,
		txn:       waitForTxn,
		ts:        waitForTs,
		access:    spanset.SpanReadWrite,
	}
	for e := l.waitingReaders.Front(); e != nil; e = e.Next() {
		g := e.Value.(*requestGuardImpl)
		g.mu.Lock()
		g.mu.state = state
		if l.distinguishedWaiter == g {
			g.mu.state.stateType = waitForDistinguished
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
		g.mu.Lock()
		g.mu.state = state
		if l.distinguishedWaiter == g {
			g.mu.state.stateType = waitForDistinguished
		}
		g.notify()
		g.mu.Unlock()
	}
}

// When the active waiters have shrunk and the distinguished waiter has gone, try to make a new
// distinguished waiter if there is at least 1 active waiter.
// REQUIRES: l.mu is locked.
func (l *lockState) tryMakeNewDistinguished() {
	var g *requestGuardImpl
	if l.waitingReaders.Len() > 0 {
		g = l.waitingReaders.Front().Value.(*requestGuardImpl)
	} else if l.queuedWriters.Len() > 0 {
		for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
			qg := e.Value.(*queuedGuard)
			if qg.active {
				g = qg.guard
				break
			}
		}
	}
	if g != nil {
		l.distinguishedWaiter = g
		g.mu.Lock()
		g.mu.state.stateType = waitForDistinguished
		// The rest of g.state is already up-to-date.
		g.notify()
		g.mu.Unlock()
	}
}

// Request is already in the queuedWriters or waitingReaders.
// REQUIRES: l.mu and g.mu are locked.
func (l *lockState) makeActiveWaiter(
	g *requestGuardImpl, waitForTxn *enginepb.TxnMeta, waitForTs hlc.Timestamp) {
	g.mu.key = l.key
	g.mu.startWait = true
	stateType := waitFor
	if l.distinguishedWaiter == nil {
		l.distinguishedWaiter = g
		stateType = waitForDistinguished
	}
	g.mu.state = queueState{
		stateType: stateType,
		txn:       waitForTxn,
		ts:        waitForTs,
		access:    spanset.SpanReadWrite,
	}
	g.notify()
}

// REQUIRES: l.mu is locked.
func (l *lockState) isLockedBy(id *uuid.UUID) bool {
	if l.holder.locked {
		var holderID *uuid.UUID
		if l.holder.holder[Unreplicated].txn != nil {
			holderID = &l.holder.holder[Unreplicated].txn.ID
		} else {
			holderID = &l.holder.holder[Replicated].txn.ID
		}
		return *id == *holderID
	}
	return false
}

// Called only when lockState.holder.locked is true.
// REQUIRES: l.mu is locked.
func (l *lockState) getLockerInfo() (*enginepb.TxnMeta, hlc.Timestamp) {
	index := Replicated
	if l.holder.holder[index].txn == nil || (l.holder.holder[Unreplicated].txn != nil &&
		l.holder.holder[Unreplicated].ts.Less(l.holder.holder[Replicated].ts)) {
		index = Unreplicated
	}
	return l.holder.holder[index].txn, l.holder.holder[index].ts
}

// Acquires l.mu
func (l *lockState) tryActiveWait(g *requestGuardImpl, sa spanset.SpanAccess) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// It is possible that this lock is empty and has not yet been deleted.
	empty, err := l.isEmptyLock()
	if err != nil {
		return false, err
	}
	if empty {
		return false, nil
	}

	// Lock is not empty.

	if g.txn != nil && l.isLockedBy(&g.txn.ID) {
		return false, nil
	}

	if l.reservation == g {
		// Already reserved for write.
		if sa != spanset.SpanReadWrite {
			return false, fmt.Errorf("lockTable bug or caller has used overlapping spans for SpanReadOnly and SpanReadWrite")
		}
		return false, nil
	}

	if l.reservation != nil && sa == spanset.SpanReadWrite && l.tryBreakReservation(g.seqNum) {
		l.reservation = g
		// There cannot be waitingReaders, since they do not wait for reservations. And the set of active
		// queuedWriters has not changed, but they do need to be told about the change in who they are
		// waiting for.
		l.informActiveWaiters()
		return false, nil
	}

	// Need to wait.
	waitForTxn, waitForTs := l.getLockerInfo()
	if waitForTxn == nil {
		waitForTxn = l.reservation.txn
		waitForTs = l.reservation.ts
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if sa == spanset.SpanReadWrite {
		if _, inQueue := g.mu.locks[l]; inQueue {
			// Already in queue and must be in the right position, so mark as active waiter there.
			var qg *queuedGuard
			for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
				qqg := e.Value.(*queuedGuard)
				if qqg.guard == g {
					qg = qqg
					break
				}
			}
			if qg == nil {
				return false, fmt.Errorf("lockTable bug")
			}
			qg.active = true
		} else {
			// Not in queue so insert as active waiter.
			qg := &queuedGuard{
				guard:  g,
				active: true,
			}
			if l.queuedWriters.Len() == 0 {
				l.queuedWriters.PushFront(qg)
			} else {
				var e *list.Element
				for e = l.queuedWriters.Front(); e != nil; e = e.Next() {
					qqg := e.Value.(*queuedGuard)
					if qqg.guard.seqNum > qg.guard.seqNum {
						break
					}
				}
				l.queuedWriters.InsertBefore(qg, e)
			}
		}
	} else {
		l.waitingReaders.PushFront(g)
	}
	l.makeActiveWaiter(g, waitForTxn, waitForTs)
	return true, nil
}

// Acquires l.mu
func (l *lockState) acquireLock(_ LockStrength, durability LockDurability, g *requestGuardImpl) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.holder.locked {
		// Already held.
		if !l.isLockedBy(&g.txn.ID) {
			return fmt.Errorf("caller violated contract")
		}
		if l.holder.holder[durability].txn == nil {
			l.holder.holder[durability].txn = g.txn
			l.holder.holder[durability].ts = g.ts
		}
		// Else, already held with same durability. We don't update the TxnMeta -- that should be done
		// via updateLock().
		return nil
	}
	// Not already held, so must have reserved.
	if l.reservation != g {
		return fmt.Errorf("caller violated contract")
	}
	l.reservation = nil
	l.holder.locked = true
	l.holder.holder[durability].txn = g.txn
	l.holder.holder[durability].ts = g.ts
	// No effect on queuedWriters.

	g.mu.Lock()
	delete(g.mu.locks, l)
	g.mu.Unlock()
	return nil
}

// Acquires l.mu.
func (l *lockState) discoveredLock(
	txn *enginepb.TxnMeta, ts hlc.Timestamp, g *requestGuardImpl, sa spanset.SpanAccess) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	informWaiters := true
	if l.holder.locked {
		if !l.isLockedBy(&txn.ID) {
			return fmt.Errorf("bug in caller or lockTable")
		}
		if l.holder.holder[Replicated].txn == nil {
			l.holder.holder[Replicated].txn = txn
			l.holder.holder[Replicated].ts = ts
		}
		informWaiters = false
	} else {
		l.holder.locked = true
		l.holder.holder[Replicated].txn = txn
		l.holder.holder[Replicated].ts = ts
	}
	// Queue the existing reservation holder.
	var hadReservation bool
	if l.reservation != nil {
		if l.reservation == g {
			hadReservation = true
		} else if sa == spanset.SpanReadWrite {
			// There was a reservation and it was not this request -- this is wrong since this request
			// should not have evaluated and discovered this lock.
			return fmt.Errorf("caller violated contract")
		}
		qg := &queuedGuard{
			guard:  l.reservation,
			active: false,
		}
		l.queuedWriters.PushFront(qg)
		l.reservation = nil
	} else {
		// No reservation, so either the lock was already known to be held in which the active waiters
		// know about the holder, or it was not held and so there are no active waiters.
		informWaiters = false
	}

	if !hadReservation && sa == spanset.SpanReadWrite {
		// Put self in queue as inactive waiter. Since did not have the reservation the lock must not
		// have been known to be held so the queue must be empty.
		if l.queuedWriters.Len() > 0 {
			return fmt.Errorf("lockTable bug")
		}
		qg := &queuedGuard{
			guard:  g,
			active: false,
		}
		l.queuedWriters.PushFront(qg)
		g.mu.Lock()
		g.mu.locks[l] = struct{}{}
		g.mu.Unlock()
		informWaiters = false
	}

	// Active waiters need to be told about who they are waiting for.
	if informWaiters {
		l.informActiveWaiters()
	}
	return nil
}

// Acquires l.mu.
func (l *lockState) tryReleaseLock(txnID *uuid.UUID) (doneWaiting []*requestGuardImpl, gc bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isLockedBy(txnID) {
		return nil, false, nil
	}
	l.holder.locked = false
	for i := 0; i < int(NumDurability); i++ {
		l.holder.holder[i].txn = nil
		l.holder.holder[i].ts = hlc.Timestamp{}
	}
	return l.lockIsFree()
}

// Acquires l.mu.
func (l *lockState) tryClearLock() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.distinguishedWaiter == nil {
		// No active waiter
		return false
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
		g := e.Value.(*requestGuardImpl)
		curr := e
		e = e.Next()
		l.waitingReaders.Remove(curr)

		g.mu.Lock()
		g.mu.state.stateType = waitElsewhere
		g.notify()
		delete(g.mu.locks, l)
		g.mu.Unlock()
	}
	for e := l.queuedWriters.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		curr := e
		e = e.Next()
		l.queuedWriters.Remove(curr)

		g := qg.guard
		g.mu.Lock()
		delete(g.mu.locks, l)
		if qg.active {
			g.mu.state.stateType = waitElsewhere
			g.notify()
		}
		g.mu.Unlock()
	}
	return true
}

// REQUIRES: l.mu.
func (l *lockState) isEmptyLock() (bool, error) {
	if !l.holder.locked && l.reservation == nil {
		if l.waitingReaders.Len() > 0 || l.queuedWriters.Len() > 0 {
			return false, fmt.Errorf("lockTable bug")
		}
		return true, nil
	}
	return false, nil
}

// Acquires l.mu.
func (l *lockState) tryUpdateLock(
	txn *enginepb.TxnMeta, ts hlc.Timestamp) (doneWaiting []*requestGuardImpl, gc bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isLockedBy(&txn.ID) {
		return nil, false, nil
	}
	_, beforeTs := l.getLockerInfo()
	isLocked := false
	for i := 0; i < int(NumDurability); i++ {
		if l.holder.holder[i].txn == nil {
			continue
		}
		if txn.Epoch > l.holder.holder[i].txn.Epoch {
			l.holder.holder[i].txn = nil
		} else {
			l.holder.holder[i].txn = txn
			l.holder.holder[i].ts = ts
			isLocked = true
		}
	}

	if !isLocked {
		l.holder.locked = false
		return l.lockIsFree()
	} else {
		_, afterTs := l.getLockerInfo()
		if afterTs.Less(beforeTs) {
			return nil, false, fmt.Errorf("caller violated contract")
		} else if beforeTs.Less(afterTs) {
			doneWaiting := l.increasedLockTs(afterTs)
			return doneWaiting, false, nil
		}
	}
	return nil, false, nil
}

// The lock holder timestamp has increased.
// REQUIRES: l.mu is locked.
func (l *lockState) increasedLockTs(newTs hlc.Timestamp) []*requestGuardImpl {
	var doneWaiting []*requestGuardImpl
	distinguishedRemoved := false
	for e := l.waitingReaders.Front(); e != nil; {
		g := e.Value.(*requestGuardImpl)
		curr := e
		e = e.Next()
		if !g.ts.LessEq(newTs) {
			// Stop waiting.
			l.waitingReaders.Remove(curr)
			if g == l.distinguishedWaiter {
				distinguishedRemoved = true
				l.distinguishedWaiter = nil
			}
			g.mu.Lock()
			delete(g.mu.locks, l)
			g.mu.Unlock()
			doneWaiting = append(doneWaiting, g)
		}
	}
	if distinguishedRemoved {
		l.tryMakeNewDistinguished()
	}
	// Don't inform other active waiters about increased timestamp, since it does not change their
	// situation.
	return doneWaiting
}

// A request known to this lockState is done. The request could be a reserver, or waiting reader
// or writer. Acquires l.mu. Note that there is the possibility of a race and the g may no longer
// be known to l, which we treat as a noop (this race is allowed since we order l.mu > g.mu).
func (l *lockState) requestDone(g *requestGuardImpl) (doneWaiting []*requestGuardImpl, gc bool, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	g.mu.Lock()
	if _, present := g.mu.locks[l]; !present {
		g.mu.Unlock()
		return nil, false, nil
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
			gg := e.Value.(*requestGuardImpl)
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
		return nil, false, fmt.Errorf("lockTable bug")
	}
	if distinguishedRemoved {
		l.tryMakeNewDistinguished()
	}
	return nil, false, nil
}

// The lock has transitioned from locked to unlocked. There could be waiters, but there cannot be
// a reservation.
// REQUIRES: l.mu is locked.
func (l *lockState) lockIsFree() (doneWaiting []*requestGuardImpl, gc bool, err error) {
	if l.reservation != nil {
		return nil, false, fmt.Errorf("lockTable bug")
	}
	// All waiting readers don't need to wait here anymore.
	distinguishedRemoved := false
	for e := l.waitingReaders.Front(); e != nil; {
		g := e.Value.(*requestGuardImpl)
		curr := e
		e = e.Next()
		l.waitingReaders.Remove(curr)
		if g == l.distinguishedWaiter {
			distinguishedRemoved = true
			l.distinguishedWaiter = nil
		}
		g.mu.Lock()
		delete(g.mu.locks, l)
		g.mu.Unlock()
		doneWaiting = append(doneWaiting, g)
	}
	// First waiting writer gets the reservation.
	if l.queuedWriters.Len() > 0 {
		e := l.queuedWriters.Front()
		g := e.Value.(*queuedGuard).guard
		l.reservation = g
		l.queuedWriters.Remove(e)
		if g == l.distinguishedWaiter {
			distinguishedRemoved = true
			l.distinguishedWaiter = nil
		}
		doneWaiting = append(doneWaiting, g)

		// Need to tell the remaining active waiting writers who they are waiting for.
		state := queueState{
			stateType: waitFor,
			txn:       g.txn,
			ts:        g.ts,
			access:    spanset.SpanReadWrite,
		}
		for e := l.queuedWriters.Front(); e != nil; e = e.Next() {
			qg := e.Value.(*queuedGuard)
			if qg.active {
				g := qg.guard
				if distinguishedRemoved {
					l.distinguishedWaiter = g
					distinguishedRemoved = false
				}
				g.mu.Lock()
				g.mu.state = state
				if l.distinguishedWaiter == g {
					g.mu.state.stateType = waitForDistinguished
				}
				g.notify()
				g.mu.Unlock()
			}
		}
		return doneWaiting, false, nil
	} else {
		return doneWaiting, true, nil
	}
}

func (t *lockTableImpl) scanAndEnqueue(req Request, guard requestGuard) (requestGuard, error) {
	var g *requestGuardImpl
	if guard == nil {
		seqNum := atomic.AddUint64(&t.seqNum, 1)
		g = &requestGuardImpl{
			seqNum: seqNum,
			txn:    req.txnMeta(),
			ts:     req.ts(),
			spans:  req.spans(),
			table:  t,
		}
		g.mu.index = -1
		g.mu.signal = make(chan struct{})
		g.mu.locks = make(map[*lockState]struct{})
	} else {
		g = guard.(*requestGuardImpl)
		g.mu.Lock()
		g.mu.index = -1
		g.mu.sa = spanset.SpanAccess(0)
		g.mu.ss = spanset.SpanScope(0)
		g.mu.startWait = false
		g.mu.Unlock()
	}
	err := t.findNextLockAfter(g)
	if err != nil {
		_ = t.done(g)
		return nil, err
	}
	return g, nil
}

func (t *lockTableImpl) done(guard requestGuard) error {
	g := guard.(*requestGuardImpl)
	var candidateLocks []*lockState
	g.mu.Lock()
	g.mu.done = true
	for l := range g.mu.locks {
		candidateLocks = append(candidateLocks, l)
	}
	g.mu.Unlock()
	var doneWaiting []*requestGuardImpl
	var locksToGC [spanset.NumSpanScope][]*lockState
	var err error
	for _, l := range candidateLocks {
		dw, gc, err2 := l.requestDone(g)
		doneWaiting = append(doneWaiting, dw...)
		err = firstError(err, err2)
		if gc {
			locksToGC[l.ss] = append(locksToGC[l.ss], l)
		}
	}

	for i := 0; i < len(locksToGC); i++ {
		if len(locksToGC[i]) > 0 {
			err = firstError(err, t.tryGCLocks(&t.locks[i], locksToGC[i]))
		}
	}
	for _, g = range doneWaiting {
		err = firstError(err, t.findNextLockAfter(g))
	}
	if err != nil {
		// This is a lockTable bug. Get rid of active waiters.
		t.clearMostLocks()
	}
	return err
}

func (t *lockTableImpl) acquireLock(
	key roachpb.Key, strength LockStrength, durability LockDurability, guard requestGuard) error {
	if strength != Exclusive {
		return fmt.Errorf("caller violated contract")
	}
	g := guard.(*requestGuardImpl)
	ss := spanset.SpanGlobal
	if keys.IsLocal(key) {
		ss = spanset.SpanLocal
	}
	var l *lockState
	tree := &t.locks[ss]
	tree.mu.Lock()
	// Can't release tree.mu until call l.acquireLock() since someone may find an empty lock and
	// remove it from the tree. If we expect that lockState will already be in tree we can optimize
	// this by first trying with a tree.mu.RLock().
	i := tree.Get(&lockState{key: key})
	if i == nil {
		if durability == Replicated {
			// Don't remember uncontended replicated locks.
			return nil
		}
		l = &lockState{key: key, ss: ss, queuedWriters: list.New(), waitingReaders: list.New()}
		tree.ReplaceOrInsert(l)
		atomic.AddInt64(&tree.numLocks, 1)
	} else {
		l = i.(*lockState)
	}
	err := l.acquireLock(strength, durability, g)
	tree.mu.Unlock()

	var totalLocks int64
	for i := 0; i < len(t.locks); i++ {
		totalLocks += atomic.LoadInt64(&t.locks[i].numLocks)
	}
	if totalLocks > t.maxLocks {
		t.clearMostLocks()
	}
	return err
}

func (t *lockTableImpl) clearMostLocks() {
	// Remove all locks that have active waiters. A replicated lock which has been discovered by a
	// request but the request is not yet actively waiting on it will be preserved.
	for i := 0; i < int(spanset.NumSpanScope); i++ {
		tree := &t.locks[i]
		var cleared int64
		tree.mu.Lock()
		tree.Ascend(func(it btree.Item) bool {
			l := it.(*lockState)
			if l.tryClearLock() {
				tree.Delete(l)
				cleared++
			}
			return true
		})
		atomic.AddInt64(&tree.numLocks, -cleared)
		tree.mu.Unlock()
	}
}

func (t *lockTableImpl) releaseLocks(txnID *uuid.UUID, span roachpb.Span, ss spanset.SpanScope) error {
	tree := &t.locks[ss]
	var err error
	var doneWaiting []*requestGuardImpl
	var locksToGC []*lockState
	releaseFunc := func(i btree.Item) bool {
		l := i.(*lockState)
		dw, gc, err2 := l.tryReleaseLock(txnID)
		doneWaiting = append(doneWaiting, dw...)
		err = firstError(err, err2)
		if gc {
			locksToGC = append(locksToGC, l)
		}
		return true
	}
	tree.mu.RLock()
	if len(span.EndKey) > 0 {
		tree.AscendRange(&lockState{key: span.Key}, &lockState{key: span.EndKey}, releaseFunc)
	} else {
		if i := tree.Get(&lockState{key: span.Key}); i != nil {
			releaseFunc(i)
		}
	}
	tree.mu.RUnlock()
	if len(locksToGC) > 0 {
		err = firstError(err, t.tryGCLocks(tree, locksToGC))
	}
	for _, g := range doneWaiting {
		err = firstError(err, t.findNextLockAfter(g))
	}
	if err != nil {
		// This is a lockTable bug. Get rid of active waiters.
		t.clearMostLocks()
	}
	return err
}

func (t *lockTableImpl) addDiscoveredLock(
	key roachpb.Key, txn *enginepb.TxnMeta, ts hlc.Timestamp, guard requestGuard, sa spanset.SpanAccess) error {
	g := guard.(*requestGuardImpl)
	ss := spanset.SpanGlobal
	if keys.IsLocal(key) {
		ss = spanset.SpanLocal
	}
	var l *lockState
	tree := &t.locks[ss]
	tree.mu.Lock()
	// Can't release tree.mu until call l.discoveredLock() since someone may find an empty lock and
	// remove it from the tree. If we expect that lockState will already be in tree we can optimize
	// this by first trying with a tree.mu.RLock().
	defer tree.mu.Unlock()
	i := tree.Get(&lockState{key: key})
	if i == nil {
		l = &lockState{key: key, ss: ss, queuedWriters: list.New(), waitingReaders: list.New()}
		tree.ReplaceOrInsert(l)
		atomic.AddInt64(&tree.numLocks, 1)
	} else {
		l = i.(*lockState)
	}
	return l.discoveredLock(txn, ts, g, sa)
}

func (t *lockTableImpl) tryGCLocks(tree *treeMu, locks []*lockState) error {
	var err error
	tree.mu.Lock()
	defer tree.mu.Unlock()
	for _, l := range locks {
		i := tree.Get(l)
		// Since the same lockState can go from non-empty to empty multiple times
		// it is possible that multiple threads are racing to delete it and multiple find it empty
		// and one wins. If a concurrent thread made the lockState non-empty, or if it added a different
		// lockState for the same key we do now want to delete it accidentally if it is non-empty.
		if i == nil {
			continue
		}
		l = i.(*lockState)
		l.mu.Lock()
		empty, err2 := l.isEmptyLock()
		l.mu.Unlock()
		err = firstError(err, err2)
		if err2 == nil && empty {
			tree.Delete(l)
			atomic.AddInt64(&tree.numLocks, -1)
		}
	}
	return err
}

func (t *lockTableImpl) updateLocks(
	txn *enginepb.TxnMeta, ts hlc.Timestamp, span roachpb.Span, ss spanset.SpanScope) error {
	tree := &t.locks[ss]
	var err error
	var doneWaiting []*requestGuardImpl
	var locksToGC []*lockState
	changeFunc := func(i btree.Item) bool {
		l := i.(*lockState)
		dw, gc, err2 := l.tryUpdateLock(txn, ts)
		doneWaiting = append(doneWaiting, dw...)
		err = firstError(err, err2)
		if gc {
			locksToGC = append(locksToGC, l)
		}
		return true
	}
	tree.mu.RLock()
	if len(span.EndKey) > 0 {
		tree.AscendRange(&lockState{key: span.Key}, &lockState{key: span.EndKey}, changeFunc)
	} else {
		if i := tree.Get(&lockState{key: span.Key}); i != nil {
			changeFunc(i)
		}
	}
	tree.mu.RUnlock()
	if len(locksToGC) > 0 {
		err = firstError(err, t.tryGCLocks(tree, locksToGC))
	}
	for _, g := range doneWaiting {
		err = firstError(err, t.findNextLockAfter(g))
	}
	// Non-nil error could be due to lockTable bug or caller violating contract. Could differentiate
	// between them here and call clearMostLocks() for the former case.
	return err
}

// REQUIRES: g.mu is locked.
func stepToNextSpan(g *requestGuardImpl) *spanset.Span {
	spans := g.spans.GetSpans(g.mu.sa, g.mu.ss)
	g.mu.index++
	for g.mu.index == len(spans) {
		// Step to next in (sa, ss).
		g.mu.ss++
		if g.mu.ss == spanset.NumSpanScope {
			g.mu.ss = spanset.SpanScope(0)
			g.mu.sa++
		}
		if g.mu.sa == spanset.NumSpanAccess {
			return nil
		}
		spans = g.spans.GetSpans(g.mu.sa, g.mu.ss)
		g.mu.index = 0
	}
	span := &g.spans.GetSpans(g.mu.sa, g.mu.ss)[g.mu.index]
	g.mu.key = span.Key
	return span
}

// Acquires g.mu. Acquires treeMu.mu's in read mode.
func (t *lockTableImpl) findNextLockAfter(g *requestGuardImpl) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.mu.done {
		return nil
	}
	spans := g.spans.GetSpans(g.mu.sa, g.mu.ss)
	var span *spanset.Span
	resumingInSameSpan := false
	if g.mu.index == -1 || len(spans[g.mu.index].EndKey) == 0 {
		span = stepToNextSpan(g)
	} else {
		span = &spans[g.mu.index]
		resumingInSameSpan = true
	}
	for span != nil {
		tree := &t.locks[g.mu.ss]
		sa := g.mu.sa
		g.mu.Unlock()
		if len(span.EndKey) == 0 {
			// NB: !resumingInSameSpan
			tree.mu.RLock()
			i := tree.Get(&lockState{key: span.Key})
			tree.mu.RUnlock()
			if i != nil {
				l := i.(*lockState)
				waiting, err := l.tryActiveWait(g, sa)
				g.mu.Lock()
				if err != nil {
					return err
				}
				if waiting {
					return nil
				}
			} else {
				g.mu.Lock()
			}
		} else {
			startKey := span.Key
			if resumingInSameSpan {
				startKey = g.mu.key
			}
			waiting := false
			var err error
			tree.mu.RLock()
			tree.AscendRange(&lockState{key: startKey}, &lockState{key: span.EndKey},
				func(i btree.Item) bool {
					if resumingInSameSpan {
						resumingInSameSpan = false
						return true
					}
					l := i.(*lockState)
					waiting, err = l.tryActiveWait(g, sa)
					if err != nil || waiting {
						return false
					}
					return true
				})
			tree.mu.RUnlock()
			g.mu.Lock()
			if err != nil {
				return err
			}
			if waiting {
				return nil
			}
		}
		span = stepToNextSpan(g)
	}
	g.mu.state.stateType = doneWaiting
	g.notify()
	return nil
}

func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
