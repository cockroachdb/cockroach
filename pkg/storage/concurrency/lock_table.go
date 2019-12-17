package concurrency

import (
	"container/list"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/google/btree"
)

type Request interface {
	// nil when not a transactional request.
	txnMeta() *enginepb.TxnMeta
	spans() *spanset.SpanSet
	// Equal to the Span.Timestamp in all of the spans in the SpanSet.
	ts() hlc.Timestamp
}

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
// higher sequence number and overlapping span to proceed if R2 does not encounter contention.
// If we did not desire such concurrency, the existing latches would be sufficient for sequencing
// and could be held until the request was able to evaluate.
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
	txn    *enginepb.TxnMeta // nil for a non-transaction
	ts     hlc.Timestamp
	access spanset.SpanAccess // Currently only SpanReadWrite.
}

// Request evaluation usage (rough):
//
// g = nil
// for {
//   acquire all latches for req.spans()
//   // Discovers "all" locks and queues in req.spans() and queues itself where necessary.
//   g, err := lockTable.scanAndEnqueue(..., g)
//   releasedLatches := false
//   if g.startWaiting() {
//     // Either there is a lock held by some other txn for which this request has queued, or
//     // there isn't a lock but this request is not at the front of the queue so needs to wait
//     // its turn.
//     release all span latches
//     releasedLatches = true
//   }
//   var timer *time.Timer
//   retryScan = false
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
//   }
//   if releasedLatches {
//     // Have to reacquire latches and repeat in case some other request slipped ahead and
//     // created a lock on a key that was not previously locked (so this request did not wait
//     // on that key).
//     continue
//   }
//   // "Evaluate" request while holding latches
//   ...
//   if found an exclusive-replicated lock {
//      lockTable.addDiscoveredExclusiveReplicatedLock(...)
//      release latches
//      continue
//   }
//   // May call lockTable.acquireExclusiveUnreplicatedLocks() if wants to lock something for
//   // later requests in this transaction.
//   // May call lockTable.exclusiveReplicatedLockAcquired() if placed a write intent and it
//   // has been applied to the state machine.
//
//   if doneWithEvaluation {
//     lockTable.done(handle)  // Does not release locks.
//   }
// }
//
// Transaction is done:
//  call releaseExclusiveUnreplicatedLocks(), exclusiveReplicatedLocksReleased()
//
// Transaction is pushed and holds some locks:
//  call lockTable.changeLocksTs()
type lockTable interface {
	// Used to find locks and queues to add the request to. If !requestGuard.startWaiting() on the
	// returned requestGuard, proceed to evaluation without releasing latches. Else release the latches and
	// continue interacting with requestGuard. When done waiting on requestGuard, latches need
	// to be reacquired, the next call scanAndEnqueue() should reuse the requestGuard so that the
	// lockTable can fairly order this request that has already waited in case it needs to be added
	// to new queues.
	scanAndEnqueue(req Request, guard requestGuard) (requestGuard, error)

	// Request is done with all the queues it is in, whether it evaluated or not. This causes it
	// to be removed from all the queues. Does not release any locks.
	done(guard requestGuard) error

	// Lock "upgrade":
	// - already hold an exclusive-unreplicated lock and then acquire an exclusive-replicated lock:
	//   lock table may choose to remember the former since harmless -- we expect both release methods
	//   below to be called.
	// - hold an exclusive-replicated lock but lock table does not know about it. It lets the same
	//   transaction acquire an exclusive-unreplicated lock -- this is also harmless.
	//
	// Locks can only be acquired/released for transactional requests.

	// Exclusive-unreplicated locks.
	//
	// Must be called in evaluation phase before calling done().
	// Must be holding latches. Acquiring a lock that is already held is a noop (including the case
	// where the caller already holds an exclusive-replicated lock). This call is used for SFU.
	acquireExclusiveUnreplicatedLocks(keys []roachpb.Key, guard requestGuard) error

	// Can be called during request evaluation or after. If during evaluation do not try to reacquire
	// the lock during the same evaluation (it makes the lockTable implementation more complicated).
	// Note that spans can be wider than the actual keys on which locks were acquired, and it is ok
	// if no locks are found.
	releaseExclusiveUnreplicatedLocks(txnID *uuid.UUID, spans *spanset.SpanSet) error

	// Exclusive-replicated locks. These will be cleaner in the future with the segregated lock table.
	//
	// An exclusive lock held by a different transaction was discovered when reading the MVCC keys.
	// Adds the lock and enqueues this requester.
	addDiscoveredExclusiveReplicatedLock(
		key roachpb.Key, txn *enginepb.TxnMeta, ts hlc.Timestamp, guard requestGuard) error

	// Called after the intent has been committed to the replicated state machine by this request.
	exclusiveReplicatedLockAcquired(key roachpb.Key, guard requestGuard) error

	// This will be called after the intent removal has been applied to the replicated state machine.
	exclusiveReplicatedLocksReleased(txnID *uuid.UUID, spans *spanset.SpanSet) error

	// Changes the lock timestamps for a transaction that is pushed. This can permit some readers
	// to proceed. The spans include both the ones for exclusive-replicated and unreplicated locks.
	// For exclusive-replicated this is informational in that it can be used to change the state of
	// queues (the source of truth in the state machine has already been updated), while for
	// unreplicated locks the lockTable is responsible for the source of truth.
	changeLocksTs(txnID *uuid.UUID, ts hlc.Timestamp, replicated bool, spans *spanset.SpanSet) error
}

// Implementation
// TODO(sbhola):
// - synchronization
// - fmt.Errorf strings
// - implementation of persistence functions
// - refactoring to reduce code duplication in behavior for replicated and unreplicated locks
//   in lockTableImpl functions.
// - define proto to store as the persistent lock value for the unreplicated locks. The code below
//   uses txnMeta since we only have exclusive locks but we should make it future-proof. And the use
//   of WriteTimestamp is probably wrong.
// - Clarify whether queues need to be preserved when ranges split or merged (not if requests are
//   specific to a particular replica and need to be reissued). Add a ClearPersistentState()
//   function to lockTable when it is no longer the leaseholder for the replica.

type lockTableImpl struct {
	nextSeqNum uint64
	// Containers for lockState structs
	locks [spanset.NumSpanScope]btree.BTree
	// To read/write persistent state of exclusive-unreplicated locks -- used to initialize lockState
	// for such locks.
	engine engine.Engine

	// For constraining memory consumption.
	numInMemLocks   int
	numQueueEntries int
}

var _ lockTable = &lockTableImpl{}

// Persistence methods that use lockTableImpl.engine.
// TODO: write TxnMeta, hlc.Timestamp, SpanAccess for the exclusive lock as the lock state.
// Can one assume that TxnMeta.WriteTimestamp is the timestamp?
func (t *lockTableImpl) getLockIterator(startKey roachpb.Key, endKey roachpb.Key) engine.SimpleIterator {
	// TODO: transform startKey, endKey to lock table key space and call engine.NewIterator()
	return nil
}
func (t *lockTableImpl) tryGetLock(key roachpb.Key) *enginepb.TxnMeta {
	// TODO: transform key to the lock table key space and ...
	return nil
}

func lockKeyToMVCC(key roachpb.Key) engine.MVCCKey {
	// TODO
	return engine.MVCCKey{}
}

func mvccKeyToLockKey(mvccKey engine.MVCCKey) roachpb.Key {
	// TODO
	return nil
}

// Per lock state in lockTableImpl.
// Invariants:
// - Lock is held: replicatedHeld || unreplicatedHeld.
// - Lock is reserved when reservedFor != nil: such a lock is not held.
// - The txn holding the lock does not have a request in the queue.
// - The request reserving the lock is not in the queue.
// - A lockState that has an empty queue is removed from memory. We do not bother keeping track of
//   a reserved lockState with no queue since it is similar to a key for which a request did not
//   encounter any contention.
type lockState struct {
	key roachpb.Key
	ss  spanset.SpanScope

	// Both can be true.
	replicatedHeld  bool
	unreplictedHeld bool

	// Holder info. SpanAccess is known to be SpanReadWrite.
	txn *enginepb.TxnMeta
	ts  hlc.Timestamp

	// If not held, can be reserved for a read-write request. A reservation prevents conflicting requests
	// with higher seqNums from proceeding, but requests with smaller seqNums will either break the
	// reservation (if a read-write request) or ignore it (if read-only request).
	reservedFor *requestGuardImpl

	// Contains *queuedGuard. When a lock transitions to !held, all the readers at the front of the
	// queue are removed. If there is a writer after the readers it is given the reservation and also
	// removed from the queue.
	queue *list.List
}

func (l *lockState) Less(i btree.Item) bool {
	return l.key.Compare(i.(*lockState).key) < 0
}

func (l *lockState) tryActiveWait(g *requestGuardImpl, sa spanset.SpanAccess) bool {
	if (l.replicatedHeld || l.unreplictedHeld) && g.txn != nil && l.txn.ID == g.txn.ID {
		return false
	}
	if l.reservedFor == g {
		return false
	}
	var txn *enginepb.TxnMeta
	var ts hlc.Timestamp
	if l.replicatedHeld || l.unreplictedHeld {
		txn = l.txn
		ts = l.ts
	} else {
		if l.reservedFor.seqNum > g.seqNum {
			if sa == spanset.SpanReadWrite {
				// Break reservation.
				qg := &queuedGuard{guard: l.reservedFor, sa: spanset.SpanReadWrite, active: false, distinguished: false}
				l.queue.PushFront(qg)
				l.reservedFor = g
			}
			// Else read-only. Don't need to wait on reservation -- either tryActiveWait() is happening
			// while holding latches so the request with the reservation is not evaluating, or latches
			// have been dropped in which case there will be another call to tryActiveWait() with latches
			// held before evaluating this request.
			return false
		}
		txn = l.reservedFor.txn // may be nil
		ts = l.reservedFor.ts
	}
	var qg *queuedGuard
	if _, ok := g.locks[l]; ok {
		// Already in queue.
		for e := l.queue.Front(); e != nil; e = e.Next() {
			if e.Value.(*queuedGuard).guard == g {
				qg = e.Value.(*queuedGuard)
				break
			}
		}
		if qg == nil || qg.active {
			panic("")
		}
		qg.active = true
	} else {
		// Not in queue, so compute the position in the queue.
		smallestTS := ts
		var e *list.Element
		for e = l.queue.Front(); e != nil; e = e.Next() {
			qqg := e.Value.(*queuedGuard)
			if qqg.guard == g {
				panic("")
			}
			if qqg.guard.seqNum > g.seqNum {
				break
			}
			if qqg.sa == spanset.SpanReadWrite && qqg.guard.ts.Less(smallestTS) {
				smallestTS = qqg.guard.ts
			}
		}
		// If reading may not need to wait if everyone ahead in the queue is not contending.
		if sa == spanset.SpanReadOnly && g.ts.Less(smallestTS) {
			return false
		}
		qg = &queuedGuard{guard: g, sa: sa, active: false, distinguished: false}
		l.queue.InsertBefore(qg, e)
		g.locks[l] = struct{}{}
	}
	// Request is in queue and qg is its entry in the queue.
	g.key = l.key
	g.startWait = true
	// Decide whether qg should be distinguished.
	distinguished := true
	for e := l.queue.Front(); e != nil; e = e.Next() {
		qqg := e.Value.(*queuedGuard)
		if qqg.active && qqg.distinguished {
			distinguished = false
			break
		}
	}
	qg.distinguished = distinguished
	stateType := waitFor
	if distinguished {
		stateType = waitForDistinguished
	}
	qg.guard.state = queueState{
		stateType: stateType,
		txn:       txn,
		ts:        ts,
		access:    spanset.SpanReadWrite,
	}
	select {
	case qg.guard.signal <- struct{}{}:
	}
	return true
}

func (l *lockState) acquireLock(replicated bool, g *requestGuardImpl) error {
	// If already held, confirm that held by self.
	if l.txn != nil && l.txn.ID != g.txn.ID {
		return fmt.Errorf("")
	}
	// If not already held, must have reserved (otherwise lockState would not exist).
	if !l.unreplictedHeld && !l.replicatedHeld && l.reservedFor != g {
		return fmt.Errorf("")
	}
	if replicated {
		l.replicatedHeld = true
	} else {
		l.unreplictedHeld = true
	}
	l.txn = g.txn
	l.ts = g.ts
	l.reservedFor = nil
	delete(g.locks, l)
	return nil
}

func discoveredLock(
	key roachpb.Key, txn *enginepb.TxnMeta, ts hlc.Timestamp, g *requestGuardImpl, sa spanset.SpanAccess) *lockState {
	l := &lockState{
		key:             key,
		replicatedHeld:  true,
		unreplictedHeld: false,
		txn:             txn,
		ts:              ts,
		queue:           list.New(),
	}
	qg := &queuedGuard{
		guard:         g,
		sa:            sa,
		active:        false,
		distinguished: false,
	}
	l.queue.PushBack(qg)
	g.locks[l] = struct{}{}
	return l
}

func (l *lockState) releaseLock(replicated bool, txnID *uuid.UUID) (gc bool, err error) {
	if *txnID != l.txn.ID {
		return false, fmt.Errorf("")
	}
	if replicated {
		l.replicatedHeld = false
	} else {
		l.unreplictedHeld = false
	}
	if l.replicatedHeld || l.unreplictedHeld {
		return false, nil
	}
	// Lock is released.
	l.txn = nil
	l.ts = hlc.Timestamp{}
	gc = l.lockIsFree()
	return gc, nil
}

func (l *lockState) changeLockTs(ts hlc.Timestamp) (gc bool, err error) {
	if ts.Less(l.ts) {
		return false, fmt.Errorf("")
	}
	l.ts = ts
	smallestTS := l.ts
	makeNewDistinguished := false
	for e := l.queue.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		curr := e
		e = e.Next()
		if qg.sa == spanset.SpanReadWrite {
			if qg.guard.ts.Less(smallestTS) {
				smallestTS = qg.guard.ts
			}
		} else {
			if qg.guard.ts.Less(smallestTS) {
				l.queue.Remove(curr)
				if qg.distinguished {
					makeNewDistinguished = true
				}
				delete(qg.guard.locks, l)
				_, _ = qg.guard.table.findNextLockAfter(qg.guard)
			} else {
				break
			}
		}
	}
	for e := l.queue.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		if qg.active {
			if makeNewDistinguished {
				qg.distinguished = true
				makeNewDistinguished = false
			}
			if qg.distinguished {
				qg.guard.state.stateType = waitForDistinguished
			}
			qg.guard.state.ts = l.ts
			select {
			case qg.guard.signal <- struct{}{}:
			}
		}
	}
	return l.queue.Len() == 0, nil
}

func (l *lockState) requestDone(g *requestGuardImpl) (gc bool, err error) {
	if l.reservedFor == g {
		l.reservedFor = nil
		if l.replicatedHeld || l.unreplictedHeld {
			panic("")
		}
		gc := l.lockIsFree()
		return gc, nil
	}
	// TODO: may be able to unblock readers behind g that were waiting only because of g.
	for e := l.queue.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		if qg.guard == g {
			l.queue.Remove(e)
			break
		}
	}
	return l.queue.Len() == 0, nil
}

func (l *lockState) lockIsFree() bool {
	smallestTS := hlc.MaxTimestamp
	first := true
	makeNewDistinguished := false
	for e := l.queue.Front(); e != nil; {
		qg := e.Value.(*queuedGuard)
		curr := e
		e = e.Next()
		if qg.sa == spanset.SpanReadWrite {
			if qg.guard.ts.Less(smallestTS) {
				smallestTS = qg.guard.ts
			}
			if first {
				l.reservedFor = qg.guard
				l.queue.Remove(curr)
				first = false
				if qg.distinguished {
					makeNewDistinguished = true
				}
				_, _ = qg.guard.table.findNextLockAfter(qg.guard)
			}
		} else {
			if qg.guard.ts.Less(smallestTS) {
				l.queue.Remove(curr)
				if qg.distinguished {
					makeNewDistinguished = true
				}
				delete(qg.guard.locks, l)
				_, _ = qg.guard.table.findNextLockAfter(qg.guard)
			} else {
				break
			}
		}
	}
	for e := l.queue.Front(); e != nil; e = e.Next() {
		qg := e.Value.(*queuedGuard)
		if qg.active {
			if makeNewDistinguished {
				qg.distinguished = true
				makeNewDistinguished = false
			}
			stateType := waitFor
			if qg.distinguished {
				stateType = waitForDistinguished
			}
			qg.guard.state = queueState{
				stateType: stateType,
				txn:       l.reservedFor.txn,
				ts:        l.reservedFor.ts,
				access:    spanset.SpanReadWrite,
			}
			select {
			case qg.guard.signal <- struct{}{}:
			}
		}
	}

	return l.queue.Len() == 0
}

type queuedGuard struct {
	guard         *requestGuardImpl
	sa            spanset.SpanAccess
	active        bool
	distinguished bool
}

// Implementation of requestGuard.
type requestGuardImpl struct {
	seqNum uint64

	// Information about this request.
	txn   *enginepb.TxnMeta
	ts    hlc.Timestamp
	spans *spanset.SpanSet

	table *lockTableImpl

	startWait bool
	// State for the queue where the request is actively waiting.
	sa    spanset.SpanAccess
	ss    spanset.SpanScope
	index int

	key    roachpb.Key
	state  queueState
	signal chan struct{}

	// locks that are reserved or in the queue.
	locks map[*lockState]struct{}
}

var _ requestGuard = &requestGuardImpl{}

func (g *requestGuardImpl) startWaiting() bool {
	return g.startWait
}

func (g *requestGuardImpl) newState() <-chan struct{} {
	return g.signal
}
func (g *requestGuardImpl) currentState() queueState {
	return g.state
}

func (t *lockTableImpl) scanAndEnqueue(req Request, guard requestGuard) (requestGuard, error) {
	var g *requestGuardImpl
	if guard == nil {
		g = &requestGuardImpl{
			seqNum: t.nextSeqNum,
			txn:    req.txnMeta(),
			ts:     req.ts(),
			spans:  req.spans(),
			table:  t,
			index:  -1,
			signal: make(chan struct{}),
			locks:  make(map[*lockState]struct{}),
		}
	} else {
		g = guard.(*requestGuardImpl)
		g.index = -1
		g.sa = spanset.SpanAccess(0)
		g.ss = spanset.SpanScope(0)
		g.startWait = false
	}
	found, err := t.findNextLockAfter(g)
	if err != nil {
		return g, err
	}
	g.startWait = found
	return g, nil
}

func (t *lockTableImpl) done(guard requestGuard) error {
	g := guard.(*requestGuardImpl)
	var err error
	for l, _ := range g.locks {
		gc, err2 := l.requestDone(g)
		err = firstError(err, err2)
		if gc {
			t.locks[l.ss].Delete(l)
		}
	}
	return err
}

func (t *lockTableImpl) acquireExclusiveUnreplicatedLocks(lockKeys []roachpb.Key, guard requestGuard) error {
	// TODO: fix error handling such that if return an error, no locks should have been acquired.
	g := guard.(*requestGuardImpl)
	b := t.engine.NewWriteOnlyBatch()
	valBytes, err := protoutil.Marshal(g.txn)
	if err != nil {
		return err
	}
	for _, k := range lockKeys {
		ss := spanset.SpanGlobal
		if keys.IsLocal(k) {
			ss = spanset.SpanLocal
		}
		i := t.locks[ss].Get(&lockState{key: k})
		if i != nil {
			l := i.(*lockState)
			if err := l.acquireLock(false, g); err != nil {
				return err
			}
		}
		if err := b.Put(lockKeyToMVCC(k), valBytes); err != nil {
			return err
		}
	}
	return b.Commit(false)
}

func (t *lockTableImpl) releaseExclusiveUnreplicatedLocks(
	txnID *uuid.UUID, spans *spanset.SpanSet) error {
	// TODO: change ordering so that persistent state changes happen before in-memory changes.
	var err error
	var batch engine.Batch
	for ss := spanset.SpanScope(0); ss < spanset.NumSpanScope; ss++ {
		spans := spans.GetSpans(spanset.SpanReadWrite, ss)
		locks := t.locks[ss]
		releaseFunc := func(i btree.Item) bool {
			l := i.(*lockState)
			if !l.unreplictedHeld {
				err = firstError(err, fmt.Errorf(""))
				return true
			}
			if l.txn == nil || *txnID != l.txn.ID {
				return true
			}
			gc, err2 := l.releaseLock(false, txnID)
			if err2 != nil {
				err = firstError(err, err2)
			}
			if gc {
				t.locks[ss].Delete(l)
			}
			return true
		}
		for _, s := range spans {
			if len(s.EndKey) > 0 {
				locks.AscendRange(&lockState{key: s.Key}, &lockState{key: s.EndKey}, releaseFunc)
				iter := t.getLockIterator(s.Key, s.EndKey)
				for {
					if ok, err2 := iter.Valid(); err2 != nil {
						err = firstError(err, err2)
						break
					} else if !ok {
						break
					}
					var txn *enginepb.TxnMeta
					if err2 := protoutil.Unmarshal(iter.UnsafeValue(), txn); err2 != nil {
						err = firstError(err, err2)
						continue
					}
					if txn.ID == *txnID {
						if batch == nil {
							batch = t.engine.NewWriteOnlyBatch()
						}
						batch.Clear(iter.UnsafeKey())
					}
					iter.Next()
				}
			} else {
				if i := locks.Get(&lockState{key: s.Key}); i != nil {
					releaseFunc(i)
				}
				if txn := t.tryGetLock(s.Key); txn != nil && txn.ID == *txnID {
					if batch == nil {
						batch = t.engine.NewWriteOnlyBatch()
					}
					batch.Clear(lockKeyToMVCC(s.Key))
				}
			}
		}
	}
	if batch != nil {
		if err2 := batch.Commit(false); err2 != nil {
			err = firstError(err, err2)
		}
	}
	return err
}

func (t *lockTableImpl) addDiscoveredExclusiveReplicatedLock(
	key roachpb.Key, txn *enginepb.TxnMeta, ts hlc.Timestamp, guard requestGuard) error {
	g := guard.(*requestGuardImpl)
	ss := spanset.SpanGlobal
	if keys.IsLocal(key) {
		ss = spanset.SpanLocal // TODO: can replicated locks be acquired on local keys?
	}
	if i := t.locks[ss].Get(&lockState{key: key}); i != nil {
		return fmt.Errorf("")
	}
	l := discoveredLock(key, txn, ts, g, spanset.SpanReadWrite)
	t.locks[ss].ReplaceOrInsert(l)
	return nil
}

func (t *lockTableImpl) exclusiveReplicatedLockAcquired(key roachpb.Key, guard requestGuard) error {
	g := guard.(*requestGuardImpl)
	ss := spanset.SpanGlobal
	if keys.IsLocal(key) {
		ss = spanset.SpanLocal // TODO: can replicated locks be acquired on local keys?
	}
	i := t.locks[ss].Get(&lockState{key: key})
	if i == nil {
		return nil
	}
	l := i.(*lockState)
	if err := l.acquireLock(true, g); err != nil {
		return err
	}
	return nil
}

func (t *lockTableImpl) exclusiveReplicatedLocksReleased(
	txnID *uuid.UUID, spans *spanset.SpanSet) error {
	var err error
	for ss := spanset.SpanScope(0); ss < spanset.NumSpanScope; ss++ {
		spans := spans.GetSpans(spanset.SpanReadWrite, ss)
		locks := t.locks[ss]
		releasedFunc := func(i btree.Item) bool {
			l := i.(*lockState)
			if !l.replicatedHeld {
				err = firstError(err, fmt.Errorf(""))
				return true
			}
			if l.txn == nil || *txnID != l.txn.ID {
				return true
			}
			gc, err2 := l.releaseLock(true, txnID)
			if err2 != nil {
				err = firstError(err, err2)
			}
			if gc {
				t.locks[ss].Delete(l)
			}
			return true
		}
		for _, s := range spans {
			if len(s.EndKey) > 0 {
				locks.AscendRange(&lockState{key: s.Key}, &lockState{key: s.EndKey}, releasedFunc)
			} else {
				if i := locks.Get(&lockState{key: s.Key}); i != nil {
					releasedFunc(i)
				}
			}
		}
	}
	return err
}

func (t *lockTableImpl) changeLocksTs(
	txnID *uuid.UUID, ts hlc.Timestamp, replicated bool, spans *spanset.SpanSet) error {
	var err error
	var batch engine.Batch
	if !replicated {
		batch = t.engine.NewWriteOnlyBatch()
	}
	// TODO: change ordering such that persistent state changes happen before in-memory changes.
	for ss := spanset.SpanScope(0); ss < spanset.NumSpanScope; ss++ {
		spans := spans.GetSpans(spanset.SpanReadWrite, ss)
		locks := t.locks[ss]
		changeFunc := func(i btree.Item) bool {
			l := i.(*lockState)
			if (replicated && !l.replicatedHeld) || (!replicated && !l.unreplictedHeld) {
				err = firstError(err, fmt.Errorf(""))
				return true
			}
			if l.txn == nil || l.txn.ID != *txnID {
				return true
			}
			gc, err2 := l.changeLockTs(ts)
			if err2 != nil {
				err = firstError(err, err2)
			}
			if gc {
				t.locks[ss].Delete(l)
			}
			return true
		}
		for _, s := range spans {
			if len(s.EndKey) > 0 {
				locks.AscendRange(&lockState{key: s.Key}, &lockState{key: s.EndKey}, changeFunc)
				if replicated {
					continue
				}
				iter := t.getLockIterator(s.Key, s.EndKey)
				for {
					if ok, err2 := iter.Valid(); err2 != nil {
						err = firstError(err, err2)
						break
					} else if !ok {
						break
					}
					var txn *enginepb.TxnMeta
					if err2 := protoutil.Unmarshal(iter.UnsafeValue(), txn); err2 != nil {
						err = firstError(err, err2)
						continue
					}
					if txn.ID == *txnID {
						txn.WriteTimestamp = ts
						bytes, err2 := protoutil.Marshal(txn)
						if err2 != nil {
							err = firstError(err, err2)
						} else {
							batch.Put(iter.UnsafeKey(), bytes)
						}
					}
					iter.Next()
				}
			} else {
				if i := locks.Get(&lockState{key: s.Key}); i != nil {
					changeFunc(i)
				}
				if replicated {
					continue
				}
				if txn := t.tryGetLock(s.Key); txn != nil && txn.ID == *txnID {
					txn.WriteTimestamp = ts
					bytes, err2 := protoutil.Marshal(txn)
					if err2 != nil {
						err = firstError(err, err2)
					} else {
						batch.Put(lockKeyToMVCC(s.Key), bytes)
					}
				}
			}
		}
	}
	if batch != nil {
		if err2 := batch.Commit(false); err2 != nil {
			err = firstError(err, err2)
		}
	}
	return err
}

func stepToNextSpan(g *requestGuardImpl) *spanset.Span {
	spans := g.spans.GetSpans(g.sa, g.ss)
	g.index++
	for g.index == len(spans) {
		// Step to next in (sa, ss).
		g.ss++
		if g.ss == spanset.NumSpanScope {
			g.ss = spanset.SpanScope(0)
			g.sa++
		}
		if g.sa == spanset.NumSpanAccess {
			return nil
		}
		spans = g.spans.GetSpans(g.sa, g.ss)
		g.index = 0
	}
	span := &g.spans.GetSpans(g.sa, g.ss)[g.index]
	g.key = span.Key
	return span
}

func (t *lockTableImpl) findNextLockAfter(g *requestGuardImpl) (found bool, err error) {
	spans := g.spans.GetSpans(g.sa, g.ss)
	var span *spanset.Span
	if g.index == -1 || len(spans[g.index].EndKey) == 0 {
		span = stepToNextSpan(g)
	} else {
		span = &spans[g.index]
	}
	for span != nil {
		if len(span.EndKey) == 0 {
			if i := t.locks[g.ss].Get(&lockState{key: span.Key}); i != nil {
				l := i.(*lockState)
				if l.tryActiveWait(g, g.sa) {
					return true, nil
				}
			} else if txn := t.tryGetLock(span.Key); txn != nil && (g.txn == nil || txn.ID != g.txn.ID) {
				l := &lockState{key: span.Key, unreplictedHeld: true, txn: txn, ts: txn.WriteTimestamp, queue: list.New()}
				if l.tryActiveWait(g, g.sa) {
					t.locks[g.ss].ReplaceOrInsert(l)
					return true, nil
				}
			}
		} else {
			iter := t.getLockIterator(g.key, span.EndKey)
			firstKey := g.key
			for {
				if ok, err := iter.Valid(); err != nil {
					return false, err
				} else if !ok {
					break
				}
				var txn *enginepb.TxnMeta
				if err := protoutil.Unmarshal(iter.UnsafeValue(), txn); err != nil {
					return false, err
				}
				if g.txn == nil || txn.ID != g.txn.ID {
					// Candidate to wait. But there may be an earlier replicated lock.
					lastKey := mvccKeyToLockKey(iter.UnsafeKey())
					found := false
					t.locks[g.ss].AscendRange(&lockState{key: firstKey}, &lockState{key: lastKey}, func(i btree.Item) bool {
						l := i.(*lockState)
						if l.tryActiveWait(g, g.sa) {
							found = true
							return false
						}
						return true
					})
					if found {
						return true, nil
					}
					firstKey = lastKey
					l := &lockState{key: lastKey, unreplictedHeld: true, txn: txn, ts: txn.WriteTimestamp, queue: list.New()}
					if l.tryActiveWait(g, g.sa) {
						return true, nil
					}
				}
				iter.Next()
			}
		}
		span = stepToNextSpan(g)
	}
	return false, nil
}

func firstError(err0, err1 error) error {
	if err0 != nil {
		return err0
	}
	return err1
}
