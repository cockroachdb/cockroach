package concurrency

import (
	"container/list"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/google/btree"
)


// Request doesn't belong in this file.
type Request interface {
	context() context.Context
	// nil when not a transactional request. Is there any "ID" for such a request that one can use?
	txn() *uuid.UUID
	txnMeta() *enginepb.TxnMeta
	spans() *spanset.SpanSet
	// Equal to the Span.Timestamp in all of the spans in the SpanSet.
	ts() hlc.Timestamp
	header() roachpb.Header
}

// lockTable provides locks and associated queues that are used to implement turn-taking at the
// level of individual requests. Requests associated with a transaction can hold a lock beyond the
// lifetime of a request, which allows subsequent requests from the same transaction to not have to
// wait their turn. If a request needs to wait its turn, it enters one or more queues. The logic
// in these queues is not concerned with the bigger picture of transaction prioritization and
// whether it is fair to wait for the lock holder, or whether this waiting will result in deadlock
// -- these queues simply externalize facts about this waiting as events that can be listened
// to and used by external code to make decisions like whether a transaction holding the lock should be
// pushed, aborted etc. Those external decisions will of course eventually impact the state in the
// lockTable, but the lockTable is ignorant of those external complexities. Note that the turn-taking
// use of lockTable applies also to requests that have no intention of acquiring locks (specifically,
// readers). Also, once a lock is acquired there are no guarantees of what versions the
// lock holder may find when actually examining the MVCC data -- it may find that there are already
// writes greater than the timestamp the lock holder wants to write at: the lockTable is only about
// turn-taking and locking.
//
// There is a 2x2 matrix of lock types:
// - Exclusive or Shared
// - Replicated or Unreplicated
// The current code only implements exclusive-replicated and exclusive-unreplicated but we first
// briefly discuss all of these to set the stage for future changes.
// The current write intents are exclusive-replicated and the locks we need for SFU are
// exclusive-unreplicated. Exclusive locks are taken by writers or prospective writers and
// block all other writers. An exclusive lock also has a timestamp, and only blocks readers that are
// reading above that timestamp. Shared locks are taken by readers who want to block all writers,
// both below and above their read timestamp. Preventing writes below their read timestamps is
// already accomplished using the ts cache, which does not require writers to queue since queueing
// is not going to accomplish anything (the write can never happen at that timestamp). The
// lockTable is intended to complement the ts cache, in that any reader that acquired a shared lock
// when evaluating its request also updated the ts cache with the corresponding ts. So ideally a shared
// lock held by a group of txns with timestamps ts1, ts2, ...tsn, would only block writers trying to
// write at a ts > max(ts1...tsn). Writers writing below that max timestamp would be allowed to proceed
// since they will immediately discover the ts cache to be blocking their writes (though this is not
// required for correctness, and only for fairness) and will start pushing themselves (without
// wasting unnecessary time waiting in a queue). A similar situation arises with exclusive locks: an
// exclusive lock at ts2 should not block a writer that is trying to write at ts1 < ts2 -- this
// writer may want to push itself above ts2 immediately, and it that push fails it hasn't wasted
// time waiting in a queue. Both of these behaviors are left to the
// external event listeners. In the first case, the listener will note that a shared lock is held at
// a timestamp higher than what it is trying to write to, and it can decide to immediately push
// itself (while not losing its turn in the queue -- the next call to scanAndEnqueue for that
// request may use a higher ts). In the second case, the listener can decide between pushing the
// lock holder or pushing itself. So the lockTable queueing semantics become simple: lock conflicts
// always cause queueing. One also needs to consider what happens when the lock holder or queued
// request is pushed: a pushed queued request (read or write) will continue to conflict with the
// lock holder; a pushed lock holder (exclusive lock) may no longer conflict with a waiting reader.
//
// The previous discussion made a distinction between readers/writers and exclusive/shared locks.
// This is because a reader does not necessarily want to acquire a shared lock but still needs to
// be sequenced here. The lockTable requires each request specify its txnID (if any), the spans
// it is interested in, whether it is a reader or (prospective) writer of those spans, and the ts
// for its reads or writes. All operations on the lockTable require that the corresponding span
// latches are held. The expected usage is as follows.
//
// Request evaluation:
// var handle queueHandle  // accumulates all the queues this request is in.
// for {
//   acquire all span latches for req.spans()
//   // Discovers all locks and queues in req.spans() and queues itself where necessary.
//   handle, err := lockTable.scanAndEnqueue(..., handle)
//   releasedLatches := false
//   for handle.shouldWait() {
//     // Either there is a lock held by some other txn for which this request has queued, or
//     // there isn't a lock but this request is not at the front of the queue so needs to wait
//     // its turn.
//     release all span latches
//     releasedLatches = true
//     select {
//     case c <- handle.newEvents:
//       events := dequeueQueuedEvents();
//       // Act on dequeued events: deadlock detection, pushing other txns etc. Some of these
//       // may cause other txns to release their locks or alter the ts of their locks, which
//       // would make handle.shouldWait() false.
//     }
//   }
//   if releasedLatches {
//     // Have to reacquire latches and repeat in case some other request slipped ahead and
//     // created a lock on a key that was not previously locked (so this request did not wait
//     // on that key).
//     continue
//   }
//   // "Evaluate" request.
//   // This may call lockTable.acquireLock() if wants to lock something for later requests in this
//   // transaction.
//   // Given the current reality of intents intermingled in the MVCC key space the evaluation may
//   // discover an intent from another txn and will call lockTable.addDiscoveredExclusiveLock(),
//   // release the latches and continue. See more discussion about this below.
//   if doneWithEvaluation {
//     lockTable.done(handle)  // Does not release locks.
//   }
// }
//
// Transaction is done:
// call lockTable.releaseLocks(...)
//
// Transaction is pushed and holds some locks:
// call lockTable.changeLocksTs()
//
//
// Current Reality: (a) only exclusive locks, (b) exclusive-replicated locks are
// intermingled with data so their presence it sometimes hidden from the lockTable.
//
// Due to (a) we simplify the queueing for readers: all waiting readers are pooled together, and
// this pool is not ordered with the other waiting writers. When the lock is released, all the readers
// in the pool are notified that they have reached the "front" of the queue.
// If a request (with some reads) is in the front of all of its queues it will immediately try to
// acquire the span latches and call scanAndEnqueue() again -- if the locks (for the reads) are not
// held it can proceed with evaluation (it will not acquire locks for those reads of course).
// It is possible that when the lock was released there was a waiting writer that also
// reached the "front" of the real queue. It will need to compete with these readers in grabbing the
// span latches. If it does so before the readers, it can proceed to evaluation and acquire locks.
// If it loses the race it has to wait for the readers to give up their span latches by which time
// they may have updated the ts cache. So there is a race which can cause unfairness. We could make
// this fair by ordering these pools of readers in the queue too, but that has 2 consequences:
// - The event notifications that tell the writer who it is waiting on (for external actions like
//   deadlock detection) need to list all the readers that the writer is waiting on. We will
//   eventually need to do this when we have shared locks but skip it for simplicity.
// - Not explicitly putting reads in the queue also means writers do not need to wait for them. For,
//   example consider the case of request r1 that wants to read keys k1 and k2, and request r2 that
//   wants to write k1. Say r1 arrived before r2, and both k1 and k2 are locked so r1 needs to wait.
//   Then r2 arrives and is also waiting. When k1 is released, r1 being in the pool of waiting
//   readers does not stop r2 from proceeding. In general, fairness comes at the cost of reduced
//   concurrency.
// Note that when we introduce shared locks we may want to start distinguishing between read accesses
// that are SpanReadOnly and SpanReadWithSharedLock in that only the latter would participate in a
// queue.
//
// (b) has one necessary consequence and other consequences of convenience. The necessary one is
// that an exclusive-replicated lock held by a different transaction may be discovered during
// evaluation. This is handled by calling addDiscoveredExclusiveLock() and getting back in the
// queue. The convenience is not to deal with
// any persistent state updates for such locks and related MVCC value cleanup in the lockTable
// interface. During evaluation, after an intent is proposed by external code, it should call
// lockTable.acquireLock() to inform the lockTable (it can release the latches after informing the
// lockTable). During intent cleanup, after the persistent
// state changes have applied, lockTable.releaseLocks() should be called to inform the lockTable.
// This also implies that the current lockTable implementation does not need to track which
// exclusive locks are replicated and which are unreplicated (the calling code knows the difference).
// After the future change to segregate the replicated lock table we will be do the intent proposal
// for exclusive-replicated locks as part of acquireLock() and do intent cleanup as part of
// releaseLocks() -- both the acquire and release codes will make the non-persistent state updates
// without waiting for replication to complete. The early release reduces the contention footprint.
// And the early acquire allows the span latches to be released earlier which means txns can
// discover earlier that they are doing something that requires pushing themselves or the current
// lock holder.

type eventType int
const (
	// Event types corresponding to each queue, where the queue is identified by queueEvent.key.
	//
	// A waiting requester in a queue is interested in who it is waiting for. It is either the current lock
	// holder, or if the lock is not held, the requester at the head of the queue (since the requester
	// at the head may be waiting on other queues, so cannot proceed yet). For each queue it is in,
	// the first event identifies who it is initially waiting for and subsequent events identify
	// changes. Finally, it is informed that it is at the front of the queue and the lock is not held.
	// So the stream of events for a queue is: (frontUpdated)+, atFront.
	//
	// Events for a queue may be collapsed if not dequeued since only the latest one is interesting.
	//
	// It is possible after a repeat call to scanAndEnqueue() that a request that had reached the
	// front of a queue is no longer at the front -- this currently can only happen for reads.
	frontUpdated eventType = iota
	atFront
)

type queueEvent struct {
	key roachpb.Key
	eventType eventType

	// Populated for frontUpdated type.
	txn *enginepb.TxnMeta  // nil for a non-transaction
	ts hlc.Timestamp
	access spanset.SpanAccess  // Currently only SpanReadWrite.
}

// queueHandle is used to poll whether the request shouldWait() (initially and every time some
// events are dequeued). The newEvents() channel is a channel with buffer length of 1 that serves
// as a signal. The implementation of queueHandle will update the list of events that are waiting
// to be processed and then do a non-blocking write to the channel. The client of queueHandle
// should select on this channel and when signalled, dequeue all the waiting events.
type queueHandle interface {
	shouldWait() bool
	newEvents() <-chan struct{}
	dequeueQueueEvents() []queueEvent
}

// The minimal information needed to adjust an existing lock (if the transaction has been pushed),
// or release an existing lock. The txnID and access are just for error checking that the caller
// is indeed the lock holder.
type lock struct {
	key roachpb.Key
	txnID *uuid.UUID
	access spanset.SpanAccess
}

// TODO: proper comments.
// See usage comment earlier. scanAndEnqueue(), acquireLock() must be called while
// holding the span latches. queueHandle methods are ok to call after releasing latches.
// acquireLock() must be called after queueHandle.shouldWait() is false.
type lockTable interface {
	// Used to find locks and queues to add the request to. If the returned queueHandle.shouldWait()
	// returns false, proceed to evaluation without releasing latches else release the latches and
	// continue interacting with queueHandle until queueHandle.shouldWait() becomes false or the
	// request is cancelled.
	scanAndEnqueue(ctx context.Context, req Request, h queueHandle) (queueHandle, error)
	// Request is done with all the queues it is in, whether it evaluated or not. This causes it
	// to be removed from all the queues. Does not release any locks.
	done(ctx context.Context, h queueHandle) error
	// Must be called in evaluation phase before calling done().
	// Must be holding latches. Acquiring a lock that is already held is a noop.
	// Two cases this will be called:
	// - SFU, for an exclusive-unreplicated lock.
	// - Normal write path, for an exclusive-replicated lock: intent is proposed and this is added.
	//
	// access must be SpanReadWrite since don't yet support shared locks.
	acquireLock(key roachpb.Key, access spanset.SpanAccess, req Request) error
	// Changes the lock timestamps for a transaction that is pushed. This can permit some readers
	// to proceed.
	changeLocksTs(locks []lock, ts hlc.Timestamp) error
	// Can be called during evaluation or after. If during evaluation do not try to reacquire
	// the lock during the same evaluation.
	// Two cases this will be called:
	// - SFU and there was never an intent.
	// - SFU and Normal write: there was an intent and it has been resolved.
	//
	// If releaseLocks will only be called when transaction is committing or aborting should one have
	// a method that takes only the txnID (in case we don't know which locks it has acquired in the
	// past)?
	releaseLocks(locks []lock) error

	// An exclusive lock held by a different transaction was discovered when reading the MVCC keys.
	// Add it here so that future lock attempts see it. This should only ever happen at a new
	// leaseholder when an intent was applied during a previous lease.
	// Note that the requester will get back in the queue for this lock after calling this. But it
	// is possible that eventually the requester aborts and we are left holding state for a (long held)
	// exclusive lock with no queue. In general, if we are concerned about the memory overhead of
	// tracking exclusive locks that have no queued requests we can change the acquireLock interface
	// to tell the lockTable about which locks are exclusive-replicated and tell it again when the
	// corresponding intent has been applied -- at that point the lockTable could GC these locks
	// since the persistent state will allow it to discover them again when there is contention.
	addDiscoveredExclusiveLock(key roachpb.Key, txn *enginepb.TxnMeta, ts hlc.Timestamp) error
}

// TODO: add synchronization. Add code comments.

// When a requester is in a queue, the requester's queueHandleImpl maintains this state.
type requesterQueueState struct {
	// The access it desires.
	access spanset.SpanAccess
	// The element containing the queueHandleImpl in the lockState.writersQueue or lockState.readersList.
	elem *list.Element // non-nil
	// Is this requester still waiting for someone ahead of it in the queue. Always true for a
	// reader since when it is no longer waiting it is removed from lockState.readersList. Is true
	// for a writer until it reaches the front of the queue and the lock is not held (so it is
	// the writer's turn).
	waiting bool
	// The index in queueHandleImpl.events for the latest event for this queue for this requester.
	// -1 if no event is waiting
	latestEventIndex int
}

// Implementation of queueHandle.
type queueHandleImpl struct {
	// Information about this request.
	txn *enginepb.TxnMeta
	ts hlc.Timestamp

	// State for each queue it is in
	queueStates map[string]*requesterQueueState

	// The events that have not been delivered yet.
	events []queueEvent
	signal chan struct{}
}
var _ queueHandle = &queueHandleImpl{}

func (h *queueHandleImpl) shouldWait() bool {
	for _, v := range h.queueStates {
		if v.waiting {
			return true
		}
	}
	return false
}

func (h *queueHandleImpl) newEvents() <-chan struct{} {
	return h.signal
}

func (h *queueHandleImpl) dequeueQueueEvents() []queueEvent {
	rv := append([]queueEvent(nil), h.events...)
	h.events = h.events[:0]
	for _, v := range h.queueStates {
		v.latestEventIndex = -1
	}
	return rv
}

func (h *queueHandleImpl) addEvent(event queueEvent) error {
	s := h.queueStates[string(event.key)]
	if !s.waiting {
		return fmt.Errorf("logic error")
	}
	if s.latestEventIndex == -1 {
		s.latestEventIndex = len(h.events)
		h.events = append(h.events, event)
	} else {
		h.events[s.latestEventIndex] = event
	}
	if event.eventType == atFront {
		s.waiting = false
	}
	select {
	case h.signal <- struct{}{}:
	}
	return nil
}

func (h *queueHandleImpl) addedToQueue(key roachpb.Key, access spanset.SpanAccess, elem *list.Element, initialEvent queueEvent) {
	if h.queueStates == nil {
		h.queueStates = make(map[string]*requesterQueueState)
	}
	if h.signal == nil {
		h.signal = make(chan struct{}, 1)
	}
	s := &requesterQueueState{access: access, elem: elem, waiting: true, latestEventIndex: len(h.events)}
	h.queueStates[string(key)] = s
	h.events = append(h.events, initialEvent)
}

// Per lock state in lockTableImpl.
// Invariants:
// - Lock is held: Both writersQueue and readersList may be non-empty. The holder is not in the queues.
// - Lock is not held: readersList must be empty. The writersQueue must not be empty since unheld
//   locks with no one queued will disappear from the lock table. The first entry
//   in that queue is being given a chance to acquire the lock.
type lockState struct {
	key roachpb.Key
	held bool
	// Holder info. SpanAccess is known to be SpanReadWrite.
	txn *enginepb.TxnMeta
	ts hlc.Timestamp
	// Contain *queueHandleImpls. Whenever state changes here need to go through them and add events.
	//
	// Waiting writers. If !held, the first writer may be evaluating or waiting to reach the front
	// of all the queues it is in.
	writersQueue *list.List
	// Waiting readers. Removed from here when notified that at the front.
	readersList *list.List
}

type lockTableImpl struct {
	// An ordered set of lockStates.
	locks btree.BTree
}
var _ lockTable = &lockTableImpl{}

func (t *lockTableImpl) scanAndEnqueue(ctx context.Context, req Request, h queueHandle) (queueHandle, error) {
	handle := h.(*queueHandleImpl)
	if handle == nil {
		handle = &queueHandleImpl{txn: req.txnMeta(), ts: req.ts()}
	} else {
		// Only allowed to advance. So if waiting in queue will continue to conflict with lock holder.
		handle.ts = req.ts()
	}
	var addedToQueues bool
	for {
		// TODO: iteration over btree in the range of the request.

		// Discovered a lock.
		var lockState *lockState  // the discovered lock
		var access spanset.SpanAccess  // the desired access to this key
		// Does it already hold the lock.
		if lockState.held && lockState.txn != nil && req.txn() != nil && *req.txn() == lockState.txn.ID {
			continue
		}
		// Does not hold lock, so check if already in the queue.
		if handle.queueStates != nil {
			s := handle.queueStates[string(lockState.key)]
			if s != nil {
				// Nothing to do.
				continue
			}
		}
		// Does not hold lock and not in the queue. Two cases:
		// - Read: if lock is not held or compatible with holder, proceed, else add to readersList.
		// - Write: add to writersList.
		if access == spanset.SpanReadOnly && (!lockState.held || req.ts().Less(lockState.ts)) {
			continue
		}
		addedToQueues = true
		initialEvent := &queueEvent{key: lockState.key, eventType: frontUpdated, access: spanset.SpanReadWrite}
		if lockState.held {
			initialEvent.txn = lockState.txn
			initialEvent.ts = lockState.ts
		} else {
			frontHandle := lockState.writersQueue.Front().Value.(queueHandleImpl)
			initialEvent.txn = frontHandle.txn
			initialEvent.ts = frontHandle.ts
		}
		if access == spanset.SpanReadOnly {
			if lockState.readersList == nil {
				lockState.readersList = list.New()
			}
			elem := lockState.readersList.PushBack(handle)
			handle.addedToQueue(lockState.key, spanset.SpanReadOnly, elem, *initialEvent)
		} else {
			if lockState.writersQueue == nil {
				lockState.writersQueue = list.New()
			}
			elem := lockState.writersQueue.PushBack(handle)
			handle.addedToQueue(lockState.key, spanset.SpanReadWrite, elem, *initialEvent)
		}
	}
	if addedToQueues {
		select {
		case handle.signal <- struct{}{}:
		}
	}
	return handle, nil
}

func (t *lockTableImpl) done(ctx context.Context, h queueHandle) error {
	handle := h.(*queueHandleImpl)
	for k, v := range handle.queueStates {
		// TODO: find lockState in btree using k
		var lockState *lockState
		if v.access == spanset.SpanReadOnly {
			if !v.waiting {
				return fmt.Errorf("readers in the list are always waiting")
			}
			if !lockState.held {
				return fmt.Errorf("not held locks should not have any waiting readers")
			}
			lockState.readersList.Remove(v.elem)
			continue
		}
		// Write
		if !v.waiting {
			// Must be in the front of the queue
			if lockState.writersQueue.Front() != v.elem {
				return fmt.Errorf("")
			}
			// Lock must not be held.
			if lockState.held {
				return fmt.Errorf("")
			}
			// Lock is not held. There must not be any waiting readers.
			if lockState.readersList.Len() != 0 {
				return fmt.Errorf("")
			}
			lockState.writersQueue.Remove(v.elem)
			// There may be waiting writers.
			if lockState.writersQueue.Len() > 0 {
				// There is a new front of the queue.
				frontHandle := lockState.writersQueue.Front().Value.(*queueHandleImpl)
				v2 := frontHandle.queueStates[k]
				// Tell this new front that it is no longer waiting.
				v2.waiting = false
				roachKey := roachpb.Key(k)
				frontHandle.addEvent(queueEvent{key: roachKey, eventType: atFront})

				waiters := lockState.writersQueue.Front().Next()
				if waiters != nil {
					// The front request may no longer be waiting if !frontHandle.shouldWait().
					// Ideally we could delay informing those that
					// are behind it since it may evaluate and deadlock detection etc. may not be needed.
					// But this is tricky since this request that is no longer waiting will reacquire
					// spanlatches and then call scanAndEnqueue() again which may cause it to wait. So we
					// do the simple thing and inform others.
					frontEvent := queueEvent{
						key:       roachKey,
						eventType: frontUpdated,
						txn:       frontHandle.txn,
						ts:        frontHandle.ts,
						access:    spanset.SpanReadWrite,
					}
					for ; waiters != nil; waiters = waiters.Next() {
						waiters.Value.(queueHandleImpl).addEvent(frontEvent)
					}
				}
			}
		} else {
			// Must not be the front of the queue. Just remove it.
			lockState.writersQueue.Remove(v.elem)
		}
	}
	return nil
}

func (t *lockTableImpl) acquireLock(key roachpb.Key, access spanset.SpanAccess, req Request) error {
	if access != spanset.SpanReadWrite {
		return fmt.Errorf("only support exclusive locks")
	}
	return t.acquireExclusiveLockInternal(key, req.txnMeta(), req.ts())
}

func (t *lockTableImpl) changeLocksTs(locks []lock, ts hlc.Timestamp) error {
	for _, l := range locks {
		if l.access != spanset.SpanReadWrite {
			return fmt.Errorf("")
		}
		// TODO: find lockState in btree using l.key
		var state *lockState
		if !state.held {
			return fmt.Errorf("")
		}
		if ts.Less(state.ts) {
			return fmt.Errorf("cannot decrease lock ts")
		}
		state.ts = ts
		eventFront := queueEvent{
			key: state.key,
			eventType: atFront,
		}
		eventFrontUpdated := queueEvent{
			key:       state.key,
			eventType: frontUpdated,
			txn:       state.txn,
			ts:        state.ts,
			access:    spanset.SpanReadWrite,
		}
		// Scan through all waiting readers to see if any can stop waiting.
		for l := state.readersList.Front(); l != nil; l = l.Next() {
			h := l.Value.(*queueHandleImpl)
			if ts.Less(h.ts) {
				qState := h.queueStates[string(state.key)]
				h.addEvent(eventFront)
				state.readersList.Remove(qState.elem)
				delete(h.queueStates, string(state.key))
			} else {
				h.addEvent(eventFrontUpdated)
			}
		}
		for l := state.writersQueue.Front(); l != nil; l = l.Next() {
			h := l.Value.(*queueHandleImpl)
			h.addEvent(eventFrontUpdated)
		}
	}
	return nil
}

func (t *lockTableImpl) releaseLocks(locks []lock) error {
	for _, l := range locks {
		if l.access != spanset.SpanReadWrite {
			return fmt.Errorf("")
		}
		// TODO: find lockState in btree using l.key
		var state *lockState
		if !state.held {
			return fmt.Errorf("")
		}
		// TODO: more error checking.

		event := queueEvent{
			key: state.key,
			eventType: atFront,
		}
		// Scan through all waiting readers and tell them to stop waiting.
		for l := state.readersList.Front(); l != nil; l = l.Next() {
			h := l.Value.(*queueHandleImpl)
			qState := h.queueStates[string(state.key)]
			h.addEvent(event)
			state.readersList.Remove(qState.elem)
			delete(h.queueStates, string(state.key))
		}
		// If there is a waiting writer tell it to stop waiting.
		l := state.writersQueue.Front()
		if l != nil {
			h := l.Value.(*queueHandleImpl)
			h.addEvent(event)
			waiters := l.Next()
			if waiters != nil {
				frontEvent := queueEvent{
					key:       state.key,
					eventType: frontUpdated,
					txn:       h.txn,
					ts:        h.ts,
					access:    spanset.SpanReadWrite,
				}
				for ; waiters != nil; waiters = waiters.Next() {
					waiters.Value.(queueHandleImpl).addEvent(frontEvent)
				}
			}
		}
	}
	return nil
}

func (t *lockTableImpl) addDiscoveredExclusiveLock(key roachpb.Key, txn *enginepb.TxnMeta, ts hlc.Timestamp) error {
	return t.acquireExclusiveLockInternal(key, txn, ts)
}

func (t *lockTableImpl) acquireExclusiveLockInternal(key roachpb.Key, txn *enginepb.TxnMeta, ts hlc.Timestamp) error {
	// TODO: find lockState in btree using key.
	var state *lockState
	if state == nil {
		state = &lockState{
			key:          key,
			held:         true,
			txn:          txn,
			ts:           ts,
		}
		// TODO: insert into btree
		return nil
	}
	if state.held {
		// Already held. Assume acquired by the same request or a previous request in the same txn.
		// TODO: do better error checking.
		return nil
	}
	state.held = true
	// Must have been at the front of the queue. TODO: do some error checking of txn ids.
	handle := state.writersQueue.Front().Value.(*queueHandleImpl)
	state.txn = handle.txn
	state.ts = handle.ts
	// Remove from the front of the queue.
	state.writersQueue.Remove(state.writersQueue.Front())
	delete(handle.queueStates, string(state.key))
	return nil
}
