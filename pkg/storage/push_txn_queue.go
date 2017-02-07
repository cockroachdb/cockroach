// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storage

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// shouldPushImmediately returns whether the PushTxn request should
// proceed without queueing. This is true for TOUCH and QUERY pushes,
// but also for ABORT and TIMESTAMP pushes where the pushee has min
// priority or pusher has max priority.
func shouldPushImmediately(req *roachpb.PushTxnRequest) bool {
	if !(req.PushType == roachpb.PUSH_ABORT || req.PushType == roachpb.PUSH_TIMESTAMP) {
		return true
	}
	p1, p2 := req.PusherTxn.Priority, req.PusheeTxn.Priority
	if p1 > p2 && (p1 == roachpb.MaxTxnPriority || p2 == roachpb.MinTxnPriority) {
		return true
	}
	return false
}

// isPushed returns whether the PushTxn request has already been
// fulfilled by the current transaction state. This may be true
// for transactions with pushed timestamps.
func isPushed(req *roachpb.PushTxnRequest, txn *roachpb.Transaction) bool {
	return txn != nil &&
		(txn.Status != roachpb.PENDING ||
			(req.PushType == roachpb.PUSH_TIMESTAMP && req.PushTo.Less(txn.Timestamp)))
}

// A waitingPush represents a waiting PushTxn command. It also
// maintains a transitive set of all txns which are waiting on this
// txn in order to detect dependency cycles.
type waitingPush struct {
	req     *roachpb.PushTxnRequest
	pending chan *roachpb.Transaction // closed when complete
	mu      struct {
		syncutil.Mutex
		dependents map[uuid.UUID]struct{} // transitive set of txns waiting on this txn
	}
}

// A pendingTxn represents a transaction waiting to be pushed by one
// or more PushTxn requests.
type pendingTxn struct {
	txn      atomic.Value   // the most recent txn record, atomically updated
	updating int32          // pushee is being updated; atomically updated
	waiters  []*waitingPush // slice of waiting push txn requests
}

// A pushTxnQueue manages a map of queues (keyed by txn ID) for txns
// which have pending PushTxn commands waiting to abort the txn or
// push the timestamp forward in order to resolve conflicting intents.
//
// pushTxnQueue is thread safe.
type pushTxnQueue struct {
	store *Store
	mu    struct {
		sync.Locker                           // Protects all variables in the mu struct
		txns        map[uuid.UUID]*pendingTxn // Map from txn ID to pendingTxn
	}
}

func newPushTxnQueue(store *Store) *pushTxnQueue {
	ptq := pushTxnQueue{
		store: store,
	}
	ptq.mu.Locker = new(syncutil.Mutex)
	ptq.mu.txns = map[uuid.UUID]*pendingTxn{}
	return &ptq
}

// Clear empties the queue and returns all waiters. This method should
// be invoked when the replica loses or transfers its lease.
func (ptq *pushTxnQueue) Clear() {
	ptq.mu.Lock()
	var allWaiters []chan *roachpb.Transaction
	for _, pt := range ptq.mu.txns {
		for _, w := range pt.waiters {
			allWaiters = append(allWaiters, w.pending)
		}
		pt.waiters = nil
	}
	ptq.mu.txns = map[uuid.UUID]*pendingTxn{}
	ptq.mu.Unlock()

	// Send on the pending waiter channels outside of the mutex lock.
	for _, w := range allWaiters {
		w <- nil
	}
}

// Enqueue creates a new pendingTxn for the target txn of a failed
// PushTxn command. Subsequent PushTxn requests for the same txn
// will be enqueued behind the pendingTxn via MaybeWait().
func (ptq *pushTxnQueue) Enqueue(txn *roachpb.Transaction) {
	ptq.mu.Lock()
	defer ptq.mu.Unlock()
	// If the txn which failed to push is already pending, update the
	// transaction status.
	if pt, ok := ptq.mu.txns[*txn.ID]; ok {
		pt.txn.Store(txn)
	} else {
		pt = &pendingTxn{}
		pt.txn.Store(txn)
		ptq.mu.txns[*txn.ID] = pt
	}
}

// UpdateTxn is invoked to update a transaction's status after a
// successful PushTxn or EndTransaction command. It unblocks all
// pending waiters.
func (ptq *pushTxnQueue) UpdateTxn(txn *roachpb.Transaction) {
	ptq.mu.Lock()
	pending, ok := ptq.mu.txns[*txn.ID]
	if !ok {
		ptq.mu.Unlock()
		return
	}
	waiters := pending.waiters
	pending.txn.Store(txn)
	pending.waiters = nil
	delete(ptq.mu.txns, *txn.ID)
	ptq.mu.Unlock()

	// Send on pending waiter channels outside of the mutex lock.
	for _, w := range waiters {
		w.pending <- txn
	}
}

// GetDependents returns a slice of transactions waiting on the specified
// txn either directly or indirectly. Returns nil if none.
func (ptq *pushTxnQueue) GetDependents(txnID uuid.UUID) []uuid.UUID {
	ptq.mu.Lock()
	defer ptq.mu.Unlock()
	if pending, ok := ptq.mu.txns[txnID]; ok {
		set := map[uuid.UUID]struct{}{}
		for _, push := range pending.waiters {
			if push.req.PusherTxn.ID != nil {
				set[*push.req.PusherTxn.ID] = struct{}{}
				push.mu.Lock()
				if push.mu.dependents != nil {
					for txnID := range push.mu.dependents {
						set[txnID] = struct{}{}
					}
				}
				push.mu.Unlock()
			}
		}
		dependents := make([]uuid.UUID, 0, len(set))
		for txnID := range set {
			dependents = append(dependents, txnID)
		}
		return dependents
	}
	return []uuid.UUID(nil)
}

// MaybeWait checks whether there is a queue already established for
// pushing the transaction. If not, of if the PushTxn request isn't
// queueable, return immediately. If there is a queue, enqueue this
// request as a waiter and enter a select loop waiting for resolution.
//
// If the transaction is successfully pushed while this method is waiting,
// the first return value is a non-nil PushTxnResponse object.
func (ptq *pushTxnQueue) MaybeWait(
	ctx context.Context, req *roachpb.PushTxnRequest,
) (*roachpb.PushTxnResponse, *roachpb.Error) {
	if shouldPushImmediately(req) {
		return nil, nil
	}

	ptq.mu.Lock()

	// If there's no pending queue for this txn, return not pushed. If
	// already pushed, return push success.
	pending, ok := ptq.mu.txns[*req.PusheeTxn.ID]
	if !ok || (ok && isPushed(req, pending.txn.Load().(*roachpb.Transaction))) {
		defer ptq.mu.Unlock()
		if !ok {
			return nil, nil
		}
		return &roachpb.PushTxnResponse{PusheeTxn: *pending.txn.Load().(*roachpb.Transaction)}, nil
	}

	push := &waitingPush{
		req:     req,
		pending: make(chan *roachpb.Transaction, 1),
	}
	pending.waiters = append(pending.waiters, push)

	ptq.mu.Unlock()

	// Periodically refresh the pusher txn (with an exponential backoff),
	// in order to determine whether it's status or priority have changed,
	// and also to get a list of dependent transactions.
	var queryTxnCh <-chan time.Time
	var r retry.Retry
	if req.PusherTxn.ID != nil {
		ptq.store.mu.Lock()
		retryOpts := ptq.store.cfg.RangeRetryOptions
		ptq.store.mu.Unlock()
		r = retry.Start(retryOpts)
		queryTxnCh = r.NextCh()
	}

	for {
		var txnExpiresCh <-chan time.Time
		now := ptq.store.Clock().Now().WallTime
		// Get a channel to check for the pushee txn's expiration. If
		// another waiter is already checking the last heartbeat of the
		// pushee (updating is non-zero), then this defaults to current
		// time plus twice the heartbeat interval. Otherwise, based on last
		// active timestamp plus twice the heartbeat interval.
		expires := now + 2*base.DefaultHeartbeatInterval.Nanoseconds()
		if atomic.LoadInt32(&pending.updating) == 0 {
			expires = pending.txn.Load().(*roachpb.Transaction).LastActive().WallTime +
				2*base.DefaultHeartbeatInterval.Nanoseconds()
		}
		txnExpiresCh = time.After(time.Duration(expires - now))

		select {
		case <-ctx.Done():
			// Caller has given up.
			return nil, roachpb.NewError(ctx.Err())

		case txn := <-push.pending:
			// If txn is nil, the queue was cleared, presumably because the
			// replica lost the range lease. Return not pushed so request
			// proceeds and is redirected to the new range lease holder.
			if txn == nil {
				return nil, nil
			}
			// Transaction was committed, aborted or had its timestamp
			// pushed. If this PushTxn request is satisfied, return
			// successful PushTxn response.
			if isPushed(req, txn) {
				return &roachpb.PushTxnResponse{PusheeTxn: *txn}, nil
			}
			// If not successfully pushed, return not pushed so request proceeds.
			return nil, nil

		case <-txnExpiresCh:
			// Periodically check whether the txn has been abandoned.
			if atomic.CompareAndSwapInt32(&pending.updating, 0, 1) {
				updatedPushee, _, pErr := ptq.queryTxnStatus(ctx, req.PusheeTxn, ptq.store.Clock().Now())
				if pErr != nil {
					return nil, pErr
				} else if updatedPushee != nil {
					pending.txn.Store(updatedPushee)
				}
				atomic.StoreInt32(&pending.updating, 0)
			}

		case <-queryTxnCh:
			// Query the transaction periodically to get updated status.
			updatedPusher, waitingTxns, pErr := ptq.queryTxnStatus(ctx, req.PusherTxn.TxnMeta, ptq.store.Clock().Now())
			if pErr != nil {
				return nil, pErr
			} else if updatedPusher != nil {
				req.PusherTxn = *updatedPusher

				// Check for dependency cycle to find and break deadlocks.
				push.mu.Lock()
				if push.mu.dependents == nil {
					push.mu.dependents = map[uuid.UUID]struct{}{}
				}
				for _, txnID := range waitingTxns {
					push.mu.dependents[txnID] = struct{}{}
				}
				// Check for a dependency cycle.
				_, haveDependency := push.mu.dependents[*req.PusheeTxn.ID]
				push.mu.Unlock()

				if haveDependency {
					// Break the deadlock if the pusher has higher priority.
					p1, p2 := req.PusheeTxn.Priority, req.PusherTxn.Priority
					if p1 < p2 || (p1 == p2 && bytes.Compare(req.PusheeTxn.ID.GetBytes(), req.PusherTxn.ID.GetBytes()) < 0) {
						req.Force = true
						return nil, nil
					}
				}
			}

			if !r.ManualNext() {
				return nil, nil
			}
			queryTxnCh = r.NextCh()
		}
	}
}

// queryTxnStatus does a "query" push on the specified transaction
// to glean possible changes, such as a higher timestamp and/or
// priority. It turns out this is necessary while a request is waiting
// to push a transaction, as two txns can have circular dependencies
// where both are unable to push because they have different
// information about their own txns.
//
// Returns the updated transaction (or nil if not updated) as well as
// the list of transactions which are waiting on the updated txn.
func (ptq *pushTxnQueue) queryTxnStatus(
	ctx context.Context, txnMeta enginepb.TxnMeta, now hlc.Timestamp,
) (*roachpb.Transaction, []uuid.UUID, *roachpb.Error) {
	b := &client.Batch{}
	b.AddRawRequest(&roachpb.PushTxnRequest{
		Span: roachpb.Span{
			Key: txnMeta.Key,
		},
		Now:       now,
		PusheeTxn: txnMeta,
		PushType:  roachpb.PUSH_QUERY,
	})
	if err := ptq.store.db.Run(ctx, b); err != nil {
		// TODO(tschottdorf):
		// We shouldn't catch an error here (unless it's from the abort cache, in
		// which case we would not get the crucial information that we've been
		// aborted; instead we'll go around thinking we're still PENDING,
		// potentially caught in an infinite loop).  Same issue: we must not use
		// RunWithResponse on this level - we're trying to do internal kv stuff
		// through the public interface. Likely not exercised in tests, so I'd be
		// ok tackling this separately.
		//
		// Scenario:
		// - we're aborted and don't know if we have a read-write conflict
		// - the push above fails and we get a WriteIntentError
		// - we try to update our transaction (right here, and if we don't we might
		// be stuck in a race, that's why we do this - the txn proto we're using
		// might be outdated)
		// - query fails because our home range has the abort cache populated we catch
		// a TransactionAbortedError, but with a pending transaction (since we lose
		// the original txn, and you just use the txn we had...)
		//
		// so something is sketchy here, but it should all resolve nicely when we
		// don't use store.db for these internal requests any more.
		return nil, nil, roachpb.NewError(err)
	}
	br := b.RawResponse()
	// ID can be nil if no BeginTransaction has been sent yet.
	resp := br.Responses[0].GetInner().(*roachpb.PushTxnResponse)
	if updatedTxn := &resp.PusheeTxn; updatedTxn.ID != nil {
		switch updatedTxn.Status {
		case roachpb.COMMITTED:
			return nil, nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError("already committed"), updatedTxn)
		case roachpb.ABORTED:
			return nil, nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), updatedTxn)
		}
		return updatedTxn, resp.WaitingTxns, nil
	}
	return nil, nil, nil
}
