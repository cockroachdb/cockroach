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
	"sync/atomic"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// shouldPushImmediately returns whether the PushTxn request should
// proceed without queueing. This is true for pushes which are neither
// ABORT nor TIMESTAMP, but also for ABORT and TIMESTAMP pushes where
// the pushee has min priority or pusher has max priority.
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
	return (txn.Status != roachpb.PENDING ||
		(req.PushType == roachpb.PUSH_TIMESTAMP && req.PushTo.Less(txn.Timestamp)))
}

func txnExpiration(txn *roachpb.Transaction) hlc.Timestamp {
	return txn.LastActive().Add(2*base.DefaultHeartbeatInterval.Nanoseconds(), 0)
}

func isExpired(now hlc.Timestamp, txn *roachpb.Transaction) bool {
	return txnExpiration(txn).Less(now)
}

// createPushTxnResponse returns a PushTxnResponse struct with a
// copy of the supplied transaction. It is necessary to fully copy
// each field in the transaction to avoid race conditions.
func createPushTxnResponse(txn *roachpb.Transaction) *roachpb.PushTxnResponse {
	return &roachpb.PushTxnResponse{PusheeTxn: txn.Clone()}
}

// A waitingPush represents a waiting PushTxn command. It also
// maintains a transitive set of all txns which are waiting on this
// txn in order to detect dependency cycles.
type waitingPush struct {
	req *roachpb.PushTxnRequest
	// pending channel receives updated, pushed txn or nil if queue is cleared.
	pending chan *roachpb.Transaction
	mu      struct {
		syncutil.Mutex
		dependents map[uuid.UUID]struct{} // transitive set of txns waiting on this txn
	}
}

// A pendingTxn represents a transaction waiting to be pushed by one
// or more PushTxn requests.
type pendingTxn struct {
	txn     atomic.Value // the most recent txn record
	waiters []*waitingPush
}

func (pt *pendingTxn) getTxn() *roachpb.Transaction {
	return pt.txn.Load().(*roachpb.Transaction)
}

// A pushTxnQueue enqueues PushTxn requests which are waiting on
// extant txns with conflicting intents to abort or commit.
//
// Internally, it maintains a map from extant txn IDs to queues of
// pending PushTxn requests.
//
// pushTxnQueue is thread safe.
type pushTxnQueue struct {
	store *Store
	mu    struct {
		syncutil.Mutex
		txns map[uuid.UUID]*pendingTxn
	}
}

func newPushTxnQueue(store *Store) *pushTxnQueue {
	return &pushTxnQueue{
		store: store,
	}
}

// Enable allows transactions to be enqueued and waiting pushers
// added. This method must be idempotent as it can be invoked multiple
// times as range leases are updated for the same replica.
func (ptq *pushTxnQueue) Enable() {
	ptq.mu.Lock()
	defer ptq.mu.Unlock()
	if ptq.mu.txns == nil {
		ptq.mu.txns = map[uuid.UUID]*pendingTxn{}
	}
}

// Clear empties the queue and returns all waiters. This method should
// be invoked when the replica loses or transfers its lease. If
// `disable` is true, future transactions may not be enqueued or
// waiting pushers added. Call Enable() once the lease is again
// acquired by the replica.
func (ptq *pushTxnQueue) Clear(disable bool) {
	ptq.mu.Lock()
	var allWaiters []chan *roachpb.Transaction
	for _, pt := range ptq.mu.txns {
		for _, w := range pt.waiters {
			allWaiters = append(allWaiters, w.pending)
		}
		if log.V(1) {
			log.Infof(
				context.Background(),
				"clearing %d waiters for %s",
				len(pt.waiters),
				pt.getTxn().ID.Short(),
			)
		}
		pt.waiters = nil
	}
	if disable {
		ptq.mu.txns = nil
	}
	ptq.mu.Unlock()

	// Send on the pending waiter channels outside of the mutex lock.
	for _, w := range allWaiters {
		w <- nil
	}
}

func (ptq *pushTxnQueue) isEnabled() bool {
	ptq.mu.Lock()
	defer ptq.mu.Unlock()
	return ptq.mu.txns != nil
}

// Enqueue creates a new pendingTxn for the target txn of a failed
// PushTxn command. Subsequent PushTxn requests for the same txn
// will be enqueued behind the pendingTxn via MaybeWait().
func (ptq *pushTxnQueue) Enqueue(txn *roachpb.Transaction) {
	ptq.mu.Lock()
	defer ptq.mu.Unlock()
	if ptq.mu.txns == nil {
		// Not enabled; do nothing.
		return
	}
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
	if ptq.mu.txns == nil {
		// Not enabled; do nothing.
		ptq.mu.Unlock()
		return
	}
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

	if log.V(1) {
		log.Infof(context.Background(), "updating %d waiters for %s", len(waiters), txn.ID.Short())
	}
	// Send on pending waiter channels outside of the mutex lock.
	for _, w := range waiters {
		w.pending <- txn
	}
}

// GetDependents returns a slice of transactions waiting on the specified
// txn either directly or indirectly.
func (ptq *pushTxnQueue) GetDependents(txnID uuid.UUID) []uuid.UUID {
	ptq.mu.Lock()
	defer ptq.mu.Unlock()
	if ptq.mu.txns == nil {
		// Not enabled; do nothing.
		return nil
	}
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
	return nil
}

var errDeadlock = roachpb.NewErrorf("deadlock detected")

// MaybeWait checks whether there is a queue already established for
// pushing the transaction. If not, or if the PushTxn request isn't
// queueable, return immediately. If there is a queue, enqueue this
// request as a waiter and enter a select loop waiting for resolution.
//
// If the transaction is successfully pushed while this method is waiting,
// the first return value is a non-nil PushTxnResponse object.
//
// In the event of a dependency cycle of pushers leading to deadlock,
// this method will return an errDeadlock error.
func (ptq *pushTxnQueue) MaybeWait(
	ctx context.Context, repl *Replica, req *roachpb.PushTxnRequest,
) (*roachpb.PushTxnResponse, *roachpb.Error) {
	if shouldPushImmediately(req) {
		return nil, nil
	}

	ptq.mu.Lock()
	// If the push txn queue is not enabled or if the request is not
	// contained within the replica, do nothing. The request can fall
	// outside of the replica after a split or merge. Note that the
	// ContainsKey check is done under the push txn queue's lock to
	// ensure that it's not cleared before an incorrect insertion happens.
	if ptq.mu.txns == nil || !repl.ContainsKey(req.Key) {
		ptq.mu.Unlock()
		return nil, nil
	}

	// If there's no pending queue for this txn, return not pushed. If
	// already pushed, return push success.
	pending, ok := ptq.mu.txns[*req.PusheeTxn.ID]
	if !ok {
		ptq.mu.Unlock()
		return nil, nil
	}
	if txn := pending.getTxn(); isPushed(req, txn) {
		ptq.mu.Unlock()
		return createPushTxnResponse(txn), nil
	}

	push := &waitingPush{
		req:     req,
		pending: make(chan *roachpb.Transaction, 1),
	}
	pending.waiters = append(pending.waiters, push)
	if log.V(2) {
		if req.PusherTxn.ID != nil {
			log.Infof(
				ctx,
				"%s pushing %s (%d pending)",
				req.PusherTxn.ID.Short(),
				req.PusheeTxn.ID.Short(),
				len(pending.waiters),
			)
		} else {
			log.Infof(ctx, "pushing %s (%d pending)", req.PusheeTxn.ID.Short(), len(pending.waiters))
		}
	}
	ptq.mu.Unlock()

	// Periodically refresh the pusher and pushee txns (with an
	// exponential backoff), in order to determine whether statuses or
	// priorities have changed, and also to get a list of dependent
	// transactions.
	r := retry.Start(ptq.store.cfg.RangeRetryOptions)
	queryCh := r.NextCh()

	for {
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
				return createPushTxnResponse(txn), nil
			}
			// If not successfully pushed, return not pushed so request proceeds.
			return nil, nil

		case <-queryCh:
			// Query the pusher & pushee txns periodically to get updated
			// status or notice either has expired.
			updatedPushee, _, pErr := ptq.queryTxnStatus(ctx, req.PusheeTxn, ptq.store.Clock().Now())
			if pErr != nil {
				return nil, pErr
			} else if updatedPushee == nil {
				// Continue with push.
				return nil, nil
			}
			pusheePriority := updatedPushee.Priority
			pending.txn.Store(updatedPushee)
			if isExpired(ptq.store.Clock().Now(), updatedPushee) {
				if log.V(1) {
					log.Warningf(ctx, "pushing expired txn %s", req.PusheeTxn.ID.Short())
				}
				return nil, nil
			}

			if req.PusherTxn.ID != nil {
				updatedPusher, waitingTxns, pErr := ptq.queryTxnStatus(ctx, req.PusherTxn.TxnMeta, ptq.store.Clock().Now())
				if pErr != nil {
					return nil, pErr
				} else if updatedPusher != nil {
					switch updatedPusher.Status {
					case roachpb.COMMITTED:
						return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError("already committed"), updatedPusher)
					case roachpb.ABORTED:
						return nil, roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(), updatedPusher)
					}
					pusherPriority := updatedPusher.Priority
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
					dependents := make([]string, 0, len(push.mu.dependents))
					for id := range push.mu.dependents {
						dependents = append(dependents, id.Short())
					}
					if log.V(2) {
						log.Infof(
							ctx,
							"%s (%d), pushing %s (%d), has dependencies=%s",
							req.PusherTxn.ID.Short(),
							pusherPriority,
							req.PusheeTxn.ID.Short(),
							pusheePriority,
							dependents,
						)
					}
					push.mu.Unlock()

					if haveDependency {
						// Break the deadlock if the pusher has higher priority.
						p1, p2 := pusheePriority, pusherPriority
						if p1 < p2 || (p1 == p2 && bytes.Compare(req.PusheeTxn.ID.GetBytes(), req.PusherTxn.ID.GetBytes()) < 0) {
							if log.V(1) {
								log.Infof(
									ctx,
									"%s breaking deadlock by force push of %s; dependencies=%s",
									req.PusherTxn.ID.Short(),
									req.PusheeTxn.ID.Short(),
									dependents,
								)
							}
							return nil, errDeadlock
						}
					}
				}
			}

			if queryCh = r.NextCh(); queryCh == nil {
				return nil, nil
			}
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
	b.AddRawRequest(&roachpb.QueryTxnRequest{
		Span: roachpb.Span{
			Key: txnMeta.Key,
		},
		Txn: txnMeta,
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
	resp := br.Responses[0].GetInner().(*roachpb.QueryTxnResponse)
	// ID can be nil if no BeginTransaction has been sent yet.
	if updatedTxn := &resp.QueriedTxn; updatedTxn.ID != nil {
		return updatedTxn, resp.WaitingTxns, nil
	}
	return nil, nil, nil
}
