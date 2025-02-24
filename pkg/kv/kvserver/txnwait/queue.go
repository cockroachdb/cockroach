// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnwait

import (
	"bytes"
	"container/list"
	"context"
	"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const maxWaitForQueryTxn = 50 * time.Millisecond

// TxnLivenessHeartbeatMultiplier specifies what multiple the transaction
// liveness threshold should be of the transaction heartbeat interval.
var TxnLivenessHeartbeatMultiplier = envutil.EnvOrDefaultInt(
	"COCKROACH_TXN_LIVENESS_HEARTBEAT_MULTIPLIER", 5)

// TxnLivenessThreshold is the maximum duration between transaction heartbeats
// before the transaction is considered expired by Queue. It is exposed and
// mutable to allow tests to override it.
//
// Use TestingOverrideTxnLivenessThreshold to override the value in tests.
//
// This is an atomic to avoid data races as some tests want to update this
// in a running cluster.
var TxnLivenessThreshold = func() *atomic.Int64 {
	var a atomic.Int64
	a.Store(int64(TxnLivenessHeartbeatMultiplier) * int64(base.DefaultTxnHeartbeatInterval))
	return &a
}()

// TestingOverrideTxnLivenessThreshold allows tests to override the transaction
// liveness threshold. The function returns a closure that should be called to
// reset the value.
func TestingOverrideTxnLivenessThreshold(t time.Duration) func() {
	old := TxnLivenessThreshold.Swap(int64(t))
	return func() {
		TxnLivenessThreshold.Store(old)
	}
}

// ShouldPushImmediately returns whether the PushTxn request should proceed
// without queueing. This is true for pushes which are neither ABORT nor
// TIMESTAMP, but also for ABORT and TIMESTAMP pushes with WaitPolicy_Error or
// where the pusher has priority.
func ShouldPushImmediately(
	req *kvpb.PushTxnRequest, pusheeStatus roachpb.TransactionStatus, wp lock.WaitPolicy,
) bool {
	if req.Force || wp == lock.WaitPolicy_Error {
		return true
	}
	return CanPushWithPriority(
		req.PushType,
		req.PusherTxn.IsoLevel, req.PusheeTxn.IsoLevel,
		req.PusherTxn.Priority, req.PusheeTxn.Priority,
		pusheeStatus,
	)
}

// CanPushWithPriority returns true if the pusher can perform the specified push
// type on the pushee, based on the two txns' isolation levels, their priorities,
// and the pushee's status.
func CanPushWithPriority(
	pushType kvpb.PushTxnType,
	pusherIso, pusheeIso isolation.Level,
	pusherPri, pusheePri enginepb.TxnPriority,
	pusheeStatus roachpb.TransactionStatus,
) bool {
	// Normalize the transaction priorities so that normal user priorities are
	// considered equal for the purposes of pushing.
	normalize := func(p enginepb.TxnPriority) enginepb.TxnPriority {
		switch p {
		case enginepb.MinTxnPriority, enginepb.MaxTxnPriority:
			return p
		default /* normal txn priorities */ :
			return enginepb.MinTxnPriority + 1
		}
	}
	pusherPri, pusheePri = normalize(pusherPri), normalize(pusheePri)
	switch pushType {
	case kvpb.PUSH_ABORT:
		// If the pushee transaction is prepared, never let a PUSH_ABORT through.
		// A prepared transaction must be guaranteed to succeed if it decides to
		// commit.
		if pusheeStatus == roachpb.PREPARED {
			return false
		}

		// Otherwise, let the ABORT through if the pusher has a higher priority.
		return pusherPri > pusheePri
	case kvpb.PUSH_TIMESTAMP:
		// If the pushee transaction is prepared, never let a PUSH_TIMESTAMP through
		// either. Even for isolation levels which tolerate write skew, this could
		// prevent the transaction from committing due to schema-imposed commit
		// deadlines. A prepared transaction must be guaranteed to succeed if it
		// decides to commit.
		// TODO(nvanbenschoten): if we did want to allow prepared transactions at
		// weak isolation levels to be pushed, we would need to do something about
		// commit deadlines. Instead of leasing schema objects and placing deadlines
		// on the transactions that use those leases, we would probably need to lock
		// schema objects (for share) and continue to hold those locks while a
		// transaction is prepared. This would prevent schema changes that would
		// invalidate the prepared transactions. See #137549.
		if pusheeStatus == roachpb.PREPARED {
			return false
		}

		// If the pushee transaction is STAGING, only let the PUSH_TIMESTAMP through
		// to disrupt the transaction commit if the pusher has a higher priority. If
		// the priorities are equal, the PUSH_TIMESTAMP should wait for the commit
		// to complete.
		if pusheeStatus == roachpb.STAGING {
			return pusherPri > pusheePri
		}
		// Otherwise, the pushee has not yet started committing...

		// If the pushee transaction tolerates write skew, the PUSH_TIMESTAMP is
		// harmless, so let it through.
		return pusheeIso.ToleratesWriteSkew() ||
			// Else, if the pushee transaction does not tolerate write skew but the
			// pusher transaction does (and expects other to), let the PUSH_TIMESTAMP
			// proceed as long as the pusher has at least the same priority.
			(pusherIso.ToleratesWriteSkew() && pusherPri >= pusheePri) ||
			// Otherwise, if neither transaction tolerates write skew, let the
			// PUSH_TIMESTAMP proceed only if the pusher has a higher priority.
			(pusherPri > pusheePri)
	case kvpb.PUSH_TOUCH:
		return true
	default:
		panic("unreachable")
	}
}

// isPushed returns whether the PushTxn request has already been
// fulfilled by the current transaction state. This may be true
// for transactions with pushed timestamps.
func isPushed(req *kvpb.PushTxnRequest, txn *roachpb.Transaction) bool {
	return txn.Status.IsFinalized() ||
		(req.PushType == kvpb.PUSH_TIMESTAMP && req.PushTo.LessEq(txn.WriteTimestamp))
}

// TxnExpiration computes the timestamp after which the transaction will be
// considered expired.
func TxnExpiration(txn *roachpb.Transaction) hlc.Timestamp {
	if txn.Status == roachpb.PREPARED {
		// Prepared transactions have no expiration.
		return hlc.MaxTimestamp
	}
	return txn.LastActive().Add(TxnLivenessThreshold.Load(), 0)
}

// IsExpired is true if the given transaction is expired.
func IsExpired(now hlc.Timestamp, txn *roachpb.Transaction) bool {
	return TxnExpiration(txn).Less(now)
}

// createPushTxnResponse returns a PushTxnResponse struct with a
// copy of the supplied transaction. It is necessary to fully copy
// each field in the transaction to avoid race conditions.
func createPushTxnResponse(txn *roachpb.Transaction) *kvpb.PushTxnResponse {
	return &kvpb.PushTxnResponse{PusheeTxn: *txn}
}

// A waitingPush represents a PushTxn command that is waiting on the
// pushee transaction to commit or abort. It maintains a transitive
// set of all txns which are waiting on this txn in order to detect
// dependency cycles.
type waitingPush struct {
	req *kvpb.PushTxnRequest
	// pending channel receives updated, pushed txn or nil if queue is cleared.
	pending chan *roachpb.Transaction
	mu      struct {
		syncutil.Mutex
		dependents map[uuid.UUID]struct{} // transitive set of txns waiting on this txn
	}
}

// A waitingQueries object represents one or more QueryTxn commands that are
// waiting on the same target transaction to change status or acquire new
// dependencies.
type waitingQueries struct {
	pending chan struct{}
	count   int
}

// A pendingTxn represents a transaction waiting to be pushed by one
// or more PushTxn requests.
type pendingTxn struct {
	txn atomic.Value // the most recent txn record

	// waitingPushes is a list that contains *waitingPush elements. We use a
	// *list.List instead of a list.List directly because it makes ownership of
	// the list easier to manage (see takeWaitingPushes). Using a pointer allows
	// the List.Remove method to properly no-op if the list has been cleared and
	// ensures that the List.PushBack method panics if the list has been cleared,
	// instead of either of these operations corrupting the internal state of the
	// list after it has been transferred away.
	//
	// However, to avoid an additional heap allocation, we embed a list.List value
	// in this struct which the *list.List initially points to.
	waitingPushes      *list.List
	waitingPushesAlloc list.List
}

func (pt *pendingTxn) getTxn() *roachpb.Transaction {
	return pt.txn.Load().(*roachpb.Transaction)
}

// takeWaitingPushes returns the pendingTxn's waitingPushes list, leaving a nil
// list in its place. This can be used to transfer ownership of the list and its
// interior references to the caller.
func (pt *pendingTxn) takeWaitingPushes() *list.List {
	wp := pt.waitingPushes
	pt.waitingPushes = nil
	return wp
}

func (pt *pendingTxn) getDependentsSet() map[uuid.UUID]struct{} {
	set := map[uuid.UUID]struct{}{}
	for e := pt.waitingPushes.Front(); e != nil; e = e.Next() {
		push := e.Value.(*waitingPush)
		if id := push.req.PusherTxn.ID; id != (uuid.UUID{}) {
			set[id] = struct{}{}
			push.mu.Lock()
			for txnID := range push.mu.dependents {
				set[txnID] = struct{}{}
			}
			push.mu.Unlock()
		}
	}
	return set
}

// Config contains the dependencies to construct a Queue.
type Config struct {
	RangeDesc *roachpb.RangeDescriptor
	DB        *kv.DB
	Clock     *hlc.Clock
	Stopper   *stop.Stopper
	Metrics   *Metrics
	Knobs     TestingKnobs
}

// TestingKnobs represents testing knobs for a Queue.
type TestingKnobs struct {
	// OnTxnWaitEnqueue is called when a would-be pusher joins a wait queue.
	OnPusherBlocked func(ctx context.Context, push *kvpb.PushTxnRequest)
	// OnTxnUpdate is called by Queue.UpdateTxn.
	OnTxnUpdate func(ctx context.Context, txn *roachpb.Transaction)
}

// Queue enqueues PushTxn requests which are waiting on extant txns
// with conflicting intents to abort or commit.
//
// Internally, it maintains a map from extant txn IDs to queues of pending
// PushTxn requests.
//
// When a write intent is encountered, the command which encountered it (called
// the "pusher" here) initiates a PushTxn request to determine the disposition
// of the intent's transaction (called the "pushee" here). This queue is where a
// PushTxn request will wait if it discovers that the pushee's transaction is
// still pending, and cannot be otherwise aborted or pushed forward.
//
// Queue is thread safe.
type Queue struct {
	cfg   Config
	every log.EveryN // dictates logging for transaction aborts resulting from deadlock detection
	mu    struct {
		syncutil.RWMutex
		txns    map[uuid.UUID]*pendingTxn
		queries map[uuid.UUID]*waitingQueries
	}
}

// NewQueue instantiates a new Queue.
func NewQueue(cfg Config) *Queue {
	return &Queue{
		cfg:   cfg,
		every: log.Every(1 * time.Second),
	}
}

// Enable allows transactions to be enqueued and waiting pushers
// added. This method must be idempotent as it can be invoked multiple
// times as range leases are updated for the same replica.
func (q *Queue) Enable(_ roachpb.LeaseSequence) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.mu.txns == nil {
		q.mu.txns = map[uuid.UUID]*pendingTxn{}
	}
	if q.mu.queries == nil {
		q.mu.queries = map[uuid.UUID]*waitingQueries{}
	}
}

// Clear empties the queue and returns all waiters. This method should
// be invoked when the replica loses or transfers its lease. If
// `disable` is true, future transactions may not be enqueued or
// waiting pushers added. Call Enable() once the lease is again
// acquired by the replica.
func (q *Queue) Clear(disable bool) {
	q.mu.Lock()

	waitingPushesLists := make([]*list.List, 0, len(q.mu.txns))
	waitingPushesCount := 0
	for _, pt := range q.mu.txns {
		waitingPushes := pt.takeWaitingPushes()
		waitingPushesLists = append(waitingPushesLists, waitingPushes)
		waitingPushesCount += waitingPushes.Len()
	}

	waitingQueriesMap := q.mu.queries
	waitingQueriesCount := 0
	for _, waitingQueries := range q.mu.queries {
		waitingQueriesCount += waitingQueries.count
	}

	metrics := q.cfg.Metrics
	metrics.PusheeWaiting.Dec(int64(len(q.mu.txns)))

	if log.V(1) {
		log.Infof(
			context.Background(),
			"clearing %d push waiters and %d query waiters",
			waitingPushesCount,
			waitingQueriesCount,
		)
	}

	if disable {
		q.mu.txns = nil
		q.mu.queries = nil
	} else {
		q.mu.txns = map[uuid.UUID]*pendingTxn{}
		q.mu.queries = map[uuid.UUID]*waitingQueries{}
	}
	q.mu.Unlock()
	// Notify wait channels outside of the mutex lock.
	q.notifyingWaitingPushesAndQueries(waitingPushesLists, waitingQueriesMap)

}

// ClearGE removes anyone waiting on a key greater or equal to the
// given key.
//
// This method is invoked as part of range split handling.
func (q *Queue) ClearGE(key roachpb.Key) []roachpb.LockAcquisition {
	q.mu.Lock()

	txnsToClear := make(map[uuid.UUID]struct{}, len(q.mu.txns))
	waitingPushesLists := make([]*list.List, 0, len(q.mu.txns))
	waitingPushesCount := 0
	for uuid, pt := range q.mu.txns {
		if roachpb.Key(pt.getTxn().Key).Less(key) {
			continue
		}
		txnsToClear[uuid] = struct{}{}
		waitingPushes := pt.takeWaitingPushes()
		waitingPushesLists = append(waitingPushesLists, waitingPushes)
		waitingPushesCount += waitingPushes.Len()
	}

	waitingQueriesMap := make(map[uuid.UUID]*waitingQueries, len(q.mu.txns))
	waitingQueriesCount := 0
	for uuid, waitingQueries := range q.mu.queries {
		if _, ok := txnsToClear[uuid]; ok {
			waitingQueriesMap[uuid] = waitingQueries
			waitingQueriesCount += waitingQueries.count
		}
	}

	if log.V(1) {
		log.Infof(
			context.Background(),
			"clearing %d push waiters and %d query waiters",
			waitingPushesCount,
			waitingQueriesCount,
		)
	}

	metrics := q.cfg.Metrics
	metrics.PusheeWaiting.Dec(int64(len(txnsToClear)))
	for uuid := range txnsToClear {
		delete(q.mu.txns, uuid)
		delete(q.mu.queries, uuid)
	}
	q.mu.Unlock()
	// Notify wait channels outside of the mutex lock.
	q.notifyingWaitingPushesAndQueries(waitingPushesLists, waitingQueriesMap)
	return nil
}

func (q *Queue) notifyingWaitingPushesAndQueries(
	pushes []*list.List, queries map[uuid.UUID]*waitingQueries,
) {
	for _, w := range pushes {
		for e := w.Front(); e != nil; e = e.Next() {
			push := e.Value.(*waitingPush)
			push.pending <- nil
		}
	}
	for _, w := range queries {
		close(w.pending)
	}
}

// IsEnabled is true if the queue is enabled.
func (q *Queue) IsEnabled() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.mu.txns != nil
}

// OnRangeDescUpdated informs the Queue that its Range has been updated.
func (q *Queue) OnRangeDescUpdated(desc *roachpb.RangeDescriptor) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.cfg.RangeDesc = desc
}

// RangeContainsKeyLocked returns whether the Queue's Range contains the
// specified key.
func (q *Queue) RangeContainsKeyLocked(key roachpb.Key) bool {
	return kvserverbase.ContainsKey(q.cfg.RangeDesc, key)
}

// EnqueueTxn creates a new pendingTxn for the target txn of a failed
// PushTxn command. Subsequent PushTxn requests for the same txn
// will be enqueued behind the pendingTxn via MaybeWait().
func (q *Queue) EnqueueTxn(txn *roachpb.Transaction) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.mu.txns == nil {
		// Not enabled; do nothing.
		return
	}
	// If the txn which failed to push is already pending, update the
	// transaction status.
	if pt, ok := q.mu.txns[txn.ID]; ok {
		pt.txn.Store(txn)
	} else {
		q.cfg.Metrics.PusheeWaiting.Inc(1)
		pt = &pendingTxn{}
		pt.txn.Store(txn)
		pt.waitingPushes = &pt.waitingPushesAlloc
		pt.waitingPushes.Init()
		q.mu.txns[txn.ID] = pt
	}
}

// UpdateTxn is invoked to update a transaction's status after a successful
// PushTxn or EndTxn command. It unblocks all pending waiters.
func (q *Queue) UpdateTxn(ctx context.Context, txn *roachpb.Transaction) {
	txn.AssertInitialized(ctx)
	q.mu.Lock()
	if f := q.cfg.Knobs.OnTxnUpdate; f != nil {
		f(ctx, txn)
	}

	q.releaseWaitingQueriesLocked(ctx, txn.ID)

	if q.mu.txns == nil {
		// Not enabled; do nothing.
		q.mu.Unlock()
		return
	}

	pending, ok := q.mu.txns[txn.ID]
	if !ok {
		q.mu.Unlock()
		return
	}
	waitingPushes := pending.takeWaitingPushes()
	pending.txn.Store(txn)
	delete(q.mu.txns, txn.ID)
	q.mu.Unlock()

	metrics := q.cfg.Metrics
	metrics.PusheeWaiting.Dec(1)

	if log.V(1) && waitingPushes.Len() > 0 {
		log.Infof(ctx, "updating %d push waiters for %s", waitingPushes.Len(), txn.ID.Short())
	}
	// Send on pending waiter channels outside of the mutex lock.
	for e := waitingPushes.Front(); e != nil; e = e.Next() {
		push := e.Value.(*waitingPush)
		push.pending <- txn
	}
}

// GetDependents returns a slice of transactions waiting on the specified
// txn either directly or indirectly.
func (q *Queue) GetDependents(txnID uuid.UUID) []uuid.UUID {
	q.mu.RLock()
	defer q.mu.RUnlock()
	if q.mu.txns == nil {
		// Not enabled; do nothing.
		return nil
	}
	if pending, ok := q.mu.txns[txnID]; ok {
		set := pending.getDependentsSet()
		dependents := make([]uuid.UUID, 0, len(set))
		for txnID := range set {
			dependents = append(dependents, txnID)
		}
		return dependents
	}
	return nil
}

// isTxnUpdated returns whether the transaction specified in
// the QueryTxnRequest has had its status or priority updated
// or whether the known set of dependent transactions has
// changed.
func (q *Queue) isTxnUpdated(pending *pendingTxn, req *kvpb.QueryTxnRequest) bool {
	// First check whether txn status or priority has changed.
	txn := pending.getTxn()
	if txn.Status.IsFinalized() || txn.Priority > req.Txn.Priority {
		return true
	}
	// Next, see if there is any discrepancy in the set of known dependents.
	set := pending.getDependentsSet()
	if len(req.KnownWaitingTxns) != len(set) {
		return true
	}
	for _, txnID := range req.KnownWaitingTxns {
		if _, ok := set[txnID]; !ok {
			return true
		}
	}
	return false
}

func (q *Queue) releaseWaitingQueriesLocked(ctx context.Context, txnID uuid.UUID) {
	if w, ok := q.mu.queries[txnID]; ok {
		log.VEventf(ctx, 2, "releasing %d waiting queries for %s", w.count, txnID.Short())
		close(w.pending)
		delete(q.mu.queries, txnID)
	}
}

// MaybeWaitForPush checks whether there is a queue already
// established for pushing the transaction. If not, or if the PushTxn
// request isn't queueable, return immediately. If there is a queue,
// enqueue this request as a waiter and enter a select loop waiting
// for resolution.
//
// If the transaction is successfully pushed while this method is waiting,
// the first return value is a non-nil PushTxnResponse object.
func (q *Queue) MaybeWaitForPush(
	ctx context.Context, req *kvpb.PushTxnRequest, wp lock.WaitPolicy,
) (*kvpb.PushTxnResponse, *kvpb.Error) {
	q.mu.Lock()
	// If the txn wait queue is not enabled or if the request is not
	// contained within the replica, do nothing. The request can fall
	// outside of the replica after a split or merge. Note that the
	// ContainsKey check is done under the txn wait queue's lock to
	// ensure that it's not cleared before an incorrect insertion happens.
	if q.mu.txns == nil || !q.RangeContainsKeyLocked(req.Key) {
		q.mu.Unlock()
		return nil, nil
	}

	// If there's no pending queue for this txn, return not pushed. If
	// already pushed, return push success.
	pending, ok := q.mu.txns[req.PusheeTxn.ID]
	if !ok {
		q.mu.Unlock()
		return nil, nil
	}
	if txn := pending.getTxn(); isPushed(req, txn) {
		q.mu.Unlock()
		return createPushTxnResponse(txn), nil
	} else if ShouldPushImmediately(req, txn.Status, wp) {
		q.mu.Unlock()
		return nil, nil
	}

	push := &waitingPush{
		req:     req,
		pending: make(chan *roachpb.Transaction, 1),
	}
	pushElem := pending.waitingPushes.PushBack(push)
	waitingPushesCount := pending.waitingPushes.Len()
	if f := q.cfg.Knobs.OnPusherBlocked; f != nil {
		f(ctx, req)
	}
	// Because we're adding another dependent on the pending
	// transaction, send on the waiting queries' channel to
	// indicate there is a new dependent and they should proceed
	// to execute the QueryTxn command.
	q.releaseWaitingQueriesLocked(ctx, req.PusheeTxn.ID)
	q.mu.Unlock()

	// When we return, remove our push from the pending queue. This will be a
	// no-op if the waitingPushes list has already been taken from the waitingPush
	// with a call to takeWaitingPushes.
	defer func() {
		q.mu.Lock()
		pending.waitingPushes.Remove(pushElem)
		q.mu.Unlock()
	}()

	pusherStr := "non-txn"
	if req.PusherTxn.ID != (uuid.UUID{}) {
		pusherStr = req.PusherTxn.ID.Short().String()
	}
	log.VEventf(
		ctx,
		2,
		"%s pushing %s (%d pending)",
		pusherStr,
		req.PusheeTxn.ID.Short(),
		waitingPushesCount,
	)
	var res *kvpb.PushTxnResponse
	var err *kvpb.Error
	labels := pprof.Labels("pushee", req.PusheeTxn.ID.String(), "pusher", pusherStr)
	pprof.Do(ctx, labels, func(ctx context.Context) {
		res, err = q.waitForPush(ctx, req, push, pending)
	})
	return res, err
}

func (q *Queue) waitForPush(
	ctx context.Context, req *kvpb.PushTxnRequest, push *waitingPush, pending *pendingTxn,
) (*kvpb.PushTxnResponse, *kvpb.Error) {
	// Wait for any updates to the pusher txn to be notified when
	// status, priority, or dependents (for deadlock detection) have
	// changed.
	var queryPusherCh <-chan *roachpb.Transaction // accepts updates to the pusher txn
	var queryPusherErrCh <-chan *kvpb.Error       // accepts errors querying the pusher txn
	var readyCh chan struct{}                     // signaled when pusher txn should be queried

	// Query the pusher if it's a valid read-write transaction.
	if req.PusherTxn.ID != uuid.Nil && req.PusherTxn.IsLocking() {
		// Create a context which will be canceled once this call completes.
		// This ensures that the goroutine created to query the pusher txn
		// is properly cleaned up.
		var cancel func()
		ctx, cancel = context.WithCancel(ctx)
		readyCh = make(chan struct{}, 1)
		queryPusherCh, queryPusherErrCh = q.startQueryPusherTxn(ctx, push, readyCh)
		// Ensure that the pusher querying goroutine is complete at exit.
		defer func() {
			cancel()
			if queryPusherErrCh != nil {
				<-queryPusherErrCh
			}
		}()
	}
	pusherPriority := req.PusherTxn.Priority
	pusheePriority := req.PusheeTxn.Priority

	metrics := q.cfg.Metrics
	metrics.PusherWaiting.Inc(1)
	defer metrics.PusherWaiting.Dec(1)
	tBegin := timeutil.Now()
	defer func() { metrics.PusherWaitTime.RecordValue(timeutil.Since(tBegin).Nanoseconds()) }()

	slowTimerThreshold := time.Minute
	var slowTimer timeutil.Timer
	defer slowTimer.Stop()
	slowTimer.Reset(slowTimerThreshold)

	var pusheeTxnTimer timeutil.Timer
	defer pusheeTxnTimer.Stop()
	// The first time we want to check the pushee's txn record immediately:
	// the pushee might be gone by the time the pusher gets here if it cleaned
	// itself up after the pusher saw an intent but before it entered this
	// queue.
	pusheeTxnTimer.Reset(0)
	for {
		select {
		case <-slowTimer.C:
			slowTimer.Read = true
			metrics.PusherSlow.Inc(1)
			log.Warningf(ctx, "pusher %s: have been waiting %.2fs for pushee %s",
				req.PusherTxn.ID.Short(),
				timeutil.Since(tBegin).Seconds(),
				req.PusheeTxn.ID.Short(),
			)
			//nolint:deferloop TODO(#137605)
			defer func() {
				metrics.PusherSlow.Dec(1)
				log.Warningf(ctx, "pusher %s: finished waiting after %.2fs for pushee %s",
					req.PusherTxn.ID.Short(),
					timeutil.Since(tBegin).Seconds(),
					req.PusheeTxn.ID.Short(),
				)
			}()
		case <-ctx.Done():
			// Caller has given up.
			log.VEvent(ctx, 2, "pusher giving up due to context cancellation")
			return nil, kvpb.NewError(ctx.Err())
		case <-q.cfg.Stopper.ShouldQuiesce():
			// Let the push out so that they can be sent looking elsewhere.
			return nil, nil
		case txn := <-push.pending:
			log.VEventf(ctx, 2, "result of pending push: %v", txn)
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
				log.VEvent(ctx, 2, "push request is satisfied")
				return createPushTxnResponse(txn), nil
			}
			// If not successfully pushed, return not pushed so request proceeds.
			log.VEvent(ctx, 2, "not pushed; returning to caller")
			return nil, nil

		case <-pusheeTxnTimer.C:
			log.VEvent(ctx, 2, "querying pushee")
			pusheeTxnTimer.Read = true
			// Periodically check whether the pushee txn has been abandoned.
			updatedPushee, _, pErr := q.queryTxnStatus(
				ctx, req.PusheeTxn, false, nil,
			)
			if pErr != nil {
				return nil, pErr
			} else if updatedPushee == nil {
				// Continue with push.
				log.VEvent(ctx, 2, "pushee not found, push should now succeed")
				return nil, nil
			}
			pusheePriority = updatedPushee.Priority
			pending.txn.Store(updatedPushee)
			if updatedPushee.Status.IsFinalized() {
				log.VEvent(ctx, 2, "push request is satisfied")
				if updatedPushee.Status == roachpb.ABORTED {
					// Inform any other waiting pushers that the transaction is now
					// finalized. Intuitively we would expect that if any pusher was
					// stuck waiting for the transaction to be finalized then it would
					// have heard about the update when the transaction record moved
					// into its finalized state. This is correct for cases where a
					// command explicitly wrote the transaction record with a finalized
					// status.
					//
					// However, this does not account for the case where a transaction
					// becomes uncommittable due a loss of resolution in the store's
					// timestamp cache. In that case, a transaction may suddenly become
					// uncommittable without an associated write to its record. When
					// this happens, no one else will immediately inform the other
					// pushers about the uncommittable transaction. Eventually the
					// pushee's coordinator will come along and roll back its record,
					// but that's only if the pushee isn't itself waiting on the result
					// of one of the pushers here. If there is such a dependency cycle
					// then the other pushers may have to wait for up to the transaction
					// expiration to query the pushee again and notice that the pushee
					// is now uncommittable.
					q.UpdateTxn(ctx, updatedPushee)
				}
				return createPushTxnResponse(updatedPushee), nil
			}
			if IsExpired(q.cfg.Clock.Now(), updatedPushee) {
				log.VEventf(ctx, 1, "pushing expired txn %s", req.PusheeTxn.ID.Short())
				return nil, nil
			}
			// Set the timer to check for the pushee txn's expiration.
			expiration := TxnExpiration(updatedPushee).GoTime()
			now := q.cfg.Clock.Now().GoTime()
			pusheeTxnTimer.Reset(expiration.Sub(now))

		case updatedPusher := <-queryPusherCh:
			switch updatedPusher.Status {
			case roachpb.COMMITTED:
				log.VEventf(ctx, 1, "pusher committed: %v", updatedPusher)
				return nil, kvpb.NewErrorWithTxn(kvpb.NewTransactionStatusError(
					kvpb.TransactionStatusError_REASON_TXN_COMMITTED,
					"already committed"),
					updatedPusher)
			case roachpb.ABORTED:
				log.VEventf(ctx, 1, "pusher aborted: %v", updatedPusher)
				return nil, kvpb.NewErrorWithTxn(
					kvpb.NewTransactionAbortedError(kvpb.ABORT_REASON_PUSHER_ABORTED), updatedPusher)
			}
			log.VEventf(ctx, 2, "pusher was updated: %v", updatedPusher)
			if updatedPusher.Priority > pusherPriority {
				pusherPriority = updatedPusher.Priority
			}

			// Check for dependency cycle to find and break deadlocks.
			push.mu.Lock()
			_, haveDependency := push.mu.dependents[req.PusheeTxn.ID]
			dependents := make([]string, 0, len(push.mu.dependents))
			for id := range push.mu.dependents {
				dependents = append(dependents, id.Short().String())
			}
			log.VEventf(
				ctx,
				2,
				"%s (%d), pushing %s (%d), has dependencies=%s",
				req.PusherTxn.ID.Short(),
				pusherPriority,
				req.PusheeTxn.ID.Short(),
				pusheePriority,
				dependents,
			)
			push.mu.Unlock()

			// Since the pusher has been updated, clear any waiting queries
			// so that they continue with a query of new dependents added here.
			q.mu.Lock()
			q.releaseWaitingQueriesLocked(ctx, req.PusheeTxn.ID)
			q.mu.Unlock()

			if haveDependency {
				// Break the deadlock if the pusher has higher priority.
				p1, p2 := pusheePriority, pusherPriority
				if p1 < p2 || (p1 == p2 && bytes.Compare(req.PusheeTxn.ID.GetBytes(), req.PusherTxn.ID.GetBytes()) < 0) {
					// NB: It's useful to have logs indicating the transactions involved
					// in a deadlock, but we don't want these to be too spammy.
					level := log.Level(1)
					if q.every.ShouldLog() {
						level = 0 // will behave like a log.Infof
					}
					log.VEventf(
						ctx,
						level,
						"%s breaking deadlock by force push of %s; dependencies=%s",
						req.PusherTxn.ID.Short(),
						req.PusheeTxn.ID.Short(),
						dependents,
					)
					metrics.DeadlocksTotal.Inc(1)
					return q.forcePushAbort(ctx, req)
				}
			}
			// Signal the pusher query txn loop to continue.
			readyCh <- struct{}{}

		case pErr := <-queryPusherErrCh:
			queryPusherErrCh = nil
			return nil, pErr
		}
	}
}

// MaybeWaitForQuery checks whether there is a queue already
// established for pushing the transaction. If not, or if the QueryTxn
// request hasn't specified WaitForUpdate, return immediately. If
// there is a queue, enqueue this request as a waiter and enter a
// select loop waiting for any updates to the target transaction.
func (q *Queue) MaybeWaitForQuery(ctx context.Context, req *kvpb.QueryTxnRequest) *kvpb.Error {
	if !req.WaitForUpdate {
		return nil
	}
	q.mu.Lock()
	// If the txn wait queue is not enabled or if the request is not
	// contained within the replica, do nothing. The request can fall
	// outside of the replica after a split or merge. Note that the
	// ContainsKey check is done under the txn wait queue's lock to
	// ensure that it's not cleared before an incorrect insertion happens.
	if q.mu.txns == nil || !q.RangeContainsKeyLocked(req.Key) {
		q.mu.Unlock()
		return nil
	}

	var maxWaitCh <-chan time.Time
	// If the transaction we're waiting to query has a queue of txns
	// in turn waiting on it, and is _already_ updated from what the
	// caller is expecting, return to query the updates immediately.
	if pending, ok := q.mu.txns[req.Txn.ID]; ok && q.isTxnUpdated(pending, req) {
		q.mu.Unlock()
		return nil
	} else if !ok {
		// If the transaction we're querying has no queue established,
		// it's possible that it's no longer pending. To avoid waiting
		// forever for an update that isn't forthcoming, we set a maximum
		// time to wait for updates before allowing the query to
		// proceed.
		maxWaitCh = time.After(maxWaitForQueryTxn)
	}

	// Add a new query to wait for updates to the transaction. If a query
	// already exists, we can just increment its reference count.
	query, ok := q.mu.queries[req.Txn.ID]
	if ok {
		query.count++
	} else {
		query = &waitingQueries{
			pending: make(chan struct{}),
			count:   1,
		}
		q.mu.queries[req.Txn.ID] = query
	}
	q.mu.Unlock()

	// When we return, make sure to unregister the query. If the query's reference
	// count drops to 0, remove it from the queries map. Only do so if the map
	// still contains the same waitingQueries reference, as it may have been
	// removed already and replaced.
	defer func() {
		q.mu.Lock()
		query.count--
		if query.count == 0 && query == q.mu.queries[req.Txn.ID] {
			delete(q.mu.queries, req.Txn.ID)
		}
		q.mu.Unlock()
	}()

	metrics := q.cfg.Metrics
	metrics.QueryWaiting.Inc(1)
	defer metrics.QueryWaiting.Dec(1)
	tBegin := timeutil.Now()
	defer func() { metrics.QueryWaitTime.RecordValue(timeutil.Since(tBegin).Nanoseconds()) }()

	log.VEventf(ctx, 2, "waiting on query for %s", req.Txn.ID.Short())
	select {
	case <-ctx.Done():
		// Caller has given up.
		return kvpb.NewError(ctx.Err())
	case <-maxWaitCh:
		return nil
	case <-query.pending:
		return nil
	}
}

// startQueryPusherTxn starts a goroutine to send QueryTxn requests to
// fetch updates to the pusher's own transaction until the context is
// done or an error occurs while querying. Returns two channels: one
// for updated versions of the pusher transaction, and the other for
// errors encountered while querying. The readyCh parameter is used by
// the caller to signal when the next query to the pusher should be
// sent, and is mostly intended to avoid an extra RPC in the event that
// the QueryTxn returns sufficient information to determine a dependency
// cycle exists and must be broken.
//
// Note that the contents of the pusher transaction including updated
// priority and set of known waiting transactions (dependents) are
// accumulated over iterations and supplied with each successive
// invocation of QueryTxn in order to avoid busy querying.
func (q *Queue) startQueryPusherTxn(
	ctx context.Context, push *waitingPush, readyCh <-chan struct{},
) (<-chan *roachpb.Transaction, <-chan *kvpb.Error) {
	ch := make(chan *roachpb.Transaction, 1)
	errCh := make(chan *kvpb.Error, 1)
	push.mu.Lock()
	var waitingTxns []uuid.UUID
	if push.mu.dependents != nil {
		waitingTxns = make([]uuid.UUID, 0, len(push.mu.dependents))
		for txnID := range push.mu.dependents {
			waitingTxns = append(waitingTxns, txnID)
		}
	}
	pusher := push.req.PusherTxn.Clone()
	push.mu.Unlock()

	if err := q.cfg.Stopper.RunAsyncTask(
		ctx, "monitoring pusher txn",
		func(ctx context.Context) {
			// We use a backoff/retry here in case the pusher transaction
			// doesn't yet exist.
			for r := retry.StartWithCtx(ctx, base.DefaultRetryOptions()); r.Next(); {
				var pErr *kvpb.Error
				var updatedPusher *roachpb.Transaction
				updatedPusher, waitingTxns, pErr = q.queryTxnStatus(
					ctx, pusher.TxnMeta, true, waitingTxns,
				)
				if pErr != nil {
					errCh <- pErr
					return
				} else if updatedPusher == nil {
					// No pusher to query; the pusher's record hasn't yet been
					// created. Continue in order to backoff and retry.
					// TODO(nvanbenschoten): we shouldn't hit this case in a 2.2
					// cluster now that QueryTxn requests synthesize
					// transactions from their provided TxnMeta. However, we
					// need to keep the logic while we want to support
					// compatibility with 2.1 nodes. Remove this in 2.3.
					log.Event(ctx, "no pusher found; backing off")
					continue
				}

				// Update the pending pusher's set of dependents. These accumulate
				// and are used to propagate the transitive set of dependencies for
				// distributed deadlock detection.
				push.mu.Lock()
				if push.mu.dependents == nil {
					push.mu.dependents = map[uuid.UUID]struct{}{}
				}
				for _, txnID := range waitingTxns {
					push.mu.dependents[txnID] = struct{}{}
				}
				push.mu.Unlock()

				// Send an update of the pusher txn.
				pusher.Update(updatedPusher)
				ch <- pusher

				// Wait for context cancellation or indication on readyCh that the
				// push waiter requires another query of the pusher txn.
				select {
				case <-ctx.Done():
					errCh <- kvpb.NewError(ctx.Err())
					return
				case <-readyCh:
				}
				// Reset the retry to query again immediately.
				r.Reset()
			}
			errCh <- kvpb.NewError(ctx.Err())
		}); err != nil {
		errCh <- kvpb.NewError(err)
	}
	return ch, errCh
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
func (q *Queue) queryTxnStatus(
	ctx context.Context, txnMeta enginepb.TxnMeta, wait bool, dependents []uuid.UUID,
) (*roachpb.Transaction, []uuid.UUID, *kvpb.Error) {
	b := &kv.Batch{}
	b.Header.Timestamp = q.cfg.Clock.Now()
	b.AddRawRequest(&kvpb.QueryTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txnMeta.Key,
		},
		Txn:              txnMeta,
		WaitForUpdate:    wait,
		KnownWaitingTxns: dependents,
	})
	if err := q.cfg.DB.Run(ctx, b); err != nil {
		// TODO(tschottdorf):
		// We shouldn't catch an error here (unless it's from the AbortSpan, in
		// which case we would not get the crucial information that we've been
		// aborted; instead we'll go around thinking we're still PENDING,
		// potentially caught in an infinite loop).  Same issue: we must not use
		// RunWithResponse on this level - we're trying to do internal kv stuff
		// through the public interface. Likely not exercised in tests, so I'd be
		// ok tackling this separately.
		//
		// Scenario:
		// - we're aborted and don't know if we have a read-write conflict
		// - the push above fails and we get a LockConflictError
		// - we try to update our transaction (right here, and if we don't we might
		// be stuck in a race, that's why we do this - the txn proto we're using
		// might be outdated)
		// - query fails because our home range has the AbortSpan populated we catch
		// a TransactionAbortedError, but with a pending transaction (since we lose
		// the original txn, and you just use the txn we had...)
		//
		// so something is sketchy here, but it should all resolve nicely when we
		// don't use store.db for these internal requests any more.
		return nil, nil, kvpb.NewError(err)
	}
	br := b.RawResponse()
	resp := br.Responses[0].GetInner().(*kvpb.QueryTxnResponse)
	return &resp.QueriedTxn, resp.WaitingTxns, nil
}

// forcePushAbort upgrades the PushTxn request to a "forced" push abort, which
// overrides the normal expiration and priority checks to ensure that it aborts
// the pushee. This mechanism can be used to break deadlocks between conflicting
// transactions.
func (q *Queue) forcePushAbort(
	ctx context.Context, req *kvpb.PushTxnRequest,
) (*kvpb.PushTxnResponse, *kvpb.Error) {
	log.VEventf(ctx, 1, "force pushing %v to break deadlock", req.PusheeTxn.ID)
	forcePush := *req
	forcePush.Force = true
	forcePush.PushType = kvpb.PUSH_ABORT
	b := &kv.Batch{}
	b.Header.Timestamp = q.cfg.Clock.Now()
	b.Header.Timestamp.Forward(req.PushTo)
	b.AddRawRequest(&forcePush)
	if err := q.cfg.DB.Run(ctx, b); err != nil {
		return nil, b.MustPErr()
	}
	return b.RawResponse().Responses[0].GetPushTxn(), nil
}

// TrackedTxns returns a (newly minted) set containing the transaction IDs which
// are being tracked (i.e. waited on).
//
// For testing purposes only.
func (q *Queue) TrackedTxns() map[uuid.UUID]struct{} {
	m := make(map[uuid.UUID]struct{})
	q.mu.RLock()
	for k := range q.mu.txns {
		m[k] = struct{}{}
	}
	q.mu.RUnlock()
	return m
}
