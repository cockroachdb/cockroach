// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// abortTxnAsyncTimeout is the context timeout for abortTxnAsyncLocked()
// rollbacks. If the intent resolver has spare async task capacity, this timeout
// only needs to be long enough for the EndTxn request to make it through Raft,
// but if the cleanup task is synchronous (to backpressure clients) then cleanup
// will be abandoned when the timeout expires. We generally want to clean up if
// possible, but not at any cost, so we set it high at 1 minute.
const abortTxnAsyncTimeout = time.Minute

// heartbeatTxnBufferPeriod is a buffer period used to determine when to start
// the heartbeat loop for the transaction. If the first locking operation
// occurs within this buffer period of expiration of the transaction, the
// transaction heartbeat should happen immediately, otherwise the heartbeat
// loop should start (at the latest) by this buffer period prior to the
// expiration. The buffer period should ensure that it is not possible
// for intents to be written prior to the transaction being considered expired.
// This attempts to avoid a transaction being considered expired (due to
// lacking a transaction record) by another pushing transaction that encounters
// its intents, as this will result in the transaction being aborted.
const heartbeatTxnBufferPeriod = 200 * time.Millisecond

// RandomizedTxnAnchorKeyEnabled dictates whether a transactions anchor key is
// chosen at random from all keys being locked in its first locking batch;
// otherwise, it's set to the first ever key that's locked by the transaction.
var RandomizedTxnAnchorKeyEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"kv.transaction.randomized_anchor_key.enabled",
	"dictates whether a transactions anchor key is randomized or not",
	metamorphic.ConstantWithTestBool("kv.transaction.randomized_anchor_key.enabled", false),
	settings.WithPublic,
)

// txnHeartbeater is a txnInterceptor in charge of a transaction's heartbeat
// loop. Transaction coordinators heartbeat their transaction record
// periodically to indicate the liveness of their transaction. Other actors like
// concurrent transactions and GC processes observe a transaction record's last
// heartbeat time to learn about its disposition and to determine whether it
// should be considered abandoned. When a transaction is considered abandoned,
// other actors are free to abort it at will. As such, it is important for a
// transaction coordinator to heartbeat its transaction record with a
// periodicity well below the abandonment threshold.
//
// Transaction coordinators only need to perform heartbeats for transactions
// that risk running for longer than the abandonment duration. For transactions
// that finish well beneath this time, a heartbeat will never be sent and the
// EndTxn request will create and immediately finalize the transaction. However,
// for transactions that live long enough that they risk running into issues
// with other's perceiving them as abandoned, the first HeartbeatTxn request
// they send will create the transaction record in the PENDING state. Future
// heartbeats will update the transaction record to indicate progressively
// larger heartbeat timestamps.
//
// NOTE: there are other mechanisms by which concurrent actors could determine
// the liveness of transactions. One proposal is to have concurrent actors
// communicate directly with transaction coordinators themselves. This would
// avoid the need for transaction heartbeats and the PENDING transaction state
// entirely. Another proposal is to detect abandoned transactions and failed
// coordinators at an entirely different level - by maintaining a node health
// plane. This would function under the idea that if the node a transaction's
// coordinator is running on is alive then that transaction is still in-progress
// unless it specifies otherwise. These are both approaches we could consider in
// the future.
type txnHeartbeater struct {
	log.AmbientContext
	stopper      *stop.Stopper
	clock        *hlc.Clock
	metrics      *TxnMetrics
	loopInterval time.Duration
	st           *cluster.Settings
	knobs        *ClientTestingKnobs

	// wrapped is the next sender in the interceptor stack.
	wrapped lockedSender
	// gatekeeper is the sender to which heartbeat requests need to be sent. It is
	// set to the gatekeeper interceptor, so sending directly to it will bypass
	// all the other interceptors; heartbeats don't need them and they can only
	// hurt - we don't want heartbeats to get sequence numbers or to check any
	// intents. Note that the async rollbacks that this interceptor sometimes
	// sends got through `wrapped`, not directly through `gatekeeper`.
	gatekeeper lockedSender

	// mu contains state protected by the TxnCoordSender's mutex.
	mu struct {
		sync.Locker

		// txn is a reference to the TxnCoordSender's proto.
		txn *roachpb.Transaction

		// loopStarted indicates whether the heartbeat loop has been launched
		// for the transaction or not. It remains true once the loop terminates.
		loopStarted bool

		// loopCancel is a function to cancel the context of the heartbeat loop.
		// Non-nil if the heartbeat loop is currently running.
		loopCancel func()

		// finalObservedStatus is the finalized status that the heartbeat loop
		// observed while heartbeating the transaction's record. As soon as the
		// heartbeat loop observes a finalized status, it shuts down.
		//
		// If the status here is COMMITTED then the transaction definitely
		// committed. However, if the status here is ABORTED then the
		// transaction may or may not have been aborted. Instead, it's possible
		// that the transaction was committed by an EndTxn request and then its
		// record was garbage collected before the heartbeat request reached the
		// record. The only way to distinguish this situation from a truly
		// aborted transaction is to consider whether or not the transaction
		// coordinator sent an EndTxn request and, if so, consider whether it
		// succeeded or not.
		//
		// Because of this ambiguity, the status is not used to immediately
		// update txn in case the heartbeat loop raced with an EndTxn request.
		// Instead, it is used by the transaction coordinator to reject any
		// future requests sent though it (which indicates that the heartbeat
		// loop did not race with an EndTxn request).
		finalObservedStatus roachpb.TransactionStatus

		// ifReqs tracks the number of in-flight requests. This is expected to
		// be either 0 or 1, but we let the txnLockGatekeeper enforce that.
		//
		// This is used to make sure we don't send EndTxn(commit=false) from
		// abortTxnAsyncLocked() concurrently with another in-flight request.
		// The TxnCoordSender assumes synchronous operation; in particular,
		// the txnPipeliner must update its lock spans with pending responses
		// before attaching the final lock spans to the EndTxn request.
		ifReqs uint8

		// abortTxnAsyncPending, if true, signals that an abortTxnAsyncLocked()
		// call is waiting for in-flight requests to complete. Once the last
		// request returns (setting ifReqs=0), it calls abortTxnAsyncLocked().
		abortTxnAsyncPending bool

		// abortTxnAsyncResultC is non-nil when an abortTxnAsyncLocked()
		// rollback is in-flight. If a client rollback arrives concurrently, it
		// will wait for the result on this channel, collapsing the requests to
		// prevent concurrent rollbacks. Only EndTxn(commit=false) requests can
		// arrive during rollback, the TxnCoordSender blocks any others due to
		// finalObservedStatus.
		abortTxnAsyncResultC chan abortTxnAsyncResult
	}
}

type abortTxnAsyncResult struct {
	br   *kvpb.BatchResponse
	pErr *kvpb.Error
}

// init initializes the txnHeartbeater. This method exists instead of a
// constructor because txnHeartbeaters live in a pool in the TxnCoordSender.
func (h *txnHeartbeater) init(
	ac log.AmbientContext,
	stopper *stop.Stopper,
	clock *hlc.Clock,
	metrics *TxnMetrics,
	loopInterval time.Duration,
	gatekeeper lockedSender,
	mu sync.Locker,
	txn *roachpb.Transaction,
	settings *cluster.Settings,
	testingKnobs *ClientTestingKnobs,
) {
	if testingKnobs == nil {
		testingKnobs = &ClientTestingKnobs{}
	}
	h.AmbientContext = ac
	h.stopper = stopper
	h.clock = clock
	h.metrics = metrics
	h.loopInterval = loopInterval
	h.st = settings
	h.knobs = testingKnobs
	h.gatekeeper = gatekeeper
	h.mu.Locker = mu
	h.mu.txn = txn
}

// SendLocked is part of the txnInterceptor interface.
func (h *txnHeartbeater) SendLocked(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, *kvpb.Error) {
	etArg, hasET := ba.GetArg(kvpb.EndTxn)
	randLockingIdx, pErr := h.randLockingIndex(ba)
	if pErr != nil {
		return nil, pErr
	}
	if randLockingIdx != -1 {
		// If the anchor key for the transaction's txn record is unset, we set it
		// here to a random key that it's locking in the supplied batch. If it's
		// already set, however, make sure we keep the anchor key the same.
		if len(h.mu.txn.Key) == 0 {
			anchor := ba.Requests[randLockingIdx].GetInner().Header().Key
			h.mu.txn.Key = anchor
			// Put the anchor also in the ba's copy of the txn, since this batch
			// was prepared before we had an anchor.
			ba.Txn.Key = anchor
		}

		// Start the heartbeat loop if it has not already started.
		if !h.mu.loopStarted {
			h.startHeartbeatLoopLocked(ctx)
		}
	}

	if hasET {
		et := etArg.(*kvpb.EndTxnRequest)

		// Preemptively stop the heartbeat loop in case of transaction abort.
		// In case of transaction commit we don't want to do this because commit
		// could fail with retryable error and transaction would be restarted
		// with the next epoch.
		if !et.Commit {
			h.cancelHeartbeatLoopLocked()

			// If an abortTxnAsyncLocked() rollback is in flight, we'll wait for
			// its result here to avoid sending a concurrent rollback.
			// Otherwise, txnLockGatekeeper would error since it does not allow
			// concurrent requests (to enforce a synchronous client protocol).
			if resultC := h.mu.abortTxnAsyncResultC; resultC != nil {
				// We have to unlock the mutex while waiting, to allow the
				// txnLockGatekeeper to acquire the mutex when receiving the
				// async abort response. Once we receive our copy of the
				// response, we re-acquire the lock to return it to the client.
				h.mu.Unlock()
				defer h.mu.Lock()
				select {
				case res := <-resultC:
					return res.br, res.pErr
				case <-ctx.Done():
					return nil, kvpb.NewError(ctx.Err())
				}
			}
		}
	}

	// Forward the batch through the wrapped lockedSender, recording the
	// in-flight request to coordinate with abortTxnAsyncLocked(). Recall that
	// the mutex is unlocked for the duration of the SendLocked() call.
	h.mu.ifReqs++
	br, pErr := h.wrapped.SendLocked(ctx, ba)
	h.mu.ifReqs--

	// If an abortTxnAsyncLocked() call is waiting for this in-flight
	// request to complete, call it. At this point, finalObservedStatus has
	// already been set, so we don't have to worry about additional incoming
	// requests (except rollbacks) -- the TxnCoordSender will block them.
	if h.mu.abortTxnAsyncPending && h.mu.ifReqs == 0 {
		h.abortTxnAsyncLocked(ctx)
		h.mu.abortTxnAsyncPending = false
	}

	return br, pErr
}

// setWrapped is part of the txnInterceptor interface.
func (h *txnHeartbeater) setWrapped(wrapped lockedSender) {
	h.wrapped = wrapped
}

// populateLeafInputState is part of the txnInterceptor interface.
func (*txnHeartbeater) populateLeafInputState(*roachpb.LeafTxnInputState) {}

// populateLeafFinalState is part of the txnInterceptor interface.
func (*txnHeartbeater) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState is part of the txnInterceptor interface.
func (*txnHeartbeater) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) error {
	return nil
}

// epochBumpedLocked is part of the txnInterceptor interface.
func (h *txnHeartbeater) epochBumpedLocked() {}

// createSavepointLocked is part of the txnInterceptor interface.
func (*txnHeartbeater) createSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (*txnHeartbeater) rollbackToSavepointLocked(context.Context, savepoint) {}

// closeLocked is part of the txnInterceptor interface.
func (h *txnHeartbeater) closeLocked() {
	h.cancelHeartbeatLoopLocked()
}

// startHeartbeatLoopLocked starts a heartbeat loop in a different goroutine.
func (h *txnHeartbeater) startHeartbeatLoopLocked(ctx context.Context) {
	if h.loopInterval < 0 {
		log.Infof(ctx, "coordinator heartbeat loop disabled")
		return
	}
	if h.mu.loopStarted {
		log.Fatal(ctx, "attempting to start a second heartbeat loop")
	}
	log.VEventf(ctx, 2, kvbase.SpawningHeartbeatLoopMsg)
	h.mu.loopStarted = true
	// NB: we can't do this in init() because the txn isn't populated yet then
	// (it's zero).
	h.AmbientContext.AddLogTag("txn-hb", h.mu.txn.Short())

	// Create a new context so that the heartbeat loop doesn't inherit the
	// caller's cancelation or span.
	hbCtx, hbCancel := context.WithCancel(h.AnnotateCtx(context.Background()))

	// If, by the time heartbeatTxnBufferPeriod has passed, this transaction would
	// be considered expired, then synchronously attempt to heartbeat immediately
	// before spawning the loop.
	heartbeatLoopDelay := h.loopInterval
	now := h.clock.Now()
	if txnwait.IsExpired(
		now.Add(heartbeatTxnBufferPeriod.Nanoseconds(), 0 /* logical */),
		h.mu.txn,
	) {
		log.VEventf(ctx, 2, "heartbeating immediately to avoid expiration")
		h.heartbeatLocked(ctx)
	} else {
		timeUntilExpiry := txnwait.TxnExpiration(h.mu.txn).GoTime().Sub(now.GoTime())
		if (timeUntilExpiry - heartbeatTxnBufferPeriod) < heartbeatLoopDelay {
			log.VEventf(ctx, 2, "scheduling heartbeat early to avoid expiration")
			heartbeatLoopDelay = timeUntilExpiry - heartbeatTxnBufferPeriod
		}
	}
	// Delay spawning the loop goroutine until the first loopInterval passes or
	// until a defined buffer period prior to expiration (whichever is first) to
	// avoid the associated cost for small write transactions. In benchmarks,
	// this gave a 3% throughput increase for point writes at high concurrency.
	timer := time.AfterFunc(heartbeatLoopDelay, func() {
		const taskName = "kv.TxnCoordSender: heartbeat loop"
		var span *tracing.Span
		hbCtx, span = h.AmbientContext.Tracer.StartSpanCtx(hbCtx, taskName)
		defer span.Finish()

		// Only errors on quiesce, which is safe to ignore.
		_ = h.stopper.RunTask(hbCtx, taskName, h.heartbeatLoop)
	})

	h.mu.loopCancel = func() {
		timer.Stop()
		hbCancel()
	}
}

func (h *txnHeartbeater) cancelHeartbeatLoopLocked() {
	// If the heartbeat loop has already started, cancel it.
	if h.heartbeatLoopRunningLocked() {
		h.mu.loopCancel()
		h.mu.loopCancel = nil
	}
}

func (h *txnHeartbeater) heartbeatLoopRunningLocked() bool {
	return h.mu.loopCancel != nil
}

// heartbeatLoop periodically sends a HeartbeatTxn request to the transaction
// record, stopping in the event the transaction is aborted or committed after
// attempting to resolve the intents.
func (h *txnHeartbeater) heartbeatLoop(ctx context.Context) {
	defer func() {
		h.mu.Lock()
		h.cancelHeartbeatLoopLocked()
		h.mu.Unlock()
	}()

	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(h.loopInterval)
		tickChan = ticker.C
		defer ticker.Stop()
	}

	// Loop is only spawned after loopInterval, so heartbeat immediately.
	if !h.heartbeat(ctx) {
		return
	}

	// Loop with ticker for periodic heartbeats.
	for {
		select {
		case <-tickChan:
			if !h.heartbeat(ctx) {
				// The heartbeat noticed a finalized transaction,
				// so shut down the heartbeat loop.
				return
			}
		case <-ctx.Done():
			// Transaction finished normally.
			return
		case <-h.stopper.ShouldQuiesce():
			return
		}
	}
}

// heartbeat is a convenience method to be called by the heartbeat loop, acquiring
// the mutex and issuing a request using heartbeatLocked before releasing it.
// See comment on heartbeatLocked for more explanation.
func (h *txnHeartbeater) heartbeat(ctx context.Context) bool {
	// Like with the TxnCoordSender, the locking here is peculiar. The lock is not
	// held continuously throughout the heartbeatLocked method: we acquire the
	// lock here and then, inside the wrapped.Send() call, the interceptor at the
	// bottom of the stack will unlock until it receives a response.
	h.mu.Lock()
	defer h.mu.Unlock()

	// The heartbeat loop might have raced with the cancellation of the heartbeat.
	if ctx.Err() != nil {
		return false
	}

	return h.heartbeatLocked(ctx)
}

// heartbeatLocked sends a HeartbeatTxnRequest to the txn record.
// Returns true if heartbeating should continue, false if the transaction is no
// longer Pending and so there's no point in heartbeating further.
func (h *txnHeartbeater) heartbeatLocked(ctx context.Context) bool {
	switch h.mu.txn.Status {
	case roachpb.PENDING:
		// Continue heartbeating.
	case roachpb.PREPARED:
		// If the transaction is prepared, there's no point in heartbeating. The
		// transaction will remain active without heartbeats until it is committed
		// or rolled back.
		return false
	case roachpb.ABORTED:
		// If the transaction is aborted, there's no point in heartbeating. The
		// client needs to send a rollback.
		return false
	case roachpb.COMMITTED:
		log.Fatalf(ctx, "txn committed but heartbeat loop hasn't been signaled to stop: %s", h.mu.txn)
	default:
		log.Fatalf(ctx, "unexpected txn status in heartbeat loop: %s", h.mu.txn)
	}

	// Clone the txn in order to put it in the heartbeat request.
	txn := h.mu.txn.Clone()
	if txn.Key == nil {
		log.Fatalf(ctx, "attempting to heartbeat txn without anchor key: %v", txn)
	}
	ba := &kvpb.BatchRequest{}
	ba.Txn = txn
	ba.Add(&kvpb.HeartbeatTxnRequest{
		RequestHeader: kvpb.RequestHeader{
			Key: txn.Key,
		},
		Now: h.clock.Now(),
	})

	// Send the heartbeat request directly through the gatekeeper interceptor.
	// See comment on h.gatekeeper for a discussion of why.
	log.VEvent(ctx, 2, "heartbeat")
	br, pErr := h.gatekeeper.SendLocked(ctx, ba)

	// If the txn is no longer pending, ignore the result of the heartbeat
	// and tear down the heartbeat loop.
	if h.mu.txn.Status != roachpb.PENDING {
		return false
	}

	var respTxn *roachpb.Transaction
	if pErr != nil {
		log.VEventf(ctx, 2, "heartbeat failed for %s: %s", h.mu.txn, pErr)

		// We need to be prepared here to handle the case of a
		// TransactionAbortedError with no transaction proto in it.
		//
		// TODO(nvanbenschoten): Make this the only case where we get back an
		// Aborted txn.
		if _, ok := pErr.GetDetail().(*kvpb.TransactionAbortedError); ok {
			// Note that it's possible that the txn actually committed but its
			// record got GC'ed. In that case, aborting won't hurt anyone though,
			// since all intents have already been resolved.
			// The only thing we must ascertain is that we don't tell the client
			// about this error - it will get either a definitive result of
			// its commit or an ambiguous one and we have nothing to offer that
			// provides more clarity. We do however prevent it from running more
			// requests in case it isn't aware that the transaction is over.
			log.VEventf(ctx, 1, "Heartbeat detected aborted txn, cleaning up for %s", h.mu.txn)
			h.abortTxnAsyncLocked(ctx)
			h.mu.finalObservedStatus = roachpb.ABORTED
			return false
		}

		respTxn = pErr.GetTxn()
	} else {
		respTxn = br.Txn
	}

	// Tear down the heartbeat loop if the response transaction is finalized.
	if respTxn != nil && respTxn.Status.IsFinalized() {
		switch respTxn.Status {
		case roachpb.COMMITTED:
			// Shut down the heartbeat loop without doing anything else.
			// We must have raced with an EndTxn(commit=true).
		case roachpb.ABORTED:
			// Roll back the transaction record to clean up intents and
			// then shut down the heartbeat loop.
			log.VEventf(ctx, 1, "Heartbeat detected aborted txn, cleaning up for %s", h.mu.txn)
			h.abortTxnAsyncLocked(ctx)
		}
		h.mu.finalObservedStatus = respTxn.Status
		return false
	}
	return true
}

// abortTxnAsyncLocked sends an EndTxn(commit=false) asynchronously.
// The purpose of the async cleanup is to resolve transaction intents as soon
// as possible when a transaction coordinator observes an ABORTED transaction.
func (h *txnHeartbeater) abortTxnAsyncLocked(ctx context.Context) {

	// If a request is in flight, we must wait for it to complete first such
	// that txnPipeliner can record its lock spans and attach them to the EndTxn
	// request we'll send.
	if h.mu.ifReqs > 0 {
		h.mu.abortTxnAsyncPending = true
		log.VEventf(ctx, 2, "async abort waiting for in-flight request for txn %s", h.mu.txn)
		return
	}

	// Construct a batch with an EndTxn request.
	txn := h.mu.txn.Clone()
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: txn}
	ba.Add(&kvpb.EndTxnRequest{
		Commit: false,
		// Resolved intents should maintain an abort span entry to prevent
		// concurrent requests from failing to notice the transaction was aborted.
		Poison: true,
	})
	// NB: Setting `Source: kvpb.AdmissionHeader_OTHER` means this request will
	// bypass AC.
	ba.AdmissionHeader = kvpb.AdmissionHeader{
		Priority:   txn.AdmissionPriority,
		CreateTime: timeutil.Now().UnixNano(),
		Source:     kvpb.AdmissionHeader_OTHER,
	}

	const taskName = "txnHeartbeater: aborting txn"
	log.VEventf(ctx, 2, "async abort for txn: %s", txn)
	if err := h.stopper.RunAsyncTask(h.AnnotateCtx(context.Background()), taskName,
		func(ctx context.Context) {
			if err := timeutil.RunWithTimeout(ctx, taskName, abortTxnAsyncTimeout,
				func(ctx context.Context) error {
					h.mu.Lock()
					defer h.mu.Unlock()

					// If we find an abortTxnAsyncResultC, that means an async
					// rollback request is already in flight, so there's no
					// point in us running another. This can happen because the
					// TxnCoordSender also calls abortTxnAsyncLocked()
					// independently of the heartbeat loop.
					if h.mu.abortTxnAsyncResultC != nil {
						log.VEventf(ctx, 2,
							"skipping async abort due to concurrent async abort for %s", txn)
						return nil
					}

					// TxnCoordSender allows EndTxn(commit=false) through even
					// after we set finalObservedStatus, and that request can
					// race with us for the mutex. Thus, if we find an in-flight
					// request here, after checking ifReqs=0 before being spawned,
					// we deduce that it must have been a rollback and there's no
					// point in sending another rollback.
					if h.mu.ifReqs > 0 {
						log.VEventf(ctx, 2,
							"skipping async abort due to client rollback for %s", txn)
						return nil
					}

					// Set up a result channel to signal to an incoming client
					// rollback that an async rollback is already in progress,
					// and pass it the result. The buffer allows storing the
					// result even when no client rollback arrives. Recall that
					// the SendLocked() call below releases the mutex while
					// running, allowing concurrent incoming requests.
					h.mu.abortTxnAsyncResultC = make(chan abortTxnAsyncResult, 1)

					// Send the abort request through the interceptor stack. This is
					// important because we need the txnPipeliner to append lock spans
					// to the EndTxn request.
					br, pErr := h.wrapped.SendLocked(ctx, ba)
					if pErr != nil {
						log.VErrEventf(ctx, 1, "async abort failed for %s: %s ", txn, pErr)
						h.metrics.AsyncRollbacksFailed.Inc(1)
					}

					// Pass the result to a waiting client rollback, if any, and
					// remove the channel since we're no longer in flight.
					h.mu.abortTxnAsyncResultC <- abortTxnAsyncResult{br: br, pErr: pErr}
					h.mu.abortTxnAsyncResultC = nil
					return nil
				},
			); err != nil {
				log.VEventf(ctx, 1, "async abort failed for %s: %s", txn, err)
			}
		},
	); err != nil {
		log.Warningf(ctx, "%v", err)
		h.metrics.AsyncRollbacksFailed.Inc(1)
	}
}

// randLockingIndex returns the index of the first request that acquires locks
// in the BatchRequest. Returns -1 if the batch has no intention to acquire
// locks. It also verifies that if an EndTxnRequest is included, then it is the
// last request in the batch.
func (h *txnHeartbeater) randLockingIndex(ba *kvpb.BatchRequest) (int, *kvpb.Error) {
	// We don't know the number of locking requests in the supplied batch request,
	// if any. We'll use reservoir sampling to get a uniform distribution for our
	// random pick. To do so, we need to keep track of the number of locking
	// requests we've seen so far and the index of our pick.
	numLocking := 0
	idx := -1
	disableRandomization := h.knobs.DisableTxnAnchorKeyRandomization ||
		!RandomizedTxnAnchorKeyEnabled.Get(&h.st.SV)
	for i, ru := range ba.Requests {
		args := ru.GetInner()
		if i < len(ba.Requests)-1 /* if not last */ {
			if _, ok := args.(*kvpb.EndTxnRequest); ok {
				return -1, kvpb.NewErrorf("%s sent as non-terminal call", args.Method())
			}
		}
		if kvpb.IsLocking(args) {
			if disableRandomization {
				return i, nil // return the index of the first locking request if randomization is disabled
			}
			numLocking++
			if numLocking == 1 { // fastpath; no need to generate a random number
				idx = i
			} else if randutil.FastUint32()%uint32(numLocking) == 0 {
				// Reservoir sampling picks a locking request with prob =
				// 1/numLockingRequestsSeenSoFar.
				idx = i
			}
		}
	}
	return idx, nil
}
