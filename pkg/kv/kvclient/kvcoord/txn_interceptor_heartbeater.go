// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	opentracing "github.com/opentracing/opentracing-go"
)

// txnHeartbeatDuring1PC defines whether the txnHeartbeater should launch a
// heartbeat loop for 1PC transactions. The value defaults to false even though
// 1PC transactions leave intents around on retriable errors if the batch has
// been split between ranges and may be pushed when in lock wait-queues because
// we consider that unlikely enough so we prefer to not pay for a goroutine.
var txnHeartbeatFor1PC = envutil.EnvOrDefaultBool("COCKROACH_TXN_HEARTBEAT_DURING_1PC", false)

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
	}
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
) {
	h.AmbientContext = ac
	h.stopper = stopper
	h.clock = clock
	h.metrics = metrics
	h.loopInterval = loopInterval
	h.gatekeeper = gatekeeper
	h.mu.Locker = mu
	h.mu.txn = txn
}

// SendLocked is part of the txnInterceptor interface.
func (h *txnHeartbeater) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	firstLockingIndex, pErr := firstLockingIndex(&ba)
	if pErr != nil {
		return nil, pErr
	}
	if firstLockingIndex != -1 {
		// Set txn key based on the key of the first transactional write if not
		// already set. If it is already set, make sure we keep the anchor key
		// the same.
		if len(h.mu.txn.Key) == 0 {
			anchor := ba.Requests[firstLockingIndex].GetInner().Header().Key
			h.mu.txn.Key = anchor
			// Put the anchor also in the ba's copy of the txn, since this batch
			// was prepared before we had an anchor.
			ba.Txn.Key = anchor
		}

		// Start the heartbeat loop if it has not already started.
		if !h.mu.loopStarted {
			_, haveEndTxn := ba.GetArg(roachpb.EndTxn)
			if !haveEndTxn || txnHeartbeatFor1PC {
				if err := h.startHeartbeatLoopLocked(ctx); err != nil {
					return nil, roachpb.NewError(err)
				}
			}
		}
	}

	// Forward the batch through the wrapped lockedSender.
	return h.wrapped.SendLocked(ctx, ba)
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
func (*txnHeartbeater) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) {}

// epochBumpedLocked is part of the txnInterceptor interface.
func (h *txnHeartbeater) epochBumpedLocked() {}

// createSavepointLocked is part of the txnReqInterceptor interface.
func (*txnHeartbeater) createSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnReqInterceptor interface.
func (*txnHeartbeater) rollbackToSavepointLocked(context.Context, savepoint) {}

// closeLocked is part of the txnInterceptor interface.
func (h *txnHeartbeater) closeLocked() {
	h.cancelHeartbeatLoopLocked()
}

// startHeartbeatLoopLocked starts a heartbeat loop in a different goroutine.
func (h *txnHeartbeater) startHeartbeatLoopLocked(ctx context.Context) error {
	if h.mu.loopStarted {
		log.Fatal(ctx, "attempting to start a second heartbeat loop")
	}
	log.VEventf(ctx, 2, "coordinator spawns heartbeat loop")
	h.mu.loopStarted = true
	// NB: we can't do this in init() because the txn isn't populated yet then
	// (it's zero).
	h.AmbientContext.AddLogTag("txn-hb", h.mu.txn.Short())

	// Create a new context so that the heartbeat loop doesn't inherit the
	// caller's cancelation.
	// We want the loop to run in a span linked to the current one, though, so we
	// put our span in the new context and expect RunAsyncTask to fork it
	// immediately.
	hbCtx := h.AnnotateCtx(context.Background())
	hbCtx = opentracing.ContextWithSpan(hbCtx, opentracing.SpanFromContext(ctx))
	hbCtx, h.mu.loopCancel = context.WithCancel(hbCtx)

	return h.stopper.RunAsyncTask(hbCtx, "kv.TxnCoordSender: heartbeat loop", h.heartbeatLoop)
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

// heartbeat sends a HeartbeatTxnRequest to the txn record.
// Returns true if heartbeating should continue, false if the transaction is no
// longer Pending and so there's no point in heartbeating further.
func (h *txnHeartbeater) heartbeat(ctx context.Context) bool {
	// Like with the TxnCoordSender, the locking here is peculiar. The lock is not
	// held continuously throughout this method: we acquire the lock here and
	// then, inside the wrapped.Send() call, the interceptor at the bottom of the
	// stack will unlock until it receives a response.
	h.mu.Lock()
	defer h.mu.Unlock()

	// The heartbeat loop might have raced with the cancelation of the heartbeat.
	if ctx.Err() != nil {
		return false
	}

	if h.mu.txn.Status != roachpb.PENDING {
		if h.mu.txn.Status == roachpb.COMMITTED {
			log.Fatalf(ctx, "txn committed but heartbeat loop hasn't been signaled to stop: %s", h.mu.txn)
		}
		// If the transaction is aborted, there's no point in heartbeating. The
		// client needs to send a rollback.
		return false
	}

	// Clone the txn in order to put it in the heartbeat request.
	txn := h.mu.txn.Clone()
	if txn.Key == nil {
		log.Fatalf(ctx, "attempting to heartbeat txn without anchor key: %v", txn)
	}
	ba := roachpb.BatchRequest{}
	ba.Txn = txn
	ba.Add(&roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
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
		log.VEventf(ctx, 2, "heartbeat failed: %s", pErr)

		// We need to be prepared here to handle the case of a
		// TransactionAbortedError with no transaction proto in it.
		//
		// TODO(nvanbenschoten): Make this the only case where we get back an
		// Aborted txn.
		if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); ok {
			// Note that it's possible that the txn actually committed but its
			// record got GC'ed. In that case, aborting won't hurt anyone though,
			// since all intents have already been resolved.
			// The only thing we must ascertain is that we don't tell the client
			// about this error - it will get either a definitive result of
			// its commit or an ambiguous one and we have nothing to offer that
			// provides more clarity. We do however prevent it from running more
			// requests in case it isn't aware that the transaction is over.
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
			h.abortTxnAsyncLocked(ctx)
		}
		h.mu.finalObservedStatus = respTxn.Status
		return false
	}
	return true
}

// abortTxnAsyncLocked send an EndTxn(commmit=false) asynchronously.
// The purpose of the async cleanup is to resolve transaction intents as soon
// as possible when a transaction coordinator observes an ABORTED transaction.
func (h *txnHeartbeater) abortTxnAsyncLocked(ctx context.Context) {
	log.VEventf(ctx, 1, "Heartbeat detected aborted txn. Cleaning up.")

	// NB: We use context.Background() here because we don't want a canceled
	// context to interrupt the aborting.
	ctx = h.AnnotateCtx(context.Background())

	// Construct a batch with an EndTxn request.
	txn := h.mu.txn.Clone()
	ba := roachpb.BatchRequest{}
	ba.Header = roachpb.Header{Txn: txn}
	ba.Add(&roachpb.EndTxnRequest{
		Commit: false,
		// Resolved intents should maintain an abort span entry to prevent
		// concurrent requests from failing to notice the transaction was aborted.
		Poison: true,
	})

	log.VEventf(ctx, 2, "async abort for txn: %s", txn)
	if err := h.stopper.RunAsyncTask(
		ctx, "txnHeartbeater: aborting txn", func(ctx context.Context) {
			// Send the abort request through the interceptor stack. This is
			// important because we need the txnPipeliner to append lock spans
			// to the EndTxn request.
			h.mu.Lock()
			defer h.mu.Unlock()
			_, pErr := h.wrapped.SendLocked(ctx, ba)
			if pErr != nil {
				log.VErrEventf(ctx, 1, "async abort failed for %s: %s ", txn, pErr)
			}
		},
	); err != nil {
		log.Warningf(ctx, "%v", err)
	}
}

// firstLockingIndex returns the index of the first request that acquires locks
// in the BatchRequest. Returns -1 if the batch has no intention to acquire
// locks. It also verifies that if an EndTxnRequest is included, then it is the
// last request in the batch.
func firstLockingIndex(ba *roachpb.BatchRequest) (int, *roachpb.Error) {
	for i, ru := range ba.Requests {
		args := ru.GetInner()
		if i < len(ba.Requests)-1 /* if not last*/ {
			if _, ok := args.(*roachpb.EndTxnRequest); ok {
				return -1, roachpb.NewErrorf("%s sent as non-terminal call", args.Method())
			}
		}
		if roachpb.IsLocking(args) {
			return i, nil
		}
	}
	return -1, nil
}
