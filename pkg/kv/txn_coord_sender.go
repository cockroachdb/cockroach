// Copyright 2014 The Cockroach Authors.
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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"fmt"
	"sync/atomic"
	"time"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	statusLogInterval = 5 * time.Second
	opTxnCoordSender  = "txn coordinator"
	opHeartbeatLoop   = "heartbeat"
)

var errNoState = errors.New("writing transaction timed out or ran on multiple coordinators")

// txnMetadata holds information about an ongoing transaction, as
// seen from the perspective of this coordinator. It records all
// keys (and key ranges) mutated as part of the transaction for
// resolution upon transaction commit or abort.
type txnMetadata struct {
	// txn is a copy of the transaction record, updated with each request.
	txn roachpb.Transaction

	// keys stores key ranges affected by this transaction through this
	// coordinator. By keeping this record, the coordinator will be able
	// to update the write intent when the transaction is committed.
	keys []roachpb.Span

	// lastUpdateNanos is the latest wall time in nanos the client sent
	// transaction operations to this coordinator. Accessed and updated
	// atomically.
	lastUpdateNanos int64

	// Analogous to lastUpdateNanos, this is the wall time at which the
	// transaction was instantiated.
	firstUpdateNanos int64

	// timeoutDuration is the time after which the transaction should be
	// considered abandoned by the client. That is, when
	// current_timestamp > lastUpdateTS + timeoutDuration.
	timeoutDuration time.Duration

	// txnEnd is closed when the transaction is aborted or committed,
	// terminating the associated heartbeat instance.
	txnEnd chan struct{}
}

// setLastUpdate updates the wall time (in nanoseconds) since the most
// recent client operation for this transaction through the coordinator.
func (tm *txnMetadata) setLastUpdate(nowNanos int64) {
	atomic.StoreInt64(&tm.lastUpdateNanos, nowNanos)
}

// getLastUpdate atomically loads the nanosecond wall time of the most
// recent client operation.
func (tm *txnMetadata) getLastUpdate() int64 {
	return atomic.LoadInt64(&tm.lastUpdateNanos)
}

// hasClientAbandonedCoord returns true if the transaction has not
// been updated by the client adding a request within the allowed
// timeout.
func (tm *txnMetadata) hasClientAbandonedCoord(nowNanos int64) bool {
	timeout := nowNanos - tm.timeoutDuration.Nanoseconds()
	return tm.getLastUpdate() < timeout
}

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts     *metric.CounterWithRates
	Commits    *metric.CounterWithRates
	Commits1PC *metric.CounterWithRates // Commits which finished in a single phase
	Abandons   *metric.CounterWithRates
	Durations  *metric.Histogram

	// Restarts is the number of times we had to restart the transaction.
	Restarts *metric.Histogram
}

var (
	metaAbortsRates = metric.Metadata{
		Name: "txn.aborts",
		Help: "Number of aborted KV transactions"}
	metaCommitsRates = metric.Metadata{
		Name: "txn.commits",
		Help: "Number of committed KV transactions (including 1PC)"}
	metaCommits1PCRates = metric.Metadata{
		Name: "txn.commits1PC",
		Help: "Number of committed one-phase KV transactions"}
	metaAbandonsRates = metric.Metadata{
		Name: "txn.abandons",
		Help: "Number of abandoned KV transactions"}
	metaDurationsHistograms = metric.Metadata{
		Name: "txn.durations",
		Help: "KV transaction durations"}
	metaRestartsHistogram = metric.Metadata{
		Name: "txn.restarts",
		Help: "Number of restarted KV transactions"}
)

// MakeTxnMetrics returns a TxnMetrics struct that contains metrics whose
// windowed portions retain data for approximately histogramWindow.
func MakeTxnMetrics(histogramWindow time.Duration) TxnMetrics {
	return TxnMetrics{
		Aborts:     metric.NewCounterWithRates(metaAbortsRates),
		Commits:    metric.NewCounterWithRates(metaCommitsRates),
		Commits1PC: metric.NewCounterWithRates(metaCommits1PCRates),
		Abandons:   metric.NewCounterWithRates(metaAbandonsRates),
		Durations:  metric.NewLatency(metaDurationsHistograms, histogramWindow),
		Restarts:   metric.NewHistogram(metaRestartsHistogram, histogramWindow, 100, 3),
	}
}

// A TxnCoordSender is an implementation of client.Sender which
// wraps a lower-level Sender (either a storage.Stores or a DistSender)
// to which it sends commands. It acts as a man-in-the-middle,
// coordinating transaction state for clients.  After a transaction is
// started, the TxnCoordSender starts asynchronously sending heartbeat
// messages to that transaction's txn record, to keep it live. It also
// keeps track of each written key or key range over the course of the
// transaction. When the transaction is committed or aborted, it
// clears accumulated write intents for the transaction.
type TxnCoordSender struct {
	log.AmbientContext

	wrapped           client.Sender
	clock             *hlc.Clock
	heartbeatInterval time.Duration
	clientTimeout     time.Duration
	txnMu             struct {
		syncutil.Mutex
		txns map[uuid.UUID]*txnMetadata // txn key to metadata
	}
	linearizable bool // enables linearizable behaviour
	stopper      *stop.Stopper
	metrics      TxnMetrics
}

var _ client.Sender = &TxnCoordSender{}

// NewTxnCoordSender creates a new TxnCoordSender for use from a KV
// distributed DB instance.
// ctx is the base context and is used for logs and traces when there isn't a
// more specific context available; it must have a Tracer set.
func NewTxnCoordSender(
	ambient log.AmbientContext,
	wrapped client.Sender,
	clock *hlc.Clock,
	linearizable bool,
	stopper *stop.Stopper,
	txnMetrics TxnMetrics,
) *TxnCoordSender {
	tc := &TxnCoordSender{
		AmbientContext:    ambient,
		wrapped:           wrapped,
		clock:             clock,
		heartbeatInterval: base.DefaultHeartbeatInterval,
		clientTimeout:     defaultClientTimeout,
		linearizable:      linearizable,
		stopper:           stopper,
		metrics:           txnMetrics,
	}
	tc.txnMu.txns = map[uuid.UUID]*txnMetadata{}

	ctx := tc.AnnotateCtx(context.Background())
	tc.stopper.RunWorker(ctx, func(ctx context.Context) {
		tc.printStatsLoop(ctx)
	})
	return tc
}

// printStatsLoop blocks and periodically logs transaction statistics
// (throughput, success rates, durations, ...). Note that this only captures
// write txns, since read-only txns are stateless as far as TxnCoordSender is
// concerned. stats).
func (tc *TxnCoordSender) printStatsLoop(ctx context.Context) {
	res := time.Millisecond // for duration logging resolution
	var statusLogTimer timeutil.Timer
	defer statusLogTimer.Stop()
	scale := metric.Scale1M
	for {
		statusLogTimer.Reset(statusLogInterval)
		select {
		case <-statusLogTimer.C:
			statusLogTimer.Read = true
			// Take a snapshot of metrics. There's some chance of skew, since the snapshots are
			// not done atomically, but that should be fine for these debug stats.
			metrics := tc.metrics
			durations, durationsWindow := metrics.Durations.Windowed()
			restarts, restartsWindow := metrics.Restarts.Windowed()
			if restartsWindow != durationsWindow {
				log.Fatalf(ctx,
					"misconfigured windowed histograms: %s != %s",
					restartsWindow,
					durationsWindow,
				)
			}
			commitRate := metrics.Commits.Rates[scale].Value()
			commit1PCRate := metrics.Commits1PC.Rates[scale].Value()
			abortRate := metrics.Aborts.Rates[scale].Value()
			abandonRate := metrics.Abandons.Rates[scale].Value()

			// Show transaction stats over the last minute. Maybe this should
			// be shorter in the future. We'll revisit if we get sufficient
			// feedback.
			totalRate := commitRate + abortRate + abandonRate
			var pCommitted, pCommitted1PC, pAbandoned, pAborted float64
			if totalRate > 0 {
				pCommitted = 100 * (commitRate / totalRate)
				pCommitted1PC = 100 * (commit1PCRate / totalRate)
				pAborted = 100 * (abortRate / totalRate)
				pAbandoned = 100 * (abandonRate / totalRate)
			}

			dMean := durations.Mean()
			dDev := durations.StdDev()
			dMax := durations.Max()
			rMean := restarts.Mean()
			rDev := restarts.StdDev()
			rMax := restarts.Max()
			num := durations.TotalCount()

			// We could skip calculating everything if !log.V(1) but we want to make
			// sure the code above doesn't silently break.
			if log.V(1) {
				log.Infof(ctx,
					"txn coordinator: %.2f txn/sec, %.2f/%.2f/%.2f/%.2f %%cmmt/cmmt1pc/abrt/abnd, "+
						"%s/%s/%s avg/σ/max duration, %.1f/%.1f/%d avg/σ/max restarts (%d samples over %s)",
					totalRate, pCommitted, pCommitted1PC, pAborted, pAbandoned,
					util.TruncateDuration(time.Duration(dMean), res),
					util.TruncateDuration(time.Duration(dDev), res),
					util.TruncateDuration(time.Duration(dMax), res),
					rMean, rDev, rMax, num, restartsWindow,
				)
			}
		case <-tc.stopper.ShouldStop():
			return
		}
	}
}

// Send implements the batch.Sender interface. If the request is part of a
// transaction, the TxnCoordSender adds the transaction to a map of active
// transactions and begins heartbeating it. Every subsequent request for the
// same transaction updates the lastUpdate timestamp to prevent live
// transactions from being considered abandoned and garbage collected.
// Read/write mutating requests have their key or key range added to the
// transaction's interval tree of key ranges for eventual cleanup via resolved
// write intents; they're tagged to an outgoing EndTransaction request, with
// the receiving replica in charge of resolving them.
func (tc *TxnCoordSender) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	ctx = tc.AnnotateCtx(ctx)

	// Start new or pick up active trace. From here on, there's always an active
	// Trace, though its overhead is small unless it's sampled.
	sp := opentracing.SpanFromContext(ctx)
	var tracer opentracing.Tracer
	if sp == nil {
		tracer = tc.AmbientContext.Tracer
		sp = tracer.StartSpan(opTxnCoordSender)
		defer sp.Finish()
		ctx = opentracing.ContextWithSpan(ctx, sp)
	} else {
		tracer = sp.Tracer()
	}

	startNS := tc.clock.PhysicalNow()

	if ba.Txn != nil {
		// If this request is part of a transaction...
		if err := tc.validateTxnForBatch(&ba); err != nil {
			return nil, roachpb.NewError(err)
		}

		txnID := *ba.Txn.ID

		// Associate the txnID with the trace. We need to do this after the
		// maybeBeginTxn call. We set both a baggage item and a tag because only
		// tags show up in the Lightstep UI.
		txnIDStr := txnID.String()
		sp.SetTag("txnID", txnIDStr)
		sp.SetBaggageItem("txnID", txnIDStr)

		var et *roachpb.EndTransactionRequest
		var hasET bool
		{
			var rArgs roachpb.Request
			rArgs, hasET = ba.GetArg(roachpb.EndTransaction)
			if hasET {
				et = rArgs.(*roachpb.EndTransactionRequest)
				if len(et.Key) != 0 {
					return nil, roachpb.NewErrorf("EndTransaction must not have a Key set")
				}
				et.Key = ba.Txn.Key
				if len(et.IntentSpans) > 0 {
					// TODO(tschottdorf): it may be useful to allow this later.
					// That would be part of a possible plan to allow txns which
					// write on multiple coordinators.
					return nil, roachpb.NewErrorf("client must not pass intents to EndTransaction")
				}
			}
		}

		if pErr := func() *roachpb.Error {
			tc.txnMu.Lock()
			defer tc.txnMu.Unlock()
			if pErr := tc.maybeRejectClientLocked(ctx, *ba.Txn); pErr != nil {
				return pErr
			}

			if !hasET {
				return nil
			}
			// Everything below is carried out only when trying to commit.

			// Populate et.IntentSpans, taking into account both any existing
			// and new writes, and taking care to perform proper deduplication.
			txnMeta := tc.txnMu.txns[txnID]
			distinctSpans := true
			if txnMeta != nil {
				et.IntentSpans = txnMeta.keys
				// Defensively set distinctSpans to false if we had any previous
				// requests in this transaction. This effectively limits the distinct
				// spans optimization to 1pc transactions.
				distinctSpans = len(txnMeta.keys) == 0
			}
			// We can't pass in a batch response here to better limit the key
			// spans as we don't know what is going to be affected. This will
			// affect queries such as `DELETE FROM my.table LIMIT 10` when
			// executed as a 1PC transaction. e.g.: a (BeginTransaction,
			// DeleteRange, EndTransaction) batch.
			ba.IntentSpanIterate(nil, func(key, endKey roachpb.Key) {
				et.IntentSpans = append(et.IntentSpans, roachpb.Span{
					Key:    key,
					EndKey: endKey,
				})
			})
			// TODO(peter): Populate DistinctSpans on all batches, not just batches
			// which contain an EndTransactionRequest.
			var distinct bool
			// The request might already be used by an outgoing goroutine, so
			// we can't safely mutate anything in-place (as MergeSpans does).
			et.IntentSpans = append([]roachpb.Span(nil), et.IntentSpans...)
			et.IntentSpans, distinct = roachpb.MergeSpans(et.IntentSpans)
			ba.Header.DistinctSpans = distinct && distinctSpans
			if len(et.IntentSpans) == 0 {
				// If there aren't any intents, then there's factually no
				// transaction to end. Read-only txns have all of their state
				// in the client.
				return roachpb.NewErrorf("cannot commit a read-only transaction")
			}
			if txnMeta != nil {
				txnMeta.keys = et.IntentSpans
			}
			return nil
		}(); pErr != nil {
			return nil, pErr
		}

		if hasET && log.V(1) {
			for _, intent := range et.IntentSpans {
				log.Eventf(ctx, "intent: [%s,%s)", intent.Key, intent.EndKey)
			}
		}
	}

	// Embed the trace metadata into the header for use by RPC recipients. We need
	// to do this after the maybeBeginTxn call above.
	// TODO(tschottdorf): To get rid of the spurious alloc below we need to
	// implement the carrier interface on ba.Header or make Span non-nullable,
	// both of which force all of ba on the Heap. It's already there, so may
	// not be a big deal, but ba should live on the stack. Also not easy to use
	// a buffer pool here since anything that goes into the RPC layer could be
	// used by goroutines we didn't wait for.
	if ba.TraceContext == nil {
		ba.TraceContext = &tracing.SpanContextCarrier{}
	} else {
		// We didn't make this object but are about to mutate it, so we
		// have to take a copy - the original might already have been
		// passed to the RPC layer.
		ba.TraceContext = protoutil.Clone(ba.TraceContext).(*tracing.SpanContextCarrier)
	}
	// TODO(andrei): we shouldn't be injecting the span here; we should be
	// injecting it at a much lower level, when we're actually sending the RPC
	// (i.e. in gRPCTransport). Injecting it here causes the server-side spans to
	// not be children of the client's leaf spans.
	if err := tracer.Inject(sp.Context(), basictracer.Delegator, ba.TraceContext); err != nil {
		return nil, roachpb.NewError(err)
	}
	// Clear the trace context if it wasn't initialized.
	if ba.TraceContext.TraceID == 0 {
		ba.TraceContext = nil
	}

	// Send the command through wrapped sender, taking appropriate measures
	// on error.
	var br *roachpb.BatchResponse
	{
		var pErr *roachpb.Error
		br, pErr = tc.wrapped.Send(ctx, ba)

		if _, ok := pErr.GetDetail().(*roachpb.OpRequiresTxnError); ok {
			br, pErr = tc.resendWithTxn(ctx, ba)
		}

		if pErr = tc.updateState(ctx, startNS, ba, br, pErr); pErr != nil {
			log.Eventf(ctx, "error: %s", pErr)
			return nil, pErr
		}
	}

	if br.Txn == nil {
		return br, nil
	}

	if _, ok := ba.GetArg(roachpb.EndTransaction); !ok {
		return br, nil
	}
	// If the --linearizable flag is set, we want to make sure that
	// all the clocks in the system are past the commit timestamp
	// of the transaction. This is guaranteed if either
	// - the commit timestamp is MaxOffset behind startNS
	// - MaxOffset ns were spent in this function
	// when returning to the client. Below we choose the option
	// that involves less waiting, which is likely the first one
	// unless a transaction commits with an odd timestamp.
	if tsNS := br.Txn.Timestamp.WallTime; startNS > tsNS {
		startNS = tsNS
	}
	sleepNS := tc.clock.MaxOffset() -
		time.Duration(tc.clock.PhysicalNow()-startNS)
	if tc.linearizable && sleepNS > 0 {
		defer func() {
			if log.V(1) {
				log.Infof(ctx, "%v: waiting %s on EndTransaction for linearizability", br.Txn.Short(), util.TruncateDuration(sleepNS, time.Millisecond))
			}
			time.Sleep(sleepNS)
		}()
	}
	if br.Txn.Status != roachpb.PENDING {
		tc.txnMu.Lock()
		tc.cleanupTxnLocked(ctx, *br.Txn)
		tc.txnMu.Unlock()
	}
	return br, nil
}

// maybeRejectClientLocked checks whether the (transactional) request is in a
// state that prevents it from continuing, such as the coordinator having
// considered the client abandoned, or a heartbeat having reported an error.
func (tc *TxnCoordSender) maybeRejectClientLocked(
	ctx context.Context, txn roachpb.Transaction,
) *roachpb.Error {

	if !txn.Writing {
		return nil
	}
	txnMeta, ok := tc.txnMu.txns[*txn.ID]
	// Check whether the transaction is still tracked and has a chance of
	// completing. It's possible that the coordinator learns about the
	// transaction having terminated from a heartbeat, and GC queue correctness
	// (along with common sense) mandates that we don't let the client
	// continue.
	switch {
	case !ok:
		log.VEventf(ctx, 2, "rejecting unknown txn: %s", txn.ID)
		// TODO(spencerkimball): Could add coordinator node ID to the
		// transaction session so that we can definitively return the right
		// error between these possible errors. Or update the code to make an
		// educated guess based on the incoming transaction timestamp.
		return roachpb.NewError(errNoState)
	case txnMeta.txn.Status == roachpb.ABORTED:
		txn := txnMeta.txn.Clone()
		tc.cleanupTxnLocked(ctx, txn)
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionAbortedError(),
			&txn)
	case txnMeta.txn.Status == roachpb.COMMITTED:
		txn := txnMeta.txn.Clone()
		tc.cleanupTxnLocked(ctx, txn)
		return roachpb.NewErrorWithTxn(roachpb.NewTransactionStatusError(
			"transaction is already committed"), &txn)
	default:
		return nil
	}
}

// validateTxn validates properties of a txn specified on a request.
// The transaction is expected to be initialized by the time it reaches
// the TxnCoordSender. Furthermore, no transactional writes are allowed
// unless preceded by a begin transaction request within the same batch.
// The exception is if the transaction is already in state txn.Writing=true.
func (tc *TxnCoordSender) validateTxnForBatch(ba *roachpb.BatchRequest) error {
	if len(ba.Requests) == 0 {
		return errors.Errorf("empty batch with txn")
	}
	if !ba.Txn.IsInitialized() {
		return errors.Errorf("uninitialized txn found on BatchRequest passed to TxnCoordSender: %v", ba)
	}

	// Check for a begin transaction to set txn key based on the key of
	// the first transactional write. Also enforce that no transactional
	// writes occur before a begin transaction.
	var haveBeginTxn bool
	for _, req := range ba.Requests {
		args := req.GetInner()
		if _, ok := args.(*roachpb.BeginTransactionRequest); ok {
			if haveBeginTxn || ba.Txn.Writing {
				return errors.Errorf("begin transaction requested twice in the same txn: %s", ba.Txn)
			}
			if ba.Txn.Key == nil {
				return errors.Errorf("transaction with BeginTxnRequest missing anchor key: %v", ba)
			}
			haveBeginTxn = true
		}
	}
	return nil
}

// cleanupTxnLocked is called when a transaction ends. The transaction record
// is updated and the heartbeat goroutine signaled to clean up the transaction
// gracefully.
func (tc *TxnCoordSender) cleanupTxnLocked(ctx context.Context, txn roachpb.Transaction) {
	log.Event(ctx, "coordinator stops")
	txnMeta, ok := tc.txnMu.txns[*txn.ID]
	// The heartbeat might've already removed the record. Or we may have already
	// closed txnEnd but we are racing with the heartbeat cleanup.
	if !ok || txnMeta.txnEnd == nil {
		return
	}

	// The supplied txn may be newer than the one in txnMeta, which is relevant
	// for stats.
	txnMeta.txn = txn
	// Trigger heartbeat shutdown.
	close(txnMeta.txnEnd)
	txnMeta.txnEnd = nil
}

// unregisterTxn deletes a txnMetadata object from the sender
// and collects its stats. It assumes the lock is held. Returns
// the duration, restarts, and finalized txn status.
func (tc *TxnCoordSender) unregisterTxnLocked(
	txnID uuid.UUID,
) (duration, restarts int64, status roachpb.TransactionStatus) {
	txnMeta := tc.txnMu.txns[txnID] // guaranteed to exist
	if txnMeta == nil {
		panic(fmt.Sprintf("attempt to unregister non-existent transaction: %s", txnID))
	}
	duration = tc.clock.PhysicalNow() - txnMeta.firstUpdateNanos
	restarts = int64(txnMeta.txn.Epoch)
	status = txnMeta.txn.Status

	txnMeta.keys = nil

	delete(tc.txnMu.txns, txnID)

	return duration, restarts, status
}

// heartbeatLoop periodically sends a HeartbeatTxn RPC to an extant transaction,
// stopping in the event the transaction is aborted or committed after
// attempting to resolve the intents. When the heartbeat stops, the transaction
// is unregistered from the coordinator.
//
// TODO(dan): The Context we use for this is currently the one from the first
// request in a Txn, but the semantics of this aren't good. Each context has its
// own associated lifetime and we're ignoring all but the first. It happens now
// that we pass the same one in every request, but it's brittle to rely on this
// forever.
// TODO(wiz): Update (*DBServer).Batch to not use context.TODO().
func (tc *TxnCoordSender) heartbeatLoop(ctx context.Context, txnID uuid.UUID) {
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(tc.heartbeatInterval)
		tickChan = ticker.C
		defer ticker.Stop()
	}
	defer func() {
		tc.txnMu.Lock()
		duration, restarts, status := tc.unregisterTxnLocked(txnID)
		tc.txnMu.Unlock()
		tc.updateStats(duration, restarts, status, false)
	}()

	var closer <-chan struct{}
	// TODO(tschottdorf): this should join to the trace of the request
	// which starts this goroutine.
	sp := tc.AmbientContext.Tracer.StartSpan(opHeartbeatLoop)
	defer sp.Finish()
	ctx = opentracing.ContextWithSpan(ctx, sp)

	{
		tc.txnMu.Lock()
		txnMeta := tc.txnMu.txns[txnID] // do not leak to outer scope
		closer = txnMeta.txnEnd
		tc.txnMu.Unlock()
	}
	if closer == nil {
		// Avoid race in which a Txn is cleaned up before the heartbeat
		// goroutine gets a chance to start.
		return
	}
	// Loop with ticker for periodic heartbeats.
	for {
		select {
		case <-tickChan:
			if !tc.heartbeat(ctx, txnID) {
				return
			}
		case <-closer:
			// Transaction finished normally.
			return
		case <-ctx.Done():
			// Note that if ctx is not cancelable, then ctx.Done() returns a nil
			// channel, which blocks forever. In this case, the heartbeat loop is
			// responsible for timing out transactions. If ctx.Done() is not nil, then
			// then heartbeat loop ignores the timeout check and this case is
			// responsible for client timeouts.
			tc.tryAsyncAbort(txnID)
			return
		case <-tc.stopper.ShouldQuiesce():
			return
		}
	}
}

// tryAsyncAbort (synchronously) grabs a copy of the txn proto and the intents
// (which it then clears from txnMeta), and asynchronously tries to abort the
// transaction.
func (tc *TxnCoordSender) tryAsyncAbort(txnID uuid.UUID) {
	tc.txnMu.Lock()
	txnMeta := tc.txnMu.txns[txnID]
	// Clone the intents and the txn to avoid data races.
	intentSpans, _ := roachpb.MergeSpans(append([]roachpb.Span(nil), txnMeta.keys...))
	txnMeta.keys = nil
	txn := txnMeta.txn.Clone()
	tc.txnMu.Unlock()

	// Since we don't hold the lock continuously, it's possible that two aborts
	// raced here. That's fine (and probably better than the alternative, which
	// is missing new intents sometimes).
	if txn.Status != roachpb.PENDING {
		return
	}

	ba := roachpb.BatchRequest{}
	ba.Txn = &txn

	et := &roachpb.EndTransactionRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
		Commit:      false,
		IntentSpans: intentSpans,
	}
	ba.Add(et)
	// NB: use context.Background() here because we may be called when the
	// caller's context has been cancelled.
	ctx := tc.AnnotateCtx(context.Background())
	if err := tc.stopper.RunAsyncTask(ctx, func(ctx context.Context) {
		// Use the wrapped sender since the normal Sender does not allow
		// clients to specify intents.
		if _, pErr := tc.wrapped.Send(ctx, ba); pErr != nil {
			if log.V(1) {
				log.Warningf(ctx, "abort due to inactivity failed for %s: %s ", txn, pErr)
			}
		}
	}); err != nil {
		log.Warning(ctx, err)
	}
}

func (tc *TxnCoordSender) heartbeat(ctx context.Context, txnID uuid.UUID) bool {
	tc.txnMu.Lock()
	txnMeta := tc.txnMu.txns[txnID]
	txn := txnMeta.txn.Clone()
	hasAbandoned := txnMeta.hasClientAbandonedCoord(tc.clock.PhysicalNow())
	tc.txnMu.Unlock()

	if txn.Status != roachpb.PENDING {
		// A previous iteration has already determined that the transaction is
		// already finalized, so we wait for the client to realize that and
		// want to keep our state for the time being (to dish out the right
		// error once it returns).
		return true
	}

	// Before we send a heartbeat, determine whether this transaction should be
	// considered abandoned. If so, exit heartbeat. If ctx.Done() is not nil, then
	// it is a cancellable Context and we skip this check and use the ctx lifetime
	// instead of a timeout.
	//
	// TODO(andrei): We should disallow non-cancellable contexts in the heartbeat
	// goroutine and enforce that our kv client cancels the context when it's
	// done. We get non-cancellable contexts from remote clients
	// (roachpb.ExternalClient) because we override the gRPC context to make it
	// non-cancellable in DBServer.Batch (as that context is not tied to a txn
	// lifetime).
	// Further note that, unfortunately, the Sender interface generally makes it
	// difficult for the TxnCoordSender to get a context with the same lifetime as
	// the transaction (the TxnCoordSender associates the context of the txn's
	// first write with the txn). We should move to using only use local clients
	// (i.e. merge, or at least co-locate client.Txn and the TxnCoordSender). At
	// that point, we probably don't even need to deal with context cancellation
	// any more; the client will be trusted to always send an EndRequest when it's
	// done with a transaction.
	if ctx.Done() == nil && hasAbandoned {
		if log.V(1) {
			log.Infof(ctx, "transaction %s abandoned; stopping heartbeat", txnMeta.txn)
		}
		tc.tryAsyncAbort(txnID)
		return false
	}

	ba := roachpb.BatchRequest{}
	ba.Txn = &txn

	hb := &roachpb.HeartbeatTxnRequest{
		Now: tc.clock.Now(),
	}
	hb.Key = txn.Key
	ba.Add(hb)

	log.Event(ctx, "heartbeat")
	br, pErr := tc.wrapped.Send(ctx, ba)

	// Correctness mandates that when we can't heartbeat the transaction, we
	// make sure the client doesn't keep going. This is particularly relevant
	// in the case of an ABORTED transaction, but if we can't reach the
	// transaction record at all, we're going to have to assume we're aborted
	// as well.
	if pErr != nil {
		log.Warningf(ctx, "heartbeat to %s failed: %s", txn, pErr)
		// We're not going to let the client carry out additional requests, so
		// try to clean up.
		tc.tryAsyncAbort(*txn.ID)
		txn.Status = roachpb.ABORTED
	} else {
		txn.Update(br.Responses[0].GetInner().(*roachpb.HeartbeatTxnResponse).Txn)
	}

	// Give the news to the txn in the txns map. This will update long-running
	// transactions (which may find out that they have to restart in that way),
	// but in particular makes sure that they notice when they've been aborted
	// (in which case we'll give them an error on their next request).
	tc.txnMu.Lock()
	tc.txnMu.txns[txnID].txn.Update(&txn)
	tc.txnMu.Unlock()

	return true
}

// updateState updates the transaction state in both the success and
// error cases, applying those updates to the corresponding txnMeta
// object when adequate. It also updates certain errors with the
// updated transaction for use by client restarts.
func (tc *TxnCoordSender) updateState(
	ctx context.Context,
	startNS int64,
	ba roachpb.BatchRequest,
	br *roachpb.BatchResponse,
	pErr *roachpb.Error,
) *roachpb.Error {

	tc.txnMu.Lock()
	defer tc.txnMu.Unlock()

	if ba.Txn == nil {
		// Not a transactional request.
		return pErr
	}

	var newTxn roachpb.Transaction
	newTxn.Update(ba.Txn)
	if pErr == nil {
		newTxn.Update(br.Txn)
	} else if errTxn := pErr.GetTxn(); errTxn != nil {
		newTxn.Update(errTxn)
	}

	switch t := pErr.GetDetail().(type) {
	case *roachpb.OpRequiresTxnError:
		panic("OpRequiresTxnError must not happen at this level")
	case *roachpb.ReadWithinUncertaintyIntervalError:
		// If the reader encountered a newer write within the uncertainty
		// interval, we advance the txn's timestamp just past the last observed
		// timestamp from the node.
		restartTS, ok := newTxn.GetObservedTimestamp(pErr.OriginNode)
		if !ok {
			pErr = roachpb.NewError(errors.Errorf("no observed timestamp for node %d found on uncertainty restart", pErr.OriginNode))
		} else {
			newTxn.Timestamp.Forward(restartTS)
			newTxn.Restart(ba.UserPriority, newTxn.Priority, newTxn.Timestamp)
		}
	case *roachpb.TransactionAbortedError:
		// Increase timestamp if applicable.
		newTxn.Timestamp.Forward(pErr.GetTxn().Timestamp)
		newTxn.Priority = pErr.GetTxn().Priority
		// Clean up the freshly aborted transaction in defer(), avoiding a
		// race with the state update below.
		defer tc.cleanupTxnLocked(ctx, newTxn)
	case *roachpb.TransactionPushError:
		// Increase timestamp if applicable, ensuring that we're
		// just ahead of the pushee.
		newTxn.Timestamp.Forward(t.PusheeTxn.Timestamp)
		newTxn.Restart(ba.UserPriority, t.PusheeTxn.Priority-1, newTxn.Timestamp)
	case *roachpb.TransactionRetryError:
		// Increase timestamp so on restart, we're ahead of any timestamp
		// cache entries or newer versions which caused the restart.
		newTxn.Restart(ba.UserPriority, pErr.GetTxn().Priority, newTxn.Timestamp)
	case *roachpb.WriteTooOldError:
		newTxn.Restart(ba.UserPriority, newTxn.Priority, t.ActualTimestamp)
	case nil:
		// Nothing to do here, avoid the default case.
	default:
		// Do not clean up the transaction since we're leaving cancellation of
		// the transaction up to the client. For example, on seeing an error,
		// like TransactionStatusError or ConditionFailedError, the client
		// will call Txn.CleanupOnError() which will cleanup the transaction
		// and its intents. Therefore leave the transaction in the PENDING
		// state and do not call cleanTxnLocked().
	}

	txnID := *newTxn.ID

	txnMeta := tc.txnMu.txns[txnID]
	// For successful transactional requests, keep the written intents and
	// the updated transaction record to be sent along with the reply.
	// The transaction metadata is created with the first writing operation.
	// A tricky edge case is that of a transaction which "fails" on the
	// first writing request, but actually manages to write some intents
	// (for example, due to being multi-range). In this case, there will
	// be an error, but the transaction will be marked as Writing and the
	// coordinator must track the state, for the client's retry will be
	// performed with a Writing transaction which the coordinator rejects
	// unless it is tracking it (on top of it making sense to track it;
	// after all, it **has** laid down intents and only the coordinator
	// can augment a potential EndTransaction call). See #3303.
	if txnMeta != nil || pErr == nil || newTxn.Writing {
		// Adding the intents even on error reduces the likelihood of dangling
		// intents blocking concurrent writers for extended periods of time.
		// See #3346.
		var keys []roachpb.Span
		if txnMeta != nil {
			keys = txnMeta.keys
		}
		ba.IntentSpanIterate(br, func(key, endKey roachpb.Key) {
			keys = append(keys, roachpb.Span{
				Key:    key,
				EndKey: endKey,
			})
		})

		if txnMeta != nil {
			txnMeta.keys = keys
		} else if len(keys) > 0 {
			// If the transaction is already over, there's no point in
			// launching a one-off coordinator which will shut down right
			// away. If we ended up here with an error, we'll always start
			// the coordinator - the transaction has laid down intents, so
			// we expect it to be committed/aborted at some point in the
			// future.
			if _, isEnding := ba.GetArg(roachpb.EndTransaction); pErr != nil || !isEnding {
				log.Event(ctx, "coordinator spawns")
				txnMeta = &txnMetadata{
					txn:              newTxn,
					keys:             keys,
					firstUpdateNanos: startNS,
					lastUpdateNanos:  tc.clock.PhysicalNow(),
					timeoutDuration:  tc.clientTimeout,
					txnEnd:           make(chan struct{}),
				}
				tc.txnMu.txns[txnID] = txnMeta

				if err := tc.stopper.RunAsyncTask(ctx, func(ctx context.Context) {
					tc.heartbeatLoop(ctx, txnID)
				}); err != nil {
					// The system is already draining and we can't start the
					// heartbeat. We refuse new transactions for now because
					// they're likely not going to have all intents committed.
					// In principle, we can relax this as needed though.
					tc.unregisterTxnLocked(txnID)
					return roachpb.NewError(err)
				}
			} else {
				// If this was a successful one phase commit, update stats
				// directly as they won't otherwise be updated on heartbeat
				// loop shutdown.
				etArgs, ok := br.Responses[len(br.Responses)-1].GetInner().(*roachpb.EndTransactionResponse)
				tc.updateStats(tc.clock.PhysicalNow()-startNS, 0, newTxn.Status, ok && etArgs.OnePhaseCommit)
			}
		}
	}

	// Update our record of this transaction, even on error.
	if txnMeta != nil {
		txnMeta.txn.Update(&newTxn)
		txnMeta.setLastUpdate(tc.clock.PhysicalNow())
	}

	if pErr == nil {
		// For successful transactional requests, always send the updated txn
		// record back. Note that we make sure not to share data with newTxn
		// (which may have made it into txnMeta).
		if br.Txn != nil {
			br.Txn.Update(&newTxn)
		} else {
			clonedTxn := newTxn.Clone()
			br.Txn = &clonedTxn
		}
	} else if pErr.GetTxn() != nil {
		// Avoid changing existing errors because sometimes they escape into
		// goroutines and data races can occur.
		pErrShallow := *pErr
		pErrShallow.SetTxn(&newTxn) // SetTxn clones newTxn
		pErr = &pErrShallow
	}

	return pErr
}

// TODO(tschottdorf): this method is somewhat awkward but unless we want to
// give this error back to the client, our options are limited. We'll have to
// run the whole thing for them, or any restart will still end up at the client
// which will not be prepared to be handed a Txn.
func (tc *TxnCoordSender) resendWithTxn(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Run a one-off transaction with that single command.
	if log.V(1) {
		log.Infof(ctx, "%s: auto-wrapping in txn and re-executing: ", ba)
	}
	// TODO(bdarnell): need to be able to pass other parts of DBContext
	// through here.
	dbCtx := client.DefaultDBContext()
	dbCtx.UserPriority = ba.UserPriority
	tmpDB := client.NewDBWithContext(tc, tc.clock, dbCtx)
	var br *roachpb.BatchResponse
	err := tmpDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetDebugName("auto-wrap")
		b := txn.NewBatch()
		b.Header = ba.Header
		for _, arg := range ba.Requests {
			req := arg.GetInner()
			b.AddRawRequest(req)
		}
		err := txn.CommitInBatch(ctx, b)
		br = b.RawResponse()
		return err
	})
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	br.Txn = nil // hide the evidence
	return br, nil
}

// updateStats updates transaction metrics after a transaction finishes.
func (tc *TxnCoordSender) updateStats(
	duration, restarts int64, status roachpb.TransactionStatus, onePC bool,
) {
	tc.metrics.Durations.RecordValue(duration)
	tc.metrics.Restarts.RecordValue(restarts)
	switch status {
	case roachpb.ABORTED:
		tc.metrics.Aborts.Inc(1)
	case roachpb.PENDING:
		tc.metrics.Abandons.Inc(1)
	case roachpb.COMMITTED:
		tc.metrics.Commits.Inc(1)
		if onePC {
			tc.metrics.Commits1PC.Inc(1)
		}
	}
}
