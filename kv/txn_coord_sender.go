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
	"sync"
	"sync/atomic"
	"time"

	basictracer "github.com/opentracing/basictracer-go"
	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/interval"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	statusLogInterval = 5 * time.Second
	opTxnCoordSender  = "txn coordinator"
	opHeartbeatLoop   = "heartbeat"
)

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
	keys interval.RangeGroup

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

// addKeyRange adds the specified key range to the range group,
// taking care not to add this range if existing entries already
// completely cover the range.
func addKeyRange(keys interval.RangeGroup, start, end roachpb.Key) {
	// This gives us a memory-efficient end key if end is empty.
	// The most common case for keys in the intents interval map
	// is for single keys. However, the range group requires
	// a non-empty interval, so we create two key slices which
	// share the same underlying byte array.
	if len(end) == 0 {
		end = start.ShallowNext()
		start = end[:len(start)]
	}
	keyR := interval.Range{
		Start: interval.Comparable(start),
		End:   interval.Comparable(end),
	}
	keys.Add(keyR)
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

// collectIntentSpans collects the spans of the intents to be resolved for the
// transaction. It does not create copies, so the caller must not alter the
// returned data. Usually called with txnMeta.keys.
func collectIntentSpans(keys interval.RangeGroup) []roachpb.Span {
	intents := make([]roachpb.Span, 0, keys.Len())
	if err := keys.ForEach(func(r interval.Range) error {
		sp := roachpb.Span{
			Key: roachpb.Key(r.Start),
		}
		if endKey := roachpb.Key(r.End); !sp.Key.IsPrev(endKey) {
			sp.EndKey = endKey
		}
		intents = append(intents, sp)
		return nil
	}); err != nil {
		panic(err)
	}
	return intents
}

// TxnMetrics holds all metrics relating to KV transactions.
type TxnMetrics struct {
	Aborts    metric.Rates
	Commits   metric.Rates
	Abandons  metric.Rates
	Durations metric.Histograms

	// Restarts is the number of times we had to restart the transaction.
	Restarts *metric.Histogram
}

const (
	abortsPrefix    = "aborts"
	commitsPrefix   = "commits"
	abandonsPrefix  = "abandons"
	durationsPrefix = "durations"
	restartsKey     = "restarts"
)

// NewTxnMetrics returns a new instance of txnMetrics that contains metrics which have
// been registered with the provided Registry.
func NewTxnMetrics(txnRegistry *metric.Registry) *TxnMetrics {
	return &TxnMetrics{
		Aborts:    txnRegistry.Rates(abortsPrefix),
		Commits:   txnRegistry.Rates(commitsPrefix),
		Abandons:  txnRegistry.Rates(abandonsPrefix),
		Durations: txnRegistry.Latency(durationsPrefix),
		Restarts:  txnRegistry.Histogram(restartsKey, 60*time.Second, 100, 3),
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
	wrapped           client.Sender
	clock             *hlc.Clock
	heartbeatInterval time.Duration
	clientTimeout     time.Duration
	sync.Mutex                                   // protects txns and txnStats
	txns              map[uuid.UUID]*txnMetadata // txn key to metadata
	linearizable      bool                       // enables linearizable behaviour
	tracer            opentracing.Tracer
	stopper           *stop.Stopper
	metrics           *TxnMetrics
}

var _ client.Sender = &TxnCoordSender{}

// NewTxnCoordSender creates a new TxnCoordSender for use from a KV
// distributed DB instance.
func NewTxnCoordSender(wrapped client.Sender, clock *hlc.Clock, linearizable bool, tracer opentracing.Tracer, stopper *stop.Stopper, txnMetrics *TxnMetrics) *TxnCoordSender {
	if tracer == nil {
		panic("nil tracer supplied")
	}
	tc := &TxnCoordSender{
		wrapped:           wrapped,
		clock:             clock,
		heartbeatInterval: storage.DefaultHeartbeatInterval,
		clientTimeout:     defaultClientTimeout,
		txns:              map[uuid.UUID]*txnMetadata{},
		linearizable:      linearizable,
		tracer:            tracer,
		stopper:           stopper,
		metrics:           txnMetrics,
	}

	tc.stopper.RunWorker(tc.startStats)
	return tc
}

// startStats blocks and periodically logs transaction statistics (throughput,
// success rates, durations, ...). Note that this only captures write txns,
// since read-only txns are stateless as far as TxnCoordSender is concerned.
// stats).
func (tc *TxnCoordSender) startStats() {
	res := time.Millisecond // for duration logging resolution
	var statusLogTimer util.Timer
	defer statusLogTimer.Stop()
	scale := metric.Scale1M
	for {
		statusLogTimer.Reset(statusLogInterval)
		select {
		case <-statusLogTimer.C:
			statusLogTimer.Read = true
			if !log.V(1) {
				continue
			}

			// Take a snapshot of metrics. There's some chance of skew, since the snapshots are
			// not done atomically, but that should be fine for these debug stats.
			metrics := tc.metrics
			durations := metrics.Durations[scale].Current()
			restarts := metrics.Restarts.Current()
			commitRate := metrics.Commits.Rates[scale].Value()
			abortRate := metrics.Aborts.Rates[scale].Value()
			abandonRate := metrics.Abandons.Rates[scale].Value()

			// Show transaction stats over the last minute. Maybe this should be shorter in the future.
			// We'll revisit if we get sufficient feedback.
			totalRate := commitRate + abortRate + abandonRate
			var pCommitted, pAbandoned, pAborted float64
			if totalRate > 0 {
				pCommitted = 100 * (commitRate / totalRate)
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

			log.Infof(
				"txn coordinator: %.2f txn/sec, %.2f/%.2f/%.2f %%cmmt/abrt/abnd, %s/%s/%s avg/σ/max duration, %.1f/%.1f/%d avg/σ/max restarts (%d samples)",
				totalRate, pCommitted, pAborted, pAbandoned,
				util.TruncateDuration(time.Duration(dMean), res),
				util.TruncateDuration(time.Duration(dDev), res),
				util.TruncateDuration(time.Duration(dMax), res),
				rMean, rDev, rMax, num,
			)

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
func (tc *TxnCoordSender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	{
		// Start new or pick up active trace and embed its trace metadata into
		// header for use by RPC recipients. From here on, there's always an active
		// Trace, though its overhead is small unless it's sampled.
		sp := opentracing.SpanFromContext(ctx)
		if sp == nil {
			sp = tc.tracer.StartSpan(opTxnCoordSender)
			defer sp.Finish()
			ctx = opentracing.ContextWithSpan(ctx, sp)
		}
		// TODO(tschottdorf): To get rid of the spurious alloc below we need to
		// implement the carrier interface on ba.Header or make Span non-nullable,
		// both of which force all of ba on the Heap. It's already there, so may
		// not be a big deal, but ba should live on the stack. Also not easy to use
		// a buffer pool here since anything that goes into the RPC layer could be
		// used by goroutines we didn't wait for.
		if ba.Header.Trace == nil {
			ba.Header.Trace = &tracing.Span{}
		}
		if err := tc.tracer.Inject(sp, basictracer.Delegator, ba.Trace); err != nil {
			return nil, roachpb.NewError(err)
		}
	}

	if err := tc.maybeBeginTxn(&ba); err != nil {
		return nil, roachpb.NewError(err)
	}
	var startNS int64

	if ba.Txn != nil {
		// If this request is part of a transaction...
		txnID := *ba.Txn.ID
		// Verify that if this Transaction is not read-only, we have it on file.
		// If not, refuse further operations - the transaction was aborted due
		// to a timeout or the client must have issued a write on another
		// coordinator previously.
		if ba.Txn.Writing {
			tc.Lock()
			_, ok := tc.txns[txnID]
			tc.Unlock()
			if !ok {
				pErr := roachpb.NewErrorf("writing transaction timed out, was aborted, " +
					"or ran on multiple coordinators")
				return nil, pErr
			}
		}

		if rArgs, ok := ba.GetArg(roachpb.EndTransaction); ok {
			et := rArgs.(*roachpb.EndTransactionRequest)
			if len(et.Key) != 0 {
				return nil, roachpb.NewErrorf("EndTransaction must not have a Key set")
			}
			et.Key = ba.Txn.Key
			// Remember when EndTransaction started in case we want to
			// be linearizable.
			startNS = tc.clock.PhysicalNow()
			if len(et.IntentSpans) > 0 {
				// TODO(tschottdorf): it may be useful to allow this later.
				// That would be part of a possible plan to allow txns which
				// write on multiple coordinators.
				return nil, roachpb.NewErrorf("client must not pass intents to EndTransaction")
			}
			tc.Lock()
			txnMeta, metaOK := tc.txns[txnID]
			{
				// Populate et.IntentSpans, taking into account both existing
				// writes (if any) and new writes in this batch, and taking
				// care to perform proper deduplication.
				var keys interval.RangeGroup
				if metaOK {
					keys = txnMeta.keys
				} else {
					keys = interval.NewRangeTree()
				}
				ba.IntentSpanIterate(func(key, endKey roachpb.Key) {
					addKeyRange(keys, key, endKey)
				})
				et.IntentSpans = collectIntentSpans(keys)
			}
			tc.Unlock()

			if len(et.IntentSpans) > 0 {
				// All good, proceed.
			} else if !metaOK {
				// If we don't have the transaction, then this must be a retry
				// by the client. We can no longer reconstruct a correct
				// request so we must fail.
				//
				// TODO(bdarnell): if we had a GetTransactionStatus API then
				// we could lookup the transaction and return either nil or
				// TransactionAbortedError instead of this ambivalent error.
				return nil, roachpb.NewErrorf("transaction is already committed or aborted")
			}
			if len(et.IntentSpans) == 0 {
				// If there aren't any intents, then there's factually no
				// transaction to end. Read-only txns have all of their state in
				// the client.
				return nil, roachpb.NewErrorf("cannot commit a read-only transaction")
			}
			if log.V(1) {
				for _, intent := range et.IntentSpans {
					log.Trace(ctx, fmt.Sprintf("intent: [%s,%s)", intent.Key, intent.EndKey))
				}
			}
		}
	}

	// Send the command through wrapped sender, taking appropriate measures
	// on error.
	var br *roachpb.BatchResponse
	{
		var pErr *roachpb.Error
		br, pErr = tc.wrapped.Send(ctx, ba)

		if _, ok := pErr.GetDetail().(*roachpb.OpRequiresTxnError); ok {
			// TODO(tschottdorf): needs to keep the trace.
			br, pErr = tc.resendWithTxn(ba)
		}

		if pErr = tc.updateState(ctx, ba, br, pErr); pErr != nil {
			log.Trace(ctx, fmt.Sprintf("error: %s", pErr))
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
				log.Infof("%v: waiting %s on EndTransaction for linearizability", br.Txn.Short(), util.TruncateDuration(sleepNS, time.Millisecond))
			}
			time.Sleep(sleepNS)
		}()
	}
	if br.Txn.Status != roachpb.PENDING {
		tc.cleanupTxn(ctx, *br.Txn)
	}
	return br, nil
}

// maybeBeginTxn begins a new transaction if a txn has been specified
// in the request but has a nil ID. The new transaction is initialized
// using the name and isolation in the otherwise uninitialized txn.
// The Priority, if non-zero is used as a minimum.
//
// No transactional writes are allowed unless preceded by a begin
// transaction request within the same batch. The exception is if the
// transaction is already in state txn.Writing=true.
func (tc *TxnCoordSender) maybeBeginTxn(ba *roachpb.BatchRequest) error {
	if ba.Txn == nil {
		return nil
	}
	if len(ba.Requests) == 0 {
		return util.Errorf("empty batch with txn")
	}
	if ba.Txn.ID == nil {
		// Create transaction without a key. The key is set when a begin
		// transaction request is received.

		// The initial timestamp may be communicated by a higher layer.
		// If so, use that. Otherwise make up a new one.
		timestamp := ba.Txn.OrigTimestamp
		if timestamp == roachpb.ZeroTimestamp {
			timestamp = tc.clock.Now()
		}
		newTxn := roachpb.NewTransaction(ba.Txn.Name, nil, ba.UserPriority,
			ba.Txn.Isolation, timestamp, tc.clock.MaxOffset().Nanoseconds())
		// Use existing priority as a minimum. This is used on transaction
		// aborts to ratchet priority when creating successor transaction.
		if newTxn.Priority < ba.Txn.Priority {
			newTxn.Priority = ba.Txn.Priority
		}
		ba.Txn = newTxn
	}

	// Check for a begin transaction to set txn key based on the key of
	// the first transactional write. Also enforce that no transactional
	// writes occur before a begin transaction.
	var haveBeginTxn bool
	for _, req := range ba.Requests {
		args := req.GetInner()
		if bt, ok := args.(*roachpb.BeginTransactionRequest); ok {
			if haveBeginTxn || ba.Txn.Writing {
				return util.Errorf("begin transaction requested twice in the same transaction: %s", ba.Txn)
			}
			haveBeginTxn = true
			if ba.Txn.Key == nil {
				ba.Txn.Key = bt.Key
			}
		}
		if roachpb.IsTransactionWrite(args) && !haveBeginTxn && !ba.Txn.Writing {
			return util.Errorf("transactional write before begin transaction")
		}
	}
	return nil
}

// cleanupTxn is called when a transaction ends. The transaction record is
// updated and the heartbeat goroutine signaled to clean up the transaction
// gracefully.
func (tc *TxnCoordSender) cleanupTxn(ctx context.Context, txn roachpb.Transaction) {
	log.Trace(ctx, "coordinator stops")
	tc.Lock()
	defer tc.Unlock()
	txnMeta, ok := tc.txns[*txn.ID]
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
// the status and starts for the removed transaction.
func (tc *TxnCoordSender) unregisterTxnLocked(txnID uuid.UUID) (duration, restarts int64, status roachpb.TransactionStatus) {
	txnMeta := tc.txns[txnID] // guaranteed to exist
	if txnMeta == nil {
		panic(fmt.Sprintf("attempt to unregister non-existent transaction: %s", txnID))
	}
	duration = tc.clock.PhysicalNow() - txnMeta.firstUpdateNanos
	restarts = int64(txnMeta.txn.Epoch)
	status = txnMeta.txn.Status

	txnMeta.keys.Clear()

	delete(tc.txns, txnID)

	return duration, restarts, status
}

// heartbeatLoop periodically sends a HeartbeatTxn RPC to an extant
// transaction, stopping in the event the transaction is aborted or
// committed after attempting to resolve the intents. When the
// heartbeat stops, the transaction is unregistered from the
// coordinator,
func (tc *TxnCoordSender) heartbeatLoop(txnID uuid.UUID) {
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(tc.heartbeatInterval)
		tickChan = ticker.C
		defer ticker.Stop()
	}
	defer func() {
		tc.Lock()
		duration, restarts, status := tc.unregisterTxnLocked(txnID)
		tc.Unlock()
		tc.updateStats(duration, int64(restarts), status)
	}()

	var closer <-chan struct{}
	// TODO(tschottdorf): this should join to the trace of the request
	// which starts this goroutine.
	sp := tc.tracer.StartSpan(opHeartbeatLoop)
	defer sp.Finish()
	ctx := opentracing.ContextWithSpan(context.Background(), sp)

	{
		tc.Lock()
		txnMeta := tc.txns[txnID] // do not leak to outer scope
		closer = txnMeta.txnEnd
		tc.Unlock()
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

		case <-tc.stopper.ShouldDrain():
			return
		}
	}
}

func (tc *TxnCoordSender) heartbeat(ctx context.Context, txnID uuid.UUID) bool {
	tc.Lock()
	proceed := true
	txnMeta := tc.txns[txnID]
	var intentSpans []roachpb.Span
	// Before we send a heartbeat, determine whether this transaction
	// should be considered abandoned. If so, exit heartbeat.
	if txnMeta.hasClientAbandonedCoord(tc.clock.PhysicalNow()) {
		// TODO(tschottdorf): should we be more proactive here?
		// The client might be continuing the transaction
		// through another coordinator, but in the most likely
		// case it's just gone and the open transaction record
		// could block concurrent operations.
		if log.V(1) {
			log.Infof("transaction %s abandoned; stopping heartbeat",
				txnMeta.txn)
		}
		proceed = false
		// Grab the intents here to avoid potential race.
		intentSpans = collectIntentSpans(txnMeta.keys)
		txnMeta.keys.Clear()
	}
	// txnMeta.txn is possibly replaced concurrently,
	// so grab a copy before unlocking.
	txn := txnMeta.txn.Clone()
	tc.Unlock()

	ba := roachpb.BatchRequest{}
	ba.Txn = &txn

	if !proceed {
		// Actively abort the transaction and its intents since we assume it's abandoned.
		et := &roachpb.EndTransactionRequest{
			Span: roachpb.Span{
				Key: txn.Key,
			},
			Commit:      false,
			IntentSpans: intentSpans,
		}
		ba.Add(et)
		tc.stopper.RunAsyncTask(func() {
			// Use the wrapped sender since the normal Sender
			// does not allow clients to specify intents.
			// TODO(tschottdorf): not using the existing context here since that
			// leads to use-after-finish of the contained trace. Should fork off
			// before the goroutine.
			if _, pErr := tc.wrapped.Send(context.Background(), ba); pErr != nil {
				if log.V(1) {
					log.Warningf("abort due to inactivity failed for %s: %s ", txn, pErr)
				}
			}
		})
		return false
	}

	hb := &roachpb.HeartbeatTxnRequest{
		Now: tc.clock.Now(),
	}
	hb.Key = txn.Key
	ba.Add(hb)

	log.Trace(ctx, "heartbeat")
	_, err := tc.wrapped.Send(ctx, ba)
	// If the transaction is not in pending state, then we can stop
	// the heartbeat. It's either aborted or committed, and we resolve
	// write intents accordingly.
	if err != nil {
		log.Warningf("heartbeat to %s failed: %s", txn, err)
	}
	// TODO(bdarnell): once we have gotten a heartbeat response with
	// Status != PENDING, future heartbeats are useless. However, we
	// need to continue the heartbeatLoop until the client either
	// commits or abandons the transaction. We could save a little
	// pointless work by restructuring this loop to stop sending
	// heartbeats between the time that the transaction is aborted and
	// the client finds out. Furthermore, we could use this information
	// to send TransactionAbortedErrors to the client so it can restart
	// immediately instead of running until its EndTransaction.
	return true
}

// updateState updates the transaction state in both the success and
// error cases, applying those updates to the corresponding txnMeta
// object when adequate. It also updates certain errors with the
// updated transaction for use by client restarts.
func (tc *TxnCoordSender) updateState(ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse, pErr *roachpb.Error) *roachpb.Error {
	newTxn := &roachpb.Transaction{}
	newTxn.Update(ba.Txn)
	if pErr == nil {
		newTxn.Update(br.Txn)
	} else {
		newTxn.Update(pErr.GetTxn())
	}

	switch t := pErr.GetDetail().(type) {
	case *roachpb.TransactionStatusError:
		// Likely already committed or more obscure errors such as epoch or
		// timestamp regressions; consider txn dead.
		defer tc.cleanupTxn(ctx, *pErr.GetTxn())
	case *roachpb.OpRequiresTxnError:
		panic("OpRequiresTxnError must not happen at this level")
	case *roachpb.ReadWithinUncertaintyIntervalError:
		// If the reader encountered a newer write within the uncertainty
		// interval, we advance the txn's timestamp just past the last observed
		// timestamp from the node.
		restartTS, ok := newTxn.GetObservedTimestamp(pErr.OriginNode)
		if !ok {
			pErr = roachpb.NewError(util.Errorf("no observed timestamp for node %d found on uncertainty restart", pErr.OriginNode))
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
		defer tc.cleanupTxn(ctx, *newTxn)
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
		if pErr.GetTxn() != nil {
			if pErr.CanRetry() {
				panic("Retryable internal error must not happen at this level")
			} else {
				// Do not clean up the transaction here since the client might still
				// want to continue the transaction. For example, a client might
				// continue its transaction after receiving ConditionFailedError, which
				// can come from a unique index violation.
			}
		}
	}

	if pErr != nil && pErr.GetTxn() != nil {
		// Avoid changing existing errors because sometimes they escape into
		// goroutines and then there are races. Fairly sure there isn't one
		// here, but better safe than sorry.
		pErrShallow := *pErr
		pErrShallow.SetTxn(newTxn)
		pErr = &pErrShallow
	}

	if newTxn.ID == nil {
		return pErr
	}
	txnID := *newTxn.ID
	tc.Lock()
	defer tc.Unlock()
	txnMeta := tc.txns[txnID]
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
	var intents []roachpb.Span
	// TODO(nvanbenschoten): Iterating here to put the intents in a slice for
	// the sole purpose of later iterating again and calling addKeyRange is a
	// little wasteful and can likely be avoided.
	ba.IntentSpanIterate(func(key, endKey roachpb.Key) {
		intents = append(intents, roachpb.Span{Key: key, EndKey: endKey})
	})

	if len(intents) > 0 && (pErr == nil || newTxn.Writing) {
		if txnMeta == nil {
			if !newTxn.Writing {
				panic("txn with intents marked as non-writing")
			}
			// If the transaction is already over, there's no point in
			// launching a one-off coordinator which will shut down right
			// away. If we ended up here with an error, we'll always start
			// the coordinator - the transaction has laid down intents, so
			// we expect it to be committed/aborted at some point in the
			// future.
			if _, isEnding := ba.GetArg(roachpb.EndTransaction); pErr != nil || !isEnding {
				log.Trace(ctx, "coordinator spawns")
				txnMeta = &txnMetadata{
					txn:              *newTxn,
					keys:             interval.NewRangeTree(),
					firstUpdateNanos: tc.clock.PhysicalNow(),
					lastUpdateNanos:  tc.clock.PhysicalNow(),
					timeoutDuration:  tc.clientTimeout,
					txnEnd:           make(chan struct{}),
				}
				tc.txns[txnID] = txnMeta

				if !tc.stopper.RunAsyncTask(func() {
					tc.heartbeatLoop(txnID)
				}) {
					// The system is already draining and we can't start the
					// heartbeat. We refuse new transactions for now because
					// they're likely not going to have all intents committed.
					// In principle, we can relax this as needed though.
					tc.unregisterTxnLocked(txnID)
					return roachpb.NewError(&roachpb.NodeUnavailableError{})
				}
			}
		}
	}
	// Update our record of this transaction, even on error.
	if txnMeta != nil {
		txnMeta.txn = *newTxn
		if !txnMeta.txn.Writing {
			panic("tracking a non-writing txn")
		}
		txnMeta.setLastUpdate(tc.clock.PhysicalNow())
		// Adding the intents even on error reduces the likelihood of dangling
		// intents blocking concurrent writers for extended periods of time.
		// See #3346.
		for _, intent := range intents {
			addKeyRange(txnMeta.keys, intent.Key, intent.EndKey)
		}
	}
	if pErr == nil {
		// For successful transactional requests, always send the updated txn
		// record back.
		br.Txn = newTxn
	}
	return pErr
}

// TODO(tschottdorf): this method is somewhat awkward but unless we want to
// give this error back to the client, our options are limited. We'll have to
// run the whole thing for them, or any restart will still end up at the client
// which will not be prepared to be handed a Txn.
func (tc *TxnCoordSender) resendWithTxn(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	// Run a one-off transaction with that single command.
	if log.V(1) {
		log.Infof("%s: auto-wrapping in txn and re-executing: ", ba)
	}
	tmpDB := client.NewDBWithPriority(tc, ba.UserPriority)
	var br *roachpb.BatchResponse
	pErr := tmpDB.Txn(func(txn *client.Txn) *roachpb.Error {
		txn.SetDebugName("auto-wrap", 0)
		b := txn.NewBatch()
		b.MaxScanResults = ba.MaxScanResults
		for _, arg := range ba.Requests {
			req := arg.GetInner()
			b.InternalAddRequest(req)
		}
		var pErr *roachpb.Error
		br, pErr = txn.CommitInBatchWithResponse(b)
		return pErr
	})
	if pErr != nil {
		return nil, pErr
	}
	br.Txn = nil // hide the evidence
	return br, nil
}

// updateStats updates transaction metrics after a transaction finishes.
func (tc *TxnCoordSender) updateStats(duration, restarts int64, status roachpb.TransactionStatus) {
	tc.metrics.Durations.RecordValue(duration)
	tc.metrics.Restarts.RecordValue(restarts)
	switch status {
	case roachpb.ABORTED:
		tc.metrics.Aborts.Add(1)
	case roachpb.PENDING:
		tc.metrics.Abandons.Add(1)
	case roachpb.COMMITTED:
		tc.metrics.Commits.Add(1)
	}
}
