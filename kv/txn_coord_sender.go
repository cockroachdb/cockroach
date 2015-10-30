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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracer"
	"github.com/montanaflynn/stats"
)

const statusLogInterval = 5 * time.Second

// txnMetadata holds information about an ongoing transaction, as
// seen from the perspective of this coordinator. It records all
// keys (and key ranges) mutated as part of the transaction for
// resolution upon transaction commit or abort.
//
// Importantly, more than a single coordinator may participate in
// a transaction's execution. Client connections may be stateless
// (as through HTTP) or suffer disconnection. In those cases, other
// nodes may step in as coordinators. Each coordinator will continue
// to heartbeat the same transaction until the timeoutDuration. The
// hope is that all coordinators will see the eventual commit or
// abort and resolve any keys written during their tenure.
//
// However, coordinators might fail or the transaction may go on long
// enough using other coordinators that the original may garbage
// collect its transaction metadata state. Importantly, the system
// does not rely on coordinators keeping their state for
// cleanup. Instead, intents are garbage collected by the ranges
// periodically on their own.
type txnMetadata struct {
	// txn is a copy of the transaction record, updated with each request.
	txn roachpb.Transaction

	// keys stores key ranges affected by this transaction through this
	// coordinator. By keeping this record, the coordinator will be able
	// to update the write intent when the transaction is committed.
	keys *cache.IntervalCache

	// lastUpdateNanos is the latest wall time in nanos the client sent
	// transaction operations to this coordinator. Accessed and updated
	// atomically.
	lastUpdateNanos int64

	// Analogous to lastUpdateNanos, this is the wall time at which the
	// transaction was instantiated.
	firstUpdateNanos int64

	// timeoutDuration is the time after which the transaction should be
	// considered abandoned by the client. That is, when
	// current_timestamp > lastUpdateTS + timeoutDuration If this value
	// is set to 0, a default timeout will be used.
	timeoutDuration time.Duration

	// txnEnd is closed when the transaction is aborted or committed,
	// terminating the associated heartbeat instance.
	txnEnd chan struct{}
}

// addKeyRange adds the specified key range to the interval cache,
// taking care not to add this range if existing entries already
// completely cover the range.
func (tm *txnMetadata) addKeyRange(start, end roachpb.Key) {
	// This gives us a memory-efficient end key if end is empty.
	// The most common case for keys in the intents interval map
	// is for single keys. However, the interval cache requires
	// a non-empty interval, so we create two key slices which
	// share the same underlying byte array.
	if len(end) == 0 {
		end = start.Next()
		start = end[:len(start)]
	}
	key := tm.keys.NewKey(start, end)
	for _, o := range tm.keys.GetOverlaps(start, end) {
		if o.Key.Contains(key) {
			return
		} else if key.Contains(o.Key) {
			tm.keys.Del(o.Key)
		}
	}

	// Since no existing key range fully covered this range, add it now.
	tm.keys.Add(key, nil)
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

// intents collects the intents to be resolved for the transaction. It does
// not create copies, so the caller must not alter the returned data.
func (tm *txnMetadata) intents() []roachpb.Intent {
	intents := make([]roachpb.Intent, 0, tm.keys.Len())
	for _, o := range tm.keys.GetOverlaps(roachpb.KeyMin, roachpb.KeyMax) {
		intent := roachpb.Intent{
			Key: o.Key.Start().(roachpb.Key),
		}
		if endKey := o.Key.End().(roachpb.Key); !intent.Key.IsPrev(endKey) {
			intent.EndKey = endKey
		}
		intents = append(intents, intent)
	}
	return intents
}

// txnCoordStats tallies up statistics about the transactions which have
// completed on this sender.
type txnCoordStats struct {
	committed, abandoned, aborted int

	// Store float64 since that's what we want in the end.
	durations []float64 // nanoseconds
	restarts  []float64 // restarts (as measured by epoch)
}

// A TxnCoordSender is an implementation of client.Sender which
// wraps a lower-level Sender (either a LocalSender or a DistSender)
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
	sync.Mutex                                // protects txns and txnStats
	txns              map[string]*txnMetadata // txn key to metadata
	txnStats          txnCoordStats           // statistics of recent txns
	linearizable      bool                    // enables linearizable behaviour
	tracer            *tracer.Tracer
	stopper           *stop.Stopper
}

var _ client.Sender = &TxnCoordSender{}

// NewTxnCoordSender creates a new TxnCoordSender for use from a KV
// distributed DB instance.
func NewTxnCoordSender(wrapped client.Sender, clock *hlc.Clock, linearizable bool, tracer *tracer.Tracer, stopper *stop.Stopper) *TxnCoordSender {
	tc := &TxnCoordSender{
		wrapped:           wrapped,
		clock:             clock,
		heartbeatInterval: storage.DefaultHeartbeatInterval,
		clientTimeout:     defaultClientTimeout,
		txns:              map[string]*txnMetadata{},
		linearizable:      linearizable,
		tracer:            tracer,
		stopper:           stopper,
	}

	tc.stopper.RunWorker(tc.startStats)
	return tc
}

// startStats blocks and periodically logs transaction statistics (throughput,
// success rates, durations, ...). Note that this only captures write txns,
// since read-only txns are stateless as far as TxnCoordSender is concerned.
// stats).
// TODO(mrtracy): Add this to TimeSeries.
func (tc *TxnCoordSender) startStats() {
	res := time.Millisecond // for duration logging resolution
	lastNow := tc.clock.PhysicalNow()
	for {
		select {
		case <-time.After(statusLogInterval):
			if !log.V(1) {
				continue
			}

			tc.Lock()
			curStats := tc.txnStats
			tc.txnStats = txnCoordStats{}
			tc.Unlock()

			now := tc.clock.PhysicalNow()

			// Tests have weird clocks.
			if now-lastNow <= 0 {
				continue
			}

			num := len(curStats.durations)
			// Only compute when non-empty input.
			var dMax, dMean, dDev, rMax, rMean, rDev float64
			var err error
			if num > 0 {
				// There should never be an error in the below
				// computations.
				dMax, err = stats.Max(curStats.durations)
				if err != nil {
					panic(err)
				}
				dMean, err = stats.Mean(curStats.durations)
				if err != nil {
					panic(err)
				}
				dDev, err = stats.StdDevP(curStats.durations)
				if err != nil {
					panic(err)
				}
				rMax, err = stats.Max(curStats.restarts)
				if err != nil {
					panic(err)
				}
				rMean, err = stats.Mean(curStats.restarts)
				if err != nil {
					panic(err)
				}
				rDev, err = stats.StdDevP(curStats.restarts)
				if err != nil {
					panic(err)
				}
			}

			rate := float64(int64(num)*int64(time.Second)) / float64(now-lastNow)
			var pCommitted, pAbandoned, pAborted float32

			if fNum := float32(num); fNum > 0 {
				pCommitted = 100 * float32(curStats.committed) / fNum
				pAbandoned = 100 * float32(curStats.abandoned) / fNum
				pAborted = 100 * float32(curStats.aborted) / fNum
			}
			log.Infof(
				"txn coordinator: %.2f txn/sec, %.2f/%.2f/%.2f %%cmmt/abrt/abnd, %s/%s/%s avg/σ/max duration, %.1f/%.1f/%.1f avg/σ/max restarts (%d samples)",
				rate, pCommitted, pAborted, pAbandoned,
				util.TruncateDuration(time.Duration(dMean), res),
				util.TruncateDuration(time.Duration(dDev), res),
				util.TruncateDuration(time.Duration(dMax), res),
				rMean, rDev, rMax, num,
			)
			lastNow = now
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
	if err := tc.maybeBeginTxn(&ba); err != nil {
		return nil, roachpb.NewError(err)
	}
	ba.CmdID = ba.GetOrCreateCmdID(tc.clock.PhysicalNow())
	var startNS int64

	// This is the earliest point at which the request has a ClientCmdID and/or
	// TxnID (if applicable). Begin a Trace which follows this request.
	trace := tc.tracer.NewTrace(tracer.Coord, &ba)
	defer trace.Finalize()
	defer trace.Epoch("sending batch")()
	ctx = tracer.ToCtx(ctx, trace)

	var id string // optional transaction ID
	if ba.Txn != nil {
		// If this request is part of a transaction...
		id = string(ba.Txn.ID)
		// Verify that if this Transaction is not read-only, we have it on
		// file. If not, refuse writes - the client must have issued a write on
		// another coordinator previously.
		if ba.Txn.Writing && ba.IsTransactionWrite() {
			tc.Lock()
			_, ok := tc.txns[id]
			tc.Unlock()
			if !ok {
				return nil, roachpb.NewError(util.Errorf("transaction must not write on multiple coordinators"))
			}
		}

		// Set the timestamp to the original timestamp for read-only
		// commands and to the transaction timestamp for read/write
		// commands.
		if ba.IsReadOnly() {
			ba.Timestamp = ba.Txn.OrigTimestamp
		} else {
			ba.Timestamp = ba.Txn.Timestamp
		}

		if rArgs, ok := ba.GetArg(roachpb.EndTransaction); ok {
			et := rArgs.(*roachpb.EndTransactionRequest)
			if len(et.Key) != 0 {
				return nil, roachpb.NewError(util.Errorf("EndTransaction must not have a Key set"))
			}
			et.Key = ba.Txn.Key
			// Remember when EndTransaction started in case we want to
			// be linearizable.
			startNS = tc.clock.PhysicalNow()
			if len(et.Intents) > 0 {
				// TODO(tschottdorf): it may be useful to allow this later.
				// That would be part of a possible plan to allow txns which
				// write on multiple coordinators.
				return nil, roachpb.NewError(util.Errorf("client must not pass intents to EndTransaction"))
			}
			tc.Lock()
			txnMeta, metaOK := tc.txns[id]
			if id != "" && metaOK {
				et.Intents = txnMeta.intents()
			}
			tc.Unlock()

			if intents := ba.GetIntents(); len(intents) > 0 {
				// Writes in Batch, so EndTransaction is fine. Should add
				// outstanding intents to EndTransaction, though.
				// TODO(tschottdorf): possible issues when the batch fails,
				// but the intents have been added anyways.
				// TODO(tschottdorf): some of these intents may be covered
				// by others, for example {[a,b), a}). This can lead to
				// some extra requests when those are non-local to the txn
				// record. But it doesn't seem worth optimizing now.
				et.Intents = append(et.Intents, intents...)
			} else if !metaOK {
				// If we don't have the transaction, then this must be a retry
				// by the client. We can no longer reconstruct a correct
				// request so we must fail.
				//
				// TODO(bdarnell): if we had a GetTransactionStatus API then
				// we could lookup the transaction and return either nil or
				// TransactionAbortedError instead of this ambivalent error.
				return nil, roachpb.NewError(util.Errorf("transaction is already committed or aborted"))
			}
			if len(et.Intents) == 0 {
				// If there aren't any intents, then there's factually no
				// transaction to end. Read-only txns have all of their state in
				// the client.
				return nil, roachpb.NewError(util.Errorf("cannot commit a read-only transaction"))
			}
			if log.V(1) {
				for _, intent := range et.Intents {
					trace.Event(fmt.Sprintf("intent: [%s,%s)", intent.Key, intent.EndKey))
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

		if _, ok := pErr.GoError().(*roachpb.OpRequiresTxnError); ok {
			br, pErr = tc.resendWithTxn(ba)
		}

		if pErr := tc.updateState(ctx, ba, br, pErr); pErr != nil {
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
		tc.cleanupTxn(trace, *br.Txn)
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
	if len(ba.Txn.ID) == 0 {
		// Create transaction without a key. The key is set when a begin
		// transaction request is received.
		newTxn := roachpb.NewTransaction(ba.Txn.Name, nil, ba.GetUserPriority(),
			ba.Txn.Isolation, tc.clock.Now(), tc.clock.MaxOffset().Nanoseconds())
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
				return util.Errorf("begin transaction requested twice in the same transaction")
			}
			haveBeginTxn = true
			ba.Txn.Key = bt.Key
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
func (tc *TxnCoordSender) cleanupTxn(trace *tracer.Trace, txn roachpb.Transaction) {
	trace.Event("coordinator stops")
	tc.Lock()
	defer tc.Unlock()
	txnMeta, ok := tc.txns[string(txn.ID)]
	// The heartbeat might've already removed the record.
	if !ok {
		return
	}

	// The supplied txn may be newer than the one in txnMeta, which is relevant
	// for stats.
	txnMeta.txn = txn
	// Trigger heartbeat shutdown.
	close(txnMeta.txnEnd)
}

// unregisterTxn deletes a txnMetadata object from the sender
// and collects its stats. It assumes the lock is held.
func (tc *TxnCoordSender) unregisterTxnLocked(id string) {
	txnMeta := tc.txns[id] // guaranteed to exist
	if txnMeta == nil {
		panic("attempt to unregister non-existent transaction: " + id)
	}
	tc.txnStats.durations = append(tc.txnStats.durations, float64(tc.clock.PhysicalNow()-txnMeta.firstUpdateNanos))
	tc.txnStats.restarts = append(tc.txnStats.restarts, float64(txnMeta.txn.Epoch))
	switch txnMeta.txn.Status {
	case roachpb.ABORTED:
		tc.txnStats.aborted++
	case roachpb.PENDING:
		tc.txnStats.abandoned++
	case roachpb.COMMITTED:
		tc.txnStats.committed++
	}
	txnMeta.keys.Clear()

	delete(tc.txns, id)
}

// heartbeatLoop periodically sends a HeartbeatTxn RPC to an extant
// transaction, stopping in the event the transaction is aborted or
// committed after attempting to resolve the intents. When the
// heartbeat stops, the transaction is unregistered from the
// coordinator,
func (tc *TxnCoordSender) heartbeatLoop(id string) {
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(tc.heartbeatInterval)
		tickChan = ticker.C
		defer ticker.Stop()
	}
	defer func() {
		tc.Lock()
		tc.unregisterTxnLocked(id)
		tc.Unlock()
	}()

	var closer <-chan struct{}
	var trace *tracer.Trace
	{
		tc.Lock()
		txnMeta := tc.txns[id] // do not leak to outer scope
		closer = txnMeta.txnEnd
		trace = tc.tracer.NewTrace(tracer.Coord, &txnMeta.txn)
		defer trace.Finalize()
		tc.Unlock()
	}
	if closer == nil {
		// Avoid race in which a Txn is cleaned up before the heartbeat
		// goroutine gets a chance to start.
		return
	}
	ctx := tracer.ToCtx(context.Background(), trace)
	// Loop with ticker for periodic heartbeats.
	for {
		select {
		case <-tickChan:
			if !tc.heartbeat(id, trace, ctx) {
				return
			}
		case <-closer:
			// Transaction finished normally.
			return
		}
	}
}

func (tc *TxnCoordSender) heartbeat(id string, trace *tracer.Trace, ctx context.Context) bool {
	tc.Lock()
	proceed := true
	txnMeta := tc.txns[id]
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
	}
	// txnMeta.txn is possibly replaced concurrently,
	// so grab a copy before unlocking.
	txn := txnMeta.txn
	tc.Unlock()
	if !proceed {
		return false
	}

	hb := &roachpb.HeartbeatTxnRequest{}
	hb.Key = txn.Key
	ba := roachpb.BatchRequest{}
	ba.Timestamp = tc.clock.Now()
	ba.CmdID = ba.GetOrCreateCmdID(ba.Timestamp.WallTime)
	ba.Txn = txn.Clone()
	ba.Add(hb)

	epochEnds := trace.Epoch("heartbeat")
	_, err := tc.wrapped.Send(ctx, ba)
	epochEnds()
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
	trace := tracer.FromCtx(ctx)
	newTxn := &roachpb.Transaction{}
	newTxn.Update(ba.GetTxn())
	err := pErr.GoError()
	// TODO(bdarnell): We're writing to errors here (and where using ErrorWithIndex);
	// since there's no concept of ownership copy-on-write is always preferable.
	switch t := err.(type) {
	case nil:
		newTxn.Update(br.Txn)
		// Move txn timestamp forward to response timestamp if applicable.
		// TODO(tschottdorf): see (*Replica).executeBatch and comments within.
		// Looks like this isn't necessary any more, nor did it prevent a bug
		// referenced in a TODO there.
		newTxn.Timestamp.Forward(br.Timestamp)
	case *roachpb.TransactionStatusError:
		// Likely already committed or more obscure errors such as epoch or
		// timestamp regressions; consider txn dead.
		defer tc.cleanupTxn(trace, t.Txn)
	case *roachpb.OpRequiresTxnError:
		// TODO(tschottdorf): range-spanning autowrap currently broken.
		panic("TODO(tschottdorf): disabled")
	case *roachpb.ReadWithinUncertaintyIntervalError:
		// Mark the host as certain. See the protobuf comment for
		// Transaction.CertainNodes for details.
		if t.NodeID == 0 {
			panic("no replica set in header on uncertainty restart")
		}
		newTxn.Update(&t.Txn)
		newTxn.CertainNodes.Add(t.NodeID)
		// If the reader encountered a newer write within the uncertainty
		// interval, move the timestamp forward, just past that write or
		// up to MaxTimestamp, whichever comes first.
		candidateTS := newTxn.MaxTimestamp
		candidateTS.Backward(t.ExistingTimestamp.Add(0, 1))
		newTxn.Timestamp.Forward(candidateTS)
		newTxn.Restart(ba.GetUserPriority(), newTxn.Priority, newTxn.Timestamp)
		t.Txn = *newTxn
	case *roachpb.TransactionAbortedError:
		trace.SetError()
		newTxn.Update(&t.Txn)
		// Increase timestamp if applicable.
		newTxn.Timestamp.Forward(t.Txn.Timestamp)
		newTxn.Priority = t.Txn.Priority
		t.Txn = *newTxn
		// Clean up the freshly aborted transaction in defer(), avoiding a
		// race with the state update below.
		defer tc.cleanupTxn(trace, t.Txn)
	case *roachpb.TransactionPushError:
		newTxn.Update(t.Txn)
		// Increase timestamp if applicable, ensuring that we're
		// just ahead of the pushee.
		newTxn.Timestamp.Forward(t.PusheeTxn.Timestamp.Add(0, 1))
		newTxn.Restart(ba.GetUserPriority(), t.PusheeTxn.Priority-1, newTxn.Timestamp)
		t.Txn = newTxn
	case *roachpb.TransactionRetryError:
		newTxn.Update(&t.Txn)
		newTxn.Restart(ba.GetUserPriority(), t.Txn.Priority, newTxn.Timestamp)
		t.Txn = *newTxn
	case roachpb.TransactionRestartError:
		// Assertion: The above cases should exhaust all ErrorDetails which
		// carry a Transaction.
		if pErr.Detail != nil {
			panic(fmt.Sprintf("unhandled TransactionRestartError %T", err))
		}
	default:
		trace.SetError()
	}

	return func() *roachpb.Error {
		if len(newTxn.ID) <= 0 {
			return pErr
		}
		id := string(newTxn.ID)
		tc.Lock()
		defer tc.Unlock()
		txnMeta := tc.txns[id]
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
		// can augment a potential EndTransaction call).
		// consider re-using those.
		if intents := ba.GetIntents(); len(intents) > 0 && (err == nil || newTxn.Writing) {
			if txnMeta == nil {
				if !newTxn.Writing {
					panic("txn with intents marked as non-writing")
				}
				txnMeta = &txnMetadata{
					txn:              *newTxn,
					keys:             cache.NewIntervalCache(cache.Config{Policy: cache.CacheNone}),
					firstUpdateNanos: tc.clock.PhysicalNow(),
					lastUpdateNanos:  tc.clock.PhysicalNow(),
					timeoutDuration:  tc.clientTimeout,
					txnEnd:           make(chan struct{}),
				}
				tc.txns[id] = txnMeta
				// If the transaction is already over, there's no point in
				// launching a one-off coordinator which will shut down right
				// away. If we ended up here with an error, we'll always start
				// the coordinator - the transaction has laid down intents, so
				// we expect it to be committed/aborted at some point in the
				// future.
				if _, isEnding := ba.GetArg(roachpb.EndTransaction); err != nil || !isEnding {
					trace.Event("coordinator spawns")
					if !tc.stopper.RunAsyncTask(func() {
						tc.heartbeatLoop(id)
					}) {
						// The system is already draining and we can't start the
						// heartbeat. We refuse new transactions for now because
						// they're likely not going to have all intents committed.
						// In principle, we can relax this as needed though.
						tc.unregisterTxnLocked(id)
						return roachpb.NewError(&roachpb.NodeUnavailableError{})
					}
				}
			}
			for _, intent := range intents {
				txnMeta.addKeyRange(intent.Key, intent.EndKey)
			}
		}
		// Update our record of this transaction, even on error.
		if txnMeta != nil {
			txnMeta.txn.Update(newTxn) // better to replace after #2300
			if !txnMeta.txn.Writing {
				panic("tracking a non-writing txn")
			}
			txnMeta.setLastUpdate(tc.clock.PhysicalNow())
		}
		if err == nil {
			// For successful transactional requests, always send the updated txn
			// record back.
			if br.Txn == nil {
				br.Txn = &roachpb.Transaction{}
			}
			*br.Txn = *newTxn
		}
		return pErr
	}()
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
	tmpDB := client.NewDBWithPriority(tc, ba.GetUserPriority())
	var br *roachpb.BatchResponse
	err := tmpDB.Txn(func(txn *client.Txn) error {
		txn.SetDebugName("auto-wrap", 0)
		b := &client.Batch{}
		for _, arg := range ba.Requests {
			req := arg.GetInner()
			b.InternalAddRequest(req)
		}
		var err error
		br, err = txn.CommitInBatchWithResponse(b)
		return err
	})
	if err != nil {
		return nil, roachpb.NewError(err)
	}
	br.Txn = nil // hide the evidence
	return br, nil
}
