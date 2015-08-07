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
// Author: Jiajia Han (hanjia18@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracer"
	gogoproto "github.com/gogo/protobuf/proto"
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
	// txn is the transaction struct from the initial AddRequest call.
	txn proto.Transaction

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
func (tm *txnMetadata) addKeyRange(start, end proto.Key) {
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
func (tm *txnMetadata) intents() []proto.Intent {
	intents := make([]proto.Intent, 0, tm.keys.Len())
	for _, o := range tm.keys.GetOverlaps(proto.KeyMin, proto.KeyMax) {
		intent := proto.Intent{
			Key: o.Key.Start().(proto.Key),
		}
		if endKey := o.Key.End().(proto.Key); !intent.Key.IsPrev(endKey) {
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

// Send implements the client.Sender interface. If the call is part
// of a transaction, the coordinator will initialize the transaction
// if it's not nil but has an empty ID.
func (tc *TxnCoordSender) Send(ctx context.Context, call proto.Call) {
	doSend := func() {
		header := call.Args.Header()
		tc.maybeBeginTxn(header)
		header.CmdID = header.GetOrCreateCmdID(tc.clock.PhysicalNow())

		// This is the earliest point at which the request has a ClientCmdID and/or
		// TxnID (if applicable). Begin a Trace which follows this request.
		trace := tc.tracer.NewTrace(call.Args.Header())
		defer trace.Finalize()
		defer trace.Epoch(fmt.Sprintf("sending %s", call.Method()))()
		defer func() {
			if err := call.Reply.Header().GoError(); err != nil {
				trace.Event(fmt.Sprintf("reply error: %T", err))
			}
		}()
		ctx = tracer.ToCtx(ctx, trace)

		// Process batch specially; otherwise, send via wrapped sender.
		switch args := call.Args.(type) {
		case *proto.BatchRequest:
			trace.Event("batch processing")
			tc.sendBatch(ctx, args, call.Reply.(*proto.BatchResponse))
		default:
			// TODO(tschottdorf): should treat all calls as Batch. After all, that
			// will be almost all calls.
			tc.sendOne(ctx, call)
		}
	}

	// This is currently the convenient way to handle OpRequiresTxnError.
	// Likely to be smoothed out by further batch-related refactors.
	for {
		doSend()
		switch call.Reply.Header().GoError().(type) {
		case *proto.OpRequiresTxnError:
			call.Reply.Reset()
			if header := call.Args.Header(); header.Txn != nil {
				panic("OpRequiresTxnError received for transactional call")
			} else {
				header.Txn = &proto.Transaction{Name: "auto-wrap"}
			}
			continue
		}
		break
	}
}

// maybeBeginTxn begins a new transaction if a txn has been specified
// in the request but has a nil ID. The new transaction is initialized
// using the name and isolation in the otherwise uninitialized txn.
// The Priority, if non-zero is used as a minimum.
func (tc *TxnCoordSender) maybeBeginTxn(header *proto.RequestHeader) {
	if header.Txn != nil {
		if len(header.Txn.ID) == 0 {
			newTxn := proto.NewTransaction(header.Txn.Name, keys.KeyAddress(header.Key), header.GetUserPriority(),
				header.Txn.Isolation, tc.clock.Now(), tc.clock.MaxOffset().Nanoseconds())
			// Use existing priority as a minimum. This is used on transaction
			// aborts to ratchet priority when creating successor transaction.
			if newTxn.Priority < header.Txn.Priority {
				newTxn.Priority = header.Txn.Priority
			}
			header.Txn = newTxn
		}
	}
}

// sendOne sends a single call via the wrapped sender. If the call is
// part of a transaction, the TxnCoordSender adds the transaction to a
// map of active transactions and begins heartbeating it. Every
// subsequent call for the same transaction updates the lastUpdate
// timestamp to prevent live transactions from being considered
// abandoned and garbage collected. Read/write mutating requests have
// their key or key range added to the transaction's interval tree of
// key ranges for eventual cleanup via resolved write intents.
//
// On success, and if the call is part of a transaction, the affected
// key range is recorded as live intents for eventual cleanup upon
// transaction commit. Upon successful txn commit, initiates cleanup
// of intents.
func (tc *TxnCoordSender) sendOne(ctx context.Context, call proto.Call) {
	var startNS int64
	header := call.Args.Header()
	trace := tracer.FromCtx(ctx)
	var id string // optional transaction ID
	if header.Txn != nil {
		// If this call is part of a transaction...
		id = string(header.Txn.ID)
		// Verify that if this Transaction is not read-only, we have it on
		// file. If not, refuse writes - the client must have issued a write on
		// another coordinator previously.
		if header.Txn.Writing && proto.IsTransactionWrite(call.Args) {
			tc.Lock()
			_, ok := tc.txns[id]
			tc.Unlock()
			if !ok {
				call.Reply.Header().SetGoError(util.Errorf(
					"transaction must not write on multiple coordinators"))
				return
			}
		}

		// Set the timestamp to the original timestamp for read-only
		// commands and to the transaction timestamp for read/write
		// commands.
		if proto.IsReadOnly(call.Args) {
			header.Timestamp = header.Txn.OrigTimestamp
		} else {
			header.Timestamp = header.Txn.Timestamp
		}

		if args, ok := call.Args.(*proto.EndTransactionRequest); ok {
			// Remember when EndTransaction started in case we want to
			// be linearizable.
			startNS = tc.clock.PhysicalNow()
			// EndTransaction must have its key set to that of the txn.
			header.Key = header.Txn.Key
			if len(args.Intents) > 0 {
				// TODO(tschottdorf): it may be useful to allow this later.
				// That would be part of a possible plan to allow txns which
				// write on multiple coordinators.
				call.Reply.Header().SetGoError(util.Errorf(
					"client must not pass intents to EndTransaction"))
				return
			}
			tc.Lock()
			if txnMeta := tc.txns[id]; id != "" && txnMeta != nil {
				args.Intents = txnMeta.intents()
			}
			tc.Unlock()
			// If there aren't any intents, then there's factually no
			// transaction to end. Read-only txns have all of their state in
			// the client.
			if len(args.Intents) == 0 {
				call.Reply.Header().SetGoError(util.Errorf(
					"cannot commit a read-only transaction"))
				return
			}
		}
	}

	// Send the command through wrapped sender.
	tc.wrapped.Send(ctx, call)

	// For transactional calls, need to track & update the transaction.
	if header.Txn != nil {
		respHeader := call.Reply.Header()
		if respHeader.Txn == nil {
			// When empty, simply use the request's transaction.
			// This is expected: the Range doesn't bother copying unless the
			// object changes.
			respHeader.Txn = gogoproto.Clone(header.Txn).(*proto.Transaction)
		}
		tc.updateResponseTxn(header, respHeader)
	}

	if txn := call.Reply.Header().Txn; txn != nil {
		if !header.Txn.Equal(txn) {
			panic("transaction ID changed")
		}
		tc.Lock()
		txnMeta := tc.txns[id]
		// If this transactional command leaves transactional intents, add the key
		// or key range to the intents map. If the transaction metadata doesn't yet
		// exist, create it.
		if call.Reply.Header().GoError() == nil {
			if proto.IsTransactionWrite(call.Args) {
				if txnMeta == nil {
					txn.Writing = true
					trace.Event("coordinator spawns")
					txnMeta = &txnMetadata{
						txn:              *txn,
						keys:             cache.NewIntervalCache(cache.Config{Policy: cache.CacheNone}),
						firstUpdateNanos: tc.clock.PhysicalNow(),
						lastUpdateNanos:  tc.clock.PhysicalNow(),
						timeoutDuration:  tc.clientTimeout,
						txnEnd:           make(chan struct{}),
					}
					tc.txns[id] = txnMeta
					if !tc.stopper.RunAsyncTask(func() {
						tc.heartbeat(id)
					}) {
						// The system is already draining and we can't start the
						// heartbeat. We refuse new transactions for now because
						// they're likely not going to have all intents committed.
						// In principle, we can relax this as needed though.
						call.Reply.Header().SetGoError(&proto.NodeUnavailableError{})
						tc.Unlock()
						tc.unregisterTxn(id)
						return
					}
				}
				txnMeta.addKeyRange(header.Key, header.EndKey)
			}
			// Update our record of this transaction.
			if txnMeta != nil {
				txnMeta.txn = *txn
				txnMeta.setLastUpdate(tc.clock.PhysicalNow())
			}
		}
		tc.Unlock()
	}

	// Cleanup intents and transaction map if end of transaction.
	switch t := call.Reply.Header().GoError().(type) {
	case *proto.TransactionStatusError:
		// Likely already committed or more obscure errors such as epoch or
		// timestamp regressions; consider it dead.
		tc.cleanupTxn(trace, t.Txn)
	case *proto.TransactionAbortedError:
		// If already aborted, cleanup the txn on this TxnCoordSender.
		tc.cleanupTxn(trace, t.Txn)
	case nil:
		if txn := call.Reply.Header().Txn; txn != nil {
			if _, ok := call.Args.(*proto.EndTransactionRequest); ok {
				// If the --linearizable flag is set, we want to make sure that
				// all the clocks in the system are past the commit timestamp
				// of the transaction. This is guaranteed if either
				// - the commit timestamp is MaxOffset behind startNS
				// - MaxOffset ns were spent in this function
				// when returning to the client. Below we choose the option
				// that involves less waiting, which is likely the first one
				// unless a transaction commits with an odd timestamp.
				if tsNS := txn.Timestamp.WallTime; startNS > tsNS {
					startNS = tsNS
				}
				sleepNS := tc.clock.MaxOffset() -
					time.Duration(tc.clock.PhysicalNow()-startNS)
				if tc.linearizable && sleepNS > 0 {
					defer func() {
						if log.V(1) {
							log.Infof("%v: waiting %s on EndTransaction for linearizability", txn.Short(), util.TruncateDuration(sleepNS, time.Millisecond))
						}
						time.Sleep(sleepNS)
					}()
				}
				if txn.Status != proto.PENDING {
					tc.cleanupTxn(trace, *txn)
				}

			}
		}
	}
}

// updateForBatch updates the first argument (the header of a request contained
// in a batch) from the second one (the batch header), returning an error when
// inconsistencies are found.
// It is checked that the individual call does not have a User, UserPriority
// or Txn set that differs from the batch's.
func updateForBatch(args proto.Request, bHeader proto.RequestHeader) error {
	// Disallow transaction, user and priority on individual calls, unless
	// equal.
	aHeader := args.Header()
	if aHeader.User != "" && aHeader.User != bHeader.User {
		return util.Error("conflicting user on call in batch")
	}
	if aPrio := aHeader.GetUserPriority(); aPrio != proto.Default_RequestHeader_UserPriority && aPrio != bHeader.GetUserPriority() {
		return util.Error("conflicting user priority on call in batch")
	}
	aHeader.User = bHeader.User
	aHeader.UserPriority = bHeader.UserPriority
	// Only allow individual transactions on the requests of a batch if
	// - the batch is non-transactional,
	// - the individual transaction does not write intents, and
	// - the individual transaction is initialized.
	// The main usage of this is to allow mass-resolution of intents, which
	// entails sending a non-txn batch of transactional InternalResolveIntent.
	if aHeader.Txn != nil && !aHeader.Txn.Equal(bHeader.Txn) {
		if len(aHeader.Txn.ID) == 0 || proto.IsTransactionWrite(args) || bHeader.Txn != nil {
			return util.Error("conflicting transaction in transactional batch")
		}
	} else {
		aHeader.Txn = bHeader.Txn
	}
	return nil
}

// sendBatch unrolls a batched command and sends each constituent
// command in parallel.
// TODO(tschottdorf): modify sendBatch so that it sends truly parallel requests
// when outside of a Transaction. This can then be used to address the TODO in
// (*TxnCoordSender).resolve().
func (tc *TxnCoordSender) sendBatch(ctx context.Context, batchArgs *proto.BatchRequest, batchReply *proto.BatchResponse) {
	// Prepare the calls by unrolling the batch. If the batchReply is
	// pre-initialized with replies, use those; otherwise create replies
	// as needed.
	// TODO(spencer): send calls in parallel.
	batchReply.Txn = batchArgs.Txn
	for i := range batchArgs.Requests {
		args := batchArgs.Requests[i].GetValue().(proto.Request)
		if err := updateForBatch(args, batchArgs.RequestHeader); err != nil {
			batchReply.Header().SetGoError(err)
			return
		}
		call := proto.Call{Args: args}
		// Create a reply from the method type and add to batch response.
		if i >= len(batchReply.Responses) {
			call.Reply = args.CreateReply()
			batchReply.Add(call.Reply)
		} else {
			call.Reply = batchReply.Responses[i].GetValue().(proto.Response)
		}
		tc.sendOne(ctx, call)
		// Amalgamate transaction updates and propagate first error, if applicable.
		if batchReply.Txn != nil {
			batchReply.Txn.Update(call.Reply.Header().Txn)
		}
		if call.Reply.Header().Error != nil {
			batchReply.Error = call.Reply.Header().Error
			return
		}
	}
}

// updateResponseTxn updates the response txn based on the response
// timestamp and error. The timestamp may have changed upon
// encountering a newer write or read. Both the timestamp and the
// priority may change depending on error conditions.
func (tc *TxnCoordSender) updateResponseTxn(argsHeader *proto.RequestHeader, replyHeader *proto.ResponseHeader) {
	// Move txn timestamp forward to response timestamp if applicable.
	if replyHeader.Txn.Timestamp.Less(replyHeader.Timestamp) {
		replyHeader.Txn.Timestamp = replyHeader.Timestamp
	}

	// Take action on various errors.
	switch t := replyHeader.GoError().(type) {
	case *proto.ReadWithinUncertaintyIntervalError:
		// Mark the host as certain. See the protobuf comment for
		// Transaction.CertainNodes for details.
		replyHeader.Txn.CertainNodes.Add(argsHeader.Replica.NodeID)

		// If the reader encountered a newer write within the uncertainty
		// interval, move the timestamp forward, just past that write or
		// up to MaxTimestamp, whichever comes first.
		var candidateTS proto.Timestamp
		if t.ExistingTimestamp.Less(replyHeader.Txn.MaxTimestamp) {
			candidateTS = t.ExistingTimestamp
			candidateTS.Logical++
		} else {
			candidateTS = replyHeader.Txn.MaxTimestamp
		}
		// Only change the timestamp if we're moving it forward.
		if replyHeader.Txn.Timestamp.Less(candidateTS) {
			replyHeader.Txn.Timestamp = candidateTS
		}
		replyHeader.Txn.Restart(argsHeader.GetUserPriority(), replyHeader.Txn.Priority, replyHeader.Txn.Timestamp)
	case *proto.TransactionAbortedError:
		// Increase timestamp if applicable.
		if replyHeader.Txn.Timestamp.Less(t.Txn.Timestamp) {
			replyHeader.Txn.Timestamp = t.Txn.Timestamp
		}
		replyHeader.Txn.Priority = t.Txn.Priority
	case *proto.TransactionPushError:
		// Increase timestamp if applicable.
		if replyHeader.Txn.Timestamp.Less(t.PusheeTxn.Timestamp) {
			replyHeader.Txn.Timestamp = t.PusheeTxn.Timestamp
			replyHeader.Txn.Timestamp.Logical++ // ensure this txn's timestamp > other txn
		}
		replyHeader.Txn.Restart(argsHeader.GetUserPriority(), t.PusheeTxn.Priority-1, replyHeader.Txn.Timestamp)
	case *proto.TransactionRetryError:
		// Increase timestamp if applicable.
		if replyHeader.Txn.Timestamp.Less(t.Txn.Timestamp) {
			replyHeader.Txn.Timestamp = t.Txn.Timestamp
		}
		replyHeader.Txn.Restart(argsHeader.GetUserPriority(), t.Txn.Priority, replyHeader.Txn.Timestamp)
	}
}

// cleanupTxn is called when a transaction ends. The transaction record is
// updated and the heartbeat goroutine signaled to clean up the transaction
// gracefully.
func (tc *TxnCoordSender) cleanupTxn(trace *tracer.Trace, txn proto.Transaction) {
	tc.Lock()
	defer tc.Unlock()
	txnMeta, ok := tc.txns[string(txn.ID)]
	// Only clean up once per transaction.
	if !ok || txnMeta.txnEnd == nil {
		return
	}

	// The supplied txn may be newed than the one in txnMeta, which is relevant
	// for stats.
	txnMeta.txn = txn
	// Trigger heartbeat shutdown.
	trace.Event("coordinator stops")
	close(txnMeta.txnEnd)
	txnMeta.txnEnd = nil // for idempotency; checked above
}

// unregisterTxn deletes a txnMetadata object from the sender
// and collects its stats.
func (tc *TxnCoordSender) unregisterTxn(id string) {
	tc.Lock()
	defer tc.Unlock()
	txnMeta := tc.txns[id] // guaranteed to exist
	if txnMeta == nil {
		panic("attempt to unregister non-existent transaction: " + id)
	}
	tc.txnStats.durations = append(tc.txnStats.durations, float64(tc.clock.PhysicalNow()-txnMeta.firstUpdateNanos))
	tc.txnStats.restarts = append(tc.txnStats.restarts, float64(txnMeta.txn.Epoch))
	switch txnMeta.txn.Status {
	case proto.ABORTED:
		tc.txnStats.aborted++
	case proto.PENDING:
		tc.txnStats.abandoned++
	case proto.COMMITTED:
		tc.txnStats.committed++
	}
	txnMeta.keys.Clear()
	delete(tc.txns, id)
}

// heartbeat periodically sends a HeartbeatTxn RPC to an extant
// transaction, stopping in the event the transaction is aborted or
// committed after attempting to resolve the intents. When the
// heartbeat stops, the transaction is unregistered from the
// coordinator,
func (tc *TxnCoordSender) heartbeat(id string) {
	var tickChan <-chan time.Time
	{
		ticker := time.NewTicker(tc.heartbeatInterval)
		tickChan = ticker.C
		defer ticker.Stop()
	}
	defer tc.unregisterTxn(id)

	var closer <-chan struct{}
	var trace *tracer.Trace
	{
		tc.Lock()
		txnMeta := tc.txns[id] // do not leak to outer scope
		closer = txnMeta.txnEnd
		trace = tc.tracer.NewTrace(&txnMeta.txn)
		tc.Unlock()
	}
	if closer == nil {
		// Avoid race in which a Txn is cleaned up before the heartbeat
		// goroutine gets a chance to start.
		return
	}
	ctx := tracer.ToCtx(context.Background(), trace)
	defer trace.Finalize()
	// Loop with ticker for periodic heartbeats.
	for {
		select {
		case <-tickChan:
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
				return
			}

			request := &proto.HeartbeatTxnRequest{
				RequestHeader: proto.RequestHeader{
					Key:  txn.Key,
					User: security.RootUser,
					Txn:  &txn,
				},
			}

			request.Header().Timestamp = tc.clock.Now()
			reply := &proto.HeartbeatTxnResponse{}
			call := proto.Call{
				Args:  request,
				Reply: reply,
			}

			epochEnds := trace.Epoch("heartbeat")
			tc.wrapped.Send(ctx, call)
			epochEnds()
			// If the transaction is not in pending state, then we can stop
			// the heartbeat. It's either aborted or committed, and we resolve
			// write intents accordingly.
			if reply.GoError() != nil {
				log.Warningf("heartbeat to %s failed: %s", txn, reply.GoError())
			} else if reply.Txn != nil && reply.Txn.Status != proto.PENDING {
				// Signal cleanup. Doesn't do much but stop this goroutine, but
				// let's be future-proof.
				tc.cleanupTxn(trace, *reply.Txn)
				return
			}
		case <-closer:
			// Transaction finished normally.
			return
		}
	}
}
