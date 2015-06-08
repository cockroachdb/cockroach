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
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
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

	// txnEnd is closed when the transaction is aborted or committed.
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

// close sends resolve intent commands for all key ranges this
// transaction has covered, clears the keys cache and closes the
// metadata heartbeat. Any keys listed in the resolved slice have
// already been resolved and do not receive resolve intent commands.
func (tm *txnMetadata) close(txn *proto.Transaction, resolved []proto.Key, sender client.Sender, stopper *util.Stopper) {
	close(tm.txnEnd) // stop heartbeat
	if tm.keys.Len() > 0 {
		if log.V(2) {
			log.Infof("cleaning up %d intent(s) for transaction %s", tm.keys.Len(), txn)
		}
	}
	for _, o := range tm.keys.GetOverlaps(proto.KeyMin, proto.KeyMax) {
		// If the op was range based, end key != start key: resolve a range.
		var call client.Call
		key := o.Key.Start().(proto.Key)
		endKey := o.Key.End().(proto.Key)
		if !key.Next().Equal(endKey) {
			call.Args = &proto.InternalResolveIntentRangeRequest{
				RequestHeader: proto.RequestHeader{
					Timestamp: txn.Timestamp,
					Key:       key,
					EndKey:    endKey,
					User:      storage.UserRoot,
					Txn:       txn,
				},
			}
			call.Reply = &proto.InternalResolveIntentRangeResponse{}
		} else {
			// Check if the key has already been resolved; skip if yes.
			found := false
			for _, k := range resolved {
				if key.Equal(k) {
					found = true
				}
			}
			if found {
				continue
			}
			call.Args = &proto.InternalResolveIntentRequest{
				RequestHeader: proto.RequestHeader{
					Timestamp: txn.Timestamp,
					Key:       key,
					User:      storage.UserRoot,
					Txn:       txn,
				},
			}
			call.Reply = &proto.InternalResolveIntentResponse{}
		}
		// We don't care about the reply channel; these are best
		// effort. We simply fire and forget, each in its own goroutine.
		if stopper.StartTask() {
			go func() {
				if log.V(2) {
					log.Infof("cleaning up intent %q for txn %s", call.Args.Header().Key, txn)
				}
				sender.Send(context.TODO(), call)
				if call.Reply.Header().Error != nil {
					log.Warningf("failed to cleanup %q intent: %s", call.Args.Header().Key, call.Reply.Header().GoError())
				}
				stopper.FinishTask()
			}()
		}
	}
	tm.keys.Clear()
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
	stopper           *util.Stopper
}

// NewTxnCoordSender creates a new TxnCoordSender for use from a KV
// distributed DB instance. A TxnCoordSender should be closed when no
// longer in use via Close(), which also closes the wrapped sender
// supplied here.
func NewTxnCoordSender(wrapped client.Sender, clock *hlc.Clock, linearizable bool, stopper *util.Stopper) *TxnCoordSender {
	tc := &TxnCoordSender{
		wrapped:           wrapped,
		clock:             clock,
		heartbeatInterval: storage.DefaultHeartbeatInterval,
		clientTimeout:     defaultClientTimeout,
		txns:              map[string]*txnMetadata{},
		linearizable:      linearizable,
		stopper:           stopper,
	}

	tc.stopper.RunWorker(tc.startStats)
	return tc
}

// startStats blocks and periodically logs transaction statistics (throughput,
// success rates, durations, ...).
// TODO(tschottdorf): Use a proper metrics subsystem for this (+the store-level
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
			dMax := time.Duration(stats.Max(curStats.durations))
			dMean := time.Duration(stats.Mean(curStats.durations))
			dDev := time.Duration(stats.StdDevP(curStats.durations))
			rMax := stats.Max(curStats.restarts)
			rMean := stats.Mean(curStats.restarts)
			rDev := stats.StdDevP(curStats.restarts)

			rate := float64(int64(num)*int64(time.Second)) / float64(now-lastNow)
			var pCommitted, pAbandoned, pAborted float32
			if num > 0 {
				pCommitted = 100 * float32(curStats.committed) / float32(num)
				pAbandoned = 100 * float32(curStats.abandoned) / float32(num)
				pAborted = 100 * float32(curStats.aborted) / float32(num)
			}
			log.Infof("txn coordinator: %.2f txn/sec, %.2f/%.2f/%.2f %%cmmt/abrt/abnd, %s/%s/%s avg/σ/max duration, %.1f/%.1f/%.1f avg/σ/max restarts (%d samples)",
				rate, pCommitted, pAborted, pAbandoned, util.TruncateDuration(dMean, res),
				util.TruncateDuration(dDev, res), util.TruncateDuration(dMax, res),
				rMean, rDev, rMax, num)
			lastNow = now
		case <-tc.stopper.ShouldStop():
			return
		}
	}
}

// Send implements the client.Sender interface. If the call is part
// of a transaction, the coordinator will initialize the transaction
// if it's not nil but has an empty ID.
func (tc *TxnCoordSender) Send(_ context.Context, call client.Call) {
	header := call.Args.Header()
	tc.maybeBeginTxn(header)

	// Process batch specially; otherwise, send via wrapped sender.
	switch args := call.Args.(type) {
	case *proto.InternalBatchRequest:
		tc.sendBatch(args, call.Reply.(*proto.InternalBatchResponse))
	case *proto.BatchRequest:
		// Convert the batch request to internal-batch request.
		internalArgs := &proto.InternalBatchRequest{RequestHeader: args.RequestHeader}
		internalReply := &proto.InternalBatchResponse{}
		for i := range args.Requests {
			internalArgs.Add(args.Requests[i].GetValue().(proto.Request))
		}
		tc.sendBatch(internalArgs, internalReply)
		reply := call.Reply.(*proto.BatchResponse)
		reply.ResponseHeader = internalReply.ResponseHeader
		// Convert form internal-batch response to batch response.
		for i := range internalReply.Responses {
			reply.Add(internalReply.Responses[i].GetValue().(proto.Response))
		}
	default:
		tc.sendOne(call)
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
func (tc *TxnCoordSender) sendOne(call client.Call) {
	var startNS int64
	header := call.Args.Header()
	// If this call is part of a transaction...
	if header.Txn != nil {
		// Set the timestamp to the original timestamp for read-only
		// commands and to the transaction timestamp for read/write
		// commands.
		if proto.IsReadOnly(call.Args) {
			header.Timestamp = header.Txn.OrigTimestamp
		} else {
			header.Timestamp = header.Txn.Timestamp
		}
		// EndTransaction must have its key set to that of the txn.
		if _, ok := call.Args.(*proto.EndTransactionRequest); ok {
			header.Key = header.Txn.Key
			// Remember when EndTransaction started in case we want to
			// be linearizable.
			startNS = tc.clock.PhysicalNow()
		}
	}

	// Send the command through wrapped sender.
	tc.wrapped.Send(context.TODO(), call)

	if header.Txn != nil {
		// If not already set, copy the request txn.
		if call.Reply.Header().Txn == nil {
			call.Reply.Header().Txn = gogoproto.Clone(header.Txn).(*proto.Transaction)
		}
		tc.updateResponseTxn(header, call.Reply.Header())
	}

	if txn := call.Reply.Header().Txn; txn != nil {
		tc.Lock()
		txnMeta := tc.txns[string(txn.ID)]
		// If this transactional command leaves transactional intents, add the key
		// or key range to the intents map. If the transaction metadata doesn't yet
		// exist, create it.
		if call.Reply.Header().GoError() == nil {
			if proto.IsTransactionWrite(call.Args) {
				if txnMeta == nil {
					txnMeta = &txnMetadata{
						txn:              *txn,
						keys:             cache.NewIntervalCache(cache.Config{Policy: cache.CacheNone}),
						firstUpdateNanos: tc.clock.PhysicalNow(),
						lastUpdateNanos:  tc.clock.PhysicalNow(),
						timeoutDuration:  tc.clientTimeout,
						txnEnd:           make(chan struct{}),
					}
					id := string(txn.ID)
					tc.txns[id] = txnMeta
					tc.heartbeat(id)
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
		tc.cleanupTxn(t.Txn, nil)
	case *proto.TransactionAbortedError:
		// If already aborted, cleanup the txn on this TxnCoordSender.
		tc.cleanupTxn(t.Txn, nil)
	case *proto.OpRequiresTxnError:
		// Run a one-off transaction with that single command.
		if log.V(1) {
			log.Infof("%s: auto-wrapping in txn and re-executing", call.Method())
		}
		txnOpts := &client.TransactionOptions{
			Name: "auto-wrap",
		}
		tmpDB, err := client.Open(
			fmt.Sprintf("//%s?priority=%d",
				call.Args.Header().User, call.Args.Header().GetUserPriority()),
			client.SenderOpt(tc))
		if err != nil {
			log.Warning(err)
			return
		}
		call.Reply.Reset()
		if err := tmpDB.InternalKV().RunTransaction(txnOpts, func(txn *client.Txn) error {
			return txn.Run(call)
		}); err != nil {
			log.Warning(err)
		}
	case nil:
		var resolved []proto.Key
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
				resolved = call.Reply.(*proto.EndTransactionResponse).Resolved
				if txn.Status != proto.PENDING {
					tc.cleanupTxn(*txn, resolved)
				}

			}
		}
	}
}

// sendBatch unrolls a batched command and sends each constituent
// command in parallel.
func (tc *TxnCoordSender) sendBatch(batchArgs *proto.InternalBatchRequest, batchReply *proto.InternalBatchResponse) {
	// Prepare the calls by unrolling the batch. If the batchReply is
	// pre-initialized with replies, use those; otherwise create replies
	// as needed.
	// TODO(spencer): send calls in parallel.
	batchReply.Txn = batchArgs.Txn
	for i := range batchArgs.Requests {
		args := batchArgs.Requests[i].GetValue().(proto.Request)
		call := client.Call{Args: args}
		// Disallow transaction, user and priority on individual calls, unless
		// equal.
		if args.Header().User != "" && args.Header().User != batchArgs.User {
			batchReply.Header().SetGoError(util.Error("cannot have individual user on call in batch"))
			return
		}
		args.Header().User = batchArgs.User
		if args.Header().UserPriority != nil && args.Header().GetUserPriority() != batchArgs.GetUserPriority() {
			batchReply.Header().SetGoError(util.Error("cannot have individual user priority on call in batch"))
			return
		}
		args.Header().UserPriority = batchArgs.UserPriority
		if txn := args.Header().Txn; txn != nil && !txn.Equal(batchArgs.Txn) {
			batchReply.Header().SetGoError(util.Error("cannot have individual transactional call in batch"))
			return
		}
		// Propagate batch Txn to each call.
		args.Header().Txn = batchArgs.Txn

		// Create a reply from the method type and add to batch response.
		if i >= len(batchReply.Responses) {
			call.Reply = args.CreateReply()
			batchReply.Add(call.Reply)
		} else {
			call.Reply = batchReply.Responses[i].GetValue().(proto.Response)
		}
		tc.sendOne(call)
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

// cleanupTxn is called to resolve write intents which were set down over
// the course of the transaction. The txnMetadata object is removed from
// the txns map and taken into account for statistics.
func (tc *TxnCoordSender) cleanupTxn(txn proto.Transaction, resolved []proto.Key) {
	tc.Lock()
	defer tc.Unlock()
	txnMeta, ok := tc.txns[string(txn.ID)]
	if !ok {
		return
	}

	// The supplied txn may be newed than the one in txnMeta, which is relevant
	// for stats.
	txnMeta.txn = txn
	tc.unregisterTxnLocked(txnMeta)
	txnMeta.close(&txn, resolved, tc.wrapped, tc.stopper)
}

// unregisterTxnLocked idempotently deletes a txnMetadata object from the sender
// and collects its stats.
func (tc *TxnCoordSender) unregisterTxnLocked(txnMeta *txnMetadata) {
	id := string(txnMeta.txn.ID)
	if _, ok := tc.txns[id]; !ok {
		return
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
	delete(tc.txns, id)
}

// heartbeat periodically sends an InternalHeartbeatTxn RPC to an
// extant transaction, stopping in the event the transaction is
// aborted or committed or if the TxnCoordSender is closed.
func (tc *TxnCoordSender) heartbeat(id string) {
	tc.stopper.RunWorker(func() {
		ticker := time.NewTicker(tc.heartbeatInterval)
		defer ticker.Stop()

		tc.Lock()
		var closer chan struct{}
		if txnMeta, ok := tc.txns[id]; ok {
			closer = txnMeta.txnEnd
		}
		tc.Unlock()
		if closer == nil {
			return
		}

		// Loop with ticker for periodic heartbeats.
		for {
			select {
			case <-ticker.C:
				tc.Lock()
				var txn proto.Transaction
				_, proceed := tc.txns[id]
				if proceed {
					txnMeta := tc.txns[id] // assign only here for local scope
					// Before we send a heartbeat, determine whether this transaction
					// should be considered abandoned. If so, exit heartbeat.
					if txnMeta.hasClientAbandonedCoord(tc.clock.PhysicalNow()) {
						tc.unregisterTxnLocked(txnMeta)
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
					// so grab a copy.
					txn = txnMeta.txn
				}
				tc.Unlock()
				if !proceed {
					return
				}

				request := &proto.InternalHeartbeatTxnRequest{
					RequestHeader: proto.RequestHeader{
						Key:  txn.Key,
						User: storage.UserRoot,
						Txn:  &txn,
					},
				}

				request.Header().Timestamp = tc.clock.Now()
				reply := &proto.InternalHeartbeatTxnResponse{}
				call := client.Call{
					Args:  request,
					Reply: reply,
				}

				if !tc.stopper.StartTask() {
					continue
				}
				tc.wrapped.Send(context.TODO(), call)
				// If the transaction is not in pending state, then we can stop
				// the heartbeat. It's either aborted or committed, and we resolve
				// write intents accordingly.
				if reply.GoError() != nil {
					log.Warningf("heartbeat to %s failed: %s", txn, reply.GoError())
				} else if reply.Txn != nil && reply.Txn.Status != proto.PENDING {
					tc.cleanupTxn(*reply.Txn, nil)
					proceed = false
				}
				tc.stopper.FinishTask()
				if !proceed {
					return
				}

			case <-closer:
				// Transaction finished.
				return

			case <-tc.stopper.ShouldStop():
				// System shutdown.
				return
			}
		}
	})
}
