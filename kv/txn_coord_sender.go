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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

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
	keys *util.IntervalCache

	// lastUpdateTS is the latest time when the client sent transaction
	// operations to this coordinator.
	lastUpdateTS proto.Timestamp

	// timeoutDuration is the time after which the transaction should be
	// considered abandoned by the client. That is, when
	// current_timestamp > lastUpdateTS + timeoutDuration If this value
	// is set to 0, a default timeout will be used.
	timeoutDuration time.Duration
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

// close sends resolve intent commands for all key ranges this
// transaction has covered, clears the keys cache and closes the
// metadata heartbeat. Any keys listed in the resolved slice have
// already been resolved and do not receive resolve intent commands.
func (tm *txnMetadata) close(txn *proto.Transaction, resolved []proto.Key, sender client.KVSender, stopper *util.Stopper) {
	if tm.keys.Len() > 0 {
		log.V(1).Infof("cleaning up %d intent(s) for transaction %s", tm.keys.Len(), txn)
	}
	for _, o := range tm.keys.GetOverlaps(engine.KeyMin, engine.KeyMax) {
		call := &client.Call{
			Args: &proto.InternalResolveIntentRequest{
				RequestHeader: proto.RequestHeader{
					Timestamp: txn.Timestamp,
					Key:       o.Key.Start().(proto.Key),
					User:      storage.UserRoot,
					Txn:       txn,
				},
			},
			Reply: &proto.InternalResolveIntentResponse{},
		}
		// Set the end key only if it's not equal to Key.Next(). This
		// saves us from unnecessarily clearing intents as a range.
		endKey := o.Key.End().(proto.Key)
		if !call.Args.Header().Key.Next().Equal(endKey) {
			call.Args.Header().EndKey = endKey
		} else {
			// Check if the key has already been resolved; skip if yes.
			found := false
			for _, k := range resolved {
				if call.Args.Header().Key.Equal(k) {
					found = true
				}
			}
			if found {
				continue
			}
		}
		// We don't care about the reply channel; these are best
		// effort. We simply fire and forget, each in its own goroutine.
		if stopper.StartTask() {
			go func() {
				log.V(1).Infof("cleaning up intent %q for txn %s", call.Args.Header().Key, txn)
				sender.Send(call)
				if call.Reply.Header().Error != nil {
					log.Warningf("failed to cleanup %q intent: %s", call.Args.Header().Key, call.Reply.Header().GoError())
				}
				stopper.FinishTask()
			}()
		}
	}
	tm.keys.Clear()
}

// A TxnCoordSender is an implementation of client.KVSender which
// wraps a lower-level KVSender (either a LocalSender or a DistSender)
// to which it sends commands. It acts as a man-in-the-middle,
// coordinating transaction state for clients.  After a transaction is
// started, the TxnCoordSender starts asynchronously sending heartbeat
// messages to that transaction's txn record, to keep it live. It also
// keeps track of each written key or key range over the course of the
// transaction. When the transaction is committed or aborted, it
// clears accumulated write intents for the transaction.
type TxnCoordSender struct {
	wrapped           client.KVSender
	clock             *hlc.Clock
	heartbeatInterval time.Duration
	clientTimeout     time.Duration
	sync.Mutex                                // Protects the txns map.
	txns              map[string]*txnMetadata // txn key to metadata
	linearizable      bool                    // Enables linearizable behaviour.
	stopper           *util.Stopper
}

// NewTxnCoordSender creates a new TxnCoordSender for use from a KV
// distributed DB instance. A TxnCoordSender should be closed when no
// longer in use via Close(), which also closes the wrapped sender
// supplied here.
func NewTxnCoordSender(wrapped client.KVSender, clock *hlc.Clock, linearizable bool, stopper *util.Stopper) *TxnCoordSender {
	tc := &TxnCoordSender{
		wrapped:           wrapped,
		clock:             clock,
		heartbeatInterval: storage.DefaultHeartbeatInterval,
		clientTimeout:     defaultClientTimeout,
		txns:              map[string]*txnMetadata{},
		linearizable:      linearizable,
		stopper:           stopper,
	}
	return tc
}

// Send implements the client.KVSender interface. If the call is part
// of a transaction, the coordinator will initialize the transaction
// if it's not nil but has an empty ID.
func (tc *TxnCoordSender) Send(call *client.Call) {
	header := call.Args.Header()
	tc.maybeBeginTxn(header)

	// Process batch specially; otherwise, send via wrapped sender.
	if call.Method() == proto.Batch {
		tc.sendBatch(call.Args.(*proto.BatchRequest), call.Reply.(*proto.BatchResponse))
	} else {
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
			newTxn := proto.NewTransaction(header.Txn.Name, engine.KeyAddress(header.Key), header.GetUserPriority(),
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
// subsequent call for the same transaction updates the lastUpdateTS
// to prevent live transactions from being considered abandoned and
// garbage collected. Read/write mutating requests have their key or
// key range added to the transaction's interval tree of key ranges
// for eventual cleanup via resolved write intents.
//
// On success, and if the call is part of a transaction, the affected
// key range is recorded as live intents for eventual cleanup upon
// transaction commit. Upon successful txn commit, initiates cleanup
// of intents.
func (tc *TxnCoordSender) sendOne(call *client.Call) {
	var startNS int64
	header := call.Args.Header()
	// If this call is part of a transaction...
	if header.Txn != nil {
		// Set the timestamp to the original timestamp for read-only
		// commands and to the transaction timestamp for read/write
		// commands.
		if proto.IsReadOnly(call.Method()) {
			header.Timestamp = header.Txn.OrigTimestamp
		} else {
			header.Timestamp = header.Txn.Timestamp
		}
		// End transaction must have its key set to the txn ID.
		if call.Method() == proto.EndTransaction {
			header.Key = header.Txn.Key
			// Remember when EndTransaction started in case we want to
			// be linearizable.
			startNS = tc.clock.PhysicalNow()
		}
	}

	// Send the command through wrapped sender.
	tc.wrapped.Send(call)

	if header.Txn != nil {
		// If not already set, copy the request txn.
		if call.Reply.Header().Txn == nil {
			call.Reply.Header().Txn = gogoproto.Clone(header.Txn).(*proto.Transaction)
		}
		tc.updateResponseTxn(header, call.Reply.Header())
	}

	// If successful, we're in a transaction, and the command leaves
	// transactional intents, add the key or key range to the intents map.
	// If the transaction metadata doesn't yet exist, create it.
	if call.Reply.Header().GoError() == nil && header.Txn != nil && proto.IsTransactional(call.Method()) {
		tc.Lock()
		var ok bool
		var txnMeta *txnMetadata
		if txnMeta, ok = tc.txns[string(header.Txn.ID)]; !ok {
			txnMeta = &txnMetadata{
				txn:             *header.Txn,
				keys:            util.NewIntervalCache(util.CacheConfig{Policy: util.CacheNone}),
				lastUpdateTS:    tc.clock.Now(),
				timeoutDuration: tc.clientTimeout,
			}
			tc.txns[string(header.Txn.ID)] = txnMeta
			tc.heartbeat(header.Txn)
		}
		txnMeta.lastUpdateTS = tc.clock.Now()
		txnMeta.addKeyRange(header.Key, header.EndKey)
		tc.Unlock()
	}

	// Cleanup intents and transaction map if end of transaction.
	switch t := call.Reply.Header().GoError().(type) {
	case *proto.TransactionAbortedError:
		// If already aborted, cleanup the txn on this TxnCoordSender.
		tc.cleanupTxn(&t.Txn, nil)
	case *proto.OpRequiresTxnError:
		// Run a one-off transaction with that single command.
		log.Infof("%s: auto-wrapping in txn and re-executing", call.Method())
		txnOpts := &client.TransactionOptions{
			Name: "auto-wrap",
		}
		// Must not call Close() on this KV - that would call
		// tc.Close().
		tmpKV := client.NewKV(nil, tc)
		tmpKV.User = call.Args.Header().User
		tmpKV.UserPriority = call.Args.Header().GetUserPriority()
		call.Reply.Reset()
		tmpKV.RunTransaction(txnOpts, func(txn *client.Txn) error {
			return txn.Run(call)
		})
	case nil:
		var txn *proto.Transaction
		var resolved []proto.Key
		if call.Method() == proto.EndTransaction {
			txn = call.Reply.Header().Txn
			// If the -linearizable flag is set, we want to make sure that
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
					log.V(1).Infof("%v: waiting %dms on EndTransaction for linearizability", txn.ID, sleepNS/1000000)
					time.Sleep(sleepNS)
				}()
			}
			resolved = call.Reply.(*proto.EndTransactionResponse).Resolved
		}
		if txn != nil && txn.Status != proto.PENDING {
			tc.cleanupTxn(txn, resolved)
		}
	}
}

// sendBatch unrolls a batched command and sends each constituent
// command in parallel.
func (tc *TxnCoordSender) sendBatch(batchArgs *proto.BatchRequest, batchReply *proto.BatchResponse) {
	// Prepare the calls by unrolling the batch. If the batchReply is
	// pre-initialized with replies, use those; otherwise create replies
	// as needed.
	// TODO(spencer): send calls in parallel.
	batchReply.Txn = batchArgs.Txn
	for i := range batchArgs.Requests {
		// Initialize args header values where appropriate.
		args := batchArgs.Requests[i].GetValue().(proto.Request)
		call := &client.Call{Args: args}
		if args.Header().User == "" {
			args.Header().User = batchArgs.User
		}
		if args.Header().UserPriority == nil {
			args.Header().UserPriority = batchArgs.UserPriority
		}
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
// the txns map.
func (tc *TxnCoordSender) cleanupTxn(txn *proto.Transaction, resolved []proto.Key) {
	tc.Lock()
	defer tc.Unlock()
	txnMeta, ok := tc.txns[string(txn.ID)]
	if !ok {
		return
	}
	txnMeta.close(txn, resolved, tc.wrapped, tc.stopper)
	delete(tc.txns, string(txn.ID))
}

// hasClientAbandonedCoord returns true if the transaction specified by
// txnID has not been updated by the client adding a request within
// the allowed timeout. If abandoned, the transaction is removed from
// the txns map.
func (tc *TxnCoordSender) hasClientAbandonedCoord(txnID []byte) bool {
	tc.Lock()
	defer tc.Unlock()
	txnMeta, ok := tc.txns[string(txnID)]
	if !ok {
		return true
	}
	timeout := tc.clock.Now()
	timeout.WallTime -= txnMeta.timeoutDuration.Nanoseconds()
	if txnMeta.lastUpdateTS.Less(timeout) {
		delete(tc.txns, string(txnID))
		return true
	}
	return false
}

// heartbeat periodically sends an InternalHeartbeatTxn RPC to an
// extant transaction, stopping in the event the transaction is
// aborted or committed or if the TxnCoordSender is closed.
func (tc *TxnCoordSender) heartbeat(txn *proto.Transaction) {
	tc.stopper.RunWorker(func() {
		ticker := time.NewTicker(tc.heartbeatInterval)
		request := &proto.InternalHeartbeatTxnRequest{
			RequestHeader: proto.RequestHeader{
				Key:  txn.Key,
				User: storage.UserRoot,
				Txn:  txn,
			},
		}

		// Loop with ticker for periodic heartbeats.
		for {
			select {
			case <-ticker.C:
				if !tc.stopper.StartTask() {
					continue
				}
				// Before we send a heartbeat, determine whether this transaction
				// should be considered abandoned. If so, exit heartbeat.
				if tc.hasClientAbandonedCoord(txn.ID) {
					log.V(1).Infof("transaction %q:%q abandoned; stopping heartbeat", txn.Key, txn.ID)
					tc.stopper.FinishTask()
					return
				}
				request.Header().Timestamp = tc.clock.Now()
				reply := &proto.InternalHeartbeatTxnResponse{}
				call := &client.Call{
					Args:  request,
					Reply: reply,
				}
				tc.wrapped.Send(call)
				// If the transaction is not in pending state, then we can stop
				// the heartbeat. It's either aborted or committed, and we resolve
				// write intents accordingly.
				if reply.GoError() != nil {
					log.Warningf("heartbeat to %q:%q failed: %s", txn.Key, txn.ID, reply.GoError())
				} else if reply.Txn != nil && reply.Txn.Status != proto.PENDING {
					tc.cleanupTxn(reply.Txn, nil)
					tc.stopper.FinishTask()
					return
				}
				tc.stopper.FinishTask()

			case <-tc.stopper.ShouldStop():
				return
			}
		}
	})
}
