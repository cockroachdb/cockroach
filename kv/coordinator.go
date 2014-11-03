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

	// This is the closer to close the heartbeat goroutine.
	closer chan struct{}
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
// metadata heartbeat.
func (tm *txnMetadata) close(txn *proto.Transaction, sender client.KVSender) {
	if tm.keys.Len() > 0 {
		log.V(1).Infof("cleaning up intents for transaction %s", txn)
	}
	for _, o := range tm.keys.GetOverlaps(engine.KeyMin, engine.KeyMax) {
		call := &client.Call{
			Method: proto.InternalResolveIntent,
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
		}
		// We don't care about the reply channel; these are best
		// effort. We simply fire and forget, each in its own goroutine.
		go func() {
			log.V(1).Infof("cleaning up intent %q for txn %s", call.Args.Header().Key, txn)
			sender.Send(call)
			if call.Reply.Header().Error != nil {
				log.Warningf("failed to cleanup %q intent: %s", call.Args.Header().Key, call.Reply.Header().GoError())
			}
		}()
	}
	tm.keys.Clear()
	close(tm.closer)
}

// A Coordinator is an implementation of client.KVSender which wraps a
// lower-level KVSender (either a LocalSender or a DistSender) to
// which it sends commands. It acts as a man-in-the-middle,
// coordinating transaction state for clients.  After a transaction is
// started, the Coordinator starts asynchronously sending heartbeat
// messages to that transaction's txn record, to keep it live. It also
// keeps track of each written key or key range over the course of the
// transaction. When the transaction is committed or aborted, it clears
// accumulated write intents for the transaction.
type Coordinator struct {
	wrapped           client.KVSender
	clock             *hlc.Clock
	heartbeatInterval time.Duration
	clientTimeout     time.Duration
	sync.Mutex                                // Protects the txns map.
	txns              map[string]*txnMetadata // txn key to metadata
}

// NewCoordinator creates a new Coordinator for use from a KV
// distributed DB instance. Coordinators should be closed when no
// longer in use via Close().
func NewCoordinator(wrapped client.KVSender, clock *hlc.Clock) *Coordinator {
	tc := &Coordinator{
		wrapped:           wrapped,
		clock:             clock,
		heartbeatInterval: storage.DefaultHeartbeatInterval,
		clientTimeout:     defaultClientTimeout,
		txns:              map[string]*txnMetadata{},
	}
	return tc
}

// Send implements the client.KVSender interface. If the call is part
// of a transaction, the Coordinator adds the transaction to a map of
// active transactions and begins heartbeating it. Every subsequent
// call for the same transaction updates the lastUpdateTS to prevent
// live transactions from being considered abandoned and garbage
// collected. Read/write mutating requests have their key or key range
// added to the transaction's interval tree of key ranges for eventual
// cleanup via resolved write intents.
func (tc *Coordinator) Send(call *client.Call) {
	// Handle BeginTransaction call separately.
	if call.Method == proto.BeginTransaction {
		tc.beginTxn(call.Args.(*proto.BeginTransactionRequest),
			call.Reply.(*proto.BeginTransactionResponse))
		return
	}

	header := call.Args.Header()
	// Coordinate transactional requests.
	var txnMeta *txnMetadata
	if header.Txn != nil && proto.IsTransactional(call.Method) {
		tc.Lock()
		defer tc.Unlock()
		var ok bool
		if txnMeta, ok = tc.txns[string(header.Txn.ID)]; !ok {
			txnMeta = &txnMetadata{
				txn:             *header.Txn,
				keys:            util.NewIntervalCache(util.CacheConfig{Policy: util.CacheNone}),
				lastUpdateTS:    tc.clock.Now(),
				timeoutDuration: tc.clientTimeout,
				closer:          make(chan struct{}),
			}
			tc.txns[string(header.Txn.ID)] = txnMeta

			// TODO(jiajia): Reevaluate this logic of creating a goroutine
			// for each active transaction. Spencer suggests a heap
			// containing next heartbeat timeouts which is processed by a
			// single goroutine.
			go tc.heartbeat(header.Txn, txnMeta.closer)
		}
		txnMeta.lastUpdateTS = tc.clock.Now()
	}

	// Send the call on to the wrapped sender.
	tc.wrapped.Send(call)

	// If in a transaction and this is a read-write command, add the
	// key or key range to the intents map on success.
	if header.Txn != nil && proto.IsTransactional(call.Method) &&
		proto.IsReadWrite(call.Method) && call.Reply.Header().GoError() == nil {
		// On success, append a new key range to the set of affected keys.
		txnMeta.addKeyRange(header.Key, header.EndKey)
	}

	// Cleanup intents and transaction map if end of transaction.
	switch t := call.Reply.Header().GoError().(type) {
	case *proto.TransactionAbortedError:
		// If already aborted, cleanup the txn on this Coordinator.
		tc.cleanupTxn(&t.Txn)
	case nil:
		var txn *proto.Transaction
		if call.Method == proto.EndTransaction {
			txn = call.Reply.(*proto.EndTransactionResponse).Txn
		}
		if txn != nil && txn.Status != proto.PENDING {
			tc.cleanupTxn(txn)
		}
	}
}

// Close implements the client.KVSender interface by stopping ongoing
// heartbeats for extant transactions. Close does not attempt to
// resolve existing write intents for transactions which this
// Coordinator has been managing.
func (tc *Coordinator) Close() {
	tc.Lock()
	defer tc.Unlock()
	for _, txn := range tc.txns {
		close(txn.closer)
	}
	tc.txns = map[string]*txnMetadata{}
}

// beginTxn initializes a new transaction instance using the supplied
// args and the node's clock. This method doesn't need to call through
// to any particular range as it only accesses the node's clock. It
// doesn't read or write any data.
func (tc *Coordinator) beginTxn(args *proto.BeginTransactionRequest, reply *proto.BeginTransactionResponse) {
	txn := proto.NewTransaction(args.Name, engine.KeyAddress(args.Key), args.GetUserPriority(), args.Isolation,
		tc.clock.Now(), tc.clock.MaxOffset().Nanoseconds())
	reply.Timestamp = txn.Timestamp
	reply.Txn = txn
}

// cleanupTxn is called to resolve write intents which were set down over
// the course of the transaction. The txnMetadata object is removed from
// the txns map.
func (tc *Coordinator) cleanupTxn(txn *proto.Transaction) {
	tc.Lock()
	defer tc.Unlock()
	txnMeta, ok := tc.txns[string(txn.ID)]
	if !ok {
		return
	}
	txnMeta.close(txn, tc.wrapped)
	delete(tc.txns, string(txn.ID))
}

// hasClientAbandonedCoord returns true if the transaction specified by
// txnID has not been updated by the client adding a request within
// the allowed timeout. If abandoned, the transaction is removed from
// the txns map.
func (tc *Coordinator) hasClientAbandonedCoord(txnID proto.Key) bool {
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
// aborted or committed or if the Coordinator is closed.
func (tc *Coordinator) heartbeat(txn *proto.Transaction, closer chan struct{}) {
	ticker := time.NewTicker(tc.heartbeatInterval)
	request := &proto.InternalHeartbeatTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key:  txn.ID,
			User: storage.UserRoot,
			Txn:  txn,
		},
	}

	// Loop with ticker for periodic heartbeats.
	for {
		select {
		case <-ticker.C:
			// Before we send a heartbeat, determine whether this transaction
			// should be considered abandoned. If so, exit heartbeat.
			if tc.hasClientAbandonedCoord(txn.ID) {
				log.V(1).Infof("transaction %q abandoned; stopping heartbeat", txn.ID)
				return
			}
			request.Header().Timestamp = tc.clock.Now()
			reply := &proto.InternalHeartbeatTxnResponse{}
			call := &client.Call{
				Method: proto.InternalHeartbeatTxn,
				Args:   request,
				Reply:  reply,
			}
			tc.wrapped.Send(call)
			// If the transaction is not in pending state, then we can stop
			// the heartbeat. It's either aborted or committed, and we resolve
			// write intents accordingly.
			if reply.GoError() != nil {
				log.Warningf("heartbeat to %q failed: %s", txn.ID, reply.GoError())
			} else if reply.Txn.Status != proto.PENDING {
				tc.cleanupTxn(reply.Txn)
				return
			}
		case <-closer:
			return
		}
	}
}
