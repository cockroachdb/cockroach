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

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

// This list filters the KV API into those operations which can be part of
// transaction.
var transactionalActions = map[string]struct{}{
	storage.AccumulateTS:   struct{}{},
	storage.ConditionalPut: struct{}{},
	storage.Contains:       struct{}{},
	storage.Delete:         struct{}{},
	storage.DeleteRange:    struct{}{},
	storage.EnqueueMessage: struct{}{},
	storage.EnqueueUpdate:  struct{}{},
	storage.Get:            struct{}{},
	storage.Increment:      struct{}{},
	storage.Put:            struct{}{},
	storage.ReapQueue:      struct{}{},
	storage.Scan:           struct{}{},
}

func isTransactional(method string) bool {
	_, ok := transactionalActions[method]
	return ok
}

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
	keys []engine.KeyRange

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

// A coordinator coordinates the transaction states for the clients.
// After a transaction is started, it will send heartbeat messages to
// the transaction table so that other transactions will know whether
// this transaction is live. It will keep track of the written keys
// belonging to this transaction. When the transaction is committed or
// aborted, it will also clear the write intents for the client.
type coordinator struct {
	db                *DB
	clock             *hlc.Clock
	heartbeatInterval time.Duration
	clientTimeout     time.Duration
	sync.Mutex                                // Protects the txns map.
	txns              map[string]*txnMetadata // txn key to metadata
}

// newCoordinator creates a new coordinator for use from a KV
// distributed DB instance. Coordinators should be closed when no
// longer in use via Close().
func newCoordinator(db *DB, clock *hlc.Clock) *coordinator {
	tc := &coordinator{
		db:                db,
		clock:             clock,
		heartbeatInterval: storage.DefaultHeartbeatInterval,
		clientTimeout:     defaultClientTimeout,
		txns:              map[string]*txnMetadata{},
	}
	return tc
}

// Close stops ongoing heartbeats. Close does not attempt to resolve
// existing write intents for transactions which this coordinator has
// been managing.
func (tc *coordinator) Close() {
	tc.Lock()
	defer tc.Unlock()
	for _, txn := range tc.txns {
		close(txn.closer)
	}
	tc.txns = map[string]*txnMetadata{}
}

// AddRequest is called on every client request to update the
// lastUpdateTS to prevent live transactions from being considered
// abandoned and garbage collected. Read/write mutating requests have
// their key(s) added to the transaction's keys slice for eventual
// cleanup via resolved write intents.
func (tc *coordinator) AddRequest(method string, header *proto.RequestHeader) {
	// Ignore non-transactional requests.
	if header.Txn == nil || !isTransactional(method) {
		return
	}

	tc.Lock()
	defer tc.Unlock()
	txnMeta, ok := tc.txns[string(header.Txn.ID)]
	if !ok {
		txnMeta = &txnMetadata{
			txn:             *header.Txn,
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

	// If read-only, exit now; otherwise, store the affected key range.
	if storage.IsReadOnly(method) {
		return
	}
	// Otherwise, append a new key range to the set of affected keys.
	txnMeta.keys = append(txnMeta.keys, engine.KeyRange{
		Start: header.Key,
		End:   header.EndKey,
	})
}

// EndTxn is called to resolve write intents which were set down over
// the course of the transaction. The txnMetadata object is removed from
// the txns map.
func (tc *coordinator) EndTxn(txn *proto.Transaction) {
	tc.Lock()
	defer tc.Unlock()
	txnMeta, ok := tc.txns[string(txn.ID)]
	if !ok {
		return
	}
	for _, rng := range txnMeta.keys {
		// We don't care about the reply channel; these are best
		// effort. We simply fire and forget.
		tc.db.InternalResolveIntent(&proto.InternalResolveIntentRequest{
			RequestHeader: proto.RequestHeader{
				Key:    rng.Start,
				EndKey: rng.End,
				User:   storage.UserRoot,
				Txn:    txn,
			},
		})
	}
	delete(tc.txns, string(txn.ID))
	close(txnMeta.closer)
}

// hasClientAbandonedCoord returns true if the transaction specified by
// txnID has not been updated by the client adding a request within
// the allowed timeout. If abandoned, the transaction is removed from
// the txns map.
func (tc *coordinator) hasClientAbandonedCoord(txnID engine.Key) bool {
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
// aborted or committed or if the coordinator is closed.
func (tc *coordinator) heartbeat(txn *proto.Transaction, closer chan struct{}) {
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
			reply := <-tc.db.InternalHeartbeatTxn(request)
			// If the transaction is not in pending state, then we can stop
			// the heartbeat. It's either aborted or committed, and we resolve
			// write intents accordingly.
			if reply.Error != nil {
				log.Warningf("heartbeat to %q failed: %s", txn.ID, reply.GoError())
			} else if reply.Txn.Status != proto.PENDING {
				tc.EndTxn(reply.Txn)
				return
			}
		case <-closer:
			return
		}
	}
}
