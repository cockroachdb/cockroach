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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Jiajia Han (hanjia18@gmail.com)

package kv

import (
	"time"

	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
)

const (
	heartbeatInterval = 5 * time.Second
)

// This list filters the KV API into those operations which can be part of
// transaction.
var transactionalActions = map[string]struct{}{
	"Node.AccumulateTS":   struct{}{},
	"Node.ConditionalPut": struct{}{},
	"Node.Contains":       struct{}{},
	"Node.Delete":         struct{}{},
	"Node.DeleteRange":    struct{}{},
	"Node.EnqueueMessage": struct{}{},
	"Node.EnqueueUpdate":  struct{}{},
	"Node.Get":            struct{}{},
	"Node.Increment":      struct{}{},
	"Node.Put":            struct{}{},
	"Node.ReapQueue":      struct{}{},
	"Node.Scan":           struct{}{},
}

func isTransactional(method string) bool {
	_, ok := transactionalActions[method]
	return ok
}

// A coordinator coordinates the transaction states for the clients.
// After a transaction is started, it will send heartbeat messages to the transaction
// table so that other transactions will know whether this transaction is live.
// It will keep track of the written keys belonging to this transaction.
// When the transaction is committed or aborted, it will also clear the write
// intents for the client.
type coordinator struct {
	// TransactionMap is a map from transaction ids to txnMetadata.
	TransactionMap map[string]txnMetadata
	db             *DistDB
	clock          *hlc.Clock
}

type txnMetadata struct {
	// keys and endKeys sotre the key ranges affected by this transaction
	// through this coordinator.  By keeping this record, the coordinator
	// will be able to update the write intent when the transaction is
	// committed.
	// TODO(jiajia): Reevaluate the slice datastructure if there are too
	// many duplicate keys in a transaction.  To make this as a map, we need
	// to implement a hash to the key type.
	keys    []engine.Key
	endKeys []engine.Key

	// lastUpdateTS is the latest time when the client sent transaction operations to this
	// coordinator.
	lastUpdateTS hlc.Timestamp

	// timeoutDuration is the time after which the transaction should be considered
	// abandoned by the client. That is, when
	// current_timestamp > lastUpdateTS + timeoutDuration
	// If this value is set to 0, a default timeout will be used.
	timeoutDuration time.Duration

	// This is the closer to close the heartbeat goroutine.
	closer chan struct{}
}

// NewCoordinator TODO(jiajia) Add a comment!
func NewCoordinator(db *DistDB, clock *hlc.Clock) *coordinator {
	tc := &coordinator{
		db:             db,
		TransactionMap: make(map[string]txnMetadata),
		clock:          clock,
	}
	return tc
}

func (tc *coordinator) addRequest(header *storage.RequestHeader) {
	// Ignore non-transactional requests.
	if len(header.TxID) == 0 {
		return
	}
	if _, ok := tc.TransactionMap[header.TxID]; !ok {
		tc.TransactionMap[header.TxID] = tc.newTxnMetadata()
		// TODO(jiajia): Reevaluate this logic of creating a goroutine
		// for each active transaction. Spencer suggests a heap
		// containing next heartbeat timeouts which is processed by a
		// single goroutine.
		go tc.heartbeat(engine.MakeKey(engine.KeyTransactionPrefix, engine.Key(header.TxID)), tc.TransactionMap[header.TxID].closer)
	}
	txnMeta := tc.TransactionMap[header.TxID]
	txnMeta.lastUpdateTS = tc.clock.Now()
}

func (tc *coordinator) newTxnMetadata() txnMetadata {
	return txnMetadata{
		keys:            make([]engine.Key, 0, 1),
		endKeys:         make([]engine.Key, 0, 1),
		lastUpdateTS:    tc.clock.Now(),
		timeoutDuration: defaultClientTimeout,
		closer:          make(chan struct{}),
	}
}

// TODO(jiajia): how to test this?
func (tc *coordinator) heartbeat(key engine.Key, closer chan struct{}) {
	ticker := time.NewTicker(heartbeatInterval)
	for {
		select {
		case <-ticker.C:
			replyChan := make(chan *storage.HeartbeatTransactionResponse, 1)
			tc.db.routeRPCInternal("Node.HeartbeatTransaction", &storage.HeartbeatTransactionRequest{
				RequestHeader: storage.RequestHeader{
					Key:  key,
					User: storage.UserRoot,
				},
			}, replyChan)
			response := <-replyChan
			// If the transaction is not in pending state, then we
			// can stop the heartbeat. It's either aborted or
			// commited.
			if response.Status != storage.PENDING {
				return
			}
		case <-closer:
			return
		}
	}
}
