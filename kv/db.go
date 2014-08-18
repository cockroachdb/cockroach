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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// KV is an interface for routing key-value DB commands. This
// package includes both local and distributed KV implementations.
type KV interface {
	// ExecuteCmd executes the specified command (method + args) and
	// send a reply on the provided channel.
	ExecuteCmd(method string, args storage.Request, replyChan interface{})
	// Close provides implementation-specific cleanup.
	Close()
}

// A DB is the kv package implementation of the storage.DB
// interface. Each of its methods creates a channel for the response
// then invokes executeCmd on the underlying KV interface. KV
// implementations include local (DB) and distributed (DistKV). A
// kv.DB incorporates transaction management.
type DB struct {
	kv          KV           // Local or distributed KV implementation
	coordinator *coordinator // Coordinates transaction states for clients
}

// NewDB returns a key-value implementation of storage.DB which
// connects to the Cockroach cluster via the supplied kv.KV instance.
func NewDB(kv KV, clock *hlc.Clock) *DB {
	db := &DB{kv: kv}
	db.coordinator = newCoordinator(db, clock)
	return db
}

// Close calls coordinator.Close(), stopping any ongoing transaction
// heartbeats.
func (db *DB) Close() {
	db.coordinator.Close()
	db.kv.Close()
}

// executeCmd adds the request to the transaction coordinator and
// passes the command to the underlying KV implementation.
func (db *DB) executeCmd(method string, args storage.Request, replyChan interface{}) {
	db.coordinator.AddRequest(method, args.Header())
	db.kv.ExecuteCmd(method, args, replyChan)
}

// Contains .
func (db *DB) Contains(args *storage.ContainsRequest) <-chan *storage.ContainsResponse {
	replyChan := make(chan *storage.ContainsResponse, 1)
	go db.executeCmd(storage.Contains, args, replyChan)
	return replyChan
}

// Get .
func (db *DB) Get(args *storage.GetRequest) <-chan *storage.GetResponse {
	replyChan := make(chan *storage.GetResponse, 1)
	go db.executeCmd(storage.Get, args, replyChan)
	return replyChan
}

// Put .
func (db *DB) Put(args *storage.PutRequest) <-chan *storage.PutResponse {
	replyChan := make(chan *storage.PutResponse, 1)
	go db.executeCmd(storage.Put, args, replyChan)
	return replyChan
}

// ConditionalPut .
func (db *DB) ConditionalPut(args *storage.ConditionalPutRequest) <-chan *storage.ConditionalPutResponse {
	replyChan := make(chan *storage.ConditionalPutResponse, 1)
	go db.executeCmd(storage.ConditionalPut, args, replyChan)
	return replyChan
}

// Increment .
func (db *DB) Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse {
	replyChan := make(chan *storage.IncrementResponse, 1)
	go db.executeCmd(storage.Increment, args, replyChan)
	return replyChan
}

// Delete .
func (db *DB) Delete(args *storage.DeleteRequest) <-chan *storage.DeleteResponse {
	replyChan := make(chan *storage.DeleteResponse, 1)
	go db.executeCmd(storage.Delete, args, replyChan)
	return replyChan
}

// DeleteRange .
func (db *DB) DeleteRange(args *storage.DeleteRangeRequest) <-chan *storage.DeleteRangeResponse {
	replyChan := make(chan *storage.DeleteRangeResponse, 1)
	go db.executeCmd(storage.DeleteRange, args, replyChan)
	return replyChan
}

// Scan .
func (db *DB) Scan(args *storage.ScanRequest) <-chan *storage.ScanResponse {
	replyChan := make(chan *storage.ScanResponse, 1)
	go db.executeCmd(storage.Scan, args, replyChan)
	return replyChan
}

// EndTransaction either commits or aborts an ongoing transaction.
// The reply is intercepted here in order to inform the transaction
// coordinator of the final state of the transaction.
func (db *DB) EndTransaction(args *storage.EndTransactionRequest) <-chan *storage.EndTransactionResponse {
	interceptChan := make(chan *storage.EndTransactionResponse, 1)
	replyChan := make(chan *storage.EndTransactionResponse, 1)
	go func() {
		db.executeCmd(storage.EndTransaction, args, interceptChan)
		// Intercept the reply and end transaction on coordinator
		// depending on final state.
		reply := <-interceptChan
		if reply.Error == nil {
			switch reply.Status {
			case storage.COMMITTED:
				db.coordinator.EndTxn(args.Header().TxnID, true)
				return
			case storage.ABORTED:
				db.coordinator.EndTxn(args.Header().TxnID, false)
				return
			}
		}
		// Go ahead and return the result to the client.
		replyChan <- reply
	}()
	return replyChan
}

// AccumulateTS is used to efficiently accumulate a time series of
// int64 quantities representing discrete subtimes. For example, a
// key/value might represent a minute of data. Each would contain 60
// int64 counts, each representing a second.
func (db *DB) AccumulateTS(args *storage.AccumulateTSRequest) <-chan *storage.AccumulateTSResponse {
	replyChan := make(chan *storage.AccumulateTSResponse, 1)
	go db.executeCmd(storage.AccumulateTS, args, replyChan)
	return replyChan
}

// ReapQueue scans and deletes messages from a recipient message
// queue. ReapQueueRequest invocations must be part of an extant
// transaction or they fail. Returns the reaped queue messsages, up to
// the requested maximum. If fewer than the maximum were returned,
// then the queue is empty.
func (db *DB) ReapQueue(args *storage.ReapQueueRequest) <-chan *storage.ReapQueueResponse {
	replyChan := make(chan *storage.ReapQueueResponse, 1)
	go db.executeCmd(storage.ReapQueue, args, replyChan)
	return replyChan
}

// EnqueueUpdate enqueues an update for eventual execution.
func (db *DB) EnqueueUpdate(args *storage.EnqueueUpdateRequest) <-chan *storage.EnqueueUpdateResponse {
	replyChan := make(chan *storage.EnqueueUpdateResponse, 1)
	go db.executeCmd(storage.EnqueueUpdate, args, replyChan)
	return replyChan
}

// EnqueueMessage enqueues a message for delivery to an inbox.
func (db *DB) EnqueueMessage(args *storage.EnqueueMessageRequest) <-chan *storage.EnqueueMessageResponse {
	replyChan := make(chan *storage.EnqueueMessageResponse, 1)
	go db.executeCmd(storage.EnqueueMessage, args, replyChan)
	return replyChan
}

// InternalHeartbeatTxn sends a periodic heartbeat to extant
// transaction rows to indicate the client is still alive and
// the transaction should not be considered abandoned.
func (db *DB) InternalHeartbeatTxn(args *storage.InternalHeartbeatTxnRequest) <-chan *storage.InternalHeartbeatTxnResponse {
	replyChan := make(chan *storage.InternalHeartbeatTxnResponse, 1)
	go db.executeCmd(storage.InternalHeartbeatTxn, args, replyChan)
	return replyChan
}

// InternalResolveIntent resolves existing write intents for a key or
// key range.
func (db *DB) InternalResolveIntent(args *storage.InternalResolveIntentRequest) <-chan *storage.InternalResolveIntentResponse {
	replyChan := make(chan *storage.InternalResolveIntentResponse, 1)
	go db.executeCmd(storage.InternalResolveIntent, args, replyChan)
	return replyChan
}
