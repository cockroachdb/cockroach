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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// KV is an interface for routing key-value DB commands. This
// package includes both local and distributed KV implementations.
type KV interface {
	// ExecuteCmd executes the specified command (method + args) and
	// send a reply on the provided channel.
	ExecuteCmd(method string, args proto.Request, replyChan interface{})
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
func (db *DB) executeCmd(method string, args proto.Request, replyChan interface{}) {
	db.coordinator.AddRequest(method, args.Header())
	db.kv.ExecuteCmd(method, args, replyChan)
}

// Contains determines whether the KV map contains the specified key.
func (db *DB) Contains(args *proto.ContainsRequest) <-chan *proto.ContainsResponse {
	replyChan := make(chan *proto.ContainsResponse, 1)
	go db.executeCmd(storage.Contains, args, replyChan)
	return replyChan
}

// Get fetches the value for a key from the KV map, respecting a
// possibly historical timestamp. If the timestamp is 0, returns
// the most recent value.
func (db *DB) Get(args *proto.GetRequest) <-chan *proto.GetResponse {
	replyChan := make(chan *proto.GetResponse, 1)
	go db.executeCmd(storage.Get, args, replyChan)
	return replyChan
}

// Put sets the value for a key at the specified timestamp. If the
// timestamp is 0, the value is set with the current time as timestamp.
func (db *DB) Put(args *proto.PutRequest) <-chan *proto.PutResponse {
	// Init the value checksum if not set by requesting client.
	args.Value.InitChecksum(args.Key)
	replyChan := make(chan *proto.PutResponse, 1)
	go db.executeCmd(storage.Put, args, replyChan)
	return replyChan
}

// ConditionalPut sets the value for a key if the existing value
// matches the value specified in the request. Specifying a null value
// for existing means the value must not yet exist.
func (db *DB) ConditionalPut(args *proto.ConditionalPutRequest) <-chan *proto.ConditionalPutResponse {
	// Init the value checksum if not set by requesting client.
	args.Value.InitChecksum(args.Key)
	replyChan := make(chan *proto.ConditionalPutResponse, 1)
	go db.executeCmd(storage.ConditionalPut, args, replyChan)
	return replyChan
}

// Increment increments the value at the specified key. Once called
// for a key, Put & Get will return errors; only Increment will
// continue to be a valid command. The value must be deleted before
// it can be reset using Put.
func (db *DB) Increment(args *proto.IncrementRequest) <-chan *proto.IncrementResponse {
	replyChan := make(chan *proto.IncrementResponse, 1)
	go db.executeCmd(storage.Increment, args, replyChan)
	return replyChan
}

// Delete removes the value for the specified key.
func (db *DB) Delete(args *proto.DeleteRequest) <-chan *proto.DeleteResponse {
	replyChan := make(chan *proto.DeleteResponse, 1)
	go db.executeCmd(storage.Delete, args, replyChan)
	return replyChan
}

// DeleteRange removes all values for keys which fall between
// args.Header.Key and args.Header.EndKey.
func (db *DB) DeleteRange(args *proto.DeleteRangeRequest) <-chan *proto.DeleteRangeResponse {
	replyChan := make(chan *proto.DeleteRangeResponse, 1)
	go db.executeCmd(storage.DeleteRange, args, replyChan)
	return replyChan
}

// Scan fetches the values for all keys which fall between
// args.Header.Key and args.Header.EndKey.
func (db *DB) Scan(args *proto.ScanRequest) <-chan *proto.ScanResponse {
	replyChan := make(chan *proto.ScanResponse, 1)
	go db.executeCmd(storage.Scan, args, replyChan)
	return replyChan
}

// EndTransaction either commits or aborts an ongoing transaction by
// executing an EndTransaction command on the KV. The reply is
// intercepted here in order to inform the transaction coordinator of
// the final state of the transaction.
func (db *DB) EndTransaction(args *proto.EndTransactionRequest) <-chan *proto.EndTransactionResponse {
	interceptChan := make(chan *proto.EndTransactionResponse, 1)
	replyChan := make(chan *proto.EndTransactionResponse, 1)
	go func() {
		db.executeCmd(storage.EndTransaction, args, interceptChan)
		// Intercept the reply and end transaction on coordinator
		// depending on final state.
		reply := <-interceptChan
		if reply.Error == nil {
			switch reply.Status {
			case proto.COMMITTED:
				db.coordinator.EndTxn(args.Header().TxnID, true)
				return
			case proto.ABORTED:
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
func (db *DB) AccumulateTS(args *proto.AccumulateTSRequest) <-chan *proto.AccumulateTSResponse {
	replyChan := make(chan *proto.AccumulateTSResponse, 1)
	go db.executeCmd(storage.AccumulateTS, args, replyChan)
	return replyChan
}

// ReapQueue scans and deletes messages from a recipient message
// queue. ReapQueueRequest invocations must be part of an extant
// transaction or they fail. Returns the reaped queue messsages, up to
// the requested maximum. If fewer than the maximum were returned,
// then the queue is empty.
func (db *DB) ReapQueue(args *proto.ReapQueueRequest) <-chan *proto.ReapQueueResponse {
	replyChan := make(chan *proto.ReapQueueResponse, 1)
	go db.executeCmd(storage.ReapQueue, args, replyChan)
	return replyChan
}

// EnqueueUpdate enqueues an update for eventual execution.
func (db *DB) EnqueueUpdate(args *proto.EnqueueUpdateRequest) <-chan *proto.EnqueueUpdateResponse {
	replyChan := make(chan *proto.EnqueueUpdateResponse, 1)
	go db.executeCmd(storage.EnqueueUpdate, args, replyChan)
	return replyChan
}

// EnqueueMessage enqueues a message for delivery to an inbox.
func (db *DB) EnqueueMessage(args *proto.EnqueueMessageRequest) <-chan *proto.EnqueueMessageResponse {
	replyChan := make(chan *proto.EnqueueMessageResponse, 1)
	go db.executeCmd(storage.EnqueueMessage, args, replyChan)
	return replyChan
}

// InternalHeartbeatTxn sends a periodic heartbeat to extant
// transaction rows to indicate the client is still alive and
// the transaction should not be considered abandoned.
func (db *DB) InternalHeartbeatTxn(args *proto.InternalHeartbeatTxnRequest) <-chan *proto.InternalHeartbeatTxnResponse {
	replyChan := make(chan *proto.InternalHeartbeatTxnResponse, 1)
	go db.executeCmd(storage.InternalHeartbeatTxn, args, replyChan)
	return replyChan
}

// InternalResolveIntent resolves existing write intents for a key or
// key range.
func (db *DB) InternalResolveIntent(args *proto.InternalResolveIntentRequest) <-chan *proto.InternalResolveIntentResponse {
	replyChan := make(chan *proto.InternalResolveIntentResponse, 1)
	go db.executeCmd(storage.InternalResolveIntent, args, replyChan)
	return replyChan
}

// InternalSnapshotCopy scans the key range specified by start key through
// end key up to some maximum number of results from the given snapshot_id.
// It will create a snapshot if snapshot_id is empty.
func (db *DB) InternalSnapshotCopy(args *proto.InternalSnapshotCopyRequest) <-chan *proto.InternalSnapshotCopyResponse {
	replyChan := make(chan *proto.InternalSnapshotCopyResponse, 1)
	go db.executeCmd(storage.InternalSnapshotCopy, args, replyChan)
	return replyChan
}
