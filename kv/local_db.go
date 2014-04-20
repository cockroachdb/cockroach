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
	"reflect"

	"github.com/cockroachdb/cockroach/storage"
)

// A LocalDB provides methods to access only a local, in-memory key
// value store. It utilizes a single storage/Range object, backed by
// a storage/InMem engine.
type LocalDB struct {
	rng *storage.Range
}

// NewLocalDB returns a local-only KV DB for direct access to a store.
func NewLocalDB(rng *storage.Range) *LocalDB {
	return &LocalDB{rng: rng}
}

// invokeMethod sends the specified RPC asynchronously and returns a
// channel which receives the reply struct when the call is
// complete. Returns a channel of the same type as "reply".
func (db *LocalDB) invokeMethod(method string, args, reply interface{}) interface{} {
	chanVal := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(reply)), 1)
	replyVal := reflect.ValueOf(reply)
	reflect.ValueOf(db.rng).MethodByName(method).Call([]reflect.Value{
		reflect.ValueOf(args),
		replyVal,
	})
	chanVal.Send(replyVal)

	return chanVal.Interface()
}

// Contains passes through to local range.
func (db *LocalDB) Contains(args *storage.ContainsRequest) <-chan *storage.ContainsResponse {
	return db.invokeMethod("Contains",
		args, &storage.ContainsResponse{}).(chan *storage.ContainsResponse)
}

// Get passes through to local range.
func (db *LocalDB) Get(args *storage.GetRequest) <-chan *storage.GetResponse {
	return db.invokeMethod("Get",
		args, &storage.GetResponse{}).(chan *storage.GetResponse)
}

// Put passes through to local range.
func (db *LocalDB) Put(args *storage.PutRequest) <-chan *storage.PutResponse {
	return db.invokeMethod("Put",
		args, &storage.PutResponse{}).(chan *storage.PutResponse)
}

// Increment passes through to local range.
func (db *LocalDB) Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse {
	return db.invokeMethod("Increment",
		args, &storage.IncrementResponse{}).(chan *storage.IncrementResponse)
}

// Delete passes through to local range.
func (db *LocalDB) Delete(args *storage.DeleteRequest) <-chan *storage.DeleteResponse {
	return db.invokeMethod("Delete",
		args, &storage.DeleteResponse{}).(chan *storage.DeleteResponse)
}

// DeleteRange passes through to local range.
func (db *LocalDB) DeleteRange(args *storage.DeleteRangeRequest) <-chan *storage.DeleteRangeResponse {
	return db.invokeMethod("DeleteRange",
		args, &storage.DeleteRangeResponse{}).(chan *storage.DeleteRangeResponse)
}

// Scan passes through to local range.
func (db *LocalDB) Scan(args *storage.ScanRequest) <-chan *storage.ScanResponse {
	return db.invokeMethod("Scan",
		args, &storage.ScanResponse{}).(chan *storage.ScanResponse)
}

// EndTransaction passes through to local range.
func (db *LocalDB) EndTransaction(args *storage.EndTransactionRequest) <-chan *storage.EndTransactionResponse {
	return db.invokeMethod("EndTransaction",
		args, &storage.EndTransactionResponse{}).(chan *storage.EndTransactionResponse)
}

// AccumulateTS passes through to local range.
func (db *LocalDB) AccumulateTS(args *storage.AccumulateTSRequest) <-chan *storage.AccumulateTSResponse {
	return db.invokeMethod("AccumulateTS",
		args, &storage.AccumulateTSResponse{}).(chan *storage.AccumulateTSResponse)
}

// ReapQueue passes through to local range.
func (db *LocalDB) ReapQueue(args *storage.ReapQueueRequest) <-chan *storage.ReapQueueResponse {
	return db.invokeMethod("ReapQueue",
		args, &storage.ReapQueueResponse{}).(chan *storage.ReapQueueResponse)
}

// EnqueueUpdate passes through to local range.
func (db *LocalDB) EnqueueUpdate(args *storage.EnqueueUpdateRequest) <-chan *storage.EnqueueUpdateResponse {
	return db.invokeMethod("EnqueueUpdate",
		args, &storage.EnqueueUpdateResponse{}).(chan *storage.EnqueueUpdateResponse)
}

// EnqueueMessage passes through to local range.
func (db *LocalDB) EnqueueMessage(args *storage.EnqueueMessageRequest) <-chan *storage.EnqueueMessageResponse {
	return db.invokeMethod("EnqueueMessage",
		args, &storage.EnqueueMessageResponse{}).(chan *storage.EnqueueMessageResponse)
}
