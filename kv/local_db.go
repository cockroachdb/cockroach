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
	"log"
	"reflect"

	"github.com/cockroachdb/cockroach/storage"
)

// A LocalDB provides methods to access only a local, in-memory key
// value store. It utilizes a single storage/Range object, backed by
// a storage/InMem engine.
type LocalDB struct {
	rng *storage.Range
}

// NewLocalDB returns a local-only key-value datastore client for
// unittests.
func NewLocalDB() DB {
	ldb := &LocalDB{}
	rng, err := storage.NewRange([]byte{}, storage.NewInMem(1<<30), nil)
	if err != nil {
		log.Fatal(err)
	}
	ldb.rng = rng
	return ldb
}

// invokeMethod sends the specified RPC asynchronously and returns a
// channel which receives the reply struct when the call is
// complete. Returns a channel of the same type as "reply".
func (db *LocalDB) invokeMethod(method string, args, reply interface{}) interface{} {
	chanVal := reflect.MakeChan(reflect.ChanOf(reflect.BothDir, reflect.TypeOf(reply)), 1)
	replyVal := reflect.ValueOf(reply)
	results := reflect.ValueOf(db.rng).MethodByName(method).Call([]reflect.Value{
		reflect.ValueOf(args),
		replyVal,
	})
	errVal := results[0]
	if errVal != reflect.Zero(errVal.Type()) {
		reflect.Indirect(replyVal).FieldByName("Error").Set(errVal)
	}
	chanVal.Send(replyVal)

	return chanVal.Interface()
}

func (db *LocalDB) Contains(args *storage.ContainsRequest) <-chan *storage.ContainsResponse {
	return db.invokeMethod("Contains",
		args, &storage.ContainsResponse{}).(chan *storage.ContainsResponse)
}

func (db *LocalDB) Get(args *storage.GetRequest) <-chan *storage.GetResponse {
	return db.invokeMethod("Get",
		args, &storage.GetResponse{}).(chan *storage.GetResponse)
}

func (db *LocalDB) Put(args *storage.PutRequest) <-chan *storage.PutResponse {
	return db.invokeMethod("Put",
		args, &storage.PutResponse{}).(chan *storage.PutResponse)
}

func (db *LocalDB) Increment(args *storage.IncrementRequest) <-chan *storage.IncrementResponse {
	return db.invokeMethod("Increment",
		args, &storage.IncrementResponse{}).(chan *storage.IncrementResponse)
}

func (db *LocalDB) Delete(args *storage.DeleteRequest) <-chan *storage.DeleteResponse {
	return db.invokeMethod("Delete",
		args, &storage.DeleteResponse{}).(chan *storage.DeleteResponse)
}

func (db *LocalDB) DeleteRange(args *storage.DeleteRangeRequest) <-chan *storage.DeleteRangeResponse {
	return db.invokeMethod("DeleteRange",
		args, &storage.DeleteRangeResponse{}).(chan *storage.DeleteRangeResponse)
}

func (db *LocalDB) Scan(args *storage.ScanRequest) <-chan *storage.ScanResponse {
	return db.invokeMethod("Scan",
		args, &storage.ScanResponse{}).(chan *storage.ScanResponse)
}

func (db *LocalDB) EndTransaction(args *storage.EndTransactionRequest) <-chan *storage.EndTransactionResponse {
	return db.invokeMethod("EndTransaction",
		args, &storage.EndTransactionResponse{}).(chan *storage.EndTransactionResponse)
}

func (db *LocalDB) AccumulateTS(args *storage.AccumulateTSRequest) <-chan *storage.AccumulateTSResponse {
	return db.invokeMethod("AccumulateTS",
		args, &storage.AccumulateTSResponse{}).(chan *storage.AccumulateTSResponse)
}

func (db *LocalDB) ReapQueue(args *storage.ReapQueueRequest) <-chan *storage.ReapQueueResponse {
	return db.invokeMethod("ReapQueue",
		args, &storage.ReapQueueResponse{}).(chan *storage.ReapQueueResponse)
}

func (db *LocalDB) EnqueueUpdate(args *storage.EnqueueUpdateRequest) <-chan *storage.EnqueueUpdateResponse {
	return db.invokeMethod("EnqueueUpdate",
		args, &storage.EnqueueUpdateResponse{}).(chan *storage.EnqueueUpdateResponse)
}

func (db *LocalDB) EnqueueMessage(args *storage.EnqueueMessageRequest) <-chan *storage.EnqueueMessageResponse {
	return db.invokeMethod("EnqueueMessage",
		args, &storage.EnqueueMessageResponse{}).(chan *storage.EnqueueMessageResponse)
}
