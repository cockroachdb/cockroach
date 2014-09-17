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

package storage

import (
	"reflect"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// testDB is an implementation of the DB interface which
// passes all requests through to a single store.
type testDB struct {
	store *Store
	clock *hlc.Clock
}

func newTestDB(store *Store) (*testDB, *hlc.ManualClock) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	return &testDB{store: store, clock: clock}, &manual
}

func (db *testDB) executeCmd(method string, args proto.Request, replyChan interface{}) {
	reply := reflect.New(reflect.TypeOf(replyChan).Elem().Elem()).Interface().(proto.Response)
	if rng := db.store.LookupRange(args.Header().Key, args.Header().EndKey); rng != nil {
		args.Header().Replica = *rng.Meta.GetReplica()
		db.store.ExecuteCmd(method, args, reply)
	} else {
		reply.Header().SetGoError(proto.NewRangeKeyMismatchError(args.Header().Key, args.Header().EndKey, nil))
	}
	reflect.ValueOf(replyChan).Send(reflect.ValueOf(reply))
}

func (db *testDB) Contains(args *proto.ContainsRequest) <-chan *proto.ContainsResponse {
	replyChan := make(chan *proto.ContainsResponse, 1)
	go db.executeCmd(Contains, args, replyChan)
	return replyChan
}

func (db *testDB) Get(args *proto.GetRequest) <-chan *proto.GetResponse {
	replyChan := make(chan *proto.GetResponse, 1)
	go db.executeCmd(Get, args, replyChan)
	return replyChan
}

func (db *testDB) Put(args *proto.PutRequest) <-chan *proto.PutResponse {
	replyChan := make(chan *proto.PutResponse, 1)
	go db.executeCmd(Put, args, replyChan)
	return replyChan
}

func (db *testDB) ConditionalPut(args *proto.ConditionalPutRequest) <-chan *proto.ConditionalPutResponse {
	replyChan := make(chan *proto.ConditionalPutResponse, 1)
	go db.executeCmd(ConditionalPut, args, replyChan)
	return replyChan
}

func (db *testDB) Increment(args *proto.IncrementRequest) <-chan *proto.IncrementResponse {
	replyChan := make(chan *proto.IncrementResponse, 1)
	go db.executeCmd(Increment, args, replyChan)
	return replyChan
}

func (db *testDB) Delete(args *proto.DeleteRequest) <-chan *proto.DeleteResponse {
	replyChan := make(chan *proto.DeleteResponse, 1)
	go db.executeCmd(Delete, args, replyChan)
	return replyChan
}

func (db *testDB) DeleteRange(args *proto.DeleteRangeRequest) <-chan *proto.DeleteRangeResponse {
	replyChan := make(chan *proto.DeleteRangeResponse, 1)
	go db.executeCmd(DeleteRange, args, replyChan)
	return replyChan
}

func (db *testDB) Scan(args *proto.ScanRequest) <-chan *proto.ScanResponse {
	replyChan := make(chan *proto.ScanResponse, 1)
	go db.executeCmd(Scan, args, replyChan)
	return replyChan
}

func (db *testDB) BeginTransaction(args *proto.BeginTransactionRequest) <-chan *proto.BeginTransactionResponse {
	txn := NewTransaction(args.Key, args.UserPriority, args.Isolation, db.clock)
	reply := &proto.BeginTransactionResponse{
		ResponseHeader: proto.ResponseHeader{
			Timestamp: txn.Timestamp,
		},
		Txn: txn,
	}
	replyChan := make(chan *proto.BeginTransactionResponse, 1)
	replyChan <- reply
	return replyChan
}

func (db *testDB) EndTransaction(args *proto.EndTransactionRequest) <-chan *proto.EndTransactionResponse {
	replyChan := make(chan *proto.EndTransactionResponse, 1)
	go db.executeCmd(EndTransaction, args, replyChan)
	return replyChan
}

func (db *testDB) AccumulateTS(args *proto.AccumulateTSRequest) <-chan *proto.AccumulateTSResponse {
	replyChan := make(chan *proto.AccumulateTSResponse, 1)
	go db.executeCmd(AccumulateTS, args, replyChan)
	return replyChan
}

func (db *testDB) ReapQueue(args *proto.ReapQueueRequest) <-chan *proto.ReapQueueResponse {
	replyChan := make(chan *proto.ReapQueueResponse, 1)
	go db.executeCmd(ReapQueue, args, replyChan)
	return replyChan
}

func (db *testDB) EnqueueUpdate(args *proto.EnqueueUpdateRequest) <-chan *proto.EnqueueUpdateResponse {
	replyChan := make(chan *proto.EnqueueUpdateResponse, 1)
	go db.executeCmd(EnqueueUpdate, args, replyChan)
	return replyChan
}

func (db *testDB) EnqueueMessage(args *proto.EnqueueMessageRequest) <-chan *proto.EnqueueMessageResponse {
	replyChan := make(chan *proto.EnqueueMessageResponse, 1)
	go db.executeCmd(EnqueueMessage, args, replyChan)
	return replyChan
}

func (db *testDB) InternalHeartbeatTxn(args *proto.InternalHeartbeatTxnRequest) <-chan *proto.InternalHeartbeatTxnResponse {
	replyChan := make(chan *proto.InternalHeartbeatTxnResponse, 1)
	go db.executeCmd(InternalHeartbeatTxn, args, replyChan)
	return replyChan
}

func (db *testDB) InternalPushTxn(args *proto.InternalPushTxnRequest) <-chan *proto.InternalPushTxnResponse {
	replyChan := make(chan *proto.InternalPushTxnResponse, 1)
	go db.executeCmd(InternalPushTxn, args, replyChan)
	return replyChan
}

func (db *testDB) InternalResolveIntent(args *proto.InternalResolveIntentRequest) <-chan *proto.InternalResolveIntentResponse {
	replyChan := make(chan *proto.InternalResolveIntentResponse, 1)
	go db.executeCmd(InternalResolveIntent, args, replyChan)
	return replyChan
}

func (db *testDB) InternalSnapshotCopy(args *proto.InternalSnapshotCopyRequest) <-chan *proto.InternalSnapshotCopyResponse {
	replyChan := make(chan *proto.InternalSnapshotCopyResponse, 1)
	go db.executeCmd(InternalSnapshotCopy, args, replyChan)
	return replyChan
}
