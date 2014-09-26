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
	*BaseDB
	store *Store
	clock *hlc.Clock
}

func newTestDB(store *Store) (*testDB, *hlc.ManualClock) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	db := &testDB{store: store, clock: clock}
	db.BaseDB = NewBaseDB(db.executeCmd)
	return db, &manual
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

func (db *testDB) BeginTransaction(args *proto.BeginTransactionRequest) <-chan *proto.BeginTransactionResponse {
	txn := NewTransaction(args.Name, args.Key, args.GetUserPriority(), args.Isolation, db.clock)
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
