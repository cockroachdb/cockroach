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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// createTestDB creates a test kv.DB using a LocalKV object built with
// a store using an in-memory engine. Returns the created kv.DB and
// associated clock's manual time.
func createTestDB(t *testing.T) (*DB, *hlc.Clock, *hlc.ManualClock) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 50<<20)
	kv := NewLocalKV()
	db := NewDB(kv, clock)
	store := storage.NewStore(clock, eng, db, nil)
	if err := store.Bootstrap(proto.StoreIdent{StoreID: 1}); err != nil {
		t.Fatal(err)
	}
	kv.AddStore(store)
	_, err := store.CreateRange(store.BootstrapRangeMetadata())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	return db, clock, &manual
}

// creatPutRequest returns a ready-made request using the
// specified key, value & txn ID.
func createPutRequest(key engine.Key, value, txnID []byte) *proto.PutRequest {
	return &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key: key,
			Txn: &proto.Transaction{ID: txnID},
		},
		Value: proto.Value{Bytes: value},
	}
}

// TestCoordinatorAddRequest verifies adding a request creates a
// transaction metadata and adding multiple requests with same
// transaction ID updates the last update timestamp.
func TestCoordinatorAddRequest(t *testing.T) {
	db, clock, manual := createTestDB(t)
	defer db.Close()

	txnID := engine.Key("txn")
	putReq := createPutRequest(engine.Key("a"), []byte("value"), txnID)

	// Put request will create a new transaction.
	<-db.Put(putReq)
	txnMeta, ok := db.coordinator.txns[string(txnID)]
	if !ok {
		t.Fatal("expected a transaction to be created on coordinator")
	}
	ts := txnMeta.lastUpdateTS
	if !ts.Less(clock.Now()) {
		t.Errorf("expected earlier last update timestamp; got: %+v", ts)
	}

	// Advance time and send another put request.
	// Locking the coordinator to prevent a data race.
	db.coordinator.Lock()
	*manual = hlc.ManualClock(1)
	db.coordinator.Unlock()
	<-db.Put(putReq)
	if len(db.coordinator.txns) != 1 {
		t.Errorf("expected length of transactions map to be 1; got %d", len(db.coordinator.txns))
	}
	txnMeta = db.coordinator.txns[string(txnID)]
	if !ts.Less(txnMeta.lastUpdateTS) || txnMeta.lastUpdateTS.WallTime != int64(*manual) {
		t.Errorf("expected last update time to advance; got %+v", txnMeta.lastUpdateTS)
	}
}

// TestCoordinatorMultipleTxns verifies correct operation with
// multiple outstanding transactions.
func TestCoordinatorMultipleTxns(t *testing.T) {
	db, _, _ := createTestDB(t)
	defer db.Close()

	txn1ID := engine.Key("txn1")
	txn2ID := engine.Key("txn2")
	<-db.Put(createPutRequest(engine.Key("a"), []byte("value"), txn1ID))
	<-db.Put(createPutRequest(engine.Key("b"), []byte("value"), txn2ID))

	if len(db.coordinator.txns) != 2 {
		t.Errorf("expected length of transactions map to be 2; got %d", len(db.coordinator.txns))
	}
}

// TestCoordinatorHeartbeat verifies periodic heartbeat of the
// transaction record.
func TestCoordinatorHeartbeat(t *testing.T) {
	db, _, manual := createTestDB(t)
	defer db.Close()

	// Set heartbeat interval to 1ms for testing.
	db.coordinator.heartbeatInterval = 1 * time.Millisecond

	txnID := engine.Key("txn")
	<-db.Put(createPutRequest(engine.Key("a"), []byte("value"), txnID))

	// Verify 3 heartbeats.
	var heartbeatTS proto.Timestamp
	for i := 0; i < 3; i++ {
		if err := util.IsTrueWithin(func() bool {
			ok, txn, err := getTxn(db, engine.MakeKey(engine.KeyLocalTransactionPrefix, txnID))
			if !ok || err != nil {
				return false
			}
			// Advance clock by 1ns.
			// Locking the coordinator to prevent a data race.
			db.coordinator.Lock()
			*manual = hlc.ManualClock(*manual + 1)
			db.coordinator.Unlock()
			if heartbeatTS.Less(*txn.LastHeartbeat) {
				heartbeatTS = *txn.LastHeartbeat
				return true
			}
			return false
		}, 50*time.Millisecond); err != nil {
			t.Error("expected initial heartbeat within 50ms")
		}
	}
}

// getTxn fetches the requested key and returns the transaction info.
func getTxn(db *DB, key engine.Key) (bool, *proto.Transaction, error) {
	hr := <-db.InternalHeartbeatTxn(&proto.InternalHeartbeatTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key: key,
		},
	})
	if hr.Error != nil {
		return false, nil, hr.GoError()
	}
	return true, hr.Txn, nil
}

// TestCoordinatorEndTxn verifies that ending a transaction
// sends resolve write intent requests and removes the transaction
// from the txns map.
func TestCoordinatorEndTxn(t *testing.T) {
	db, _, _ := createTestDB(t)
	defer db.Close()

	txn := &proto.Transaction{
		ID:     engine.Key("txn"),
		Status: proto.COMMITTED,
	}
	<-db.Put(createPutRequest(engine.Key("a"), []byte("value"), txn.ID))

	db.coordinator.EndTxn(txn)
	if len(db.coordinator.txns) != 0 {
		t.Errorf("expected empty transactions map; got %d", len(db.coordinator.txns))
	}

	// TODO(spencer): need to test that resolve intents were sent to key "a".
}

// TestCoordinatorGC verifies that the coordinator cleans up extant
// transactions after the lastUpdateTS exceeds the timeout.
func TestCoordinatorGC(t *testing.T) {
	db, _, manual := createTestDB(t)
	defer db.Close()

	// Set heartbeat interval to 1ms for testing.
	db.coordinator.heartbeatInterval = 1 * time.Millisecond

	txnID := engine.Key("txn")
	<-db.Put(createPutRequest(engine.Key("a"), []byte("value"), txnID))

	// Now, advance clock past the default client timeout.
	// Locking the coordinator to prevent a data race.
	db.coordinator.Lock()
	*manual = hlc.ManualClock(defaultClientTimeout.Nanoseconds() + 1)
	db.coordinator.Unlock()

	if err := util.IsTrueWithin(func() bool {
		// Locking the coordinator to prevent a data race.
		db.coordinator.Lock()
		_, ok := db.coordinator.txns[string(txnID)]
		db.coordinator.Unlock()
		return !ok
	}, 50*time.Millisecond); err != nil {
		t.Error("expected garbage collection")
	}
}
