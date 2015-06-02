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
	"bytes"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	gogoproto "github.com/gogo/protobuf/proto"
)

// createTestDB creates a local test server and starts it. The caller
// is responsible for stopping the test server.
func createTestDB(t testing.TB) *LocalTestCluster {
	s := &LocalTestCluster{}
	s.Start(t)
	return s
}

// makeTS creates a new timestamp.
func makeTS(walltime int64, logical int32) proto.Timestamp {
	return proto.Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

// getCoord type casts the db's sender to a coordinator and returns it.
func getCoord(db *client.KV) *TxnCoordSender {
	return db.Sender.(*TxnCoordSender)
}

// newTxn begins a transaction.
func newTxn(db *client.KV, clock *hlc.Clock, baseKey proto.Key) *proto.Transaction {
	return proto.NewTransaction("test", baseKey, 1, proto.SERIALIZABLE, clock.Now(), clock.MaxOffset().Nanoseconds())
}

// createPutRequest returns a ready-made request using the
// specified key, value & txn ID.
func createPutRequest(key proto.Key, value []byte, txn *proto.Transaction) *proto.PutRequest {
	return &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:       key,
			Timestamp: txn.Timestamp,
			Txn:       txn,
		},
		Value: proto.Value{Bytes: value},
	}
}

func createDeleteRangeRequest(key, endKey proto.Key, txn *proto.Transaction) *proto.DeleteRangeRequest {
	return &proto.DeleteRangeRequest{
		RequestHeader: proto.RequestHeader{
			Key:       key,
			EndKey:    endKey,
			Timestamp: txn.Timestamp,
			Txn:       txn,
		},
	}
}

// TestTxnCoordSenderAddRequest verifies adding a request creates a
// transaction metadata and adding multiple requests with same
// transaction ID updates the last update timestamp.
func TestTxnCoordSenderAddRequest(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()
	kv := s.DB.InternalKV()
	coord := getCoord(kv)

	txn := newTxn(kv, s.Clock, proto.Key("a"))
	putReq := createPutRequest(proto.Key("a"), []byte("value"), txn)

	// Put request will create a new transaction.
	if err := kv.Run(client.Call{Args: putReq, Reply: &proto.PutResponse{}}); err != nil {
		t.Fatal(err)
	}
	txnMeta, ok := coord.txns[string(txn.ID)]
	if !ok {
		t.Fatal("expected a transaction to be created on coordinator")
	}
	ts := atomic.LoadInt64(&txnMeta.lastUpdateNanos)

	// Advance time and send another put request. Lock the coordinator
	// to prevent a data race.
	coord.Lock()
	s.Manual.Set(1)
	coord.Unlock()
	if err := kv.Run(client.Call{Args: putReq, Reply: &proto.PutResponse{}}); err != nil {
		t.Fatal(err)
	}
	if len(coord.txns) != 1 {
		t.Errorf("expected length of transactions map to be 1; got %d", len(coord.txns))
	}
	txnMeta = coord.txns[string(txn.ID)]
	if lu := atomic.LoadInt64(&txnMeta.lastUpdateNanos); ts >= lu || lu != s.Manual.UnixNano() {
		t.Errorf("expected last update time to advance; got %d", lu)
	}
}

// TestTxnCoordSenderBeginTransaction verifies that a command sent with a
// not-nil Txn with empty ID gets a new transaction initialized.
func TestTxnCoordSenderBeginTransaction(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()
	kv := s.DB.InternalKV()

	reply := &proto.PutResponse{}
	key := proto.Key("key")
	kv.Sender.Send(context.Background(), client.Call{
		Args: &proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key:          key,
				User:         storage.UserRoot,
				UserPriority: gogoproto.Int32(-10), // negative user priority is translated into positive priority
				Txn: &proto.Transaction{
					Name:      "test txn",
					Isolation: proto.SNAPSHOT,
				},
			},
		},
		Reply: reply,
	})
	if reply.Error != nil {
		t.Fatal(reply.GoError())
	}
	if reply.Txn.Name != "test txn" {
		t.Errorf("expected txn name to be %q; got %q", "test txn", reply.Txn.Name)
	}
	if reply.Txn.Priority != 10 {
		t.Errorf("expected txn priority 10; got %d", reply.Txn.Priority)
	}
	if !bytes.Equal(reply.Txn.Key, key) {
		t.Errorf("expected txn Key to match %q != %q", key, reply.Txn.Key)
	}
	if reply.Txn.Isolation != proto.SNAPSHOT {
		t.Errorf("expected txn isolation to be SNAPSHOT; got %s", reply.Txn.Isolation)
	}
}

// TestTxnCoordSenderBeginTransactionMinPriority verifies that when starting
// a new transaction, a non-zero priority is treated as a minimum value.
func TestTxnCoordSenderBeginTransactionMinPriority(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()
	kv := s.DB.InternalKV()

	reply := &proto.PutResponse{}
	kv.Sender.Send(context.Background(), client.Call{
		Args: &proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key:          proto.Key("key"),
				User:         storage.UserRoot,
				UserPriority: gogoproto.Int32(-10), // negative user priority is translated into positive priority
				Txn: &proto.Transaction{
					Name:      "test txn",
					Isolation: proto.SNAPSHOT,
					Priority:  11,
				},
			},
		},
		Reply: reply,
	})
	if reply.Error != nil {
		t.Fatal(reply.GoError())
	}
	if reply.Txn.Priority != 11 {
		t.Errorf("expected txn priority 11; got %d", reply.Txn.Priority)
	}
}

// TestTxnCoordSenderKeyRanges verifies that multiple requests to same or
// overlapping key ranges causes the coordinator to keep track only of
// the minimum number of ranges.
func TestTxnCoordSenderKeyRanges(t *testing.T) {
	ranges := []struct {
		start, end proto.Key
	}{
		{proto.Key("a"), proto.Key(nil)},
		{proto.Key("a"), proto.Key(nil)},
		{proto.Key("aa"), proto.Key(nil)},
		{proto.Key("b"), proto.Key(nil)},
		{proto.Key("aa"), proto.Key("c")},
		{proto.Key("b"), proto.Key("c")},
	}

	s := createTestDB(t)
	defer s.Stop()
	kv := s.DB.InternalKV()
	coord := getCoord(kv)
	txn := newTxn(kv, s.Clock, proto.Key("a"))

	for _, rng := range ranges {
		if rng.end != nil {
			delRangeReq := createDeleteRangeRequest(rng.start, rng.end, txn)
			if err := kv.Run(client.Call{Args: delRangeReq, Reply: &proto.DeleteRangeResponse{}}); err != nil {
				t.Fatal(err)
			}
		} else {
			putReq := createPutRequest(rng.start, []byte("value"), txn)
			if err := kv.Run(client.Call{Args: putReq, Reply: &proto.PutResponse{}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Verify that the transaction metadata contains only two entries
	// in its "keys" interval cache. "a" and range "aa"-"c".
	txnMeta, ok := coord.txns[string(txn.ID)]
	if !ok {
		t.Fatalf("expected a transaction to be created on coordinator")
	}
	if txnMeta.keys.Len() != 2 {
		t.Errorf("expected 2 entries in keys interval cache; got %v", txnMeta.keys)
	}
}

// TestTxnCoordSenderMultipleTxns verifies correct operation with
// multiple outstanding transactions.
func TestTxnCoordSenderMultipleTxns(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()
	kv := s.DB.InternalKV()
	coord := getCoord(kv)

	txn1 := newTxn(kv, s.Clock, proto.Key("a"))
	txn2 := newTxn(kv, s.Clock, proto.Key("b"))
	if err := kv.Run(client.Call{
		Args:  createPutRequest(proto.Key("a"), []byte("value"), txn1),
		Reply: &proto.PutResponse{}}); err != nil {
		t.Fatal(err)
	}
	if err := kv.Run(client.Call{
		Args:  createPutRequest(proto.Key("b"), []byte("value"), txn2),
		Reply: &proto.PutResponse{}}); err != nil {
		t.Fatal(err)
	}

	if len(coord.txns) != 2 {
		t.Errorf("expected length of transactions map to be 2; got %d", len(coord.txns))
	}
}

// TestTxnCoordSenderHeartbeat verifies periodic heartbeat of the
// transaction record.
func TestTxnCoordSenderHeartbeat(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()
	kv := s.DB.InternalKV()
	coord := getCoord(kv)

	// Set heartbeat interval to 1ms for testing.
	coord.heartbeatInterval = 1 * time.Millisecond

	txn := newTxn(kv, s.Clock, proto.Key("a"))
	if err := kv.Run(client.Call{
		Args:  createPutRequest(proto.Key("a"), []byte("value"), txn),
		Reply: &proto.PutResponse{}}); err != nil {
		t.Fatal(err)
	}

	// Verify 3 heartbeats.
	var heartbeatTS proto.Timestamp
	for i := 0; i < 3; i++ {
		if err := util.IsTrueWithin(func() bool {
			ok, txn, err := getTxn(kv, txn)
			if !ok || err != nil {
				return false
			}
			// Advance clock by 1ns.
			// Locking the TxnCoordSender to prevent a data race.
			coord.Lock()
			s.Manual.Increment(1)
			coord.Unlock()
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
func getTxn(db *client.KV, txn *proto.Transaction) (bool, *proto.Transaction, error) {
	hr := &proto.InternalHeartbeatTxnResponse{}
	if err := db.Run(client.Call{
		Args: &proto.InternalHeartbeatTxnRequest{
			RequestHeader: proto.RequestHeader{
				Key: txn.Key,
				Txn: txn,
			},
		}, Reply: hr}); err != nil {
		return false, nil, err
	}
	return true, hr.Txn, nil
}

func verifyCleanup(key proto.Key, db *client.KV, eng engine.Engine, t *testing.T) {
	coord := getCoord(db)
	if len(coord.txns) != 0 {
		t.Errorf("expected empty transactions map; got %d", len(coord.txns))
	}

	if err := util.IsTrueWithin(func() bool {
		meta := &proto.MVCCMetadata{}
		ok, _, _, err := eng.GetProto(engine.MVCCEncodeKey(key), meta)
		if err != nil {
			t.Errorf("error getting MVCC metadata: %s", err)
		}
		return !ok || meta.Txn == nil
	}, 500*time.Millisecond); err != nil {
		t.Errorf("expected intents to be cleaned up within 500ms")
	}
}

// TestTxnCoordSenderEndTxn verifies that ending a transaction
// sends resolve write intent requests and removes the transaction
// from the txns map.
func TestTxnCoordSenderEndTxn(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()
	kv := s.DB.InternalKV()

	txn := newTxn(kv, s.Clock, proto.Key("a"))
	pReply := &proto.PutResponse{}
	key := proto.Key("a")
	if err := kv.Run(client.Call{
		Args:  createPutRequest(key, []byte("value"), txn),
		Reply: pReply}); err != nil {
		t.Fatal(err)
	}
	if pReply.GoError() != nil {
		t.Fatal(pReply.GoError())
	}
	etReply := &proto.EndTransactionResponse{}
	kv.Sender.Send(context.Background(), client.Call{
		Args: &proto.EndTransactionRequest{
			RequestHeader: proto.RequestHeader{
				Key:       txn.Key,
				Timestamp: txn.Timestamp,
				Txn:       txn,
			},
			Commit: true,
		},
		Reply: etReply,
	})
	if etReply.Error != nil {
		t.Fatal(etReply.GoError())
	}
	verifyCleanup(key, kv, s.Eng, t)
}

// TestTxnCoordSenderCleanupOnAborted verifies that if a txn receives a
// TransactionAbortedError, the coordinator cleans up the transaction.
func TestTxnCoordSenderCleanupOnAborted(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()
	kv := s.DB.InternalKV()

	// Create a transaction with intent at "a".
	key := proto.Key("a")
	txn := newTxn(kv, s.Clock, key)
	txn.Priority = 1
	if err := kv.Run(client.Call{
		Args:  createPutRequest(key, []byte("value"), txn),
		Reply: &proto.PutResponse{}}); err != nil {
		t.Fatal(err)
	}

	// Push the transaction to abort it.
	txn2 := newTxn(kv, s.Clock, key)
	txn2.Priority = 2
	pushArgs := &proto.InternalPushTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key: txn.Key,
			Txn: txn2,
		},
		Now:       s.Clock.Now(),
		PusheeTxn: *txn,
		PushType:  proto.ABORT_TXN,
	}
	if err := kv.Run(client.Call{
		Args:  pushArgs,
		Reply: &proto.InternalPushTxnResponse{}}); err != nil {
		t.Fatal(err)
	}

	// Now end the transaction and verify we've cleanup up, even though
	// end transaction failed.
	etArgs := &proto.EndTransactionRequest{
		RequestHeader: proto.RequestHeader{
			Key:       txn.Key,
			Timestamp: txn.Timestamp,
			Txn:       txn,
		},
		Commit: true,
	}
	err := kv.Run(client.Call{
		Args:  etArgs,
		Reply: &proto.EndTransactionResponse{}})
	switch err.(type) {
	case nil:
		t.Fatal("expected txn aborted error")
	case *proto.TransactionAbortedError:
		// Expected
	default:
		t.Fatalf("expected transaction aborted error; got %s", err)
	}
	verifyCleanup(key, kv, s.Eng, t)
}

// TestTxnCoordSenderGC verifies that the coordinator cleans up extant
// transactions after the lastUpdateNanos exceeds the timeout.
func TestTxnCoordSenderGC(t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()
	kv := s.DB.InternalKV()
	coord := getCoord(kv)

	// Set heartbeat interval to 1ms for testing.
	coord.heartbeatInterval = 1 * time.Millisecond

	txn := newTxn(kv, s.Clock, proto.Key("a"))
	if err := kv.Run(client.Call{
		Args:  createPutRequest(proto.Key("a"), []byte("value"), txn),
		Reply: &proto.PutResponse{}}); err != nil {
		t.Fatal(err)
	}

	// Now, advance clock past the default client timeout.
	// Locking the TxnCoordSender to prevent a data race.
	coord.Lock()
	s.Manual.Set(defaultClientTimeout.Nanoseconds() + 1)
	coord.Unlock()

	if err := util.IsTrueWithin(func() bool {
		// Locking the TxnCoordSender to prevent a data race.
		coord.Lock()
		_, ok := coord.txns[string(txn.ID)]
		coord.Unlock()
		return !ok
	}, 50*time.Millisecond); err != nil {
		t.Error("expected garbage collection")
	}
}

type testSender struct {
	handler func(call client.Call)
}

var _ client.Sender = &testSender{}

func newTestSender(handler func(client.Call)) *testSender {
	return &testSender{
		handler: handler,
	}
}

func (ts *testSender) Send(_ context.Context, call client.Call) {
	ts.handler(call)
}

func (ts *testSender) Close() {
}

var testPutReq = &proto.PutRequest{
	RequestHeader: proto.RequestHeader{
		Key:          proto.Key("test-key"),
		User:         storage.UserRoot,
		UserPriority: gogoproto.Int32(-1),
		Txn: &proto.Transaction{
			Name: "test txn",
		},
		Replica: proto.Replica{
			NodeID: 12345,
		},
	},
}

// TestTxnCoordSenderTxnUpdatedOnError verifies that errors adjust the
// response transaction's timestamp and priority as appropriate.
func TestTxnCoordSenderTxnUpdatedOnError(t *testing.T) {
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(20)

	testCases := []struct {
		err       error
		expEpoch  int32
		expPri    int32
		expTS     proto.Timestamp
		expOrigTS proto.Timestamp
		nodeSeen  bool
	}{
		{nil, 0, 1, makeTS(0, 1), makeTS(0, 1), false},
		{&proto.ReadWithinUncertaintyIntervalError{
			ExistingTimestamp: makeTS(10, 10)}, 1, 1, makeTS(10, 11),
			makeTS(10, 11), true},
		{&proto.TransactionAbortedError{Txn: proto.Transaction{
			Timestamp: makeTS(20, 10), Priority: 10}}, 0, 10, makeTS(20, 10),
			makeTS(0, 1), false},
		{&proto.TransactionPushError{PusheeTxn: proto.Transaction{
			Timestamp: makeTS(10, 10), Priority: int32(10)}}, 1, 9,
			makeTS(10, 11), makeTS(10, 11), false},
		{&proto.TransactionRetryError{Txn: proto.Transaction{
			Timestamp: makeTS(10, 10), Priority: int32(10)}}, 1, 10,
			makeTS(10, 10), makeTS(10, 10), false},
	}

	for i, test := range testCases {
		stopper := util.NewStopper()
		defer stopper.Stop()
		ts := NewTxnCoordSender(newTestSender(func(call client.Call) {
			call.Reply.Header().SetGoError(test.err)
		}), clock, false, stopper)
		reply := &proto.PutResponse{}
		ts.Send(context.Background(), client.Call{Args: testPutReq, Reply: reply})

		if reflect.TypeOf(test.err) != reflect.TypeOf(reply.GoError()) {
			t.Fatalf("%d: expected %T; got %T", i, test.err, reply.GoError())
		}
		if reply.Txn.Epoch != test.expEpoch {
			t.Errorf("%d: expected epoch = %d; got %d",
				i, test.expEpoch, reply.Txn.Epoch)
		}
		if reply.Txn.Priority != test.expPri {
			t.Errorf("%d: expected priority = %d; got %d",
				i, test.expPri, reply.Txn.Priority)
		}
		if !reply.Txn.Timestamp.Equal(test.expTS) {
			t.Errorf("%d: expected timestamp to be %s; got %s",
				i, test.expTS, reply.Txn.Timestamp)
		}
		if !reply.Txn.OrigTimestamp.Equal(test.expOrigTS) {
			t.Errorf("%d: expected orig timestamp to be %s + 1; got %s",
				i, test.expOrigTS, reply.Txn.OrigTimestamp)
		}
		if nodes := reply.Txn.CertainNodes.Nodes; (len(nodes) != 0) != test.nodeSeen {
			t.Errorf("%d: expected nodeSeen=%t, but list of hosts is %v",
				i, test.nodeSeen, nodes)
		}
	}
}

// TestTxnCoordSenderBatchTransaction tests that it is not possible to send
// one-off transactional calls within a batch (the batch must contain the
// transaction for all contained calls instead).
func TestTxnCoordSenderBatchTransaction(t *testing.T) {
	stopper := util.NewStopper()
	defer stopper.Stop()
	clock := hlc.NewClock(hlc.UnixNano)
	var called bool
	ts := NewTxnCoordSender(newTestSender(func(call client.Call) {
		called = true
		return
	}), clock, false, stopper)

	testCases := []struct{ batch, arg, ok bool }{
		{false, false, true},
		{true, false, true},
		{true, true, false},
		{false, true, false},
	}

	txn1 := &proto.Transaction{ID: []byte("txn1")}
	txn2 := &proto.Transaction{ID: []byte("txn2")}

	for i, tc := range testCases {
		bArgs := &proto.InternalBatchRequest{}
		bReply := &proto.InternalBatchResponse{}

		pushArgs := &proto.InternalPushTxnRequest{}
		if tc.arg {
			pushArgs.RequestHeader = proto.RequestHeader{
				Txn: txn1,
			}
		}
		bArgs.Add(pushArgs)
		if tc.batch {
			bArgs.Txn = txn2
		}
		called = false
		ts.Send(context.Background(), client.Call{Args: bArgs, Reply: bReply})
		if !tc.ok && bReply.GoError() == nil {
			t.Fatalf("%d: expected error", i)
		} else if tc.ok != called {
			t.Fatalf("%d: wanted call: %t, got call: %t", i, tc.ok, called)
		}
	}
}
