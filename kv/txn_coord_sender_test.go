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
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils/batchutil"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	gogoproto "github.com/gogo/protobuf/proto"
)

// teardownHeartbeats goes through the coordinator's active transactions and
// has the associated heartbeat tasks quit. This is useful for tests which
// don't finish transactions.
func teardownHeartbeats(tc *TxnCoordSender) {
	if r := recover(); r != nil {
		panic(r)
	}
	tc.Lock()
	for _, tm := range tc.txns {
		if tm.txnEnd != nil {
			close(tm.txnEnd)
		}
	}
	defer tc.Unlock()
}

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

// newTxn begins a transaction. For testing purposes, this comes with a ID
// pre-initialized, but with the Writing flag set to false.
func newTxn(clock *hlc.Clock, baseKey proto.Key) *proto.Transaction {
	f, l, fun := caller.Lookup(1)
	name := fmt.Sprintf("%s:%d %s", f, l, fun)
	txn := proto.NewTransaction("test", baseKey, 1, proto.SERIALIZABLE, clock.Now(), clock.MaxOffset().Nanoseconds())
	txn.Name = name
	return txn
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
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn := newTxn(s.Clock, proto.Key("a"))
	put := createPutRequest(proto.Key("a"), []byte("value"), txn)

	// Put request will create a new transaction.
	reply, err := batchutil.SendWrapped(s.Sender, put)
	if err != nil {
		t.Fatal(err)
	}
	txnMeta, ok := s.Sender.txns[string(txn.ID)]
	if !ok {
		t.Fatal("expected a transaction to be created on coordinator")
	}
	if !reply.Header().Txn.Writing {
		t.Fatal("response Txn is not marked as writing")
	}
	ts := atomic.LoadInt64(&txnMeta.lastUpdateNanos)

	// Advance time and send another put request. Lock the coordinator
	// to prevent a data race.
	s.Sender.Lock()
	s.Manual.Set(1)
	s.Sender.Unlock()
	put.Txn.Writing = true
	if _, err := batchutil.SendWrapped(s.Sender, put); err != nil {
		t.Fatal(err)
	}
	if len(s.Sender.txns) != 1 {
		t.Errorf("expected length of transactions map to be 1; got %d", len(s.Sender.txns))
	}
	txnMeta = s.Sender.txns[string(txn.ID)]
	if lu := atomic.LoadInt64(&txnMeta.lastUpdateNanos); ts >= lu || lu != s.Manual.UnixNano() {
		t.Errorf("expected last update time to advance; got %d", lu)
	}
}

// TestTxnCoordSenderBeginTransaction verifies that a command sent with a
// not-nil Txn with empty ID gets a new transaction initialized.
func TestTxnCoordSenderBeginTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	key := proto.Key("key")
	reply, err := batchutil.SendWrapped(s.Sender, &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:          key,
			UserPriority: gogoproto.Int32(-10), // negative user priority is translated into positive priority
			Txn: &proto.Transaction{
				Name:      "test txn",
				Isolation: proto.SNAPSHOT,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	pr := reply.(*proto.PutResponse)
	if pr.Txn.Name != "test txn" {
		t.Errorf("expected txn name to be %q; got %q", "test txn", pr.Txn.Name)
	}
	if pr.Txn.Priority != 10 {
		t.Errorf("expected txn priority 10; got %d", pr.Txn.Priority)
	}
	if !bytes.Equal(pr.Txn.Key, key) {
		t.Errorf("expected txn Key to match %q != %q", key, pr.Txn.Key)
	}
	if pr.Txn.Isolation != proto.SNAPSHOT {
		t.Errorf("expected txn isolation to be SNAPSHOT; got %s", pr.Txn.Isolation)
	}
}

// TestTxnCoordSenderBeginTransactionMinPriority verifies that when starting
// a new transaction, a non-zero priority is treated as a minimum value.
func TestTxnCoordSenderBeginTransactionMinPriority(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	reply, err := batchutil.SendWrapped(s.Sender, &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:          proto.Key("key"),
			UserPriority: gogoproto.Int32(-10), // negative user priority is translated into positive priority
			Txn: &proto.Transaction{
				Name:      "test txn",
				Isolation: proto.SNAPSHOT,
				Priority:  11,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if prio := reply.(*proto.PutResponse).Txn.Priority; prio != 11 {
		t.Errorf("expected txn priority 11; got %d", prio)
	}
}

// TestTxnCoordSenderKeyRanges verifies that multiple requests to same or
// overlapping key ranges causes the coordinator to keep track only of
// the minimum number of ranges.
func TestTxnCoordSenderKeyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)
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
	defer teardownHeartbeats(s.Sender)
	txn := newTxn(s.Clock, proto.Key("a"))

	for _, rng := range ranges {
		if rng.end != nil {
			delRangeReq := createDeleteRangeRequest(rng.start, rng.end, txn)
			if _, err := batchutil.SendWrapped(s.Sender, delRangeReq); err != nil {
				t.Fatal(err)
			}
		} else {
			putReq := createPutRequest(rng.start, []byte("value"), txn)
			if _, err := batchutil.SendWrapped(s.Sender, putReq); err != nil {
				t.Fatal(err)
			}
		}
		txn.Writing = true // required for all but first req
	}

	// Verify that the transaction metadata contains only two entries
	// in its "keys" interval cache. "a" and range "aa"-"c".
	txnMeta, ok := s.Sender.txns[string(txn.ID)]
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
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn1 := newTxn(s.Clock, proto.Key("a"))
	txn2 := newTxn(s.Clock, proto.Key("b"))
	put1 := createPutRequest(proto.Key("a"), []byte("value"), txn1)
	if _, err := batchutil.SendWrapped(s.Sender, put1); err != nil {
		t.Fatal(err)
	}
	put2 := createPutRequest(proto.Key("b"), []byte("value"), txn2)
	if _, err := batchutil.SendWrapped(s.Sender, put2); err != nil {
		t.Fatal(err)
	}

	if len(s.Sender.txns) != 2 {
		t.Errorf("expected length of transactions map to be 2; got %d", len(s.Sender.txns))
	}
}

// TestTxnCoordSenderHeartbeat verifies periodic heartbeat of the
// transaction record.
func TestTxnCoordSenderHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	// Set heartbeat interval to 1ms for testing.
	s.Sender.heartbeatInterval = 1 * time.Millisecond

	initialTxn := newTxn(s.Clock, proto.Key("a"))
	put := createPutRequest(proto.Key("a"), []byte("value"), initialTxn)
	if reply, err := batchutil.SendWrapped(s.Sender, put); err != nil {
		t.Fatal(err)
	} else {
		*initialTxn = *reply.Header().Txn
	}

	// Verify 3 heartbeats.
	var heartbeatTS proto.Timestamp
	for i := 0; i < 3; i++ {
		if err := util.IsTrueWithin(func() bool {
			ok, txn, err := getTxn(s.Sender, initialTxn)
			if !ok || err != nil {
				return false
			}
			// Advance clock by 1ns.
			// Locking the TxnCoordSender to prevent a data race.
			s.Sender.Lock()
			s.Manual.Increment(1)
			s.Sender.Unlock()
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
func getTxn(coord *TxnCoordSender, txn *proto.Transaction) (bool, *proto.Transaction, error) {
	hb := &proto.HeartbeatTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key: txn.Key,
			Txn: txn,
		},
	}
	reply, err := batchutil.SendWrapped(coord, hb)
	if err != nil {
		return false, nil, err
	}
	return true, reply.(*proto.HeartbeatTxnResponse).Txn, nil
}

func verifyCleanup(key proto.Key, coord *TxnCoordSender, eng engine.Engine, t *testing.T) {
	util.SucceedsWithin(t, 500*time.Millisecond, func() error {
		coord.Lock()
		l := len(coord.txns)
		coord.Unlock()
		if l != 0 {
			return fmt.Errorf("expected empty transactions map; got %d", l)
		}
		meta := &engine.MVCCMetadata{}
		ok, _, _, err := eng.GetProto(engine.MVCCEncodeKey(key), meta)
		if err != nil {
			return fmt.Errorf("error getting MVCC metadata: %s", err)
		}
		if !ok || meta.Txn == nil {
			return nil
		}
		return errors.New("intents not cleaned up")
	})
}

// TestTxnCoordSenderEndTxn verifies that ending a transaction
// sends resolve write intent requests and removes the transaction
// from the txns map.
func TestTxnCoordSenderEndTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	txn := newTxn(s.Clock, proto.Key("a"))
	key := proto.Key("a")
	put := createPutRequest(key, []byte("value"), txn)
	reply, err := batchutil.SendWrapped(s.Sender, put)
	if err != nil {
		t.Fatal(err)
	}
	pReply := reply.(*proto.PutResponse)
	if _, err := batchutil.SendWrapped(s.Sender, &proto.EndTransactionRequest{
		RequestHeader: proto.RequestHeader{
			Key:       txn.Key,
			Timestamp: txn.Timestamp,
			Txn:       pReply.Header().Txn,
		},
		Commit: true,
	}); err != nil {
		t.Fatal(err)
	}
	verifyCleanup(key, s.Sender, s.Eng, t)
}

// TestTxnCoordSenderCleanupOnAborted verifies that if a txn receives a
// TransactionAbortedError, the coordinator cleans up the transaction.
func TestTxnCoordSenderCleanupOnAborted(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	// Create a transaction with intent at "a".
	key := proto.Key("a")
	txn := newTxn(s.Clock, key)
	txn.Priority = 1
	put := createPutRequest(key, []byte("value"), txn)
	if reply, err := batchutil.SendWrapped(s.Sender, put); err != nil {
		t.Fatal(err)
	} else {
		txn = reply.Header().Txn
	}

	// Push the transaction to abort it.
	txn2 := newTxn(s.Clock, key)
	txn2.Priority = 2
	pushArgs := &proto.PushTxnRequest{
		RequestHeader: proto.RequestHeader{
			Key: txn.Key,
		},
		Now:       s.Clock.Now(),
		PusherTxn: txn2,
		PusheeTxn: *txn,
		PushType:  proto.ABORT_TXN,
	}
	if _, err := batchutil.SendWrapped(s.Sender, pushArgs); err != nil {
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
	_, err := batchutil.SendWrapped(s.Sender, etArgs)
	switch err.(type) {
	case *proto.TransactionAbortedError:
		// Expected
	default:
		t.Fatalf("expected transaction aborted error; got %s", err)
	}
	verifyCleanup(key, s.Sender, s.Eng, t)
}

// TestTxnCoordSenderGC verifies that the coordinator cleans up extant
// transactions after the lastUpdateNanos exceeds the timeout.
func TestTxnCoordSenderGC(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	// Set heartbeat interval to 1ms for testing.
	s.Sender.heartbeatInterval = 1 * time.Millisecond

	txn := newTxn(s.Clock, proto.Key("a"))
	put := createPutRequest(proto.Key("a"), []byte("value"), txn)
	if _, err := batchutil.SendWrapped(s.Sender, put); err != nil {
		t.Fatal(err)
	}

	// Now, advance clock past the default client timeout.
	// Locking the TxnCoordSender to prevent a data race.
	s.Sender.Lock()
	s.Manual.Set(defaultClientTimeout.Nanoseconds() + 1)
	s.Sender.Unlock()

	if err := util.IsTrueWithin(func() bool {
		// Locking the TxnCoordSender to prevent a data race.
		s.Sender.Lock()
		_, ok := s.Sender.txns[string(txn.ID)]
		s.Sender.Unlock()
		return !ok
	}, 50*time.Millisecond); err != nil {
		t.Error("expected garbage collection")
	}
}

// TestTxnCoordSenderTxnUpdatedOnError verifies that errors adjust the
// response transaction's timestamp and priority as appropriate.
func TestTxnCoordSenderTxnUpdatedOnError(t *testing.T) {
	defer leaktest.AfterTest(t)
	t.Skip("TODO(tschottdorf): fix up and re-enable. It depends on each logical clock tick, so not fun.")
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

	var testPutReq = &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key:          proto.Key("test-key"),
			UserPriority: gogoproto.Int32(-1),
			Txn: &proto.Transaction{
				Name: "test txn",
			},
			Replica: proto.Replica{
				NodeID: 12345,
			},
		},
	}

	for i, test := range testCases {
		stopper := stop.NewStopper()
		ts := NewTxnCoordSender(senderFn(func(_ context.Context, _ proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
			return nil, proto.NewError(test.err)
		}), clock, false, nil, stopper)
		var reply *proto.PutResponse
		if r, err := batchutil.SendWrapped(ts, gogoproto.Clone(testPutReq).(proto.Request)); err != nil {
			t.Fatal(err)
		} else {
			reply = r.(*proto.PutResponse)
		}
		teardownHeartbeats(ts)
		stopper.Stop()

		if reflect.TypeOf(test.err) != reflect.TypeOf(reply.GoError()) {
			t.Fatalf("%d: expected %T; got %T: %v", i, test.err, reply.GoError(), reply.GoError())
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

// TestTxnCoordSenderBatchTransaction tests that it is possible to send
// one-off transactional calls within a batch under certain circumstances.
func TestTxnCoordSenderBatchTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	t.Skip("TODO(tschottdorf): remove this test; behavior is more transparent now")
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	clock := hlc.NewClock(hlc.UnixNano)
	var called bool
	var alwaysError = errors.New("success")
	ts := NewTxnCoordSender(senderFn(func(_ context.Context, _ proto.BatchRequest) (*proto.BatchResponse, *proto.Error) {
		called = true
		// Returning this error is an easy way of preventing heartbeats
		// to be started for otherwise "successful" calls.
		return nil, proto.NewError(alwaysError)
	}), clock, false, nil, stopper)

	pushArg := &proto.PushTxnRequest{}
	putArg := &proto.PutRequest{}
	getArg := &proto.GetRequest{}
	testCases := []struct {
		req            proto.Request
		batch, arg, ok bool
	}{
		// Lays intents: can't have this on individual calls at all.
		{putArg, false, false, true},
		{putArg, true, false, true},
		{putArg, true, true, false},
		{putArg, false, true, false},

		// No intents: all ok, except when batch and arg have different txns.
		{pushArg, false, false, true},
		{pushArg, true, false, true},
		{pushArg, true, true, false},
		{pushArg, false, true, true},
		{getArg, false, false, true},
		{getArg, true, false, true},
		{getArg, true, true, false},
		{getArg, false, true, true},
	}

	txn1 := &proto.Transaction{ID: []byte("txn1")}
	txn2 := &proto.Transaction{ID: []byte("txn2")}

	for i, tc := range testCases {
		called = false
		tc.req.Reset()
		ba := &proto.BatchRequest{}

		if tc.arg {
			tc.req.Header().Txn = txn1
		}
		ba.Add(tc.req)
		if tc.batch {
			ba.Txn = txn2
		}
		called = false
		_, err := batchutil.SendWrapped(ts, ba)
		if !tc.ok && err == alwaysError {
			t.Fatalf("%d: expected error%s", i, err)
		} else if tc.ok != called {
			t.Fatalf("%d: wanted call: %t, got call: %t", i, tc.ok, called)
		}
	}
}

// TestTxnDrainingNode tests that pending transactions tasks' intents are resolved
// if they commit while draining, and that a NodeUnavailableError is received
// when attempting to run a new transaction on a draining node.
func TestTxnDrainingNode(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)

	done := make(chan struct{})
	// Dummy task that keeps the node in draining state.
	if !s.Stopper.RunAsyncTask(func() {
		<-done
	}) {
		t.Fatal("stopper draining prematurely")
	}

	txn := newTxn(s.Clock, proto.Key("a"))
	key := proto.Key("a")
	beginTxn := func() {
		put := createPutRequest(key, []byte("value"), txn)
		if reply, err := batchutil.SendWrapped(s.Sender, put); err != nil {
			t.Fatal(err)
		} else {
			txn = reply.Header().Txn
		}
	}
	endTxn := func() {
		if _, err := batchutil.SendWrapped(s.Sender, &proto.EndTransactionRequest{
			RequestHeader: proto.RequestHeader{
				Key:       txn.Key,
				Timestamp: txn.Timestamp,
				Txn:       txn,
			},
			Commit: true}); err != nil {
			t.Fatal(err)
		}
	}

	beginTxn() // begin before draining
	go func() {
		s.Stopper.Stop()
	}()

	util.SucceedsWithin(t, time.Second, func() error {
		if s.Stopper.RunTask(func() {}) {
			return errors.New("stopper not yet draining")
		}
		return nil
	})
	endTxn()                               // commit after draining
	verifyCleanup(key, s.Sender, s.Eng, t) // make sure intent gets resolved

	// Attempt to start another transaction, but it should be too late.
	key = proto.Key("key")
	_, err := batchutil.SendWrapped(s.Sender, &proto.PutRequest{
		RequestHeader: proto.RequestHeader{
			Key: key,
			Txn: &proto.Transaction{
				Name: "test txn",
			},
		},
	})
	if _, ok := err.(*proto.NodeUnavailableError); !ok {
		teardownHeartbeats(s.Sender)
		t.Fatal(err)
	}
	close(done)
	<-s.Stopper.IsStopped()
}

// TestTxnCoordIdempotentCleanup verifies that cleanupTxn is idempotent.
func TestTxnCoordIdempotentCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	txn := newTxn(s.Clock, proto.Key("a"))
	key := proto.Key("a")
	put := createPutRequest(key, []byte("value"), txn)
	if _, err := batchutil.SendWrapped(s.Sender, put); err != nil {
		t.Fatal(err)
	}
	s.Sender.cleanupTxn(nil, *txn) // first call
	if _, err := batchutil.SendWrapped(s.Sender, &proto.EndTransactionRequest{
		RequestHeader: proto.RequestHeader{
			Key:       txn.Key,
			Timestamp: txn.Timestamp,
			Txn:       txn,
		},
		Commit: true,
	}); /* second call */ err != nil {
		t.Fatal(err)
	}
}

// TestTxnMultipleCoord checks that a coordinator uses the Writing flag to
// enforce that only one coordinator can be used for transactional writes.
func TestTxnMultipleCoord(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	for i, tc := range []struct {
		args    proto.Request
		writing bool
		ok      bool
	}{
		{proto.NewGet(proto.Key("a")), true, true},
		{proto.NewGet(proto.Key("a")), false, true},
		{proto.NewPut(proto.Key("a"), proto.Value{}), false, true},
		{proto.NewPut(proto.Key("a"), proto.Value{}), true, false},
	} {
		{
			txn := newTxn(s.Clock, proto.Key("a"))
			txn.Writing = tc.writing
			tc.args.Header().Txn = txn
		}
		reply, err := batchutil.SendWrapped(s.Sender, tc.args)
		if err == nil != tc.ok {
			t.Errorf("%d: %T (writing=%t): success_expected=%t, but got: %v",
				i, tc.args, tc.writing, tc.ok, err)
		}
		if err != nil {
			continue
		}

		txn := reply.Header().Txn
		// The transaction should come back rw if it started rw or if we just
		// wrote.
		isWrite := proto.IsTransactionWrite(tc.args)
		if (tc.writing || isWrite) != txn.Writing {
			t.Errorf("%d: unexpected writing state: %s", i, txn)
		}
		if !isWrite {
			continue
		}
		// Abort for clean shutdown.
		if _, err := batchutil.SendWrapped(s.Sender, &proto.EndTransactionRequest{
			RequestHeader: proto.RequestHeader{
				Key:       txn.Key,
				Timestamp: txn.Timestamp,
				Txn:       txn,
			},
			Commit: false,
		}); err != nil {
			t.Fatal(err)
		}
	}
}
