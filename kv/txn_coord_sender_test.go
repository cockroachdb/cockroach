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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
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
func makeTS(walltime int64, logical int32) roachpb.Timestamp {
	return roachpb.Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

// newTxn begins a transaction. For testing purposes, this comes with a ID
// pre-initialized, but with the Writing flag set to false.
func newTxn(clock *hlc.Clock, baseKey roachpb.Key) *roachpb.Transaction {
	f, l, fun := caller.Lookup(1)
	name := fmt.Sprintf("%s:%d %s", f, l, fun)
	txn := roachpb.NewTransaction("test", baseKey, 1, roachpb.SERIALIZABLE, clock.Now(), clock.MaxOffset().Nanoseconds())
	txn.Name = name
	return txn
}

// createPutRequest returns a ready-made request using the
// specified key, value & txn ID.
func createPutRequest(key roachpb.Key, value []byte, txn *roachpb.Transaction) (*roachpb.PutRequest, roachpb.Header) {
	h := roachpb.Header{}
	h.Txn = txn
	return &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
		Value: roachpb.Value{Bytes: value},
	}, h
}

func createDeleteRangeRequest(key, endKey roachpb.Key, txn *roachpb.Transaction) (*roachpb.DeleteRangeRequest, roachpb.Header) {
	h := roachpb.Header{}
	h.Txn = txn
	return &roachpb.DeleteRangeRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    key,
			EndKey: endKey,
		},
	}, h
}

// TestTxnCoordSenderAddRequest verifies adding a request creates a
// transaction metadata and adding multiple requests with same
// transaction ID updates the last update timestamp.
func TestTxnCoordSenderAddRequest(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn := newTxn(s.Clock, roachpb.Key("a"))
	put, h := createPutRequest(roachpb.Key("a"), []byte("value"), txn)

	// Put request will create a new transaction.
	reply, err := client.SendWrappedWith(s.Sender, nil, h, put)
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
	h.Txn.Writing = true
	if _, err := client.SendWrappedWith(s.Sender, nil, h, put); err != nil {
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

	key := roachpb.Key("key")
	reply, err := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
		UserPriority: proto.Int32(-10), // negative user priority is translated into positive priority
		Txn: &roachpb.Transaction{
			Name:      "test txn",
			Isolation: roachpb.SNAPSHOT,
		},
	}, &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	pr := reply.(*roachpb.PutResponse)
	if pr.Txn.Name != "test txn" {
		t.Errorf("expected txn name to be %q; got %q", "test txn", pr.Txn.Name)
	}
	if pr.Txn.Priority != 10 {
		t.Errorf("expected txn priority 10; got %d", pr.Txn.Priority)
	}
	if !bytes.Equal(pr.Txn.Key, key) {
		t.Errorf("expected txn Key to match %q != %q", key, pr.Txn.Key)
	}
	if pr.Txn.Isolation != roachpb.SNAPSHOT {
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

	reply, err := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
		UserPriority: proto.Int32(-10), // negative user priority is translated into positive priority
		Txn: &roachpb.Transaction{
			Name:      "test txn",
			Isolation: roachpb.SNAPSHOT,
			Priority:  11,
		},
	}, &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: roachpb.Key("key"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if prio := reply.(*roachpb.PutResponse).Txn.Priority; prio != 11 {
		t.Errorf("expected txn priority 11; got %d", prio)
	}
}

// TestTxnCoordSenderKeyRanges verifies that multiple requests to same or
// overlapping key ranges causes the coordinator to keep track only of
// the minimum number of ranges.
func TestTxnCoordSenderKeyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)
	ranges := []struct {
		start, end roachpb.Key
	}{
		{roachpb.Key("a"), roachpb.Key(nil)},
		{roachpb.Key("a"), roachpb.Key(nil)},
		{roachpb.Key("aa"), roachpb.Key(nil)},
		{roachpb.Key("b"), roachpb.Key(nil)},
		{roachpb.Key("aa"), roachpb.Key("c")},
		{roachpb.Key("b"), roachpb.Key("c")},
	}

	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)
	txn := newTxn(s.Clock, roachpb.Key("a"))

	for _, rng := range ranges {
		if rng.end != nil {
			delRangeReq, h := createDeleteRangeRequest(rng.start, rng.end, txn)
			if _, err := client.SendWrappedWith(s.Sender, nil, h, delRangeReq); err != nil {
				t.Fatal(err)
			}
		} else {
			putReq, h := createPutRequest(rng.start, []byte("value"), txn)
			if _, err := client.SendWrappedWith(s.Sender, nil, h, putReq); err != nil {
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

	txn1 := newTxn(s.Clock, roachpb.Key("a"))
	txn2 := newTxn(s.Clock, roachpb.Key("b"))
	put1, h := createPutRequest(roachpb.Key("a"), []byte("value"), txn1)
	if _, err := client.SendWrappedWith(s.Sender, nil, h, put1); err != nil {
		t.Fatal(err)
	}
	put2, h := createPutRequest(roachpb.Key("b"), []byte("value"), txn2)
	if _, err := client.SendWrappedWith(s.Sender, nil, h, put2); err != nil {
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

	initialTxn := newTxn(s.Clock, roachpb.Key("a"))
	put, h := createPutRequest(roachpb.Key("a"), []byte("value"), initialTxn)
	if reply, err := client.SendWrappedWith(s.Sender, nil, h, put); err != nil {
		t.Fatal(err)
	} else {
		*initialTxn = *reply.Header().Txn
	}

	// Verify 3 heartbeats.
	var heartbeatTS roachpb.Timestamp
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
func getTxn(coord *TxnCoordSender, txn *roachpb.Transaction) (bool, *roachpb.Transaction, error) {
	hb := &roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
	}
	reply, err := client.SendWrappedWith(coord, nil, roachpb.Header{
		Txn: txn,
	}, hb)
	if err != nil {
		return false, nil, err
	}
	return true, reply.(*roachpb.HeartbeatTxnResponse).Txn, nil
}

func verifyCleanup(key roachpb.Key, coord *TxnCoordSender, eng engine.Engine, t *testing.T) {
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

	txn := newTxn(s.Clock, roachpb.Key("a"))
	key := roachpb.Key("a")
	put, h := createPutRequest(key, []byte("value"), txn)
	reply, err := client.SendWrappedWith(s.Sender, nil, h, put)
	if err != nil {
		t.Fatal(err)
	}
	pReply := reply.(*roachpb.PutResponse)
	if _, err := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
		Txn: pReply.Header().Txn,
	}, &roachpb.EndTransactionRequest{Commit: true}); err != nil {
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
	key := roachpb.Key("a")
	txn := newTxn(s.Clock, key)
	txn.Priority = 1
	put, h := createPutRequest(key, []byte("value"), txn)
	if reply, err := client.SendWrappedWith(s.Sender, nil, h, put); err != nil {
		t.Fatal(err)
	} else {
		txn = reply.Header().Txn
	}

	// Push the transaction to abort it.
	txn2 := newTxn(s.Clock, key)
	txn2.Priority = 2
	pushArgs := &roachpb.PushTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now:       s.Clock.Now(),
		PusherTxn: *txn2,
		PusheeTxn: *txn,
		PushType:  roachpb.ABORT_TXN,
	}
	if _, err := client.SendWrapped(s.Sender, nil, pushArgs); err != nil {
		t.Fatal(err)
	}

	// Now end the transaction and verify we've cleanup up, even though
	// end transaction failed.
	etArgs := &roachpb.EndTransactionRequest{
		Commit: true,
	}
	_, err := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
		Txn: txn,
	}, etArgs)
	switch err.(type) {
	case *roachpb.TransactionAbortedError:
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

	txn := newTxn(s.Clock, roachpb.Key("a"))
	put, h := createPutRequest(roachpb.Key("a"), []byte("value"), txn)
	if _, err := client.SendWrappedWith(s.Sender, nil, h, put); err != nil {
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
		expTS     roachpb.Timestamp
		expOrigTS roachpb.Timestamp
		nodeSeen  bool
	}{
		{nil, 0, 1, makeTS(0, 1), makeTS(0, 1), false},
		{&roachpb.ReadWithinUncertaintyIntervalError{
			ExistingTimestamp: makeTS(10, 10)}, 1, 1, makeTS(10, 11),
			makeTS(10, 11), true},
		{&roachpb.TransactionAbortedError{Txn: roachpb.Transaction{
			Timestamp: makeTS(20, 10), Priority: 10}}, 0, 10, makeTS(20, 10),
			makeTS(0, 1), false},
		{&roachpb.TransactionPushError{PusheeTxn: roachpb.Transaction{
			Timestamp: makeTS(10, 10), Priority: int32(10)}}, 1, 9,
			makeTS(10, 11), makeTS(10, 11), false},
		{&roachpb.TransactionRetryError{Txn: roachpb.Transaction{
			Timestamp: makeTS(10, 10), Priority: int32(10)}}, 1, 10,
			makeTS(10, 10), makeTS(10, 10), false},
	}

	var testPutReq = &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: roachpb.Key("test-key"),
		},
	}

	for i, test := range testCases {
		stopper := stop.NewStopper()
		ts := NewTxnCoordSender(senderFn(func(_ context.Context, _ roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			return nil, roachpb.NewError(test.err)
		}), clock, false, nil, stopper)
		var reply *roachpb.PutResponse
		var err error
		{
			var r roachpb.Response
			if r, err = client.SendWrappedWith(ts, nil, roachpb.Header{
				UserPriority: proto.Int32(-1),
				Txn: &roachpb.Transaction{
					Name: "test txn",
				},
				Replica: roachpb.ReplicaDescriptor{
					NodeID: 12345,
				},
			}, proto.Clone(testPutReq).(roachpb.Request)); err != nil {
				t.Fatal(err)
			}
			reply = r.(*roachpb.PutResponse)
		}
		teardownHeartbeats(ts)
		stopper.Stop()

		if reflect.TypeOf(test.err) != reflect.TypeOf(err) {
			t.Fatalf("%d: expected %T; got %T: %v", i, test.err, err, err)
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

	txn := newTxn(s.Clock, roachpb.Key("a"))
	key := roachpb.Key("a")
	beginTxn := func() {
		put, h := createPutRequest(key, []byte("value"), txn)
		if reply, err := client.SendWrappedWith(s.Sender, nil, h, put); err != nil {
			t.Fatal(err)
		} else {
			txn = reply.Header().Txn
		}
	}
	endTxn := func() {
		if _, err := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
			Txn: txn,
		}, &roachpb.EndTransactionRequest{
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
	key = roachpb.Key("key")
	_, err := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
		Txn: &roachpb.Transaction{
			Name: "test txn",
		},
	}, &roachpb.PutRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: key,
		},
	})
	if _, ok := err.(*roachpb.NodeUnavailableError); !ok {
		teardownHeartbeats(s.Sender)
		t.Fatal(err)
	}
	close(done)
	<-s.Stopper.IsStopped()
}

// TestTxnMultipleCoord checks that a coordinator uses the Writing flag to
// enforce that only one coordinator can be used for transactional writes.
func TestTxnMultipleCoord(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	for i, tc := range []struct {
		args    roachpb.Request
		writing bool
		ok      bool
	}{
		{roachpb.NewGet(roachpb.Key("a")), true, true},
		{roachpb.NewGet(roachpb.Key("a")), false, true},
		{roachpb.NewPut(roachpb.Key("a"), roachpb.Value{}), false, true},
		{roachpb.NewPut(roachpb.Key("a"), roachpb.Value{}), true, false},
	} {
		txn := newTxn(s.Clock, roachpb.Key("a"))
		txn.Writing = tc.writing
		reply, err := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
			Txn: txn,
		}, tc.args)
		if err == nil != tc.ok {
			t.Errorf("%d: %T (writing=%t): success_expected=%t, but got: %v",
				i, tc.args, tc.writing, tc.ok, err)
		}
		if err != nil {
			continue
		}

		txn = reply.Header().Txn
		// The transaction should come back rw if it started rw or if we just
		// wrote.
		isWrite := roachpb.IsTransactionWrite(tc.args)
		if (tc.writing || isWrite) != txn.Writing {
			t.Errorf("%d: unexpected writing state: %s", i, txn)
		}
		if !isWrite {
			continue
		}
		// Abort for clean shutdown.
		if _, err := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
			Txn: txn,
		}, &roachpb.EndTransactionRequest{
			Commit: false,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestTxnCoordSenderSingleRoundtripTxn checks that a batch which completely
// holds the writing portion of a Txn (including EndTransaction) does not
// launch a heartbeat goroutine at all.
func TestTxnCoordSenderSingleRoundtripTxn(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(20)

	ts := NewTxnCoordSender(senderFn(func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return ba.CreateReply(), nil
	}), clock, false, nil, stopper)

	// Stop the stopper manually, prior to trying the transaction. This has the
	// effect of returning a NodeUnavailableError for any attempts at launching
	// a heartbeat goroutine.
	stopper.Stop()

	var ba roachpb.BatchRequest
	put := &roachpb.PutRequest{}
	put.Key = roachpb.Key("test")
	ba.Add(put)
	ba.Add(&roachpb.EndTransactionRequest{})
	ba.Txn = &roachpb.Transaction{Name: "test"}
	_, pErr := ts.Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
}
