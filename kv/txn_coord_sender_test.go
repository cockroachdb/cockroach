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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
	"github.com/cockroachdb/cockroach/util/uuid"
)

// senderFn is a function that implements a Sender.
type senderFn func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

// Send implements batch.Sender.
func (f senderFn) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	return f(ctx, ba)
}

// teardownHeartbeats goes through the coordinator's active transactions and
// has the associated heartbeat tasks quit. This is useful for tests which
// don't finish transactions. This is safe to call multiple times.
func teardownHeartbeats(tc *TxnCoordSender) {
	if r := recover(); r != nil {
		panic(r)
	}
	tc.Lock()
	for _, tm := range tc.txns {
		if tm.txnEnd != nil {
			close(tm.txnEnd)
			tm.txnEnd = nil
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

// TestTxnCoordSenderAddRequest verifies adding a request creates a
// transaction metadata and adding multiple requests with same
// transaction ID updates the last update timestamp.
func TestTxnCoordSenderAddRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn := client.NewTxn(context.Background(), *s.DB)

	// Put request will create a new transaction.
	if err := txn.Put(roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	txnID := *txn.Proto.ID
	txnMeta, ok := s.Sender.txns[txnID]
	if !ok {
		t.Fatal("expected a transaction to be created on coordinator")
	}
	if !txn.Proto.Writing {
		t.Fatal("txn is not marked as writing")
	}
	ts := atomic.LoadInt64(&txnMeta.lastUpdateNanos)

	// Advance time and send another put request. Lock the coordinator
	// to prevent a data race.
	s.Sender.Lock()
	s.Manual.Set(1)
	s.Sender.Unlock()
	if err := txn.Put(roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if len(s.Sender.txns) != 1 {
		t.Errorf("expected length of transactions map to be 1; got %d", len(s.Sender.txns))
	}
	txnMeta = s.Sender.txns[txnID]
	if lu := atomic.LoadInt64(&txnMeta.lastUpdateNanos); ts >= lu || lu != s.Manual.UnixNano() {
		t.Errorf("expected last update time to advance; got %d", lu)
	}
}

// TestTxnCoordSenderBeginTransaction verifies that a command sent with a
// not-nil Txn with empty ID gets a new transaction initialized.
func TestTxnCoordSenderBeginTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn := client.NewTxn(context.Background(), *s.DB)

	// Put request will create a new transaction.
	key := roachpb.Key("key")
	txn.InternalSetPriority(10)
	txn.Proto.Isolation = roachpb.SNAPSHOT
	txn.Proto.Name = "test txn"
	if err := txn.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if txn.Proto.Name != "test txn" {
		t.Errorf("expected txn name to be %q; got %q", "test txn", txn.Proto.Name)
	}
	if txn.Proto.Priority != 10 {
		t.Errorf("expected txn priority 10; got %d", txn.Proto.Priority)
	}
	if !bytes.Equal(txn.Proto.Key, key) {
		t.Errorf("expected txn Key to match %q != %q", key, txn.Proto.Key)
	}
	if txn.Proto.Isolation != roachpb.SNAPSHOT {
		t.Errorf("expected txn isolation to be SNAPSHOT; got %s", txn.Proto.Isolation)
	}
}

// TestTxnInitialTimestamp verifies that the timestamp requested
// before the Txn is created is honored.
func TestTxnInitialTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn := client.NewTxn(context.Background(), *s.DB)

	// Request a specific timestamp.
	refTimestamp := roachpb.Timestamp{WallTime: 42, Logical: 69}
	txn.Proto.OrigTimestamp = refTimestamp

	// Put request will create a new transaction.
	key := roachpb.Key("key")
	txn.InternalSetPriority(10)
	txn.Proto.Isolation = roachpb.SNAPSHOT
	txn.Proto.Name = "test txn"
	if err := txn.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if txn.Proto.OrigTimestamp != refTimestamp {
		t.Errorf("expected txn orig ts to be %s; got %s", refTimestamp, txn.Proto.OrigTimestamp)
	}
	if txn.Proto.Timestamp != refTimestamp {
		t.Errorf("expected txn ts to be %s; got %s", refTimestamp, txn.Proto.Timestamp)
	}
}

// TestTxnCoordSenderBeginTransactionMinPriority verifies that when starting
// a new transaction, a non-zero priority is treated as a minimum value.
func TestTxnCoordSenderBeginTransactionMinPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn := client.NewTxn(context.Background(), *s.DB)

	// Put request will create a new transaction.
	key := roachpb.Key("key")
	txn.InternalSetPriority(10)
	txn.Proto.Isolation = roachpb.SNAPSHOT
	txn.Proto.Priority = 11
	if err := txn.Put(key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if prio := txn.Proto.Priority; prio != 11 {
		t.Errorf("expected txn priority 11; got %d", prio)
	}
}

// TestTxnCoordSenderKeyRanges verifies that multiple requests to same or
// overlapping key ranges causes the coordinator to keep track only of
// the minimum number of ranges.
func TestTxnCoordSenderKeyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

	txn := client.NewTxn(context.Background(), *s.DB)
	for _, rng := range ranges {
		if rng.end != nil {
			if err := txn.DelRange(rng.start, rng.end); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := txn.Put(rng.start, []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
	}

	txnID := *txn.Proto.ID

	// Verify that the transaction metadata contains only two entries
	// in its "keys" range group. "a" and range "aa"-"c".
	txnMeta, ok := s.Sender.txns[txnID]
	if !ok {
		t.Fatalf("expected a transaction to be created on coordinator")
	}
	if txnMeta.keys.Len() != 2 {
		t.Errorf("expected 2 entries in keys range group; got %v", txnMeta.keys)
	}
}

// TestTxnCoordSenderMultipleTxns verifies correct operation with
// multiple outstanding transactions.
func TestTxnCoordSenderMultipleTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn1 := client.NewTxn(context.Background(), *s.DB)
	txn2 := client.NewTxn(context.Background(), *s.DB)

	if err := txn1.Put(roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := txn2.Put(roachpb.Key("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if len(s.Sender.txns) != 2 {
		t.Errorf("expected length of transactions map to be 2; got %d", len(s.Sender.txns))
	}
}

// TestTxnCoordSenderHeartbeat verifies periodic heartbeat of the
// transaction record.
func TestTxnCoordSenderHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	// Set heartbeat interval to 1ms for testing.
	s.Sender.heartbeatInterval = 1 * time.Millisecond

	initialTxn := client.NewTxn(context.Background(), *s.DB)
	if err := initialTxn.Put(roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	// Verify 3 heartbeats.
	var heartbeatTS roachpb.Timestamp
	for i := 0; i < 3; i++ {
		util.SucceedsSoon(t, func() error {
			ok, txn, pErr := getTxn(s.Sender, &initialTxn.Proto)
			if !ok || pErr != nil {
				t.Fatalf("got txn: %t: %s", ok, pErr)
			}
			// Advance clock by 1ns.
			// Locking the TxnCoordSender to prevent a data race.
			s.Sender.Lock()
			s.Manual.Increment(1)
			s.Sender.Unlock()
			if txn.LastHeartbeat != nil && heartbeatTS.Less(*txn.LastHeartbeat) {
				heartbeatTS = *txn.LastHeartbeat
				return nil
			}
			return util.Errorf("expected heartbeat")
		})
	}

	// Sneakily send an ABORT right to DistSender (bypassing TxnCoordSender).
	{
		var ba roachpb.BatchRequest
		ba.Add(&roachpb.EndTransactionRequest{
			Commit: false,
			Span:   roachpb.Span{Key: initialTxn.Proto.Key},
		})
		ba.Txn = &initialTxn.Proto
		if _, pErr := s.distSender.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	util.SucceedsSoon(t, func() error {
		s.Sender.Lock()
		defer s.Sender.Unlock()
		if txnMeta, ok := s.Sender.txns[*initialTxn.Proto.ID]; !ok {
			t.Fatal("transaction unregistered prematurely")
		} else if txnMeta.txn.Status != roachpb.ABORTED {
			return fmt.Errorf("transaction is not aborted")
		}
		return nil
	})

	// Trying to do something else should give us a TransactionAbortedError.
	_, pErr := initialTxn.Get("a")
	if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
		t.Fatalf("expected a TransactionAbortedError, but got %v (%T)", pErr, pErr.GetDetail())
	}
}

// getTxn fetches the requested key and returns the transaction info.
func getTxn(coord *TxnCoordSender, txn *roachpb.Transaction) (bool, *roachpb.Transaction, *roachpb.Error) {
	hb := &roachpb.HeartbeatTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
	}
	reply, pErr := client.SendWrappedWith(coord, nil, roachpb.Header{
		Txn: txn,
	}, hb)
	if pErr != nil {
		return false, nil, pErr
	}
	return true, reply.(*roachpb.HeartbeatTxnResponse).Txn, nil
}

func verifyCleanup(key roachpb.Key, coord *TxnCoordSender, eng engine.Engine, t *testing.T) {
	util.SucceedsSoon(t, func() error {
		coord.Lock()
		l := len(coord.txns)
		coord.Unlock()
		if l != 0 {
			return fmt.Errorf("expected empty transactions map; got %d", l)
		}
		meta := &engine.MVCCMetadata{}
		ok, _, _, err := eng.GetProto(engine.MakeMVCCMetadataKey(key), meta)
		if err != nil {
			return fmt.Errorf("error getting MVCC metadata: %s", err)
		}
		if ok && meta.Txn != nil {
			return fmt.Errorf("found unexpected write intent: %s", meta)
		}
		return nil
	})
}

// TestTxnCoordSenderEndTxn verifies that ending a transaction
// sends resolve write intent requests and removes the transaction
// from the txns map.
func TestTxnCoordSenderEndTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	// 4 cases: no deadline, past deadline, equal deadline, future deadline.
	for i := 0; i < 4; i++ {
		key := roachpb.Key("key: " + strconv.Itoa(i))
		txn := client.NewTxn(context.Background(), *s.DB)
		// Initialize the transaction
		if pErr := txn.Put(key, []byte("value")); pErr != nil {
			t.Fatal(pErr)
		}

		{
			var pErr *roachpb.Error
			switch i {
			case 0:
				// No deadline.
				pErr = txn.CommitOrCleanup()
			case 1:
				// Past deadline.
				pErr = txn.CommitBy(txn.Proto.Timestamp.Prev())
			case 2:
				// Equal deadline.
				pErr = txn.CommitBy(txn.Proto.Timestamp)
			case 3:
				// Future deadline.
				pErr = txn.CommitBy(txn.Proto.Timestamp.Next())
			}

			switch i {
			case 0:
				// No deadline.
				if pErr != nil {
					t.Error(pErr)
				}
			case 1:
				// Past deadline.
				if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
					t.Errorf("expected TransactionAbortedError but got %T: %s", pErr, pErr)
				}
			case 2:
				// Equal deadline.
				if pErr != nil {
					t.Error(pErr)
				}
			case 3:
				// Future deadline.
				if pErr != nil {
					t.Error(pErr)
				}
			}
		}
		verifyCleanup(key, s.Sender, s.Eng, t)
	}
}

// TestTxnCoordSenderAddIntentOnError verifies that intents are tracked if
// the transaction is, even on error.
func TestTxnCoordSenderAddIntentOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	// Create a transaction with intent at "a".
	key := roachpb.Key("x")
	txn := client.NewTxn(context.Background(), *s.DB)
	// Write so that the coordinator begins tracking this txn.
	if err := txn.Put("x", "y"); err != nil {
		t.Fatal(err)
	}
	err, ok := txn.CPut(key, []byte("x"), []byte("born to fail")).GetDetail().(*roachpb.ConditionFailedError)
	if !ok {
		t.Fatal(err)
	}
	s.Sender.Lock()
	txnID := *txn.Proto.ID
	intentSpans := collectIntentSpans(s.Sender.txns[txnID].keys)
	expSpans := []roachpb.Span{{Key: key, EndKey: []byte("")}}
	equal := !reflect.DeepEqual(intentSpans, expSpans)
	s.Sender.Unlock()
	if pErr := txn.Rollback(); pErr != nil {
		t.Fatal(pErr)
	}
	if !equal {
		t.Fatalf("expected stored intents %v, got %v", expSpans, intentSpans)
	}
}

// TestTxnCoordSenderCleanupOnAborted verifies that if a txn receives a
// TransactionAbortedError, the coordinator cleans up the transaction.
func TestTxnCoordSenderCleanupOnAborted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	// Create a transaction with intent at "a".
	key := roachpb.Key("a")
	txn1 := client.NewTxn(context.Background(), *s.DB)
	txn1.InternalSetPriority(1)
	if pErr := txn1.Put(key, []byte("value")); pErr != nil {
		t.Fatal(pErr)
	}

	// Push the transaction (by writing key "a" with higher priority) to abort it.
	txn2 := client.NewTxn(context.Background(), *s.DB)
	txn2.InternalSetPriority(2)
	if pErr := txn2.Put(key, []byte("value2")); pErr != nil {
		t.Fatal(pErr)
	}

	// Now end the transaction and verify we've cleanup up, even though
	// end transaction failed.
	pErr := txn1.CommitOrCleanup()
	switch pErr.GetDetail().(type) {
	case *roachpb.TransactionAbortedError:
		// Expected
	default:
		t.Fatalf("expected transaction aborted error; got %s", pErr)
	}
	if pErr := txn2.CommitOrCleanup(); pErr != nil {
		t.Fatal(pErr)
	}
	verifyCleanup(key, s.Sender, s.Eng, t)
}

// TestTxnCoordSenderGCTimeout verifies that the coordinator cleans up extant
// transactions and intents after the lastUpdateNanos exceeds the timeout.
func TestTxnCoordSenderGCTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	// Set heartbeat interval to 1ms for testing.
	s.Sender.heartbeatInterval = 1 * time.Millisecond

	txn := client.NewTxn(context.Background(), *s.DB)
	key := roachpb.Key("a")
	if pErr := txn.Put(key, []byte("value")); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, advance clock past the default client timeout.
	// Locking the TxnCoordSender to prevent a data race.
	s.Sender.Lock()
	s.Manual.Set(defaultClientTimeout.Nanoseconds() + 1)
	s.Sender.Unlock()

	txnID := *txn.Proto.ID

	util.SucceedsSoon(t, func() error {
		// Locking the TxnCoordSender to prevent a data race.
		s.Sender.Lock()
		_, ok := s.Sender.txns[txnID]
		s.Sender.Unlock()
		if ok {
			return util.Errorf("expected garbage collection")
		}
		return nil
	})

	verifyCleanup(key, s.Sender, s.Eng, t)
}

// TestTxnCoordSenderGCWithCancel verifies that the coordinator cleans up extant
// transactions and intents after transaction context is cancelled.
func TestTxnCoordSenderGCWithCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	// Set heartbeat interval to 1ms for testing.
	s.Sender.heartbeatInterval = 1 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	txn := client.NewTxn(ctx, *s.DB)
	key := roachpb.Key("a")
	if pErr := txn.Put(key, []byte("value")); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, advance clock past the default client timeout.
	// Locking the TxnCoordSender to prevent a data race.
	s.Sender.Lock()
	s.Manual.Set(defaultClientTimeout.Nanoseconds() + 1)
	s.Sender.Unlock()

	txnID := *txn.Proto.ID

	// Verify that the transaction is alive despite the timeout having been
	// exceeded.
	errStillActive := errors.New("transaction is still active")
	// TODO(dan): Figure out how to run the heartbeat manually instead of this.
	if err := util.RetryForDuration(1*time.Second, func() error {
		// Locking the TxnCoordSender to prevent a data race.
		s.Sender.Lock()
		_, ok := s.Sender.txns[txnID]
		s.Sender.Unlock()
		if !ok {
			return nil
		}
		meta := &engine.MVCCMetadata{}
		ok, _, _, err := s.Eng.GetProto(engine.MakeMVCCMetadataKey(key), meta)
		if err != nil {
			t.Fatalf("error getting MVCC metadata: %s", err)
		}
		if !ok || meta.Txn == nil {
			return nil
		}
		return errStillActive
	}); err != errStillActive {
		t.Fatalf("expected transaction to be active, got: %v", err)
	}

	// After the context is cancelled, the transaction should be cleaned up.
	cancel()
	util.SucceedsSoon(t, func() error {
		// Locking the TxnCoordSender to prevent a data race.
		s.Sender.Lock()
		_, ok := s.Sender.txns[txnID]
		s.Sender.Unlock()
		if ok {
			return util.Errorf("expected garbage collection")
		}
		return nil
	})

	verifyCleanup(key, s.Sender, s.Eng, t)
}

// TestTxnCoordSenderTxnUpdatedOnError verifies that errors adjust the
// response transaction's timestamp and priority as appropriate.
func TestTxnCoordSenderTxnUpdatedOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	origTS := makeTS(123, 0)
	plus10 := origTS.Add(10, 10)
	plus20 := plus10.Add(10, 0)
	testCases := []struct {
		pErr             *roachpb.Error
		expEpoch         uint32
		expPri           int32
		expTS, expOrigTS roachpb.Timestamp
		nodeSeen         bool
	}{
		{
			// No error, so nothing interesting either.
			pErr:      nil,
			expEpoch:  0,
			expPri:    1,
			expTS:     origTS,
			expOrigTS: origTS,
		},
		{
			// On uncertainty error, new epoch begins and node is seen.
			// Timestamp moves ahead of the existing write.
			pErr: func() *roachpb.Error {
				pErr := roachpb.NewErrorWithTxn(
					roachpb.NewReadWithinUncertaintyIntervalError(roachpb.ZeroTimestamp, roachpb.ZeroTimestamp),
					&roachpb.Transaction{})
				const nodeID = 1
				pErr.GetTxn().UpdateObservedTimestamp(nodeID, plus10)
				pErr.OriginNode = nodeID
				return pErr
			}(),
			expEpoch:  1,
			expPri:    1,
			expTS:     plus10,
			expOrigTS: plus10,
			nodeSeen:  true,
		},
		{
			// On abort, nothing changes but we get a new priority to use for
			// the next attempt.
			pErr: roachpb.NewErrorWithTxn(&roachpb.TransactionAbortedError{},
				&roachpb.Transaction{
					TxnMeta: roachpb.TxnMeta{Timestamp: plus20, Priority: 10},
				}),
			expPri: 10,
		},
		{
			// On failed push, new epoch begins just past the pushed timestamp.
			// Additionally, priority ratchets up to just below the pusher's.
			pErr: roachpb.NewErrorWithTxn(&roachpb.TransactionPushError{
				PusheeTxn: roachpb.Transaction{
					TxnMeta: roachpb.TxnMeta{Timestamp: plus10, Priority: int32(10)},
				},
			},
				&roachpb.Transaction{}),
			expEpoch:  1,
			expPri:    9,
			expTS:     plus10,
			expOrigTS: plus10,
		},
		{
			// On retry, restart with new epoch, timestamp and priority.
			pErr: roachpb.NewErrorWithTxn(&roachpb.TransactionRetryError{},
				&roachpb.Transaction{
					TxnMeta: roachpb.TxnMeta{Timestamp: plus10, Priority: int32(10)},
				},
			),
			expEpoch:  1,
			expPri:    10,
			expTS:     plus10,
			expOrigTS: plus10,
		},
	}

	for i, test := range testCases {
		stopper := stop.NewStopper()

		manual := hlc.NewManualClock(origTS.WallTime)
		clock := hlc.NewClock(manual.UnixNano)
		clock.SetMaxOffset(20)

		ts := NewTxnCoordSender(senderFn(func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			var reply *roachpb.BatchResponse
			if test.pErr == nil {
				reply = ba.CreateReply()
			}
			return reply, test.pErr
		}), clock, false, tracing.NewTracer(), stopper, NewTxnMetrics(metric.NewRegistry()))
		db := client.NewDB(ts)
		txn := client.NewTxn(context.Background(), *db)
		txn.InternalSetPriority(1)
		txn.Proto.Name = "test txn"
		key := roachpb.Key("test-key")
		_, pErr := txn.Get(key)
		teardownHeartbeats(ts)
		stopper.Stop()

		if reflect.TypeOf(test.pErr) != reflect.TypeOf(pErr) {
			t.Fatalf("%d: expected %T; got %T: %v", i, test.pErr, pErr, pErr)
		}
		if txn.Proto.Epoch != test.expEpoch {
			t.Errorf("%d: expected epoch = %d; got %d",
				i, test.expEpoch, txn.Proto.Epoch)
		}
		if txn.Proto.Priority != test.expPri {
			t.Errorf("%d: expected priority = %d; got %d",
				i, test.expPri, txn.Proto.Priority)
		}
		if !txn.Proto.Timestamp.Equal(test.expTS) {
			t.Errorf("%d: expected timestamp to be %s; got %s",
				i, test.expTS, txn.Proto.Timestamp)
		}
		if !txn.Proto.OrigTimestamp.Equal(test.expOrigTS) {
			t.Errorf("%d: expected orig timestamp to be %s; got %s",
				i, test.expOrigTS, txn.Proto.OrigTimestamp)
		}
		if ns := txn.Proto.ObservedTimestamps; (len(ns) != 0) != test.nodeSeen {
			t.Errorf("%d: expected nodeSeen=%t, but list of hosts is %v",
				i, test.nodeSeen, ns)
		}
	}
}

// TestTxnCoordIdempotentCleanup verifies that cleanupTxn is idempotent.
func TestTxnCoordIdempotentCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn := client.NewTxn(context.Background(), *s.DB)
	ba := txn.NewBatch()
	ba.Put(roachpb.Key("a"), []byte("value"))
	if pErr := txn.Run(ba); pErr != nil {
		t.Fatal(pErr)
	}

	s.Sender.Lock()
	s.Sender.cleanupTxnLocked(context.Background(), txn.Proto)
	s.Sender.Unlock()

	ba = txn.NewBatch()
	ba.InternalAddRequest(&roachpb.EndTransactionRequest{})
	pErr := txn.Run(ba)
	if pErr != nil && !testutils.IsPError(pErr, "aborted") {
		t.Fatal(pErr)
	}
}

// TestTxnMultipleCoord checks that a coordinator uses the Writing flag to
// enforce that only one coordinator can be used for transactional writes.
func TestTxnMultipleCoord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	testCases := []struct {
		args    roachpb.Request
		writing bool
		ok      bool
	}{
		{roachpb.NewGet(roachpb.Key("a")), true, false},
		{roachpb.NewGet(roachpb.Key("a")), false, true},
		{roachpb.NewPut(roachpb.Key("a"), roachpb.Value{}), false, false}, // transactional write before begin
		{roachpb.NewPut(roachpb.Key("a"), roachpb.Value{}), true, false},  // must have switched coordinators
	}

	for i, tc := range testCases {
		txn := roachpb.NewTransaction("test", roachpb.Key("a"), 1, roachpb.SERIALIZABLE,
			s.Clock.Now(), s.Clock.MaxOffset().Nanoseconds())
		txn.Writing = tc.writing
		reply, pErr := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
			Txn: txn,
		}, tc.args)
		if pErr == nil != tc.ok {
			t.Errorf("%d: %T (writing=%t): success_expected=%t, but got: %v",
				i, tc.args, tc.writing, tc.ok, pErr)
		}
		if pErr != nil {
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
		if _, pErr := client.SendWrappedWith(s.Sender, nil, roachpb.Header{
			Txn: txn,
		}, &roachpb.EndTransactionRequest{
			Commit: false,
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}
}

// TestTxnCoordSenderSingleRoundtripTxn checks that a batch which completely
// holds the writing portion of a Txn (including EndTransaction) does not
// launch a heartbeat goroutine at all.
func TestTxnCoordSenderSingleRoundtripTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(20)

	ts := NewTxnCoordSender(senderFn(func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		txnClone := ba.Txn.Clone()
		br.Txn = &txnClone
		br.Txn.Writing = true
		return br, nil
	}), clock, false, tracing.NewTracer(), stopper, NewTxnMetrics(metric.NewRegistry()))

	// Stop the stopper manually, prior to trying the transaction. This has the
	// effect of returning a NodeUnavailableError for any attempts at launching
	// a heartbeat goroutine.
	stopper.Stop()

	var ba roachpb.BatchRequest
	key := roachpb.Key("test")
	ba.Add(&roachpb.BeginTransactionRequest{Span: roachpb.Span{Key: key}})
	ba.Add(&roachpb.PutRequest{Span: roachpb.Span{Key: key}})
	ba.Add(&roachpb.EndTransactionRequest{})
	ba.Txn = &roachpb.Transaction{Name: "test"}
	_, pErr := ts.Send(context.Background(), ba)
	if pErr != nil {
		t.Fatal(pErr)
	}
}

// TestTxnCoordSenderErrorWithIntent validates that if a transactional request
// returns an error but also indicates a Writing transaction, the coordinator
// tracks it just like a successful request.
func TestTxnCoordSenderErrorWithIntent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	clock.SetMaxOffset(20)

	testCases := []struct {
		roachpb.Error
		errMsg string
	}{
		{*roachpb.NewError(roachpb.NewTransactionRetryError()), "retry txn"},
		{*roachpb.NewError(roachpb.NewTransactionPushError(roachpb.Transaction{
			TxnMeta: roachpb.TxnMeta{
				ID: uuid.NewV4(),
			}})), "failed to push"},
		{*roachpb.NewErrorf("testError"), "testError"},
	}
	for i, test := range testCases {
		func() {
			ts := NewTxnCoordSender(senderFn(func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				txn := ba.Txn.Clone()
				txn.Writing = true
				pErr := &roachpb.Error{}
				*pErr = test.Error
				pErr.SetTxn(&txn)
				return nil, pErr
			}), clock, false, tracing.NewTracer(), stopper, NewTxnMetrics(metric.NewRegistry()))

			var ba roachpb.BatchRequest
			key := roachpb.Key("test")
			ba.Add(&roachpb.BeginTransactionRequest{Span: roachpb.Span{Key: key}})
			ba.Add(&roachpb.PutRequest{Span: roachpb.Span{Key: key}})
			ba.Add(&roachpb.EndTransactionRequest{})
			ba.Txn = &roachpb.Transaction{Name: "test"}
			_, pErr := ts.Send(context.Background(), ba)
			if !testutils.IsPError(pErr, test.errMsg) {
				t.Errorf("%d: error did not match %s: %v", i, test.errMsg, pErr)
			}

			defer teardownHeartbeats(ts)
			ts.Lock()
			defer ts.Unlock()
			if len(ts.txns) != 1 {
				t.Errorf("%d: expected transaction to be tracked", i)
			}
		}()
	}
}

// TestTxnCoordSenderReleaseTxnMeta verifies that TxnCoordSender releases the
// txnMetadata after the txn has committed successfully.
func TestTxnCoordSenderReleaseTxnMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(s.Sender)

	txn := client.NewTxn(context.Background(), *s.DB)
	ba := txn.NewBatch()
	ba.Put(roachpb.Key("a"), []byte("value"))
	ba.Put(roachpb.Key("b"), []byte("value"))
	if err := txn.CommitInBatch(ba); err != nil {
		t.Fatal(err)
	}

	txnID := *txn.Proto.ID

	if _, ok := s.Sender.txns[txnID]; ok {
		t.Fatal("expected TxnCoordSender has released the txn")
	}
}

// TestTxnCoordSenderNoDuplicateIntents verifies that TxnCoordSender does not
// generate duplicate intents and that it merges intents for overlapping ranges.
func TestTxnCoordSenderNoDuplicateIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	manual := hlc.NewManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)

	var expectedIntents []roachpb.Span

	senderFunc := func(_ context.Context, ba roachpb.BatchRequest) (
		*roachpb.BatchResponse, *roachpb.Error) {
		if rArgs, ok := ba.GetArg(roachpb.EndTransaction); ok {
			et := rArgs.(*roachpb.EndTransactionRequest)
			if !reflect.DeepEqual(et.IntentSpans, expectedIntents) {
				t.Errorf("Invalid intents: %+v; expected %+v", et.IntentSpans, expectedIntents)
			}
		}
		br := ba.CreateReply()
		txnClone := ba.Txn.Clone()
		br.Txn = &txnClone
		br.Txn.Writing = true
		return br, nil
	}
	ts := NewTxnCoordSender(senderFn(senderFunc), clock, false, tracing.NewTracer(), stopper,
		NewTxnMetrics(metric.NewRegistry()))

	defer stopper.Stop()
	defer teardownHeartbeats(ts)

	db := client.NewDB(ts)
	txn := client.NewTxn(context.Background(), *db)

	// Write to a, b, u-w before the final batch.

	pErr := txn.Put(roachpb.Key("a"), []byte("value"))
	if pErr != nil {
		t.Fatal(pErr)
	}
	pErr = txn.Put(roachpb.Key("b"), []byte("value"))
	if pErr != nil {
		t.Fatal(pErr)
	}
	pErr = txn.DelRange(roachpb.Key("u"), roachpb.Key("w"))
	if pErr != nil {
		t.Fatal(pErr)
	}

	// The final batch overwrites key a and overlaps part of the u-w range.
	b := txn.NewBatch()
	b.Put(roachpb.Key("b"), []byte("value"))
	b.Put(roachpb.Key("c"), []byte("value"))
	b.DelRange(roachpb.Key("v"), roachpb.Key("z"), false)

	// The expected intents are a, b, c, and u-z.
	expectedIntents = []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: nil},
		{Key: roachpb.Key("b"), EndKey: nil},
		{Key: roachpb.Key("c"), EndKey: nil},
		{Key: roachpb.Key("u"), EndKey: roachpb.Key("z")},
	}

	pErr = txn.CommitInBatch(b)
	if pErr != nil {
		t.Fatal(pErr)
	}
}

// checkTxnMetrics verifies that the provided Sender's transaction metrics match the expected
// values. This is done through a series of retries with increasing backoffs, to work around
// the TxnCoordSender's asynchronous updating of metrics after a transaction ends.
func checkTxnMetrics(t *testing.T, sender *TxnCoordSender, name string,
	commits, commits1PC, abandons, aborts, restarts int64) {
	metrics := sender.metrics

	util.SucceedsSoon(t, func() error {
		testcases := []struct {
			name string
			a, e int64
		}{
			{"commits", metrics.Commits.Count(), commits},
			{"commits1PC", metrics.Commits1PC.Count(), commits1PC},
			{"abandons", metrics.Abandons.Count(), abandons},
			{"aborts", metrics.Aborts.Count(), aborts},
			{"durations", metrics.Durations[metric.Scale1M].Current().TotalCount(),
				commits + abandons + aborts},
		}

		for _, tc := range testcases {
			if tc.a != tc.e {
				return util.Errorf("%s: actual %s %d != expected %d", name, tc.name, tc.a, tc.e)
			}
		}

		// Handle restarts separately, because that's a histogram. Though the histogram is approximate,
		// we're recording so few distinct values that we should be okay.
		dist := metrics.Restarts.Current().Distribution()
		var actualRestarts int64
		for _, b := range dist {
			if b.From == b.To {
				actualRestarts += b.From * b.Count
			} else {
				t.Fatalf("unexpected value in histogram: %d-%d", b.From, b.To)
			}
		}
		if a, e := actualRestarts, restarts; a != e {
			return util.Errorf("%s: actual restarts %d != expected %d", name, a, e)
		}

		return nil
	})
}

// setupMetricsTest returns a TxnCoordSender and ManualClock pointing to a newly created
// LocalTestCluster. Also returns a cleanup function to be executed at the end of the
// test.
func setupMetricsTest(t *testing.T) (*hlc.ManualClock, *TxnCoordSender, func()) {
	s := createTestDB(t)
	reg := metric.NewRegistry()
	txnMetrics := NewTxnMetrics(reg)
	sender := NewTxnCoordSender(s.distSender, s.Clock, false, tracing.NewTracer(), s.Stopper, txnMetrics)

	return s.Manual, sender, func() {
		teardownHeartbeats(sender)
		s.Stop()
	}
}

// Test a normal transaction. This and the other metrics tests below use real KV operations,
// because it took far too much mucking with TxnCoordSender internals to mock out the sender
// function as other tests do.
func TestTxnCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")
	db := client.NewDB(sender)

	// Test normal commit.
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		key := []byte("key-commit")

		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			return roachpb.NewError(err)
		}

		if pErr := txn.Put(key, value); pErr != nil {
			return pErr
		}

		if pErr := txn.CommitOrCleanup(); pErr != nil {
			return pErr
		}

		return nil
	}); pErr != nil {
		t.Fatal(pErr)
	}
	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "commit txn", 1, 0 /* not 1PC */, 0, 0, 0)
}

// TestTxnOnePhaseCommit verifies that 1PC metric tracking works.
func TestTxnOnePhaseCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")
	db := client.NewDB(sender)

	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		key := []byte("key-commit")
		b := txn.NewBatch()
		b.Put(key, value)
		return txn.CommitInBatch(b)
	}); pErr != nil {
		t.Fatal(pErr)
	}
	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "commit 1PC txn", 1, 1 /* 1PC */, 0, 0, 0)
}

func TestTxnAbandonCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")
	db := client.NewDB(sender)

	// Test abandoned transaction by making the client timeout ridiculously short. We also set
	// the sender to heartbeat very frequently, because the heartbeat detects and tears down
	// abandoned transactions.
	sender.heartbeatInterval = 2 * time.Millisecond
	sender.clientTimeout = 1 * time.Millisecond
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		key := []byte("key-abandon")

		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			return roachpb.NewError(err)
		}

		if pErr := txn.Put(key, value); pErr != nil {
			return pErr
		}

		manual.Increment(int64(sender.clientTimeout + sender.heartbeatInterval*2))

		checkTxnMetrics(t, sender, "abandon txn", 0, 0, 1, 0, 0)

		return nil
	}); !testutils.IsPError(pErr, "writing transaction timed out") {
		t.Fatalf("unexpected error: %s", pErr)
	}
}

// TestTxnReadAfterAbandon checks the fix for the condition in issue #4787:
// after a transaction is abandoned we do a read as part of that transaction
// which should fail.
func TestTxnReadAfterAbandon(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")
	db := client.NewDB(sender)

	// Test abandoned transaction by making the client timeout ridiculously short. We also set
	// the sender to heartbeat very frequently, because the heartbeat detects and tears down
	// abandoned transactions.
	sender.heartbeatInterval = 2 * time.Millisecond
	sender.clientTimeout = 1 * time.Millisecond

	pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		key := []byte("key-abandon")

		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			t.Fatal(err)
		}

		if pErr := txn.Put(key, value); pErr != nil {
			t.Fatal(pErr)
		}

		manual.Increment(int64(sender.clientTimeout + sender.heartbeatInterval*2))

		checkTxnMetrics(t, sender, "abandon txn", 0, 0, 1, 0, 0)

		_, pErr := txn.Get(key)
		if pErr == nil {
			t.Fatalf("Get succeeded on abandoned txn")
		} else if !testutils.IsPError(pErr, "writing transaction timed out") {
			t.Fatalf("unexpected error from Get on abandoned txn: %s", pErr)
		}
		return pErr
	})

	if pErr == nil {
		t.Fatalf("abandoned txn didn't fail")
	}
}

func TestTxnAbortCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()

	value := []byte("value")
	db := client.NewDB(sender)

	intentionalErrText := "intentional error to cause abort"
	// Test aborted transaction.
	if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
		key := []byte("key-abort")

		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			return roachpb.NewError(err)
		}

		if pErr := txn.Put(key, value); pErr != nil {
			t.Fatal(pErr)
		}

		return roachpb.NewErrorf(intentionalErrText)
	}); !testutils.IsPError(pErr, intentionalErrText) {
		t.Fatalf("unexpected error: %s", pErr)
	}
	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "abort txn", 0, 0, 0, 1, 0)
}

func TestTxnRestartCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()

	key := []byte("key-restart")
	value := []byte("value")
	db := client.NewDB(sender)

	// Start a transaction and do a GET. This forces a timestamp to be chosen for the transaction.
	txn := client.NewTxn(context.Background(), *db)
	if _, err := txn.Get(key); err != nil {
		t.Fatal(err)
	}

	// Outside of the transaction, read the same key as was read within the transaction. This
	// means that future attempts to write will increase the timestamp.
	if _, err := db.Get(key); err != nil {
		t.Fatal(err)
	}

	// This put will lay down an intent, txn timestamp will increase beyond original.
	if err := txn.Put(key, value); err != nil {
		t.Fatal(err)
	}
	if !txn.Proto.OrigTimestamp.Less(txn.Proto.Timestamp) {
		t.Errorf("expected timestamp to increase: %s", txn.Proto)
	}

	// Commit (should cause restart metric to increase).
	pErr := txn.CommitOrCleanup()
	if _, ok := pErr.GetDetail().(*roachpb.TransactionRetryError); !ok {
		t.Errorf("expected transaction retry err; got %s", pErr)
	}

	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "restart txn", 0, 0, 0, 1, 1)
}

func TestTxnDurations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()

	db := client.NewDB(sender)
	const puts = 10

	const incr int64 = 1000
	for i := 0; i < puts; i++ {
		key := roachpb.Key(fmt.Sprintf("key-txn-durations-%d", i))
		if pErr := db.Txn(func(txn *client.Txn) *roachpb.Error {
			if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
				return roachpb.NewError(err)
			}
			if err := txn.Put(key, []byte("val")); err != nil {
				return err
			}
			manual.Increment(incr)
			return nil
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}

	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "txn durations", puts, 0, 0, 0, 0)

	hist := sender.metrics.Durations[metric.Scale1M].Current()

	// The clock is a bit odd in these tests, so I can't test the mean without introducing
	// spurious errors or being overly lax.
	// TODO(cdo): look into cause of variance.
	if a, e := hist.TotalCount(), int64(puts); a != e {
		t.Fatalf("durations %d != expected %d", a, e)
	}

	if min := hist.Min(); min < incr {
		t.Fatalf("min %d < %d", min, incr)
	}
}

// We rely on context.Background() having a nil Done() channel. It's documented
// as "Done may return nil if this context can never be canceled", so we check
// that the "may" is actually "will".
//
// TODO(dan): If this ever breaks, we can do something with the context values:
// type contextLifetimeKey struct{}
// func SetIsContextLifetime(ctx) context.Context {
//   return context.WithValue(ctx, contextLifetimeKey{}, struct{}{})
// }
// func HasContextLifetime(ctx) bool {
//   _, ok := ctx.Value(contextLifetimeKey{})
//   return ok
// }
//
// In TxnCoordSender:
// func heartbeatLoop(ctx context.Context, ...) {
//   if !HasContextLifetime(ctx) && txnMeta.hasClientAbandonedCoord ... {
//     tc.tryAbort(txnID)
//     return
//   }
// }
func TestContextDoneNil(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if context.Background().Done() != nil {
		t.Error("context.Background().Done()'s behavior has changed")
	}
}
