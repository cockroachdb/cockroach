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
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// teardownHeartbeats goes through the coordinator's active transactions and
// has the associated heartbeat tasks quit. This is useful for tests which
// don't finish transactions. This is safe to call multiple times.
func teardownHeartbeats(tc *TxnCoordSender) {
	if r := recover(); r != nil {
		panic(r)
	}
	tc.txnMu.Lock()
	for _, tm := range tc.txnMu.txns {
		if tm.txnEnd != nil {
			close(tm.txnEnd)
			tm.txnEnd = nil
		}
	}
	defer tc.txnMu.Unlock()
}

// createTestDB creates a local test server and starts it. The caller
// is responsible for stopping the test server.
func createTestDB(t testing.TB) (*localtestcluster.LocalTestCluster, *TxnCoordSender) {
	return createTestDBWithContext(t, client.DefaultDBContext())
}

func createTestDBWithContext(
	t testing.TB, dbCtx client.DBContext,
) (*localtestcluster.LocalTestCluster, *TxnCoordSender) {
	s := &localtestcluster.LocalTestCluster{
		DBContext: &dbCtx,
	}
	s.Start(t, testutils.NewNodeTestBaseContext(), InitSenderForLocalTestCluster)
	return s, s.Sender.(*TxnCoordSender)
}

// makeTS creates a new timestamp.
func makeTS(walltime int64, logical int32) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: walltime,
		Logical:  logical,
	}
}

// TestTxnCoordSenderAddRequest verifies adding a request creates a
// transaction metadata and adding multiple requests with same
// transaction ID updates the last update timestamp.
func TestTxnCoordSenderAddRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	txn := client.NewTxn(s.DB)

	// Put request will create a new transaction.
	if err := txn.Put(context.TODO(), roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	txnID := *txn.Proto().ID
	txnMeta, ok := sender.txnMu.txns[txnID]
	if !ok {
		t.Fatal("expected a transaction to be created on coordinator")
	}
	if !txn.Proto().Writing {
		t.Fatal("txn is not marked as writing")
	}
	ts := txnMeta.getLastUpdate()

	// Advance time and send another put request. Lock the coordinator
	// to prevent a data race.
	sender.txnMu.Lock()
	s.Manual.Increment(1)
	sender.txnMu.Unlock()
	if err := txn.Put(context.TODO(), roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if len(sender.txnMu.txns) != 1 {
		t.Errorf("expected length of transactions map to be 1; got %d", len(sender.txnMu.txns))
	}
	txnMeta = sender.txnMu.txns[txnID]
	if lu := txnMeta.getLastUpdate(); ts >= lu {
		t.Errorf("expected last update time to advance past %d; got %d", ts, lu)
	} else if un := s.Manual.UnixNano(); lu != un {
		t.Errorf("expected last update time to equal %d; got %d", un, lu)
	}
}

// TestTxnCoordSenderAddRequestConcurrently verifies adding concurrent requests
// creates a transaction metadata and adding multiple requests with same
// transaction ID updates the last update timestamp.
func TestTxnCoordSenderAddRequestConcurrently(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	txn := client.NewTxn(s.DB)

	// Put requests will create a new transaction.
	sendRequests := func() error {
		// NB: we can't use errgroup.WithContext here because s.DB uses the first
		// request's context (if it's cancellable) to determine when the
		// transaction is abandoned. Since errgroup.Group.Wait cancels its
		// context, this would cause the transaction to have been aborted when
		// this function is called for the second time. See this TODO from
		// txn_coord_sender.go:
		//
		// TODO(dan): The Context we use for this is currently the one from the
		// first request in a Txn, but the semantics of this aren't good. Each
		// context has its own associated lifetime and we're ignoring all but the
		// first. It happens now that we pass the same one in every request, but
		// it's brittle to rely on this forever.
		var g errgroup.Group
		for i := 0; i < 30; i++ {
			key := roachpb.Key("a" + strconv.Itoa(i))
			g.Go(func() error {
				return txn.Put(context.Background(), key, []byte("value"))
			})
		}
		return g.Wait()
	}
	if err := sendRequests(); err != nil {
		t.Fatal(err)
	}
	txnID := *txn.Proto().ID
	txnMeta, ok := sender.txnMu.txns[txnID]
	if !ok {
		t.Fatal("expected a transaction to be created on coordinator")
	}
	if !txn.Proto().Writing {
		t.Fatal("txn is not marked as writing")
	}
	ts := txnMeta.getLastUpdate()

	// Advance time and send more put requests. Lock the coordinator
	// to prevent a data race.
	sender.txnMu.Lock()
	s.Manual.Increment(1)
	sender.txnMu.Unlock()
	if err := sendRequests(); err != nil {
		t.Fatal(err)
	}
	if len(sender.txnMu.txns) != 1 {
		t.Errorf("expected length of transactions map to be 1; got %d", len(sender.txnMu.txns))
	}
	txnMeta = sender.txnMu.txns[txnID]
	if lu := txnMeta.getLastUpdate(); ts >= lu {
		t.Errorf("expected last update time to advance past %d; got %d", ts, lu)
	} else if un := s.Manual.UnixNano(); lu != un {
		t.Errorf("expected last update time to equal %d; got %d", un, lu)
	}
}

// TestTxnCoordSenderBeginTransaction verifies that a command sent with a
// not-nil Txn with empty ID gets a new transaction initialized.
func TestTxnCoordSenderBeginTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	txn := client.NewTxn(s.DB)

	// Put request will create a new transaction.
	key := roachpb.Key("key")
	txn.InternalSetPriority(10)
	txn.SetDebugName("test txn")
	if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put(context.Background(), key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if txn.Proto().Name != "test txn" {
		t.Errorf("expected txn name to be %q; got %q", "test txn", txn.Proto().Name)
	}
	if txn.Proto().Priority != 10 {
		t.Errorf("expected txn priority 10; got %d", txn.Proto().Priority)
	}
	if !bytes.Equal(txn.Proto().Key, key) {
		t.Errorf("expected txn Key to match %q != %q", key, txn.Proto().Key)
	}
	if txn.Proto().Isolation != enginepb.SNAPSHOT {
		t.Errorf("expected txn isolation to be SNAPSHOT; got %s", txn.Proto().Isolation)
	}
}

// TestTxnInitialTimestamp verifies that the timestamp requested
// before the Txn is created is honored.
func TestTxnInitialTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	txn := client.NewTxn(s.DB)

	// Request a specific timestamp.
	refTimestamp := s.Clock.Now().Add(42, 69)
	txn.Proto().OrigTimestamp = refTimestamp

	// Put request will create a new transaction.
	key := roachpb.Key("key")
	txn.InternalSetPriority(10)
	txn.SetDebugName("test txn")
	if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put(context.TODO(), key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if txn.Proto().OrigTimestamp != refTimestamp {
		t.Errorf("expected txn orig ts to be %s; got %s", refTimestamp, txn.Proto().OrigTimestamp)
	}
	if txn.Proto().Timestamp != refTimestamp {
		t.Errorf("expected txn ts to be %s; got %s", refTimestamp, txn.Proto().Timestamp)
	}
}

// TestTxnCoordSenderBeginTransactionMinPriority verifies that when starting
// a new transaction, a non-zero priority is treated as a minimum value.
func TestTxnCoordSenderBeginTransactionMinPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	txn := client.NewTxn(s.DB)

	// Put request will create a new transaction.
	key := roachpb.Key("key")
	txn.InternalSetPriority(10)
	txn.Proto().Priority = 11
	if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
		t.Fatal(err)
	}
	if err := txn.Put(context.Background(), key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	if prio := txn.Proto().Priority; prio != 11 {
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

	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	txn := client.NewTxn(s.DB)
	for _, rng := range ranges {
		if rng.end != nil {
			if err := txn.DelRange(context.TODO(), rng.start, rng.end); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := txn.Put(context.TODO(), rng.start, []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
	}

	txnID := *txn.Proto().ID

	// Verify that the transaction metadata contains only two entries
	// in its "keys" range group. "a" and range "aa"-"c".
	txnMeta, ok := sender.txnMu.txns[txnID]
	if !ok {
		t.Fatalf("expected a transaction to be created on coordinator")
	}
	keys, _ := roachpb.MergeSpans(txnMeta.keys)
	if len(keys) != 2 {
		t.Errorf("expected 2 entries in keys range group; got %v", keys)
	}
}

// TestTxnCoordSenderMultipleTxns verifies correct operation with
// multiple outstanding transactions.
func TestTxnCoordSenderMultipleTxns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	txn1 := client.NewTxn(s.DB)
	txn2 := client.NewTxn(s.DB)

	if err := txn1.Put(context.TODO(), roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := txn2.Put(context.TODO(), roachpb.Key("b"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	if len(sender.txnMu.txns) != 2 {
		t.Errorf("expected length of transactions map to be 2; got %d", len(sender.txnMu.txns))
	}
}

// TestTxnCoordSenderHeartbeat verifies periodic heartbeat of the
// transaction record.
func TestTxnCoordSenderHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	// Set heartbeat interval to 1ms for testing.
	sender.heartbeatInterval = 1 * time.Millisecond

	initialTxn := client.NewTxn(s.DB)
	if err := initialTxn.Put(context.TODO(), roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	// Verify 3 heartbeats.
	var heartbeatTS hlc.Timestamp
	for i := 0; i < 3; i++ {
		testutils.SucceedsSoon(t, func() error {
			txn, pErr := getTxn(sender, initialTxn.Proto())
			if pErr != nil {
				t.Fatal(pErr)
			}
			// Advance clock by 1ns.
			// Locking the TxnCoordSender to prevent a data race.
			sender.txnMu.Lock()
			s.Manual.Increment(1)
			sender.txnMu.Unlock()
			if lastActive := txn.LastActive(); heartbeatTS.Less(lastActive) {
				heartbeatTS = lastActive
				return nil
			}
			return errors.Errorf("expected heartbeat")
		})
	}

	// Sneakily send an ABORT right to DistSender (bypassing TxnCoordSender).
	{
		var ba roachpb.BatchRequest
		ba.Add(&roachpb.EndTransactionRequest{
			Commit: false,
			Span:   roachpb.Span{Key: initialTxn.Proto().Key},
		})
		ba.Txn = initialTxn.Proto()
		if _, pErr := sender.wrapped.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	testutils.SucceedsSoon(t, func() error {
		sender.txnMu.Lock()
		defer sender.txnMu.Unlock()
		if txnMeta, ok := sender.txnMu.txns[*initialTxn.Proto().ID]; !ok {
			t.Fatal("transaction unregistered prematurely")
		} else if txnMeta.txn.Status != roachpb.ABORTED {
			return fmt.Errorf("transaction is not aborted")
		}
		return nil
	})

	// Trying to do something else should give us a TransactionAbortedError.
	_, err := initialTxn.Get(context.TODO(), "a")
	assertTransactionAbortedError(t, err)
}

// getTxn fetches the requested key and returns the transaction info.
func getTxn(
	coord *TxnCoordSender, txn *roachpb.Transaction,
) (*roachpb.Transaction, *roachpb.Error) {
	hb := &roachpb.HeartbeatTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
	}
	reply, pErr := client.SendWrappedWith(context.Background(), coord, roachpb.Header{
		Txn: txn,
	}, hb)
	if pErr != nil {
		return nil, pErr
	}
	return reply.(*roachpb.HeartbeatTxnResponse).Txn, nil
}

func verifyCleanup(key roachpb.Key, coord *TxnCoordSender, eng engine.Engine, t *testing.T) {
	testutils.SucceedsSoon(t, func() error {
		coord.txnMu.Lock()
		l := len(coord.txnMu.txns)
		coord.txnMu.Unlock()
		if l != 0 {
			return fmt.Errorf("expected empty transactions map; got %d", l)
		}
		meta := &enginepb.MVCCMetadata{}
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
	s, sender := createTestDB(t)
	defer s.Stop()

	// 4 cases: no deadline, past deadline, equal deadline, future deadline.
	for i := 0; i < 4; i++ {
		key := roachpb.Key("key: " + strconv.Itoa(i))
		txn := client.NewTxn(s.DB)
		// Set to SNAPSHOT so that it can be pushed without restarting.
		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			t.Fatal(err)
		}
		// Initialize the transaction.
		if pErr := txn.Put(context.TODO(), key, []byte("value")); pErr != nil {
			t.Fatal(pErr)
		}
		// Conflicting transaction that pushes the above transaction.
		conflictTxn := client.NewTxn(s.DB)
		if _, pErr := conflictTxn.Get(context.TODO(), key); pErr != nil {
			t.Fatal(pErr)
		}

		// The transaction was pushed at least to conflictTxn's timestamp (but
		// it could have been pushed more - the push takes a timestamp off the
		// HLC).
		pusheeTxn, pErr := getTxn(sender, txn.Proto())
		if pErr != nil {
			t.Fatal(pErr)
		}
		pushedTimestamp := pusheeTxn.Timestamp

		{
			var err error
			switch i {
			case 0:
				// No deadline.

			case 1:
				// Past deadline.
				if !txn.UpdateDeadlineMaybe(pushedTimestamp.Prev()) {
					t.Fatalf("did not update deadline")
				}

			case 2:
				// Equal deadline.
				if !txn.UpdateDeadlineMaybe(pushedTimestamp) {
					t.Fatalf("did not update deadline")
				}

			case 3:
				// Future deadline.

				if !txn.UpdateDeadlineMaybe(pushedTimestamp.Next()) {
					t.Fatalf("did not update deadline")
				}
			}
			err = txn.CommitOrCleanup(context.TODO())

			switch i {
			case 0:
				// No deadline.
				if err != nil {
					t.Fatal(err)
				}
			case 1:
				// Past deadline.
				if statusError, ok := err.(*roachpb.TransactionStatusError); !ok {
					t.Fatalf("expected TransactionStatusError but got %T: %s", err, err)
				} else if expected := "transaction deadline exceeded"; statusError.Msg != expected {
					t.Fatalf("expected %s, got %s", expected, statusError.Msg)
				}
			case 2:
				// Equal deadline.
				if err != nil {
					t.Fatal(err)
				}
			case 3:
				// Future deadline.
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		verifyCleanup(key, sender, s.Eng, t)
	}
}

// TestTxnCoordSenderAddIntentOnError verifies that intents are tracked if
// the transaction is, even on error.
func TestTxnCoordSenderAddIntentOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()

	// Create a transaction with intent at "a".
	key := roachpb.Key("x")
	txn := client.NewTxn(s.DB)
	// Write so that the coordinator begins tracking this txn.
	if err := txn.Put(context.TODO(), "x", "y"); err != nil {
		t.Fatal(err)
	}
	err, ok := txn.CPut(context.TODO(), key, []byte("x"), []byte("born to fail")).(*roachpb.ConditionFailedError)
	if !ok {
		t.Fatal(err)
	}
	sender.txnMu.Lock()
	txnID := *txn.Proto().ID
	intentSpans, _ := roachpb.MergeSpans(sender.txnMu.txns[txnID].keys)
	expSpans := []roachpb.Span{{Key: key, EndKey: []byte("")}}
	equal := !reflect.DeepEqual(intentSpans, expSpans)
	sender.txnMu.Unlock()
	if err := txn.Rollback(context.TODO()); err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Fatalf("expected stored intents %v, got %v", expSpans, intentSpans)
	}
}

func assertTransactionRetryError(t *testing.T, e error) {
	if retErr, ok := e.(*roachpb.HandledRetryableTxnError); ok {
		if !testutils.IsError(retErr, "TransactionRetryError") {
			t.Fatalf("expected the cause to be TransactionRetryError, but got %s",
				retErr)
		}
	} else {
		t.Fatalf("expected a retryable error, but got %s (%T)", e, e)
	}
}

func assertTransactionAbortedError(t *testing.T, e error) {
	if retErr, ok := e.(*roachpb.HandledRetryableTxnError); ok {
		if !testutils.IsError(retErr, "TransactionAbortedError") {
			t.Fatalf("expected the cause to be TransactionAbortedError, but got %s",
				retErr)
		}
	} else {
		t.Fatalf("expected a retryable error, but got %s (%T)", e, e)
	}
}

// TestTxnCoordSenderCleanupOnAborted verifies that if a txn receives a
// TransactionAbortedError, the coordinator cleans up the transaction.
func TestTxnCoordSenderCleanupOnAborted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()

	// Create a transaction with intent at "a".
	key := roachpb.Key("a")
	txn1 := client.NewTxn(s.DB)
	if err := txn1.Put(context.TODO(), key, []byte("value")); err != nil {
		t.Fatal(err)
	}

	// Push the transaction (by writing key "a" with higher priority) to abort it.
	txn2 := client.NewTxn(s.DB)
	if err := txn2.SetUserPriority(roachpb.MaxUserPriority); err != nil {
		t.Fatal(err)
	}
	if err := txn2.Put(context.TODO(), key, []byte("value2")); err != nil {
		t.Fatal(err)
	}

	// Now end the transaction and verify we've cleanup up, even though
	// end transaction failed.
	err := txn1.CommitOrCleanup(context.TODO())
	assertTransactionAbortedError(t, err)
	if err := txn2.CommitOrCleanup(context.TODO()); err != nil {
		t.Fatal(err)
	}
	verifyCleanup(key, sender, s.Eng, t)
}

func TestTxnCoordSenderCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	origSender := sender.wrapped
	sender.wrapped = client.SenderFunc(
		func(ctx context.Context, args roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			if _, hasET := args.GetArg(roachpb.EndTransaction); hasET {
				// Cancel the transaction while also sending it along. This tickled a
				// data race in TxnCoordSender.tryAsyncAbort. See #7726.
				cancel()
			}
			return origSender.Send(ctx, args)
		})

	// Create a transaction with bunch of intents.
	txn := client.NewTxn(s.DB)
	batch := txn.NewBatch()
	for i := 0; i < 100; i++ {
		key := roachpb.Key(fmt.Sprintf("%d", i))
		batch.Put(key, []byte("value"))
	}
	if err := txn.Run(ctx, batch); err != nil {
		t.Fatal(err)
	}

	// Commit the transaction. Note that we cancel the transaction when the
	// commit is sent which stresses the TxnCoordSender.tryAsyncAbort code
	// path. We'll either succeed, get a "does not exist" error, or get a
	// context canceled error. Anything else is unexpected.
	err := txn.CommitOrCleanup(ctx)
	if err != nil && err.Error() != context.Canceled.Error() &&
		!testutils.IsError(err, "does not exist") {
		t.Fatal(err)
	}
}

// TestTxnCoordSenderGCTimeout verifies that the coordinator cleans up extant
// transactions and intents after the lastUpdateNanos exceeds the timeout.
func TestTxnCoordSenderGCTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()

	// Set heartbeat interval to 1ms for testing.
	sender.heartbeatInterval = 1 * time.Millisecond

	txn := client.NewTxn(s.DB)
	key := roachpb.Key("a")
	if err := txn.Put(context.TODO(), key, []byte("value")); err != nil {
		t.Fatal(err)
	}

	// Now, advance clock past the default client timeout.
	// Locking the TxnCoordSender to prevent a data race.
	sender.txnMu.Lock()
	s.Manual.Increment(defaultClientTimeout.Nanoseconds() + 1)
	sender.txnMu.Unlock()

	txnID := *txn.Proto().ID

	testutils.SucceedsSoon(t, func() error {
		// Locking the TxnCoordSender to prevent a data race.
		sender.txnMu.Lock()
		_, ok := sender.txnMu.txns[txnID]
		sender.txnMu.Unlock()
		if ok {
			return errors.Errorf("expected garbage collection")
		}
		return nil
	})

	verifyCleanup(key, sender, s.Eng, t)
}

// TestTxnCoordSenderGCWithCancel verifies that the coordinator cleans up extant
// transactions and intents after transaction context is cancelled.
func TestTxnCoordSenderGCWithCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()

	// Set heartbeat interval to 1ms for testing.
	sender.heartbeatInterval = 1 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	txn := client.NewTxn(s.DB)
	key := roachpb.Key("a")
	if pErr := txn.Put(ctx, key, []byte("value")); pErr != nil {
		t.Fatal(pErr)
	}

	// Now, advance clock past the default client timeout.
	// Locking the TxnCoordSender to prevent a data race.
	sender.txnMu.Lock()
	s.Manual.Increment(defaultClientTimeout.Nanoseconds() + 1)
	sender.txnMu.Unlock()

	txnID := *txn.Proto().ID

	// Verify that the transaction is alive despite the timeout having been
	// exceeded.
	errStillActive := errors.New("transaction is still active")
	// TODO(dan): Figure out how to run the heartbeat manually instead of this.
	if err := util.RetryForDuration(1*time.Second, func() error {
		// Locking the TxnCoordSender to prevent a data race.
		sender.txnMu.Lock()
		_, ok := sender.txnMu.txns[txnID]
		sender.txnMu.Unlock()
		if !ok {
			return nil
		}
		meta := &enginepb.MVCCMetadata{}
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
	testutils.SucceedsSoon(t, func() error {
		// Locking the TxnCoordSender to prevent a data race.
		sender.txnMu.Lock()
		_, ok := sender.txnMu.txns[txnID]
		sender.txnMu.Unlock()
		if ok {
			return errors.Errorf("expected garbage collection")
		}
		return nil
	})

	verifyCleanup(key, sender, s.Eng, t)
}

// TestTxnCoordSenderTxnUpdatedOnError verifies that errors adjust the
// response transaction's timestamp and priority as appropriate.
func TestTxnCoordSenderTxnUpdatedOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	origTS := makeTS(123, 0)
	plus10 := origTS.Add(10, 10)
	plus20 := plus10.Add(10, 0)
	testCases := []struct {
		// The test's name.
		name             string
		pErrGen          func(txn *roachpb.Transaction) *roachpb.Error
		expEpoch         uint32
		expPri           int32
		expTS, expOrigTS hlc.Timestamp
		// Is set, we're expecting that the Transaction proto is re-initialized (as
		// opposed to just having the epoch incremented).
		expNewTransaction bool
		nodeSeen          bool
	}{
		{
			// No error, so nothing interesting either.
			name:      "nil",
			pErrGen:   func(_ *roachpb.Transaction) *roachpb.Error { return nil },
			expEpoch:  0,
			expPri:    1,
			expTS:     origTS,
			expOrigTS: origTS,
		},
		{
			// On uncertainty error, new epoch begins and node is seen.
			// Timestamp moves ahead of the existing write.
			name: "ReadWithinUncertaintyIntervalError",
			pErrGen: func(txn *roachpb.Transaction) *roachpb.Error {
				const nodeID = 1
				txn.UpdateObservedTimestamp(nodeID, plus10)
				pErr := roachpb.NewErrorWithTxn(
					roachpb.NewReadWithinUncertaintyIntervalError(
						hlc.Timestamp{}, hlc.Timestamp{}),
					txn)
				pErr.OriginNode = nodeID
				return pErr
			},
			expEpoch:  1,
			expPri:    1,
			expTS:     plus10,
			expOrigTS: plus10,
			nodeSeen:  true,
		},
		{
			// On abort, nothing changes but we get a new priority to use for
			// the next attempt.
			name: "TransactionAbortedError",
			pErrGen: func(txn *roachpb.Transaction) *roachpb.Error {
				txn.Timestamp = plus20
				txn.Priority = 10
				return roachpb.NewErrorWithTxn(&roachpb.TransactionAbortedError{}, txn)
			},
			expNewTransaction: true,
			expPri:            10,
			expTS:             plus20,
		},
		{
			// On failed push, new epoch begins just past the pushed timestamp.
			// Additionally, priority ratchets up to just below the pusher's.
			name: "TransactionPushError",
			pErrGen: func(txn *roachpb.Transaction) *roachpb.Error {
				return roachpb.NewErrorWithTxn(&roachpb.TransactionPushError{
					PusheeTxn: roachpb.Transaction{
						TxnMeta: enginepb.TxnMeta{Timestamp: plus10, Priority: int32(10)},
					},
				}, txn)
			},
			expEpoch:  1,
			expPri:    9,
			expTS:     plus10,
			expOrigTS: plus10,
		},
		{
			// On retry, restart with new epoch, timestamp and priority.
			name: "TransactionRetryError",
			pErrGen: func(txn *roachpb.Transaction) *roachpb.Error {
				txn.Timestamp = plus10
				txn.Priority = 10
				return roachpb.NewErrorWithTxn(&roachpb.TransactionRetryError{}, txn)
			},
			expEpoch:  1,
			expPri:    10,
			expTS:     plus10,
			expOrigTS: plus10,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			stopper := stop.NewStopper()

			manual := hlc.NewManualClock(origTS.WallTime)
			clock := hlc.NewClock(manual.UnixNano, 20*time.Nanosecond)

			var senderFn client.SenderFunc = func(
				_ context.Context, ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, *roachpb.Error) {
				var reply *roachpb.BatchResponse
				pErr := test.pErrGen(ba.Txn)
				if pErr == nil {
					reply = ba.CreateReply()
				}
				return reply, pErr
			}
			ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
			ts := NewTxnCoordSender(
				ambient,
				senderFn,
				clock,
				false,
				stopper,
				MakeTxnMetrics(metric.TestSampleInterval),
			)
			db := client.NewDB(ts, clock)
			key := roachpb.Key("test-key")
			origTxnProto := *roachpb.NewTransaction(
				"test txn",
				key,
				roachpb.UserPriority(0),
				enginepb.SERIALIZABLE,
				clock.Now(),
				clock.MaxOffset().Nanoseconds(),
			)
			// TODO(andrei): I've monkeyed with the priorities on this initial
			// Transaction to keep the test happy from a previous version in which the
			// Transaction was not initialized before use (which became insufficient
			// when we started testing that TransactionAbortedError's properly
			// re-initializes the proto), but this deserves cleanup. I think this test
			// is strict in what updated priorities it expects and also our mechanism
			// for assigning exact priorities doesn't work properly when faced with
			// updates.
			origTxnProto.Priority = 1
			txn := client.NewTxnWithProto(db, origTxnProto)
			txn.InternalSetPriority(1)

			_, err := txn.Get(context.TODO(), key)
			teardownHeartbeats(ts)
			stopper.Stop(context.TODO())

			if test.name != "nil" && err == nil {
				t.Fatalf("expected an error")
			}
			txnReset := !roachpb.TxnIDEqual(origTxnProto.ID, txn.Proto().ID)
			if txnReset != test.expNewTransaction {
				t.Fatalf("expected txn reset: %t and got: %t", test.expNewTransaction, txnReset)
			}
			if test.expNewTransaction {
				// Check that the timestamp has been reset.
				zeroTS := hlc.Timestamp{}
				if txn.Proto().OrigTimestamp != zeroTS {
					t.Fatalf("expected txn OrigTimestamp: %s, but got: %s", zeroTS, txn.Proto())
				}
			}
			if txn.Proto().Epoch != test.expEpoch {
				t.Errorf("expected epoch = %d; got %d",
					test.expEpoch, txn.Proto().Epoch)
			}
			if txn.Proto().Priority != test.expPri {
				t.Errorf("expected priority = %d; got %d",
					test.expPri, txn.Proto().Priority)
			}
			if txn.Proto().Timestamp != test.expTS {
				t.Errorf("expected timestamp to be %s; got %s",
					test.expTS, txn.Proto().Timestamp)
			}
			if txn.Proto().OrigTimestamp != test.expOrigTS {
				t.Errorf("expected orig timestamp to be %s; got %s",
					test.expOrigTS, txn.Proto().OrigTimestamp)
			}
			if ns := txn.Proto().ObservedTimestamps; (len(ns) != 0) != test.nodeSeen {
				t.Errorf("expected nodeSeen=%t, but list of hosts is %v",
					test.nodeSeen, ns)
			}
		})
	}
}

// TestTxnCoordIdempotentCleanup verifies that cleanupTxnLocked is idempotent.
func TestTxnCoordIdempotentCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	txn := client.NewTxn(s.DB)
	ba := txn.NewBatch()
	ba.Put(roachpb.Key("a"), []byte("value"))
	if err := txn.Run(context.TODO(), ba); err != nil {
		t.Fatal(err)
	}

	sender.txnMu.Lock()
	// Clean up twice successively.
	sender.cleanupTxnLocked(context.Background(), *txn.Proto())
	sender.cleanupTxnLocked(context.Background(), *txn.Proto())
	sender.txnMu.Unlock()

	// For good measure, try to commit (which cleans up once more if it
	// succeeds, which it may not if the previous cleanup has already
	// terminated the heartbeat goroutine)
	ba = txn.NewBatch()
	ba.AddRawRequest(&roachpb.EndTransactionRequest{})
	err := txn.Run(context.TODO(), ba)
	if err != nil && !testutils.IsError(err, errNoState.Error()) {
		t.Fatal(err)
	}
}

// TestTxnMultipleCoord checks that a coordinator uses the Writing flag to
// enforce that only one coordinator can be used for transactional writes.
func TestTxnMultipleCoord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()

	testCases := []struct {
		args    roachpb.Request
		writing bool
		ok      bool
	}{
		{roachpb.NewGet(roachpb.Key("a")), true /* writing */, false /* not ok */},
		{roachpb.NewGet(roachpb.Key("a")), false /* not writing */, true /* ok */},
		// transactional write before begin
		{roachpb.NewPut(roachpb.Key("a"), roachpb.Value{}), false /* not writing */, true /* ok */},
		// must have switched coordinators
		{roachpb.NewPut(roachpb.Key("a"), roachpb.Value{}), true /* writing */, false /* not ok */},
	}

	for i, tc := range testCases {
		txn := roachpb.NewTransaction("test", roachpb.Key("a"), 1, enginepb.SERIALIZABLE,
			s.Clock.Now(), s.Clock.MaxOffset().Nanoseconds())
		txn.Writing = tc.writing
		reply, pErr := client.SendWrappedWith(context.Background(), sender, roachpb.Header{
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
		if tc.writing != txn.Writing {
			t.Errorf("%d: unexpected writing state: %s", i, txn)
		}
		if !tc.writing {
			continue
		}
		// Abort for clean shutdown.
		if _, pErr := client.SendWrappedWith(context.Background(), sender, roachpb.Header{
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
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, 20*time.Nanosecond)

	var senderFn client.SenderFunc = func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		txnClone := ba.Txn.Clone()
		br.Txn = &txnClone
		br.Txn.Writing = true
		return br, nil
	}
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	ts := NewTxnCoordSender(
		ambient, senderFn, clock, false, stopper, MakeTxnMetrics(metric.TestSampleInterval),
	)

	// Stop the stopper manually, prior to trying the transaction. This has the
	// effect of returning a NodeUnavailableError for any attempts at launching
	// a heartbeat goroutine.
	stopper.Stop(context.TODO())

	var ba roachpb.BatchRequest
	key := roachpb.Key("test")
	ba.Add(&roachpb.BeginTransactionRequest{Span: roachpb.Span{Key: key}})
	ba.Add(&roachpb.PutRequest{Span: roachpb.Span{Key: key}})
	ba.Add(&roachpb.EndTransactionRequest{})
	ba.Txn = roachpb.NewTransaction("test", key, 0, 0, clock.Now(), 0)
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
	defer stopper.Stop(context.TODO())
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, 20*time.Nanosecond)

	u := uuid.MakeV4()

	testCases := []struct {
		roachpb.Error
		errMsg string
	}{
		{*roachpb.NewError(roachpb.NewTransactionRetryError(roachpb.RETRY_REASON_UNKNOWN)), "retry txn"},
		{
			*roachpb.NewError(roachpb.NewTransactionPushError(roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{ID: &u}}),
			), "failed to push",
		},
		{*roachpb.NewErrorf("testError"), "testError"},
	}
	for i, test := range testCases {
		func() {
			var senderFn client.SenderFunc = func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
				txn := ba.Txn.Clone()
				txn.Writing = true
				pErr := &roachpb.Error{}
				*pErr = test.Error
				pErr.SetTxn(&txn)
				return nil, pErr
			}
			ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
			ts := NewTxnCoordSender(
				ambient,
				senderFn,
				clock,
				false,
				stopper,
				MakeTxnMetrics(metric.TestSampleInterval),
			)

			var ba roachpb.BatchRequest
			key := roachpb.Key("test")
			ba.Add(&roachpb.BeginTransactionRequest{Span: roachpb.Span{Key: key}})
			ba.Add(&roachpb.PutRequest{Span: roachpb.Span{Key: key}})
			ba.Add(&roachpb.EndTransactionRequest{})
			ba.Txn = roachpb.NewTransaction("test", key, 0, 0, clock.Now(), 0)
			_, pErr := ts.Send(context.Background(), ba)
			if !testutils.IsPError(pErr, test.errMsg) {
				t.Errorf("%d: error did not match %s: %v", i, test.errMsg, pErr)
			}

			defer teardownHeartbeats(ts)
			ts.txnMu.Lock()
			defer ts.txnMu.Unlock()
			if len(ts.txnMu.txns) != 1 {
				t.Errorf("%d: expected transaction to be tracked", i)
			}
		}()
	}
}

// TestTxnCoordSenderReleaseTxnMeta verifies that TxnCoordSender releases the
// txnMetadata after the txn has committed successfully.
func TestTxnCoordSenderReleaseTxnMeta(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender := createTestDB(t)
	defer s.Stop()
	defer teardownHeartbeats(sender)

	txn := client.NewTxn(s.DB)
	ba := txn.NewBatch()
	ba.Put(roachpb.Key("a"), []byte("value"))
	ba.Put(roachpb.Key("b"), []byte("value"))
	if err := txn.CommitInBatch(context.TODO(), ba); err != nil {
		t.Fatal(err)
	}

	txnID := *txn.Proto().ID

	if _, ok := sender.txnMu.txns[txnID]; ok {
		t.Fatal("expected TxnCoordSender has released the txn")
	}
}

// TestTxnCoordSenderNoDuplicateIntents verifies that TxnCoordSender does not
// generate duplicate intents and that it merges intents for overlapping ranges.
func TestTxnCoordSenderNoDuplicateIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

	var expectedIntents []roachpb.Span

	var senderFn client.SenderFunc = func(_ context.Context, ba roachpb.BatchRequest) (
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
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	ts := NewTxnCoordSender(
		ambient,
		senderFn,
		clock,
		false,
		stopper,
		MakeTxnMetrics(metric.TestSampleInterval),
	)

	defer stopper.Stop(context.TODO())
	defer teardownHeartbeats(ts)

	db := client.NewDB(ts, clock)
	txn := client.NewTxn(db)

	// Write to a, b, u-w before the final batch.

	pErr := txn.Put(context.TODO(), roachpb.Key("a"), []byte("value"))
	if pErr != nil {
		t.Fatal(pErr)
	}
	pErr = txn.Put(context.TODO(), roachpb.Key("b"), []byte("value"))
	if pErr != nil {
		t.Fatal(pErr)
	}
	pErr = txn.DelRange(context.TODO(), roachpb.Key("u"), roachpb.Key("w"))
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

	pErr = txn.CommitInBatch(context.TODO(), b)
	if pErr != nil {
		t.Fatal(pErr)
	}
}

// checkTxnMetrics verifies that the provided Sender's transaction metrics match the expected
// values. This is done through a series of retries with increasing backoffs, to work around
// the TxnCoordSender's asynchronous updating of metrics after a transaction ends.
func checkTxnMetrics(
	t *testing.T,
	sender *TxnCoordSender,
	name string,
	commits, commits1PC, abandons, aborts, restarts int64,
) {
	metrics := sender.metrics

	testutils.SucceedsSoon(t, func() error {
		testcases := []struct {
			name string
			a, e int64
		}{
			{"commits", metrics.Commits.Count(), commits},
			{"commits1PC", metrics.Commits1PC.Count(), commits1PC},
			{"abandons", metrics.Abandons.Count(), abandons},
			{"aborts", metrics.Aborts.Count(), aborts},
			{"durations", metrics.Durations.TotalCount(),
				commits + abandons + aborts},
		}

		for _, tc := range testcases {
			if tc.a != tc.e {
				return errors.Errorf("%s: actual %s %d != expected %d", name, tc.name, tc.a, tc.e)
			}
		}

		// Handle restarts separately, because that's a histogram. Though the
		// histogram is approximate, we're recording so few distinct values
		// that we should be okay.
		dist := metrics.Restarts.Snapshot().Distribution()
		var actualRestarts int64
		for _, b := range dist {
			if b.From == b.To {
				actualRestarts += b.From * b.Count
			} else {
				t.Fatalf("unexpected value in histogram: %d-%d", b.From, b.To)
			}
		}
		if a, e := actualRestarts, restarts; a != e {
			return errors.Errorf("%s: actual restarts %d != expected %d", name, a, e)
		}

		return nil
	})
}

// setupMetricsTest returns a TxnCoordSender and ManualClock pointing to a newly created
// LocalTestCluster. Also returns a cleanup function to be executed at the end of the
// test.
func setupMetricsTest(t *testing.T) (*localtestcluster.LocalTestCluster, *TxnCoordSender, func()) {
	s, testSender := createTestDB(t)
	txnMetrics := MakeTxnMetrics(metric.TestSampleInterval)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := NewTxnCoordSender(ambient, testSender.wrapped, s.Clock, false, s.Stopper, txnMetrics)

	return s, sender, func() {
		teardownHeartbeats(sender)
		s.Stop()
	}
}

// Test a normal transaction. This and the other metrics tests below use real KV operations,
// because it took far too much mucking with TxnCoordSender internals to mock out the sender
// function as other tests do.
func TestTxnCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")
	db := client.NewDB(sender, s.Clock)

	// Test normal commit.
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		key := []byte("key-commit")

		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			return err
		}

		if err := txn.Put(ctx, key, value); err != nil {
			return err
		}

		if err := txn.CommitOrCleanup(ctx); err != nil {
			return err
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "commit txn", 1, 0 /* not 1PC */, 0, 0, 0)
}

// TestTxnOnePhaseCommit verifies that 1PC metric tracking works.
func TestTxnOnePhaseCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")
	db := client.NewDB(sender, s.Clock)

	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		key := []byte("key-commit")
		b := txn.NewBatch()
		b.Put(key, value)
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Fatal(err)
	}
	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "commit 1PC txn", 1, 1 /* 1PC */, 0, 0, 0)
}

func TestTxnAbandonCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")
	manual := s.Manual

	// client.Txn supplies a non-cancellable context, which makes the
	// transaction coordinator ignore timeout-based abandonment and use the
	// context's lifetime instead. In this test, we are testing timeout-based
	// abandonment, so we need to supply a non-cancellable context.
	var senderFn client.SenderFunc = func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return sender.Send(context.TODO(), ba)
	}

	db := client.NewDB(senderFn, s.Clock)

	// Test abandoned transaction by making the client timeout ridiculously short. We also set
	// the sender to heartbeat very frequently, because the heartbeat detects and tears down
	// abandoned transactions.
	sender.heartbeatInterval = 2 * time.Millisecond
	sender.clientTimeout = 1 * time.Millisecond
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		key := []byte("key-abandon")

		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			return err
		}

		if err := txn.Put(ctx, key, value); err != nil {
			return err
		}

		manual.Increment(int64(sender.clientTimeout + sender.heartbeatInterval*2))

		checkTxnMetrics(t, sender, "abandon txn", 0, 0, 1, 0, 0)

		return nil
	}); !testutils.IsError(err, "writing transaction timed out") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestTxnReadAfterAbandon checks the fix for the condition in issue #4787:
// after a transaction is abandoned we do a read as part of that transaction
// which should fail.
func TestTxnReadAfterAbandon(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender, cleanupFn := setupMetricsTest(t)
	manual := s.Manual
	defer cleanupFn()

	value := []byte("value")

	// client.Txn supplies a non-cancellable context, which makes the
	// transaction coordinator ignore timeout-based abandonment and use the
	// context's lifetime instead. In this test, we are testing timeout-based
	// abandonment, so we need to supply a non-cancellable context.
	var senderFn client.SenderFunc = func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return sender.Send(context.TODO(), ba)
	}

	db := client.NewDB(senderFn, s.Clock)

	// Test abandoned transaction by making the client timeout ridiculously short. We also set
	// the sender to heartbeat very frequently, because the heartbeat detects and tears down
	// abandoned transactions.
	sender.heartbeatInterval = 2 * time.Millisecond
	sender.clientTimeout = 1 * time.Millisecond

	err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		key := []byte("key-abandon")

		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			t.Fatal(err)
		}

		if err := txn.Put(ctx, key, value); err != nil {
			t.Fatal(err)
		}

		manual.Increment(int64(sender.clientTimeout + sender.heartbeatInterval*2))

		checkTxnMetrics(t, sender, "abandon txn", 0, 0, 1, 0, 0)

		_, err := txn.Get(ctx, key)
		if !testutils.IsError(err, "writing transaction timed out") {
			t.Fatalf("unexpected error from Get on abandoned txn: %v", err)
		}
		return err // appease compiler
	})

	if err == nil {
		t.Fatalf("abandoned txn didn't fail")
	}
}

func TestTxnAbortCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()

	value := []byte("value")
	db := client.NewDB(sender, s.Clock)

	intentionalErrText := "intentional error to cause abort"
	// Test aborted transaction.
	if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		key := []byte("key-abort")

		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			return err
		}

		if err := txn.Put(ctx, key, value); err != nil {
			t.Fatal(err)
		}

		return errors.New(intentionalErrText)
	}); !testutils.IsError(err, intentionalErrText) {
		t.Fatalf("unexpected error: %v", err)
	}
	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "abort txn", 0, 0, 0, 1, 0)
}

func TestTxnRestartCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()

	key := []byte("key-restart")
	value := []byte("value")
	db := client.NewDB(sender, s.Clock)

	// Start a transaction and do a GET. This forces a timestamp to be chosen for the transaction.
	txn := client.NewTxn(db)
	if _, err := txn.Get(context.TODO(), key); err != nil {
		t.Fatal(err)
	}

	// Outside of the transaction, read the same key as was read within the transaction. This
	// means that future attempts to write will increase the timestamp.
	if _, err := db.Get(context.TODO(), key); err != nil {
		t.Fatal(err)
	}

	// This put will lay down an intent, txn timestamp will increase beyond original.
	if err := txn.Put(context.TODO(), key, value); err != nil {
		t.Fatal(err)
	}
	if !txn.Proto().OrigTimestamp.Less(txn.Proto().Timestamp) {
		t.Errorf("expected timestamp to increase: %s", txn.Proto())
	}

	// Commit (should cause restart metric to increase).
	err := txn.CommitOrCleanup(context.TODO())
	assertTransactionRetryError(t, err)

	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "restart txn", 0, 0, 0, 1, 1)
}

func TestTxnDurations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sender, cleanupFn := setupMetricsTest(t)
	manual := s.Manual
	defer cleanupFn()

	db := client.NewDB(sender, s.Clock)
	const puts = 10

	const incr int64 = 1000
	for i := 0; i < puts; i++ {
		key := roachpb.Key(fmt.Sprintf("key-txn-durations-%d", i))
		if err := db.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
			if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
				return err
			}
			if err := txn.Put(ctx, key, []byte("val")); err != nil {
				return err
			}
			manual.Increment(incr)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	teardownHeartbeats(sender)
	checkTxnMetrics(t, sender, "txn durations", puts, 0, 0, 0, 0)

	hist := sender.metrics.Durations

	// The clock is a bit odd in these tests, so I can't test the mean without
	// introducing spurious errors or being overly lax.
	//
	// TODO(cdo): look into cause of variance.
	if a, e := hist.TotalCount(), int64(puts); a != e {
		t.Fatalf("durations %d != expected %d", a, e)
	}

	// Metrics lose fidelity, so we can't compare incr directly.
	if min, thresh := hist.Min(), incr-10; min < thresh {
		t.Fatalf("min %d < %d", min, thresh)
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

// TestAbortTransactionOnCommitErrors verifies that transactions are
// aborted on the correct errors.
func TestAbortTransactionOnCommitErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	clock := hlc.NewClock(hlc.UnixNano, 0)

	testCases := []struct {
		err   error
		errFn func(roachpb.Transaction) *roachpb.Error
		abort bool
	}{
		{
			errFn: func(txn roachpb.Transaction) *roachpb.Error {
				const nodeID = 0
				// ReadWithinUncertaintyIntervalErrors need a clock to have been
				// recorded on the origin.
				txn.UpdateObservedTimestamp(nodeID, makeTS(123, 0))
				return roachpb.NewErrorWithTxn(
					roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}),
					&txn)
			},
			abort: true},
		{err: &roachpb.TransactionAbortedError{}, abort: false},
		{err: &roachpb.TransactionPushError{}, abort: true},
		{err: &roachpb.TransactionRetryError{}, abort: true},
		{err: &roachpb.RangeNotFoundError{}, abort: true},
		{err: &roachpb.RangeKeyMismatchError{}, abort: true},
		{err: &roachpb.TransactionStatusError{}, abort: true},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%T", test.err), func(t *testing.T) {
			var commit, abort bool

			stopper := stop.NewStopper()
			defer stopper.Stop(context.TODO())
			var senderFn client.SenderFunc = func(
				_ context.Context, ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, *roachpb.Error) {
				br := ba.CreateReply()

				switch req := ba.Requests[0].GetInner().(type) {
				case *roachpb.BeginTransactionRequest:
					if _, ok := ba.Requests[1].GetInner().(*roachpb.PutRequest); !ok {
						t.Fatalf("expected Put")
					}
					union := &br.Responses[0] // avoid operating on copy
					union.MustSetInner(&roachpb.BeginTransactionResponse{})
					union = &br.Responses[1] // avoid operating on copy
					union.MustSetInner(&roachpb.PutResponse{})
					if ba.Txn != nil && br.Txn == nil {
						txnClone := ba.Txn.Clone()
						br.Txn = &txnClone
						br.Txn.Writing = true
						br.Txn.Status = roachpb.PENDING
					}
				case *roachpb.EndTransactionRequest:
					if req.Commit {
						commit = true
						if test.errFn != nil {
							return nil, test.errFn(*ba.Txn)
						}
						return nil, roachpb.NewErrorWithTxn(test.err, ba.Txn)
					}
					abort = true
				default:
					t.Fatalf("unexpected batch: %s", ba)
				}
				return br, nil
			}
			ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
			ts := NewTxnCoordSender(
				ambient,
				senderFn,
				clock,
				false,
				stopper,
				MakeTxnMetrics(metric.TestSampleInterval),
			)
			db := client.NewDB(ts, clock)

			txn := client.NewTxn(db)
			if pErr := txn.Put(context.Background(), "a", "b"); pErr != nil {
				t.Fatalf("put failed: %s", pErr)
			}
			if pErr := txn.CommitOrCleanup(context.Background()); pErr == nil {
				t.Fatalf("unexpected commit success")
			}

			if !commit {
				t.Errorf("%T: failed to find commit", test.err)
			}
			if test.abort && !abort {
				t.Errorf("%T: failed to find abort", test.err)
			} else if !test.abort && abort {
				t.Errorf("%T: found unexpected abort", test.err)
			}
		})
	}
}

func TestTooManyIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer settings.TestingSetInt(&maxIntents, 3)()

	ctx := context.Background()
	s, _ := createTestDB(t)
	defer s.Stop()

	txn := client.NewTxn(s.DB)
	for i := 0; i < int(maxIntents.Get()); i++ {
		key := roachpb.Key(fmt.Sprintf("a%d", i))
		if pErr := txn.Put(ctx, key, []byte("value")); pErr != nil {
			t.Fatal(pErr)
		}
	}
	// The request that puts us over the limit causes an error. Note
	// that this is a best-effort detection after the intents are
	// written.
	key := roachpb.Key(fmt.Sprintf("a%d", maxIntents.Get()))
	if err := txn.Put(ctx, key, []byte("value")); !testutils.IsError(err,
		"transaction is too large") {
		t.Fatalf("did not get expected error: %v", err)
	}
}

func TestTooManyIntentsAtCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer settings.TestingSetInt(&maxIntents, 3)()

	ctx := context.Background()
	s, _ := createTestDB(t)
	defer s.Stop()

	txn := client.NewTxn(s.DB)
	b := txn.NewBatch()
	for i := 0; i < 1+int(maxIntents.Get()); i++ {
		key := roachpb.Key(fmt.Sprintf("a%d", i))
		b.Put(key, []byte("value"))
	}
	if err := txn.CommitInBatch(ctx, b); !testutils.IsError(err,
		"transaction is too large") {
		t.Fatalf("did not get expected error: %v", err)
	}

	// Check that the "transaction too large" error was generated before
	// writing to the database instead of after.
	if kv, err := s.DB.Get(ctx, "a0"); err != nil {
		t.Fatal(err)
	} else if kv.Exists() {
		t.Fatal("did not expect value to exist")
	}
}
