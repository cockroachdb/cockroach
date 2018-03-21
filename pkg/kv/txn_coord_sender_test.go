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

package kv

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// teardownHeartbeat shuts down the coordinator's heartbeat task, if
// it's not already finished. This is useful for tests which don't
// finish transactions. This is safe to call multiple times.
func teardownHeartbeat(tc *TxnCoordSender) {
	if r := recover(); r != nil {
		panic(r)
	}
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.mu.txnEnd != nil {
		close(tc.mu.txnEnd)
		tc.mu.txnEnd = nil
	}
}

// createTestDB creates a local test server and starts it. The caller
// is responsible for stopping the test server.
func createTestDB(t testing.TB) *localtestcluster.LocalTestCluster {
	return createTestDBWithContextAndKnobs(t, client.DefaultDBContext(), nil)
}

func createTestDBWithContextAndKnobs(
	t testing.TB, dbCtx client.DBContext, knobs *storage.StoreTestingKnobs,
) *localtestcluster.LocalTestCluster {
	s := &localtestcluster.LocalTestCluster{
		DBContext:         &dbCtx,
		StoreTestingKnobs: knobs,
	}
	s.Start(t, testutils.NewNodeTestBaseContext(), InitFactoryForLocalTestCluster)
	return s
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
	s := createTestDB(t)
	defer s.Stop()

	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	tc := txn.Sender().(*TxnCoordSender)
	defer teardownHeartbeat(tc)

	// Put request will create a new transaction.
	if err := txn.Put(context.TODO(), roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if !txn.Proto().Writing {
		t.Fatal("txn is not marked as writing")
	}
	tc.mu.Lock()
	s.Manual.Increment(1)
	ts := tc.mu.lastUpdateNanos
	tc.mu.Unlock()

	if err := txn.Put(context.TODO(), roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	tc.mu.Lock()
	if lu := tc.mu.lastUpdateNanos; ts >= lu {
		t.Errorf("expected last update time to advance past %d; got %d", ts, lu)
	} else if un := s.Manual.UnixNano(); lu != un {
		t.Errorf("expected last update time to equal %d; got %d", un, lu)
	}
	tc.mu.Unlock()
}

// TestTxnCoordSenderAddRequestConcurrently verifies adding concurrent requests
// creates a transaction metadata and adding multiple requests with same
// transaction ID updates the last update timestamp.
func TestTxnCoordSenderAddRequestConcurrently(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	tc := txn.Sender().(*TxnCoordSender)
	defer teardownHeartbeat(tc)

	// Put requests will create a new transaction.
	sendRequests := func() error {
		// NB: we can't use errgroup.WithContext here because s.DB uses the first
		// request's context (if it's cancelable) to determine when the
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
	if !txn.Proto().Writing {
		t.Fatal("txn is not marked as writing")
	}
	tc.mu.Lock()
	ts := tc.mu.lastUpdateNanos
	s.Manual.Increment(1)
	tc.mu.Unlock()

	if err := sendRequests(); err != nil {
		t.Fatal(err)
	}

	tc.mu.Lock()
	if lu := tc.mu.lastUpdateNanos; ts >= lu {
		t.Errorf("expected last update time to advance past %d; got %d", ts, lu)
	} else if un := s.Manual.UnixNano(); lu != un {
		t.Errorf("expected last update time to equal %d; got %d", un, lu)
	}
	tc.mu.Unlock()
}

// TestTxnCoordSenderBeginTransaction verifies that a command sent with a
// not-nil Txn with empty ID gets a new transaction initialized.
func TestTxnCoordSenderBeginTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)

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

// TestTxnCoordSenderBeginTransactionMinPriority verifies that when starting
// a new transaction, a non-zero priority is treated as a minimum value.
func TestTxnCoordSenderBeginTransactionMinPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)

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

	s := createTestDB(t)
	defer s.Stop()

	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	tc := txn.Sender().(*TxnCoordSender)
	defer teardownHeartbeat(tc)

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

	// Verify that the transaction metadata contains only two entries
	// in its intents slice. "a" and range "aa"-"c".
	tc.mu.Lock()
	intents, _ := roachpb.MergeSpans(tc.mu.meta.Intents)
	tc.mu.Unlock()
	if len(intents) != 2 {
		t.Errorf("expected 2 entries in keys range group; got %v", intents)
	}
}

// TestTxnCoordSenderCondenseIntentSpans verifies that intent spans
// are condensed along range boundaries when they exceed the maximum
// intent bytes threshold.
func TestTxnCoordSenderCondenseIntentSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	a := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key(nil)}
	b := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key(nil)}
	c := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key(nil)}
	d := roachpb.Span{Key: roachpb.Key("dddddd"), EndKey: roachpb.Key(nil)}
	e := roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key(nil)}
	aToBClosed := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b").Next()}
	cToEClosed := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e").Next()}
	fTof0 := roachpb.Span{Key: roachpb.Key("f"), EndKey: roachpb.Key("f0")}
	g := roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key(nil)}
	g0Tog1 := roachpb.Span{Key: roachpb.Key("g0"), EndKey: roachpb.Key("g1")}
	fTog1Closed := roachpb.Span{Key: roachpb.Key("f"), EndKey: roachpb.Key("g1")}
	testCases := []struct {
		span           roachpb.Span
		expIntents     []roachpb.Span
		expIntentsSize int64
	}{
		{span: a, expIntents: []roachpb.Span{a}, expIntentsSize: 1},
		{span: b, expIntents: []roachpb.Span{a, b}, expIntentsSize: 2},
		{span: c, expIntents: []roachpb.Span{a, b, c}, expIntentsSize: 3},
		{span: d, expIntents: []roachpb.Span{a, b, c, d}, expIntentsSize: 9},
		// Note that c-e condenses and then lists first.
		{span: e, expIntents: []roachpb.Span{cToEClosed, a, b}, expIntentsSize: 5},
		{span: fTof0, expIntents: []roachpb.Span{cToEClosed, a, b, fTof0}, expIntentsSize: 8},
		{span: g, expIntents: []roachpb.Span{cToEClosed, a, b, fTof0, g}, expIntentsSize: 9},
		{span: g0Tog1, expIntents: []roachpb.Span{fTog1Closed, cToEClosed, aToBClosed}, expIntentsSize: 9},
		// Add a key in the middle of a span, which will get merged on commit.
		{span: c, expIntents: []roachpb.Span{cToEClosed, aToBClosed, fTog1Closed}, expIntentsSize: 9},
	}
	splits := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("f")},
		{Key: roachpb.Key("f"), EndKey: roachpb.Key("j")},
	}
	descs := []roachpb.RangeDescriptor{testMetaRangeDescriptor}
	for i, s := range splits {
		descs = append(descs, roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(2 + i),
			StartKey: roachpb.RKey(s.Key),
			EndKey:   roachpb.RKey(s.EndKey),
			Replicas: []roachpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1}},
		})
	}
	descDB := mockRangeDescriptorDBForDescs(descs...)
	s := createTestDB(t)
	st := s.Store.ClusterSettings()
	maxTxnIntentsBytes.Override(&st.SV, 10) /* 10 bytes and it will condense */
	defer s.Stop()

	// Check end transaction intents, which should exclude the intent at
	// key "c" as it's merged with the cToEClosed span.
	expIntents := []roachpb.Span{fTog1Closed, cToEClosed, a, b}
	var sendFn rpcSendFn = func(
		_ context.Context, _ SendOptions, _ ReplicaSlice, args roachpb.BatchRequest, _ *rpc.Context,
	) (*roachpb.BatchResponse, error) {
		if req, ok := args.GetArg(roachpb.EndTransaction); ok {
			if a, e := req.(*roachpb.EndTransactionRequest).IntentSpans, expIntents; !reflect.DeepEqual(a, e) {
				t.Errorf("expected end transaction to have intents %+v; got %+v", e, a)
			}
		}
		return args.CreateReply(), nil
	}
	cfg := DistSenderConfig{
		AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
		Clock:      s.Clock,
		TestingKnobs: DistSenderTestingKnobs{
			TransportFactory: adaptLegacyTransport(sendFn),
		},
		RangeDescriptorDB: descDB,
	}
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	tsf := NewTxnCoordSenderFactory(
		ambient,
		st,
		NewDistSender(cfg, s.Gossip),
		s.Clock,
		false, /* linearizable */
		s.Stopper,
		MakeTxnMetrics(metric.TestSampleInterval),
	)
	db := client.NewDB(tsf, s.Clock)

	txn := client.NewTxn(db, 0 /* gatewayNodeID */, client.RootTxn)
	for i, tc := range testCases {
		if tc.span.EndKey != nil {
			if err := txn.DelRange(context.TODO(), tc.span.Key, tc.span.EndKey); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := txn.Put(context.TODO(), tc.span.Key, []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		meta := txn.GetTxnCoordMeta()
		if a, e := meta.Intents, tc.expIntents; !reflect.DeepEqual(a, e) {
			t.Errorf("%d: expected keys %+v; got %+v", i, e, a)
		}
		intentsSize := int64(0)
		for _, i := range meta.Intents {
			intentsSize += int64(len(i.Key) + len(i.EndKey))
		}
		if a, e := intentsSize, tc.expIntentsSize; a != e {
			t.Errorf("%d: keys size expected %d; got %d", i, e, a)
		}
	}
}

// TestTxnCoordSenderHeartbeat verifies periodic heartbeat of the
// transaction record.
func TestTxnCoordSenderHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	initialTxn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	tc := initialTxn.Sender().(*TxnCoordSender)
	defer teardownHeartbeat(tc)
	// Set heartbeat interval to 1ms for testing.
	tc.TxnCoordSenderFactory.heartbeatInterval = 1 * time.Millisecond

	if err := initialTxn.Put(context.TODO(), roachpb.Key("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}

	// Verify 3 heartbeats.
	var heartbeatTS hlc.Timestamp
	for i := 0; i < 3; i++ {
		testutils.SucceedsSoon(t, func() error {
			txn, pErr := getTxn(s.DB, initialTxn.Proto())
			if pErr != nil {
				t.Fatal(pErr)
			}
			// Advance clock by 1ns.
			// Locking the TxnCoordSender to prevent a data race.
			tc.mu.Lock()
			s.Manual.Increment(1)
			tc.mu.Unlock()
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
			Span:   roachpb.Span{Key: initialTxn.Proto().Key},
			Commit: false,
			Poison: true,
		})
		ba.Txn = initialTxn.Proto()
		if _, pErr := tc.TxnCoordSenderFactory.wrapped.Send(context.Background(), ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Verify that the abort is discovered and the heartbeat discontinued.
	testutils.SucceedsSoon(t, func() error {
		tc.mu.Lock()
		done := tc.mu.txnEnd == nil
		tc.mu.Unlock()
		if !done {
			return fmt.Errorf("transaction is not aborted")
		}
		return nil
	})

	// Trying to do something else should give us a TransactionAbortedError.
	_, err := initialTxn.Get(context.TODO(), "a")
	assertTransactionAbortedError(t, err)
}

// getTxn fetches the requested key and returns the transaction info.
func getTxn(db *client.DB, txn *roachpb.Transaction) (*roachpb.Transaction, *roachpb.Error) {
	hb := &roachpb.HeartbeatTxnRequest{
		Span: roachpb.Span{
			Key: txn.Key,
		},
	}
	reply, pErr := client.SendWrappedWith(context.Background(), db.GetSender(), roachpb.Header{
		Txn: txn,
	}, hb)
	if pErr != nil {
		return nil, pErr
	}
	return reply.(*roachpb.HeartbeatTxnResponse).Txn, nil
}

func verifyCleanup(key roachpb.Key, eng engine.Engine, t *testing.T, coords ...*TxnCoordSender) {
	testutils.SucceedsSoon(t, func() error {
		for _, coord := range coords {
			coord.mu.Lock()
			hb := coord.mu.txnEnd != nil
			coord.mu.Unlock()
			if hb {
				return fmt.Errorf("expected no heartbeat")
			}
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
	s := createTestDB(t)
	defer s.Stop()

	// 4 cases: no deadline, past deadline, equal deadline, future deadline.
	for i := 0; i < 4; i++ {
		key := roachpb.Key("key: " + strconv.Itoa(i))
		txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
		// Set to SNAPSHOT so that it can be pushed without restarting.
		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			t.Fatal(err)
		}
		// Initialize the transaction.
		if pErr := txn.Put(context.TODO(), key, []byte("value")); pErr != nil {
			t.Fatal(pErr)
		}
		// Conflicting transaction that pushes the above transaction.
		conflictTxn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
		if _, pErr := conflictTxn.Get(context.TODO(), key); pErr != nil {
			t.Fatal(pErr)
		}

		// The transaction was pushed at least to conflictTxn's timestamp (but
		// it could have been pushed more - the push takes a timestamp off the
		// HLC).
		pusheeTxn, pErr := getTxn(s.DB, txn.Proto())
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
				if !txn.UpdateDeadlineMaybe(context.TODO(), pushedTimestamp.Prev()) {
					t.Fatalf("did not update deadline")
				}

			case 2:
				// Equal deadline.
				if !txn.UpdateDeadlineMaybe(context.TODO(), pushedTimestamp) {
					t.Fatalf("did not update deadline")
				}

			case 3:
				// Future deadline.

				if !txn.UpdateDeadlineMaybe(context.TODO(), pushedTimestamp.Next()) {
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
		verifyCleanup(key, s.Eng, t, txn.Sender().(*TxnCoordSender))
	}
}

// TestTxnCoordSenderAddIntentOnError verifies that intents are tracked if
// the transaction is, even on error.
func TestTxnCoordSenderAddIntentOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	// Create a transaction with intent at "x".
	key := roachpb.Key("x")
	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	tc := txn.Sender().(*TxnCoordSender)
	defer teardownHeartbeat(tc)
	// Write so that the coordinator begins tracking this txn.
	if err := txn.Put(context.TODO(), "x", "y"); err != nil {
		t.Fatal(err)
	}
	err, ok := txn.CPut(context.TODO(), key, []byte("x"), []byte("born to fail")).(*roachpb.ConditionFailedError)
	if !ok {
		t.Fatal(err)
	}
	tc.mu.Lock()
	intentSpans, _ := roachpb.MergeSpans(tc.mu.meta.Intents)
	expSpans := []roachpb.Span{{Key: key, EndKey: []byte("")}}
	equal := !reflect.DeepEqual(intentSpans, expSpans)
	tc.mu.Unlock()
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
	s := createTestDB(t)
	defer s.Stop()

	// Create a transaction with intent at "a".
	key := roachpb.Key("a")
	txn1 := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	if err := txn1.Put(context.TODO(), key, []byte("value")); err != nil {
		t.Fatal(err)
	}

	// Push the transaction (by writing key "a" with higher priority) to abort it.
	txn2 := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
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
	verifyCleanup(key, s.Eng, t, txn1.Sender().(*TxnCoordSender), txn2.Sender().(*TxnCoordSender))
}

func TestTxnCoordSenderCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	tc := s.DB.GetSender().(*TxnCoordSender)
	origSender := tc.TxnCoordSenderFactory.wrapped
	tc.TxnCoordSenderFactory.wrapped = client.SenderFunc(
		func(ctx context.Context, args roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			if _, hasET := args.GetArg(roachpb.EndTransaction); hasET {
				// Cancel the transaction while also sending it along. This tickled a
				// data race in TxnCoordSender.tryAsyncAbort. See #7726.
				cancel()
			}
			return origSender.Send(ctx, args)
		})

	// Create a transaction with bunch of intents.
	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
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
	if err != nil && err.Error() != context.Canceled.Error() {
		t.Fatal(err)
	}
}

// TestTxnCoordSenderGCTimeout verifies that the coordinator cleans up extant
// transactions and intents after the lastUpdateNanos exceeds the timeout.
func TestTxnCoordSenderGCTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Add a testing request filter which pauses a get request for the
	// key until after the signal channel is closed.
	key := roachpb.Key("a")
	value := []byte("value")
	signal := make(chan struct{})
	knobs := &storage.StoreTestingKnobs{
		DisableScanner:    true,
		DisableSplitQueue: true,
		TestingRequestFilter: func(ba roachpb.BatchRequest) *roachpb.Error {
			for _, req := range ba.Requests {
				if getReq, ok := req.GetInner().(*roachpb.GetRequest); ok && getReq.Key.Equal(key) {
					<-signal
				}
			}
			return nil
		},
	}

	s := createTestDBWithContextAndKnobs(t, client.DefaultDBContext(), knobs)
	defer s.Stop()

	// Set heartbeat interval to 1ms for testing.
	tc := s.DB.GetSender().(*TxnCoordSender)
	tc.TxnCoordSenderFactory.heartbeatInterval = 1 * time.Millisecond

	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	if err := txn.Put(context.TODO(), key, value); err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error)
	go func() {
		if _, err := txn.Get(context.TODO(), key); err != nil {
			errCh <- err
		} else {
			errCh <- errors.New("expected error")
		}
	}()

	// Now, advance clock past the default client timeout.
	// Locking the TxnCoordSender to prevent a data race.
	tc.mu.Lock()
	s.Manual.Increment(defaultClientTimeout.Nanoseconds() + 1)
	tc.mu.Unlock()

	testutils.SucceedsSoon(t, func() error {
		// Locking the TxnCoordSender to prevent a data race.
		tc.mu.Lock()
		done := tc.mu.txnEnd == nil
		tc.mu.Unlock()
		if !done {
			return errors.Errorf("expected garbage collection")
		}
		return nil
	})

	// Verify all intents have been cleaned up.
	verifyCleanup(key, s.Eng, t, tc)

	// Verify that the inflight get request receives a transaction
	// aborted error.
	close(signal)
	if err := <-errCh; !testutils.IsError(err, "txn aborted") {
		t.Errorf("expected transaction aborted error; got %s", err)
	}
}

// TestTxnCoordSenderGCWithCancel verifies that the coordinator cleans up extant
// transactions and intents after transaction context is canceled.
func TestTxnCoordSenderGCWithCancel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	tc := txn.Sender().(*TxnCoordSender)
	// Set heartbeat interval to 1ms for testing.
	tc.TxnCoordSenderFactory.heartbeatInterval = 1 * time.Millisecond
	key := roachpb.Key("a")
	if pErr := txn.Put(ctx, key, []byte("value")); pErr != nil {
		t.Fatal(pErr)
	}
	// Wait for heartbeat to kick in after Put.
	testutils.SucceedsSoon(t, func() error {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		if tc.mu.txnEnd != nil {
			return nil
		}
		return errors.Errorf("expected heartbeat to start")
	})

	// Now, advance clock past the default client timeout.
	// Locking the TxnCoordSender to prevent a data race.
	tc.mu.Lock()
	s.Manual.Increment(defaultClientTimeout.Nanoseconds() + 1)
	tc.mu.Unlock()

	// Verify that the transaction is alive despite the timeout having
	// been exceeded.
	errStillActive := errors.New("transaction is still active")
	if err := retry.ForDuration(1*time.Second, func() error {
		tc.mu.Lock()
		done := tc.mu.txnEnd == nil
		tc.mu.Unlock()
		if done {
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

	// After the context is canceled, the heartbeat should stop.
	cancel()
	verifyCleanup(key, s.Eng, t, tc)
}

// TestTxnCoordSenderGCWithAmbiguousResultErr verifies that the coordinator
// cleans up extant transactions and intents after an ambiguous result error is
// observed, even if the error is on the first request.
func TestTxnCoordSenderGCWithAmbiguousResultErr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "errOnFirst", func(t *testing.T, errOnFirst bool) {
		key := roachpb.Key("a")
		are := roachpb.NewAmbiguousResultError("very ambiguous")
		knobs := &storage.StoreTestingKnobs{
			TestingResponseFilter: func(ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
				for _, req := range ba.Requests {
					if putReq, ok := req.GetInner().(*roachpb.PutRequest); ok && putReq.Key.Equal(key) {
						return roachpb.NewError(are)
					}
				}
				return nil
			},
		}

		s := createTestDBWithContextAndKnobs(t, client.DefaultDBContext(), knobs)
		defer s.Stop()

		ctx, cancel := context.WithCancel(context.Background())
		txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
		tc := txn.Sender().(*TxnCoordSender)
		defer teardownHeartbeat(tc)
		if !errOnFirst {
			otherKey := roachpb.Key("other")
			if err := txn.Put(ctx, otherKey, []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		if err := txn.Put(ctx, key, []byte("value")); !testutils.IsError(err, "result is ambiguous") {
			t.Fatalf("expected error %v, found %v", are, err)
		}

		// After the context is canceled, the transaction should be cleaned up.
		cancel()
		testutils.SucceedsSoon(t, func() error {
			// Locking the TxnCoordSender to prevent a data race.
			tc.mu.Lock()
			done := tc.mu.txnEnd == nil
			tc.mu.Unlock()
			if !done {
				return errors.Errorf("expected garbage collection")
			}
			return nil
		})

		verifyCleanup(key, s.Eng, t, tc)
	})
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
						hlc.Timestamp{}, hlc.Timestamp{}, nil),
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
			expOrigTS:         plus20,
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
			tsf := NewTxnCoordSenderFactory(
				ambient,
				cluster.MakeTestingClusterSettings(),
				senderFn,
				clock,
				false, /* linearizable */
				stopper,
				MakeTxnMetrics(metric.TestSampleInterval),
			)
			db := client.NewDB(tsf, clock)
			key := roachpb.Key("test-key")
			origTxnProto := roachpb.MakeTransaction(
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
			txn := client.NewTxnWithProto(db, 0 /* gatewayNodeID */, client.RootTxn, origTxnProto)
			txn.InternalSetPriority(1)

			err := txn.Put(context.TODO(), key, []byte("value"))
			stopper.Stop(context.TODO())

			if test.name != "nil" && err == nil {
				t.Fatalf("expected an error")
			}
			txnReset := origTxnProto.ID != txn.Proto().ID
			if txnReset != test.expNewTransaction {
				t.Fatalf("expected txn reset: %t and got: %t", test.expNewTransaction, txnReset)
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
	s := createTestDB(t)
	defer s.Stop()

	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	tc := txn.Sender().(*TxnCoordSender)
	defer teardownHeartbeat(tc)
	ba := txn.NewBatch()
	ba.Put(roachpb.Key("a"), []byte("value"))
	if err := txn.Run(context.TODO(), ba); err != nil {
		t.Fatal(err)
	}

	tc.mu.Lock()
	// Clean up twice successively.
	tc.cleanupTxnLocked(context.Background(), aborted)
	tc.cleanupTxnLocked(context.Background(), aborted)
	tc.mu.Unlock()

	// For good measure, try to commit (which cleans up once more if it
	// succeeds, which it may not if the previous cleanup has already
	// terminated the heartbeat goroutine)
	ba = txn.NewBatch()
	ba.AddRawRequest(&roachpb.EndTransactionRequest{})
	err := txn.Run(context.TODO(), ba)
	assertTransactionAbortedError(t, err)
}

// TestTxnMultipleCoord checks that multiple txn coordinators can be
// used by a single transaction, and their state can be combined.
func TestTxnMultipleCoord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	ctx := context.Background()
	txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
	tc := txn.Sender().(*TxnCoordSender)
	defer teardownHeartbeat(tc)

	// Start the transaction.
	key := roachpb.Key("a")
	if err := txn.Put(ctx, key, []byte("value")); err != nil {
		t.Fatalf("expected error %s", err)
	}

	// New create a second, leaf coordinator.
	txn2 := client.NewTxnWithProto(s.DB, 0 /* gatewayNodeID */, client.LeafTxn, *txn.Proto())
	tc2 := txn2.Sender().(*TxnCoordSender)
	defer teardownHeartbeat(tc2)

	// Start the second transaction.
	key2 := roachpb.Key("b")
	if err := txn2.Put(ctx, key2, []byte("value2")); err != nil {
		t.Fatalf("expected error %s", err)
	}

	// Verify heartbeat started on root txn.
	testutils.SucceedsSoon(t, func() error {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		if tc.mu.txnEnd == nil {
			return errors.New("expected heartbeat on root coordinator")
		}
		return nil
	})
	// Verify no heartbeat started on leaf txn.
	tc2.mu.Lock()
	if tc2.mu.txnEnd != nil {
		t.Fatalf("unexpected heartbeat on leaf coordinator")
	}
	tc2.mu.Unlock()

	// Verify it's an error to commit on the leaf txn node.
	ba := txn2.NewBatch()
	ba.AddRawRequest(&roachpb.EndTransactionRequest{Commit: true})
	if err := txn2.Run(context.TODO(), ba); !testutils.IsError(err, "cannot commit on a leaf transaction coordinator") {
		t.Fatalf("expected cannot commit on leaf coordinator error; got %v", err)
	}

	// Augment txn with txn2's meta & commit.
	txn.AugmentTxnCoordMeta(txn2.GetTxnCoordMeta())
	// Verify presence of both intents.
	tc.mu.Lock()
	if a, e := tc.mu.meta.Intents, []roachpb.Span{{Key: key}, {Key: key2}}; !reflect.DeepEqual(a, e) {
		t.Fatalf("expected intents %+v; got %+v", e, a)
	}
	if a, e := tc.mu.intentsSizeBytes, int64(len(key)+len(key2)); a != e {
		t.Fatalf("expected intentsSizeBytes %d; got %d", e, a)
	}
	tc.mu.Unlock()
	ba = txn.NewBatch()
	ba.AddRawRequest(&roachpb.EndTransactionRequest{Commit: true})
	if err := txn.Run(context.TODO(), ba); err != nil {
		t.Fatal(err)
	}

	verifyCleanup(key, s.Eng, t, tc, tc2)
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
	factory := NewTxnCoordSenderFactory(
		ambient, cluster.MakeTestingClusterSettings(),
		senderFn, clock, false, stopper, MakeTxnMetrics(metric.TestSampleInterval),
	)
	tc := factory.New(client.RootTxn)

	// Stop the stopper manually, prior to trying the transaction. This has the
	// effect of returning a NodeUnavailableError for any attempts at launching
	// a heartbeat goroutine.
	stopper.Stop(context.TODO())

	var ba roachpb.BatchRequest
	key := roachpb.Key("test")
	ba.Add(&roachpb.BeginTransactionRequest{Span: roachpb.Span{Key: key}})
	ba.Add(&roachpb.PutRequest{Span: roachpb.Span{Key: key}})
	ba.Add(&roachpb.EndTransactionRequest{})
	txn := roachpb.MakeTransaction("test", key, 0, 0, clock.Now(), 0)
	ba.Txn = &txn
	_, pErr := tc.Send(context.Background(), ba)
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

	testCases := []struct {
		roachpb.Error
		errMsg string
	}{
		{*roachpb.NewError(roachpb.NewTransactionRetryError(roachpb.RETRY_REASON_UNKNOWN)), "retry txn"},
		{
			*roachpb.NewError(roachpb.NewTransactionPushError(roachpb.Transaction{
				TxnMeta: enginepb.TxnMeta{ID: uuid.MakeV4()}}),
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
			factory := NewTxnCoordSenderFactory(
				ambient,
				cluster.MakeTestingClusterSettings(),
				senderFn,
				clock,
				false,
				stopper,
				MakeTxnMetrics(metric.TestSampleInterval),
			)
			tc := factory.New(client.RootTxn)
			defer teardownHeartbeat(tc.(*TxnCoordSender))

			var ba roachpb.BatchRequest
			key := roachpb.Key("test")
			ba.Add(&roachpb.BeginTransactionRequest{Span: roachpb.Span{Key: key}})
			ba.Add(&roachpb.PutRequest{Span: roachpb.Span{Key: key}})
			ba.Add(&roachpb.EndTransactionRequest{})
			txn := roachpb.MakeTransaction("test", key, 0, 0, clock.Now(), 0)
			ba.Txn = &txn
			_, pErr := tc.Send(context.Background(), ba)
			if !testutils.IsPError(pErr, test.errMsg) {
				t.Errorf("%d: error did not match %s: %v", i, test.errMsg, pErr)
			}
		}()
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
	factory := NewTxnCoordSenderFactory(
		ambient,
		cluster.MakeTestingClusterSettings(),
		senderFn,
		clock,
		false,
		stopper,
		MakeTxnMetrics(metric.TestSampleInterval),
	)
	defer stopper.Stop(context.TODO())

	db := client.NewDB(factory, clock)
	txn := client.NewTxn(db, 0 /* gatewayNodeID */, client.RootTxn)

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
	metrics TxnMetrics,
	name string,
	commits, commits1PC, abandons, aborts, restarts int64,
) {
	testutils.SucceedsSoon(t, func() error {
		return checkTxnMetricsOnce(t, metrics, name, commits, commits1PC, abandons, aborts, restarts)
	})
}

func checkTxnMetricsOnce(
	t *testing.T,
	metrics TxnMetrics,
	name string,
	commits, commits1PC, abandons, aborts, restarts int64,
) error {
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
}

// setupMetricsTest sets the txn coord sender factory's metrics to
// have a faster sample interval and returns a cleanup function to be
// executed by callers.
func setupMetricsTest(t *testing.T) (*localtestcluster.LocalTestCluster, TxnMetrics, func()) {
	s := createTestDB(t)
	metrics := MakeTxnMetrics(metric.TestSampleInterval)
	s.DB.GetSender().(*TxnCoordSender).TxnCoordSenderFactory.metrics = metrics
	return s, metrics, func() {
		s.Stop()
	}
}

// Test a normal transaction. This and the other metrics tests below use real KV operations,
// because it took far too much mucking with TxnCoordSender internals to mock out the sender
// function as other tests do.
func TestTxnCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, metrics, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")

	// Test normal commit.
	if err := s.DB.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		key := []byte("key-commit")

		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			return err
		}

		if err := txn.Put(ctx, key, value); err != nil {
			return err
		}

		return txn.CommitOrCleanup(ctx)
	}); err != nil {
		t.Fatal(err)
	}
	checkTxnMetrics(t, metrics, "commit txn", 1, 0 /* not 1PC */, 0, 0, 0)
}

// TestTxnOnePhaseCommit verifies that 1PC metric tracking works.
func TestTxnOnePhaseCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, metrics, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")

	if err := s.DB.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		key := []byte("key-commit")
		b := txn.NewBatch()
		b.Put(key, value)
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Fatal(err)
	}
	checkTxnMetrics(t, metrics, "commit 1PC txn", 1, 1 /* 1PC */, 0, 0, 0)
}

// createNonCancelableDB returns a client DB and a sender. The sender
// will be used for every transaction sent through the DB, so use with
// care. The point is that every invocation of TxnCoordSender.Send will
// be supplied a context with Done()==nil. Note that the returned
// database will use a factory with ridiculously short client timeout
// and heartbeat interval settings to make tests fast.
func createNonCancelableDB(db *client.DB) (*client.DB, *TxnCoordSender) {
	// Create the single txn coord for this test.
	tc := db.GetSender().(*TxnCoordSender)
	tc.TxnCoordSenderFactory.heartbeatInterval = 2 * time.Millisecond
	tc.TxnCoordSenderFactory.clientTimeout = 1 * time.Millisecond

	// client.Txn supplies a non-cancelable context, which makes the
	// transaction coordinator ignore timeout-based abandonment and use the
	// context's lifetime instead. In this test, we are testing timeout-based
	// abandonment, so we need to supply a non-cancelable context.
	var factory client.TxnSenderFactoryFunc = func(_ client.TxnType) client.TxnSender {
		return client.TxnSenderFunc(func(_ context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
			return tc.Send(context.Background(), ba)
		})
	}
	return client.NewDB(factory, tc.TxnCoordSenderFactory.clock), tc
}

func TestTxnAbandonCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, metrics, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()
	value := []byte("value")
	manual := s.Manual

	ctx := context.Background()

	var count int
	doneErr := errors.New("retry on abandoned successful; exiting")

	db, tc := createNonCancelableDB(s.DB)

	if err := db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		count++
		if count == 2 {
			return doneErr
		}
		key := []byte("key-abandon")

		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			return err
		}

		if err := txn.Put(ctx, key, value); err != nil {
			return err
		}

		testutils.SucceedsSoon(t, func() error {
			// Note that we must bump the clock before every attempt (not just once)
			// because otherwise there is a race in which we bump, a heartbeat happens
			// at the new timestamp, and the transaction never expires.
			manual.Increment(int64(tc.TxnCoordSenderFactory.clientTimeout + tc.TxnCoordSenderFactory.heartbeatInterval*2))
			err := checkTxnMetricsOnce(
				t, metrics, "abandon txn", 0, 0, 1 /* abandons */, 0, 0,
			)
			if err == nil {
				return nil
			}
			// There's another race here: if (*TxnCoordSender).heartbeat sees the expired txn, it will
			// asynchronously call tryAsyncAbort, which may beat the parent heartbeat loop's stats taking.
			// In that case, we see an aborted txn.
			return checkTxnMetricsOnce(
				t, metrics, "abort txn", 0, 0, 0, 1 /* aborts */, 0,
			)
		})

		return nil
	}); err != doneErr {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestTxnReadAfterAbandon checks the fix for the condition in issue #4787:
// after a transaction is abandoned we do a read as part of that transaction
// which should fail.
func TestTxnReadAfterAbandon(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, metrics, cleanupFn := setupMetricsTest(t)
	manual := s.Manual
	defer cleanupFn()

	value := []byte("value")

	var count int
	doneErr := errors.New("retry on abandoned successful; exiting")
	db, tc := createNonCancelableDB(s.DB)
	err := db.Txn(context.Background(), func(ctx context.Context, txn *client.Txn) error {
		count++
		if count == 2 {
			return doneErr
		}
		key := []byte("key-abandon")

		if err := txn.SetIsolation(enginepb.SNAPSHOT); err != nil {
			t.Fatal(err)
		}

		if err := txn.Put(ctx, key, value); err != nil {
			t.Fatal(err)
		}

		testutils.SucceedsSoon(t, func() error {
			// See #22762 for very similar code and an explanation.
			manual.Increment(int64(tc.TxnCoordSenderFactory.clientTimeout + tc.TxnCoordSenderFactory.heartbeatInterval*2))
			err := checkTxnMetricsOnce(t, metrics, "abandon txn", 0, 0, 1 /* abandons */, 0, 0)
			if err == nil {
				return nil
			}
			return checkTxnMetricsOnce(t, metrics, "abort txn", 0, 0, 0, 1 /* aborts */, 0)
		})

		_, err := txn.Get(ctx, key)
		if !testutils.IsError(err, "txn aborted") {
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
	s, metrics, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()

	value := []byte("value")

	intentionalErrText := "intentional error to cause abort"
	// Test aborted transaction.
	if err := s.DB.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
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
	checkTxnMetrics(t, metrics, "abort txn", 0, 0, 0, 1, 0)
}

func TestTxnRestartCount(t *testing.T) {
	defer leaktest.AfterTest(t)()

	readKey := []byte("read")
	writeKey := []byte("write")
	value := []byte("value")

	for _, expRestart := range []bool{true, false} {
		t.Run(fmt.Sprintf("expected restart: %t", expRestart), func(t *testing.T) {
			s, metrics, cleanupFn := setupMetricsTest(t)
			defer cleanupFn()

			// Start a transaction and do a GET. This forces a timestamp to be
			// chosen for the transaction.
			txn := client.NewTxn(s.DB, 0 /* gatewayNodeID */, client.RootTxn)
			if _, err := txn.Get(context.TODO(), readKey); err != nil {
				t.Fatal(err)
			}

			// If expRestart is true, write the read key outside of the
			// transaction, at a higher timestamp, which will necessitate a
			// txn restart when the original read key span is updated.
			if expRestart {
				if err := s.DB.Put(context.TODO(), readKey, value); err != nil {
					t.Fatal(err)
				}
			}

			// Outside of the transaction, read the same key as will be
			// written within the transaction. This means that future
			// attempts to write will forward the txn timestamp.
			if _, err := s.DB.Get(context.TODO(), writeKey); err != nil {
				t.Fatal(err)
			}

			// This put will lay down an intent, txn timestamp will increase
			// beyond OrigTimestamp.
			if err := txn.Put(context.TODO(), writeKey, value); err != nil {
				t.Fatal(err)
			}
			if !txn.Proto().OrigTimestamp.Less(txn.Proto().Timestamp) {
				t.Errorf("expected timestamp to increase: %s", txn.Proto())
			}

			// Wait for heartbeat to start.
			tc := txn.Sender().(*TxnCoordSender)
			testutils.SucceedsSoon(t, func() error {
				tc.mu.Lock()
				defer tc.mu.Unlock()
				if tc.mu.txnEnd == nil {
					return errors.New("expected heartbeat to start")
				}
				return nil
			})

			// Commit (should cause restart metric to increase).
			err := txn.CommitOrCleanup(context.TODO())
			if expRestart {
				assertTransactionRetryError(t, err)
				checkTxnMetrics(t, metrics, "restart txn", 0, 0, 0, 1, 1)
			} else if err != nil {
				t.Fatalf("expected no restart; got %s", err)
			}
		})
	}
}

func TestTxnDurations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, metrics, cleanupFn := setupMetricsTest(t)
	manual := s.Manual
	defer cleanupFn()
	const puts = 10

	const incr int64 = 1000
	for i := 0; i < puts; i++ {
		key := roachpb.Key(fmt.Sprintf("key-txn-durations-%d", i))
		if err := s.DB.Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
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

	checkTxnMetrics(t, metrics, "txn durations", puts, 0, 0, 0, 0)

	hist := metrics.Durations
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
//     tc.tryAsyncAbort()
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
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	testCases := []struct {
		err        error
		errFn      func(roachpb.Transaction) *roachpb.Error
		asyncAbort bool
	}{
		{
			errFn: func(txn roachpb.Transaction) *roachpb.Error {
				const nodeID = 0
				// ReadWithinUncertaintyIntervalErrors need a clock to have been
				// recorded on the origin.
				txn.UpdateObservedTimestamp(nodeID, makeTS(123, 0))
				return roachpb.NewErrorWithTxn(
					roachpb.NewReadWithinUncertaintyIntervalError(hlc.Timestamp{}, hlc.Timestamp{}, nil),
					&txn)
			},
			asyncAbort: false},
		{err: &roachpb.TransactionAbortedError{}, asyncAbort: true},
		{err: &roachpb.TransactionPushError{}, asyncAbort: false},
		{err: &roachpb.TransactionRetryError{}, asyncAbort: false},
		{err: &roachpb.RangeNotFoundError{}, asyncAbort: false},
		{err: &roachpb.RangeKeyMismatchError{}, asyncAbort: false},
		{err: &roachpb.TransactionStatusError{}, asyncAbort: false},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%T", test.err), func(t *testing.T) {
			var commit, abort atomic.Value
			commit.Store(false)
			abort.Store(false)

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
						commit.Store(true)
						if test.errFn != nil {
							return nil, test.errFn(*ba.Txn)
						}
						return nil, roachpb.NewErrorWithTxn(test.err, ba.Txn)
					}
					abort.Store(true)
				default:
					t.Fatalf("unexpected batch: %s", ba)
				}
				return br, nil
			}
			ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
			factory := NewTxnCoordSenderFactory(
				ambient,
				cluster.MakeTestingClusterSettings(),
				senderFn,
				clock,
				false,
				stopper,
				MakeTxnMetrics(metric.TestSampleInterval),
			)

			db := client.NewDB(factory, clock)
			txn := client.NewTxn(db, 0 /* gatewayNodeID */, client.RootTxn)
			if pErr := txn.Put(context.Background(), "a", "b"); pErr != nil {
				t.Fatalf("put failed: %s", pErr)
			}
			if pErr := txn.CommitOrCleanup(context.Background()); pErr == nil {
				t.Fatalf("unexpected commit success")
			}

			if !commit.Load().(bool) {
				t.Errorf("%T: failed to find initial commit request", test.err)
			}
			if !test.asyncAbort && !abort.Load().(bool) {
				t.Errorf("%T: failed to find expected synchronous abort", test.err)
			} else {
				testutils.SucceedsSoon(t, func() error {
					if !abort.Load().(bool) {
						return errors.Errorf("%T: failed to find expected asynchronous abort", test.err)
					}
					return nil
				})
			}
		})
	}
}
