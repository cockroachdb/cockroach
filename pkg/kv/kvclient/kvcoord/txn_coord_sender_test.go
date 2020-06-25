// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// createTestDB creates a local test server and starts it. The caller
// is responsible for stopping the test server.
func createTestDB(t testing.TB) *localtestcluster.LocalTestCluster {
	return createTestDBWithContextAndKnobs(t, kv.DefaultDBContext(), nil)
}

func createTestDBWithContextAndKnobs(
	t testing.TB, dbCtx kv.DBContext, knobs *kvserver.StoreTestingKnobs,
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

// TestTxnCoordSenderBeginTransaction verifies that a command sent with a
// not-nil Txn with empty ID gets a new transaction initialized.
func TestTxnCoordSenderBeginTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	ctx := context.Background()

	txn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)

	// Put request will create a new transaction.
	key := roachpb.Key("key")
	txn.TestingSetPriority(10)
	txn.SetDebugName("test txn")
	if err := txn.Put(ctx, key, []byte("value")); err != nil {
		t.Fatal(err)
	}
	proto := txn.TestingCloneTxn()
	if proto.Name != "test txn" {
		t.Errorf("expected txn name to be %q; got %q", "test txn", proto.Name)
	}
	if proto.Priority != 10 {
		t.Errorf("expected txn priority 10; got %d", proto.Priority)
	}
	if !bytes.Equal(proto.Key, key) {
		t.Errorf("expected txn Key to match %q != %q", key, proto.Key)
	}
}

// TestTxnCoordSenderKeyRanges verifies that multiple requests to same or
// overlapping key ranges causes the coordinator to keep track only of
// the minimum number of ranges.
func TestTxnCoordSenderKeyRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
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

	txn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
	// Disable txn pipelining so that all write spans are immediately
	// added to the transaction's lock footprint.
	if err := txn.DisablePipelining(); err != nil {
		t.Fatal(err)
	}
	tc := txn.Sender().(*TxnCoordSender)

	for _, rng := range ranges {
		if rng.end != nil {
			if err := txn.DelRange(ctx, rng.start, rng.end); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := txn.Put(ctx, rng.start, []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Verify that the transaction coordinator is only tracking two lock
	// spans. "a" and range "aa"-"c".
	tc.interceptorAlloc.txnPipeliner.lockFootprint.mergeAndSort()
	lockSpans := tc.interceptorAlloc.txnPipeliner.lockFootprint.asSlice()
	if len(lockSpans) != 2 {
		t.Errorf("expected 2 entries in keys range group; got %v", lockSpans)
	}
}

// TestTxnCoordSenderCondenseLockSpans verifies that lock spans are condensed
// along range boundaries when they exceed the maximum intent bytes threshold.
func TestTxnCoordSenderCondenseLockSpans(t *testing.T) {
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
		span         roachpb.Span
		expLocks     []roachpb.Span
		expLocksSize int64
	}{
		{span: a, expLocks: []roachpb.Span{a}, expLocksSize: 1},
		{span: b, expLocks: []roachpb.Span{a, b}, expLocksSize: 2},
		{span: c, expLocks: []roachpb.Span{a, b, c}, expLocksSize: 3},
		{span: d, expLocks: []roachpb.Span{a, b, c, d}, expLocksSize: 9},
		// Note that c-e condenses and then lists first.
		{span: e, expLocks: []roachpb.Span{cToEClosed, a, b}, expLocksSize: 5},
		{span: fTof0, expLocks: []roachpb.Span{cToEClosed, a, b, fTof0}, expLocksSize: 8},
		{span: g, expLocks: []roachpb.Span{cToEClosed, a, b, fTof0, g}, expLocksSize: 9},
		{span: g0Tog1, expLocks: []roachpb.Span{fTog1Closed, cToEClosed, aToBClosed}, expLocksSize: 9},
		// Add a key in the middle of a span, which will get merged on commit.
		{span: c, expLocks: []roachpb.Span{aToBClosed, cToEClosed, fTog1Closed}, expLocksSize: 9},
	}
	splits := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("f")},
		{Key: roachpb.Key("f"), EndKey: roachpb.Key("j")},
	}
	descs := []roachpb.RangeDescriptor{testMetaRangeDescriptor}
	for i, s := range splits {
		descs = append(descs, roachpb.RangeDescriptor{
			RangeID:          roachpb.RangeID(2 + i),
			StartKey:         roachpb.RKey(s.Key),
			EndKey:           roachpb.RKey(s.EndKey),
			InternalReplicas: []roachpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1}},
		})
	}
	descDB := mockRangeDescriptorDBForDescs(descs...)
	s := createTestDB(t)
	st := s.Store.ClusterSettings()
	trackedWritesMaxSize.Override(&st.SV, 10) /* 10 bytes and it will condense */
	defer s.Stop()

	// Check end transaction locks, which should be condensed and split
	// at range boundaries.
	expLocks := []roachpb.Span{aToBClosed, cToEClosed, fTog1Closed}
	var sendFn simpleSendFn = func(
		_ context.Context, _ SendOptions, _ ReplicaSlice, args roachpb.BatchRequest,
	) (*roachpb.BatchResponse, error) {
		resp := args.CreateReply()
		resp.Txn = args.Txn
		if req, ok := args.GetArg(roachpb.EndTxn); ok {
			if !req.(*roachpb.EndTxnRequest).Commit {
				t.Errorf("expected commit to be true")
			}
			et := req.(*roachpb.EndTxnRequest)
			if a, e := et.LockSpans, expLocks; !reflect.DeepEqual(a, e) {
				t.Errorf("expected end transaction to have locks %+v; got %+v", e, a)
			}
			resp.Txn.Status = roachpb.COMMITTED
		}
		return resp, nil
	}
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	ds := NewDistSender(DistSenderConfig{
		AmbientCtx: ambient,
		Clock:      s.Clock,
		NodeDescs:  s.Gossip,
		RPCContext: s.Cfg.RPCContext,
		TestingKnobs: ClientTestingKnobs{
			TransportFactory: adaptSimpleTransport(sendFn),
		},
		RangeDescriptorDB: descDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	})
	tsf := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Settings:   st,
			Clock:      s.Clock,
			Stopper:    s.Stopper,
		},
		ds,
	)
	db := kv.NewDB(ambient, tsf, s.Clock)
	ctx := context.Background()

	txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
	// Disable txn pipelining so that all write spans are immediately
	// added to the transaction's lock footprint.
	if err := txn.DisablePipelining(); err != nil {
		t.Fatal(err)
	}
	for i, tc := range testCases {
		if tc.span.EndKey != nil {
			if err := txn.DelRange(ctx, tc.span.Key, tc.span.EndKey); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := txn.Put(ctx, tc.span.Key, []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		tcs := txn.Sender().(*TxnCoordSender)
		locks := tcs.interceptorAlloc.txnPipeliner.lockFootprint.asSlice()
		if a, e := locks, tc.expLocks; !reflect.DeepEqual(a, e) {
			t.Errorf("%d: expected keys %+v; got %+v", i, e, a)
		}
		locksSize := int64(0)
		for _, i := range locks {
			locksSize += int64(len(i.Key) + len(i.EndKey))
		}
		if a, e := locksSize, tc.expLocksSize; a != e {
			t.Errorf("%d: keys size expected %d; got %d", i, e, a)
		}
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

// Test that the theartbeat loop detects aborted transactions and stops.
func TestTxnCoordSenderHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDBWithContextAndKnobs(t, kv.DefaultDBContext(), &kvserver.StoreTestingKnobs{
		DisableScanner:    true,
		DisableSplitQueue: true,
		DisableMergeQueue: true,
	})
	defer s.Stop()
	ctx := context.Background()

	keyA := roachpb.Key("a")
	keyC := roachpb.Key("c")
	splitKey := roachpb.Key("b")
	if err := s.DB.AdminSplit(ctx, splitKey /* splitKey */, hlc.MaxTimestamp /* expirationTimestamp */); err != nil {
		t.Fatal(err)
	}

	// Make a db with a short heartbeat interval.
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	tsf := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			// Short heartbeat interval.
			HeartbeatInterval: time.Millisecond,
			Settings:          s.Cfg.Settings,
			Clock:             s.Clock,
			Stopper:           s.Stopper,
		},
		NewDistSenderForLocalTestCluster(
			s.Cfg.Settings, &roachpb.NodeDescriptor{NodeID: 1},
			ambient.Tracer, s.Clock, s.Latency, s.Stores, s.Stopper, s.Gossip,
		),
	)
	quickHeartbeatDB := kv.NewDB(ambient, tsf, s.Clock)

	// We're going to test twice. In both cases the heartbeat is supposed to
	// notice that its transaction is aborted, but:
	// - once the abort span is populated on the txn's range.
	// - once the abort span is not populated.
	// The two conditions are created by either clearing an intent from the txn's
	// range or not (i.e. clearing an intent from another range).
	// The difference is supposed to be immaterial for the heartbeat loop (that's
	// what we're testing). As of June 2018, HeartbeatTxnRequests don't check the
	// abort span.
	for _, pusherKey := range []roachpb.Key{keyA, keyC} {
		t.Run(fmt.Sprintf("pusher:%s", pusherKey), func(t *testing.T) {
			// Make a db with a short heartbeat interval.
			initialTxn := kv.NewTxn(ctx, quickHeartbeatDB, 0 /* gatewayNodeID */)
			tc := initialTxn.Sender().(*TxnCoordSender)

			if err := initialTxn.Put(ctx, keyA, []byte("value")); err != nil {
				t.Fatal(err)
			}
			if err := initialTxn.Put(ctx, keyC, []byte("value")); err != nil {
				t.Fatal(err)
			}

			// Verify 3 heartbeats.
			var heartbeatTS hlc.Timestamp
			for i := 0; i < 3; i++ {
				testutils.SucceedsSoon(t, func() error {
					txn, pErr := getTxn(ctx, initialTxn)
					if pErr != nil {
						t.Fatal(pErr)
					}
					// Advance clock by 1ns.
					s.Manual.Increment(1)
					if lastActive := txn.LastActive(); heartbeatTS.Less(lastActive) {
						heartbeatTS = lastActive
						return nil
					}
					return errors.Errorf("expected heartbeat")
				})
			}

			// Push our txn with another high-priority txn.
			{
				if err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
						return err
					}
					return txn.Put(ctx, pusherKey, []byte("pusher val"))
				}); err != nil {
					t.Fatal(err)
				}
			}

			// Verify that the abort is discovered and the heartbeat discontinued.
			// This relies on the heartbeat loop stopping once it figures out that the txn
			// has been aborted.
			testutils.SucceedsSoon(t, func() error {
				if tc.IsTracking() {
					return fmt.Errorf("transaction is not aborted")
				}
				return nil
			})

			// Trying to do something else should give us a TransactionAbortedError.
			_, err := initialTxn.Get(ctx, "a")
			assertTransactionAbortedError(t, err)
		})
	}
}

// getTxn fetches the requested key and returns the transaction info.
func getTxn(ctx context.Context, txn *kv.Txn) (*roachpb.Transaction, *roachpb.Error) {
	txnMeta := txn.TestingCloneTxn().TxnMeta
	qt := &roachpb.QueryTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txnMeta.Key,
		},
		Txn: txnMeta,
	}

	ba := roachpb.BatchRequest{}
	ba.Timestamp = txnMeta.WriteTimestamp
	ba.Add(qt)

	db := txn.DB()
	sender := db.NonTransactionalSender()

	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}
	return &br.Responses[0].GetInner().(*roachpb.QueryTxnResponse).QueriedTxn, nil
}

func verifyCleanup(key roachpb.Key, eng storage.Engine, t *testing.T, coords ...*TxnCoordSender) {
	testutils.SucceedsSoon(t, func() error {
		for _, coord := range coords {
			if coord.IsTracking() {
				return fmt.Errorf("expected no heartbeat")
			}
		}
		meta := &enginepb.MVCCMetadata{}
		//lint:ignore SA1019 historical usage of deprecated eng.GetProto is OK
		ok, _, _, err := eng.GetProto(storage.MakeMVCCMetadataKey(key), meta)
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
// sends resolve write intent requests.
func TestTxnCoordSenderEndTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	ctx := context.Background()

	// 4 cases: no deadline, past deadline, equal deadline, future deadline.
	for i := 0; i < 4; i++ {
		key := roachpb.Key("key: " + strconv.Itoa(i))
		txn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
		// Initialize the transaction.
		if pErr := txn.Put(ctx, key, []byte("value")); pErr != nil {
			t.Fatal(pErr)
		}
		// Conflicting transaction that pushes the above transaction.
		conflictTxn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
		conflictTxn.TestingSetPriority(enginepb.MaxTxnPriority)
		if _, pErr := conflictTxn.Get(ctx, key); pErr != nil {
			t.Fatal(pErr)
		}

		// The transaction was pushed at least to conflictTxn's timestamp (but
		// it could have been pushed more - the push takes a timestamp off the
		// HLC).
		pusheeTxn, pErr := getTxn(ctx, txn)
		if pErr != nil {
			t.Fatal(pErr)
		}
		pushedTimestamp := pusheeTxn.WriteTimestamp

		{
			var err error
			switch i {
			case 0:
				// No deadline.

			case 1:
				// Past deadline.
				if !txn.UpdateDeadlineMaybe(ctx, pushedTimestamp.Prev()) {
					t.Fatalf("did not update deadline")
				}

			case 2:
				// Equal deadline.
				if !txn.UpdateDeadlineMaybe(ctx, pushedTimestamp) {
					t.Fatalf("did not update deadline")
				}

			case 3:
				// Future deadline.

				if !txn.UpdateDeadlineMaybe(ctx, pushedTimestamp.Next()) {
					t.Fatalf("did not update deadline")
				}
			}
			err = txn.CommitOrCleanup(ctx)

			switch i {
			case 0:
				// No deadline.
				if err != nil {
					t.Fatal(err)
				}

			case 1:
				// Past deadline.
				fallthrough
			case 2:
				// Equal deadline.
				assertTransactionRetryError(t, err)
				if !testutils.IsError(err, "RETRY_COMMIT_DEADLINE_EXCEEDED") {
					t.Fatalf("expected deadline exceeded, got: %s", err)
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

// TestTxnCoordSenderAddLockOnError verifies that locks are tracked if the
// transaction is, even on error.
func TestTxnCoordSenderAddLockOnError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	ctx := context.Background()

	// Create a transaction with intent at "x".
	key := roachpb.Key("x")
	txn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
	tc := txn.Sender().(*TxnCoordSender)

	// Write so that the coordinator begins tracking this txn.
	if err := txn.Put(ctx, "x", "y"); err != nil {
		t.Fatal(err)
	}
	{
		err := txn.CPut(ctx, key, []byte("x"), kvclientutils.StrToCPutExistingValue("born to fail"))
		if !errors.HasType(err, (*roachpb.ConditionFailedError)(nil)) {
			t.Fatal(err)
		}
	}
	tc.interceptorAlloc.txnPipeliner.lockFootprint.mergeAndSort()
	lockSpans := tc.interceptorAlloc.txnPipeliner.lockFootprint.asSlice()
	expSpans := []roachpb.Span{{Key: key, EndKey: []byte("")}}
	equal := !reflect.DeepEqual(lockSpans, expSpans)
	if err := txn.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Fatalf("expected stored locks %v, got %v", expSpans, lockSpans)
	}
}

func assertTransactionRetryError(t *testing.T, e error) {
	t.Helper()
	if retErr := (*roachpb.TransactionRetryWithProtoRefreshError)(nil); errors.As(e, &retErr) {
		if !testutils.IsError(retErr, "TransactionRetryError") {
			t.Fatalf("expected the cause to be TransactionRetryError, but got %s",
				retErr)
		}
	} else {
		t.Fatalf("expected a retryable error, but got %s (%T)", e, e)
	}
}

func assertTransactionAbortedError(t *testing.T, e error) {
	if retErr := (*roachpb.TransactionRetryWithProtoRefreshError)(nil); errors.As(e, &retErr) {
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
	ctx := context.Background()

	// Create a transaction with intent at "a".
	key := roachpb.Key("a")
	txn1 := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
	if err := txn1.Put(ctx, key, []byte("value")); err != nil {
		t.Fatal(err)
	}

	// Push the transaction (by writing key "a" with higher priority) to abort it.
	txn2 := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
	if err := txn2.SetUserPriority(roachpb.MaxUserPriority); err != nil {
		t.Fatal(err)
	}
	if err := txn2.Put(ctx, key, []byte("value2")); err != nil {
		t.Fatal(err)
	}

	// Now end the transaction and verify we've cleanup up, even though
	// end transaction failed.
	err := txn1.CommitOrCleanup(ctx)
	assertTransactionAbortedError(t, err)
	if err := txn2.CommitOrCleanup(ctx); err != nil {
		t.Fatal(err)
	}
	verifyCleanup(key, s.Eng, t, txn1.Sender().(*TxnCoordSender), txn2.Sender().(*TxnCoordSender))
}

// TestTxnCoordSenderCleanupOnCommitAfterRestart verifies that if a txn restarts
// at a higher epoch and then commits before it has acquired any locks in the new
// epoch, the coordinator still cleans up the transaction. In #40466, we saw that
// this case could be detected as a 1PC transaction and the cleanup during the
// commit could be omitted.
func TestTxnCoordSenderCleanupOnCommitAfterRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	ctx := context.Background()

	// Create a transaction with intent at "a".
	key := roachpb.Key("a")
	txn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
	if err := txn.Put(ctx, key, []byte("value")); err != nil {
		t.Fatal(err)
	}

	// Restart the transaction with a new epoch.
	txn.ManualRestart(ctx, s.Clock.Now())

	// Now immediately commit.
	if err := txn.CommitOrCleanup(ctx); err != nil {
		t.Fatal(err)
	}
	verifyCleanup(key, s.Eng, t, txn.Sender().(*TxnCoordSender))
}

// TestTxnCoordSenderGCWithAmbiguousResultErr verifies that the coordinator
// cleans up extant transactions and locks after an ambiguous result error is
// observed, even if the error is on the first request.
func TestTxnCoordSenderGCWithAmbiguousResultErr(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "errOnFirst", func(t *testing.T, errOnFirst bool) {
		key := roachpb.Key("a")
		are := roachpb.NewAmbiguousResultError("very ambiguous")
		knobs := &kvserver.StoreTestingKnobs{
			TestingResponseFilter: func(ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
				for _, req := range ba.Requests {
					if putReq, ok := req.GetInner().(*roachpb.PutRequest); ok && putReq.Key.Equal(key) {
						return roachpb.NewError(are)
					}
				}
				return nil
			},
		}

		s := createTestDBWithContextAndKnobs(t, kv.DefaultDBContext(), knobs)
		defer s.Stop()

		ctx := context.Background()
		txn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
		tc := txn.Sender().(*TxnCoordSender)
		if !errOnFirst {
			otherKey := roachpb.Key("other")
			if err := txn.Put(ctx, otherKey, []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		if err := txn.Put(ctx, key, []byte("value")); !testutils.IsError(err, "result is ambiguous") {
			t.Fatalf("expected error %v, found %v", are, err)
		}

		if err := txn.Rollback(ctx); err != nil {
			t.Fatal(err)
		}

		testutils.SucceedsSoon(t, func() error {
			// Locking the TxnCoordSender to prevent a data race.
			if tc.IsTracking() {
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
	ctx := context.Background()
	origTS := makeTS(123, 0)
	plus10 := origTS.Add(10, 10)
	plus20 := origTS.Add(20, 0)
	testCases := []struct {
		// The test's name.
		name             string
		pErrGen          func(txn *roachpb.Transaction) *roachpb.Error
		expEpoch         enginepb.TxnEpoch
		expPri           enginepb.TxnPriority
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
				txn.WriteTimestamp = plus20
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
						TxnMeta: enginepb.TxnMeta{WriteTimestamp: plus10, Priority: 10},
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
				txn.WriteTimestamp = plus10
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

			var senderFn kv.SenderFunc = func(
				_ context.Context, ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, *roachpb.Error) {
				var reply *roachpb.BatchResponse
				pErr := test.pErrGen(ba.Txn)
				if pErr == nil {
					reply = ba.CreateReply()
					reply.Txn = ba.Txn
				} else if txn := pErr.GetTxn(); txn != nil {
					// Update the manual clock to simulate an
					// error updating a local hlc clock.
					manual.Set(txn.WriteTimestamp.WallTime)
				}
				return reply, pErr
			}
			ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
			tsf := NewTxnCoordSenderFactory(
				TxnCoordSenderFactoryConfig{
					AmbientCtx: ambient,
					Clock:      clock,
					Stopper:    stopper,
				},
				senderFn,
			)
			db := kv.NewDB(ambient, tsf, clock)
			key := roachpb.Key("test-key")
			now := clock.Now()
			origTxnProto := roachpb.MakeTransaction(
				"test txn",
				key,
				roachpb.UserPriority(0),
				now,
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
			txn := kv.NewTxnFromProto(ctx, db, 0 /* gatewayNodeID */, now, kv.RootTxn, &origTxnProto)
			txn.TestingSetPriority(1)

			err := txn.Put(ctx, key, []byte("value"))
			stopper.Stop(ctx)

			if test.name != "nil" && err == nil {
				t.Fatalf("expected an error")
			}
			proto := txn.TestingCloneTxn()
			txnReset := origTxnProto.ID != proto.ID
			if txnReset != test.expNewTransaction {
				t.Fatalf("expected txn reset: %t and got: %t", test.expNewTransaction, txnReset)
			}
			if proto.Epoch != test.expEpoch {
				t.Errorf("expected epoch = %d; got %d",
					test.expEpoch, proto.Epoch)
			}
			if proto.Priority != test.expPri {
				t.Errorf("expected priority = %d; got %d",
					test.expPri, proto.Priority)
			}
			if proto.WriteTimestamp != test.expTS {
				t.Errorf("expected timestamp to be %s; got %s",
					test.expTS, proto.WriteTimestamp)
			}
			if proto.ReadTimestamp != test.expOrigTS {
				t.Errorf("expected orig timestamp to be %s; got %s",
					test.expOrigTS, proto.ReadTimestamp)
			}
			if ns := proto.ObservedTimestamps; (len(ns) != 0) != test.nodeSeen {
				t.Errorf("expected nodeSeen=%t, but list of hosts is %v",
					test.nodeSeen, ns)
			}
		})
	}
}

// TestTxnMultipleCoord checks that multiple txn coordinators can be
// used for reads by a single transaction, and their state can be combined.
func TestTxnMultipleCoord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	ctx := context.Background()
	txn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)

	// Start the transaction.
	key := roachpb.Key("a")
	if _, err := txn.Get(ctx, key); err != nil {
		t.Fatal(err)
	}

	// New create a second, leaf coordinator.
	leafInputState := txn.GetLeafTxnInputState(ctx)
	txn2 := kv.NewLeafTxn(ctx, s.DB, 0 /* gatewayNodeID */, &leafInputState)

	// Start the second transaction.
	key2 := roachpb.Key("b")
	if _, err := txn2.Get(ctx, key2); err != nil {
		t.Fatal(err)
	}

	// Augment txn with txn2's meta & commit.
	tfs, err := txn2.GetLeafTxnFinalState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.UpdateRootWithLeafFinalState(ctx, &tfs); err != nil {
		t.Fatal(err)
	}

	// Verify presence of both locks.
	tcs := txn.Sender().(*TxnCoordSender)
	refreshSpans := tcs.interceptorAlloc.txnSpanRefresher.refreshFootprint.asSlice()
	require.Equal(t, []roachpb.Span{{Key: key}, {Key: key2}}, refreshSpans)

	ba := txn.NewBatch()
	ba.AddRawRequest(&roachpb.EndTxnRequest{Commit: true})
	if err := txn.Run(ctx, ba); err != nil {
		t.Fatal(err)
	}
}

// TestTxnCoordSenderNoDuplicateLockSpans verifies that TxnCoordSender does not
// generate duplicate lock spans and that it merges lock spans that have
// overlapping ranges.
func TestTxnCoordSenderNoDuplicateLockSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)

	var expectedLockSpans []roachpb.Span

	var senderFn kv.SenderFunc = func(_ context.Context, ba roachpb.BatchRequest) (
		*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		if rArgs, ok := ba.GetArg(roachpb.EndTxn); ok {
			et := rArgs.(*roachpb.EndTxnRequest)
			if !reflect.DeepEqual(et.LockSpans, expectedLockSpans) {
				t.Errorf("Invalid lock spans: %+v; expected %+v", et.LockSpans, expectedLockSpans)
			}
			br.Txn.Status = roachpb.COMMITTED
		}
		return br, nil
	}
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		senderFn,
	)
	defer stopper.Stop(ctx)

	db := kv.NewDB(ambient, factory, clock)
	txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)

	// Acquire locks on a-b, c, u-w before the final batch.
	_, pErr := txn.ReverseScanForUpdate(ctx, roachpb.Key("a"), roachpb.Key("b"), 0)
	if pErr != nil {
		t.Fatal(pErr)
	}
	pErr = txn.Put(ctx, roachpb.Key("c"), []byte("value"))
	if pErr != nil {
		t.Fatal(pErr)
	}
	pErr = txn.DelRange(ctx, roachpb.Key("u"), roachpb.Key("w"))
	if pErr != nil {
		t.Fatal(pErr)
	}

	// The final batch overwrites key c and overlaps part of the a-b and u-w ranges.
	b := txn.NewBatch()
	b.Put(roachpb.Key("b"), []byte("value"))
	b.Put(roachpb.Key("c"), []byte("value"))
	b.Put(roachpb.Key("d"), []byte("value"))
	b.ReverseScanForUpdate(roachpb.Key("v"), roachpb.Key("z"))

	// The expected locks are a-b, c, and u-z.
	expectedLockSpans = []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("b").Next()},
		{Key: roachpb.Key("c"), EndKey: nil},
		{Key: roachpb.Key("d"), EndKey: nil},
		{Key: roachpb.Key("u"), EndKey: roachpb.Key("z")},
	}

	pErr = txn.CommitInBatch(ctx, b)
	if pErr != nil {
		t.Fatal(pErr)
	}
}

// checkTxnMetrics verifies that the provided Sender's transaction metrics match the expected
// values. This is done through a series of retries with increasing backoffs, to work around
// the TxnCoordSender's asynchronous updating of metrics after a transaction ends.
func checkTxnMetrics(
	t *testing.T, metrics TxnMetrics, name string, commits, commits1PC, aborts, restarts int64,
) {
	testutils.SucceedsSoon(t, func() error {
		return checkTxnMetricsOnce(t, metrics, name, commits, commits1PC, aborts, restarts)
	})
}

func checkTxnMetricsOnce(
	t *testing.T, metrics TxnMetrics, name string, commits, commits1PC, aborts, restarts int64,
) error {
	testcases := []struct {
		name string
		a, e int64
	}{
		{"commits", metrics.Commits.Count(), commits},
		{"commits1PC", metrics.Commits1PC.Count(), commits1PC},
		{"aborts", metrics.Aborts.Count(), aborts},
		{"durations", metrics.Durations.TotalCount(), commits + aborts},
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
	dbCtx := kv.DefaultDBContext()
	s := &localtestcluster.LocalTestCluster{
		DBContext: &dbCtx,
		// Liveness heartbeat txns mess up the metrics.
		DisableLivenessHeartbeat: true,
		DontCreateSystemRanges:   true,
	}
	s.Start(t, testutils.NewNodeTestBaseContext(), InitFactoryForLocalTestCluster)

	metrics := MakeTxnMetrics(metric.TestSampleInterval)
	s.DB.GetFactory().(*TxnCoordSenderFactory).metrics = metrics
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

	// Test a write txn commit.
	if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		key := []byte("key-commit")
		return txn.Put(ctx, key, value)
	}); err != nil {
		t.Fatal(err)
	}
	checkTxnMetrics(t, metrics, "commit txn", 1 /* commits */, 0 /* commits1PC */, 0, 0)

	// Test a read-only txn.
	if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		key := []byte("key-commit")
		_, err := txn.Get(ctx, key)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	checkTxnMetrics(t, metrics, "commit txn", 2 /* commits */, 0 /* commits1PC */, 0, 0)
}

// TestTxnOnePhaseCommit verifies that 1PC metric tracking works.
func TestTxnOnePhaseCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, metrics, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()

	value := []byte("value")

	ctx := context.Background()
	if err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		key := []byte("key-commit")
		b := txn.NewBatch()
		b.Put(key, value)
		return txn.CommitInBatch(ctx, b)
	}); err != nil {
		t.Fatal(err)
	}
	kv, err := s.DB.Get(ctx, []byte("key-commit"))
	if err != nil {
		t.Fatal(err)
	}
	if kv.Value == nil {
		t.Fatal("expected value not found")
	}
	val, err := kv.Value.GetBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(val, value) {
		t.Fatalf("expected: %s, got: %s", value, val)
	}
	checkTxnMetrics(t, metrics, "commit 1PC txn", 1 /* commits */, 1 /* 1PC */, 0, 0)
}

func TestTxnAbortCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, metrics, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()

	value := []byte("value")

	const intentionalErrText = "intentional error to cause abort"
	// Test aborted transaction.
	if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		key := []byte("key-abort")

		if err := txn.Put(ctx, key, value); err != nil {
			t.Fatal(err)
		}

		return errors.New(intentionalErrText)
	}); !testutils.IsError(err, intentionalErrText) {
		t.Fatalf("unexpected error: %v", err)
	}
	checkTxnMetrics(t, metrics, "abort txn", 0, 0, 1 /* aborts */, 0)
}

func TestTxnRestartCount(t *testing.T) {
	defer leaktest.AfterTest(t)()

	readKey := []byte("read")
	writeKey := []byte("write")
	value := []byte("value")
	ctx := context.Background()

	s, metrics, cleanupFn := setupMetricsTest(t)
	defer cleanupFn()

	// Start a transaction and read a key that we're going to modify outside the
	// txn. This ensures that refreshing the txn will not succeed, so a restart
	// will be necessary.
	txn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
	if _, err := txn.Get(ctx, readKey); err != nil {
		t.Fatal(err)
	}

	// Write the read key outside of the transaction, at a higher timestamp, which
	// will necessitate a txn restart when the original read key span is updated.
	if err := s.DB.Put(ctx, readKey, value); err != nil {
		t.Fatal(err)
	}

	// Outside of the transaction, read the same key as will be
	// written within the transaction. This means that future
	// attempts to write will forward the txn timestamp.
	if _, err := s.DB.Get(ctx, writeKey); err != nil {
		t.Fatal(err)
	}

	// This put will lay down an intent, txn timestamp will increase
	// beyond DeprecatedOrigTimestamp.
	if err := txn.Put(ctx, writeKey, value); err != nil {
		t.Fatal(err)
	}
	proto := txn.TestingCloneTxn()
	if proto.WriteTimestamp.LessEq(proto.ReadTimestamp) {
		t.Errorf("expected timestamp to increase: %s", proto)
	}

	// Wait for heartbeat to start.
	tc := txn.Sender().(*TxnCoordSender)
	testutils.SucceedsSoon(t, func() error {
		if !tc.IsTracking() {
			return errors.New("expected heartbeat to start")
		}
		return nil
	})

	// Commit (should cause restart metric to increase).
	err := txn.CommitOrCleanup(ctx)
	assertTransactionRetryError(t, err)
	checkTxnMetrics(t, metrics, "restart txn", 0, 0, 1 /* aborts */, 1 /* restarts */)
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
		if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			if err := txn.Put(ctx, key, []byte("val")); err != nil {
				return err
			}
			manual.Increment(incr)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	checkTxnMetrics(t, metrics, "txn durations", puts, 0, 0, 0)

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

// TestAbortTransactionOnCommitErrors verifies that transactions are
// aborted on the correct errors.
func TestAbortTransactionOnCommitErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
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
			defer stopper.Stop(ctx)
			var senderFn kv.SenderFunc = func(
				_ context.Context, ba roachpb.BatchRequest,
			) (*roachpb.BatchResponse, *roachpb.Error) {
				br := ba.CreateReply()
				br.Txn = ba.Txn.Clone()

				if _, hasPut := ba.GetArg(roachpb.Put); hasPut {
					if _, ok := ba.Requests[0].GetInner().(*roachpb.PutRequest); !ok {
						t.Fatalf("expected Put")
					}
					union := &br.Responses[0] // avoid operating on copy
					union.MustSetInner(&roachpb.PutResponse{})
					if ba.Txn != nil && br.Txn == nil {
						br.Txn.Status = roachpb.PENDING
					}
				} else if et, hasET := ba.GetArg(roachpb.EndTxn); hasET {
					if et.(*roachpb.EndTxnRequest).Commit {
						commit.Store(true)
						if test.errFn != nil {
							return nil, test.errFn(*ba.Txn)
						}
						return nil, roachpb.NewErrorWithTxn(test.err, ba.Txn)
					}
					abort.Store(true)
				} else {
					t.Fatalf("unexpected batch: %s", ba)
				}
				return br, nil
			}
			ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
			factory := NewTxnCoordSenderFactory(
				TxnCoordSenderFactoryConfig{
					AmbientCtx: ambient,
					Clock:      clock,
					Stopper:    stopper,
					Settings:   cluster.MakeTestingClusterSettings(),
				},
				senderFn,
			)

			db := kv.NewDB(ambient, factory, clock)
			txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
			if pErr := txn.Put(ctx, "a", "b"); pErr != nil {
				t.Fatalf("put failed: %s", pErr)
			}
			if pErr := txn.CommitOrCleanup(ctx); pErr == nil {
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

// mockSender is a client.Sender implementation that passes requests to a list
// of provided matchers, in sequence. The first matcher that returns either a
// response or an error is used to provide the result for the request.
type mockSender struct {
	matchers []matcher
}

var _ kv.Sender = &mockSender{}

type matcher func(roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

// match adds a matcher to the list of matchers.
func (s *mockSender) match(m matcher) {
	s.matchers = append(s.matchers, m)
}

// Send implements the client.Sender interface.
func (s *mockSender) Send(
	_ context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	for _, m := range s.matchers {
		br, pErr := m(ba)
		if br != nil || pErr != nil {
			return br, pErr
		}
	}
	// If none of the matchers triggered, just create an empty reply.
	br := ba.CreateReply()
	br.Txn = ba.Txn.Clone()
	return br, nil
}

// Test that a rollback sent to the TxnCoordSender stops the heartbeat loop even
// if it encounters an error. As of June 2018, there's a separate code path for
// handling errors on rollback in this regard.
func TestRollbackErrorStopsHeartbeat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)
	db := kv.NewDB(ambient, factory, clock)

	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if _, ok := ba.GetArg(roachpb.EndTxn); !ok {
			resp := ba.CreateReply()
			resp.Txn = ba.Txn
			return resp, nil
		}
		return nil, roachpb.NewErrorf("injected err")
	})

	txn := kv.NewTxn(ctx, db, roachpb.NodeID(1))
	txnHeader := roachpb.Header{
		Txn: txn.TestingCloneTxn(),
	}
	if _, pErr := kv.SendWrappedWith(
		ctx, txn, txnHeader, &roachpb.PutRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: roachpb.Key("a"),
			},
		},
	); pErr != nil {
		t.Fatal(pErr)
	}
	if !txn.Sender().(*TxnCoordSender).IsTracking() {
		t.Fatalf("expected TxnCoordSender to be tracking after the write")
	}

	if _, pErr := kv.SendWrappedWith(
		ctx, txn, txnHeader,
		&roachpb.EndTxnRequest{Commit: false},
	); !testutils.IsPError(pErr, "injected err") {
		t.Fatal(pErr)
	}

	testutils.SucceedsSoon(t, func() error {
		if txn.Sender().(*TxnCoordSender).IsTracking() {
			return fmt.Errorf("still tracking")
		}
		return nil
	})
}

// Test that lock tracking behaves correctly for transactions that attempt to
// run a batch containing an EndTxn. Since in case of an error it's not easy to
// determine whether any locks have been laid down (i.e. in case the batch was
// split by the DistSender and then there was mixed success for the sub-batches,
// or in case a retriable error is returned), the test verifies that all
// possible locks are properly tracked and attached to a subsequent EndTxn.
func TestOnePCErrorTracking(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: log.AmbientContext{Tracer: tracing.NewTracer()},
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)
	db := kv.NewDB(ambient, factory, clock)
	keyA, keyB, keyC := roachpb.Key("a"), roachpb.Key("b"), roachpb.Key("c")

	// Register a matcher catching the commit attempt.
	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if et, ok := ba.GetArg(roachpb.EndTxn); !ok {
			return nil, nil
		} else if !et.(*roachpb.EndTxnRequest).Commit {
			return nil, nil
		}
		return nil, roachpb.NewErrorf("injected err")
	})
	// Register a matcher catching the rollback attempt.
	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		et, ok := ba.GetArg(roachpb.EndTxn)
		if !ok {
			return nil, nil
		}
		etReq := et.(*roachpb.EndTxnRequest)
		if etReq.Commit {
			return nil, nil
		}
		expLocks := []roachpb.Span{{Key: keyA}, {Key: keyB, EndKey: keyC}}
		locks := etReq.LockSpans
		if !reflect.DeepEqual(locks, expLocks) {
			return nil, roachpb.NewErrorf("expected locks %s, got: %s", expLocks, locks)
		}
		resp := ba.CreateReply()
		// Set the response's txn to the Aborted status (as the server would). This
		// will make the TxnCoordSender stop the heartbeat loop.
		resp.Txn = ba.Txn.Clone()
		resp.Txn.Status = roachpb.ABORTED
		return resp, nil
	})

	txn := kv.NewTxn(ctx, db, roachpb.NodeID(1))
	txnHeader := roachpb.Header{
		Txn: txn.TestingCloneTxn(),
	}
	b := txn.NewBatch()
	b.Put(keyA, "test value")
	b.ScanForUpdate(keyB, keyC)
	if err := txn.CommitInBatch(ctx, b); !testutils.IsError(err, "injected err") {
		t.Fatal(err)
	}

	// Now send a rollback and verify that the TxnCoordSender attaches the locks
	// to it.
	if _, pErr := kv.SendWrappedWith(
		ctx, txn, txnHeader,
		&roachpb.EndTxnRequest{Commit: false},
	); pErr != nil {
		t.Fatal(pErr)
	}

	// As always, check that the rollback we just sent stops the heartbeat loop.
	testutils.SucceedsSoon(t, func() error {
		if txn.Sender().(*TxnCoordSender).IsTracking() {
			return fmt.Errorf("still tracking")
		}
		return nil
	})
}

// TestCommitReadOnlyTransaction verifies that a read-only does not send an
// EndTxnRequest.
func TestCommitReadOnlyTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var calls []roachpb.Method
	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		return nil, nil
	})

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)
	testutils.RunTrueAndFalse(t, "explicit txn", func(t *testing.T, explicitTxn bool) {
		testutils.RunTrueAndFalse(t, "with get", func(t *testing.T, withGet bool) {
			calls = nil
			db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)
			if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				b := txn.NewBatch()
				if withGet {
					b.Get("foo")
				}
				if explicitTxn {
					return txn.CommitInBatch(ctx, b)
				}
				return txn.Run(ctx, b)
			}); err != nil {
				t.Fatal(err)
			}

			expectedCalls := []roachpb.Method(nil)
			if withGet {
				expectedCalls = append(expectedCalls, roachpb.Get)
			}
			if !reflect.DeepEqual(expectedCalls, calls) {
				t.Fatalf("expected %s, got %s", expectedCalls, calls)
			}
		})
	})
}

// TestCommitMutatingTransaction verifies that a transaction is committed
// upon successful invocation of the retryable func.
func TestCommitMutatingTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var calls []roachpb.Method
	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()

		calls = append(calls, ba.Methods()...)
		if !bytes.Equal(ba.Txn.Key, roachpb.Key("a")) {
			t.Errorf("expected transaction key to be \"a\"; got %s", ba.Txn.Key)
		}
		if et, ok := ba.GetArg(roachpb.EndTxn); ok {
			if !et.(*roachpb.EndTxnRequest).Commit {
				t.Errorf("expected commit to be true")
			}
			br.Txn.Status = roachpb.COMMITTED
		}
		return br, nil
	})

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)

	// Test all transactional write methods.
	testArgs := []struct {
		f         func(ctx context.Context, txn *kv.Txn) error
		expMethod roachpb.Method
		// pointWrite is set if the method is a "point write", which means that it
		// will be pipelined and we should expect a QueryIntent request at commit
		// time.
		pointWrite bool
	}{
		{
			f:          func(ctx context.Context, txn *kv.Txn) error { return txn.Put(ctx, "a", "b") },
			expMethod:  roachpb.Put,
			pointWrite: true,
		},
		{
			f:          func(ctx context.Context, txn *kv.Txn) error { return txn.CPut(ctx, "a", "b", nil) },
			expMethod:  roachpb.ConditionalPut,
			pointWrite: true,
		},
		{
			f: func(ctx context.Context, txn *kv.Txn) error {
				_, err := txn.Inc(ctx, "a", 1)
				return err
			},
			expMethod:  roachpb.Increment,
			pointWrite: true,
		},
		{
			f:          func(ctx context.Context, txn *kv.Txn) error { return txn.Del(ctx, "a") },
			expMethod:  roachpb.Delete,
			pointWrite: true,
		},
		{
			f:          func(ctx context.Context, txn *kv.Txn) error { return txn.DelRange(ctx, "a", "b") },
			expMethod:  roachpb.DeleteRange,
			pointWrite: false,
		},
	}
	for i, test := range testArgs {
		t.Run(test.expMethod.String(), func(t *testing.T) {
			calls = nil
			db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)
			if err := db.Txn(ctx, test.f); err != nil {
				t.Fatalf("%d: unexpected error on commit: %s", i, err)
			}
			expectedCalls := []roachpb.Method{test.expMethod}
			if test.pointWrite {
				expectedCalls = append(expectedCalls, roachpb.QueryIntent)
			}
			expectedCalls = append(expectedCalls, roachpb.EndTxn)
			if !reflect.DeepEqual(expectedCalls, calls) {
				t.Fatalf("%d: expected %s, got %s", i, expectedCalls, calls)
			}
		})
	}
}

// TestAbortReadOnlyTransaction verifies that aborting a read-only
// transaction does not prompt an EndTxn call.
func TestAbortReadOnlyTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var calls []roachpb.Method
	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		return nil, nil
	})

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)
	db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)
	if err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		return errors.New("foo")
	}); err == nil {
		t.Fatal("expected error on abort")
	}

	if calls != nil {
		t.Fatalf("expected no calls, got %s", calls)
	}
}

// TestEndWriteRestartReadOnlyTransaction verifies that if
// a transaction writes, then restarts and turns read-only,
// an explicit EndTxn call is still sent if retry- able
// didn't, regardless of whether there is an error or not.
func TestEndWriteRestartReadOnlyTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var calls []roachpb.Method
	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()

		calls = append(calls, ba.Methods()...)
		switch ba.Requests[0].GetInner().Method() {
		case roachpb.Put, roachpb.Scan:
			return nil, roachpb.NewErrorWithTxn(
				roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "test err"),
				ba.Txn)
		case roachpb.EndTxn:
			br.Txn.Status = roachpb.COMMITTED
		}
		return br, nil
	})

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
			TestingKnobs: ClientTestingKnobs{
				// Disable span refresh, otherwise it kicks and retries batches by
				// itself.
				MaxTxnRefreshAttempts: -1,
			},
		},
		sender,
	)
	db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)

	testutils.RunTrueAndFalse(t, "write", func(t *testing.T, write bool) {
		testutils.RunTrueAndFalse(t, "success", func(t *testing.T, success bool) {
			calls = nil
			firstIter := true
			if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				if firstIter {
					firstIter = false
					var err error
					if write {
						err = txn.Put(ctx, "consider", "phlebas")
					} else /* locking read */ {
						_, err = txn.ScanForUpdate(ctx, "a", "b", 0)
					}
					if err == nil {
						t.Fatal("missing injected retriable error")
					}
				}
				if !success {
					return errors.New("aborting on purpose")
				}
				return nil
			}); err == nil != success {
				t.Fatalf("expected error: %t, got error: %v", !success, err)
			}

			var expCalls []roachpb.Method
			if write {
				expCalls = []roachpb.Method{roachpb.Put, roachpb.EndTxn}
			} else {
				expCalls = []roachpb.Method{roachpb.Scan, roachpb.EndTxn}
			}
			if !reflect.DeepEqual(expCalls, calls) {
				t.Fatalf("expected %v, got %v", expCalls, calls)
			}
		})
	})
}

// TestTransactionKeyNotChangedInRestart verifies that if the transaction
// already has a key (we're in a restart), the key in the transaction request is
// not changed.
func TestTransactionKeyNotChangedInRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	keys := []string{"first", "second"}
	attempt := 0
	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()

		// Ignore the final EndTxnRequest.
		if _, ok := ba.GetArg(roachpb.EndTxn); ok {
			br.Txn.Status = roachpb.COMMITTED
			return br, nil
		}

		// Both attempts should have a PutRequest.
		if _, ok := ba.GetArg(roachpb.Put); !ok {
			t.Fatalf("failed to find a put request: %v", ba)
		}

		// In the first attempt, the transaction key is the key of the first write command.
		// This key is retained between restarts, so we see the same key in the second attempt.
		if expectedKey := []byte(keys[0]); !bytes.Equal(expectedKey, ba.Txn.Key) {
			t.Fatalf("expected transaction key %v, got %v", expectedKey, ba.Txn.Key)
		}

		if attempt == 0 {
			return nil, roachpb.NewErrorWithTxn(
				roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "test err"),
				ba.Txn)
		}
		return br, nil
	})
	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)
	db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)

	if err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		defer func() { attempt++ }()
		b := txn.NewBatch()
		b.Put(keys[attempt], "b")
		return txn.Run(ctx, b)
	}); err != nil {
		t.Errorf("unexpected error on commit: %s", err)
	}
	minimumAttempts := 2
	if attempt < minimumAttempts {
		t.Errorf("expected attempt count >= %d, got %d", minimumAttempts, attempt)
	}
}

// TestSequenceNumbers verifies Requests are given sequence numbers and that
// they are incremented on successive commands.
func TestSequenceNumbers(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	var expSequence enginepb.TxnSeq
	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		for _, ru := range ba.Requests {
			args := ru.GetInner()
			if args.Method() == roachpb.QueryIntent {
				// QueryIntent requests don't have sequence numbers.
				continue
			}
			expSequence++
			if seq := args.Header().Sequence; expSequence != seq {
				t.Errorf("expected Request sequence %d; got %d. request: %T",
					expSequence, seq, args)
			}
		}
		br := ba.CreateReply()
		br.Txn = ba.Txn
		return br, nil
	})

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)
	db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)
	txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)

	for i := 0; i < 5; i++ {
		var ba roachpb.BatchRequest
		for j := 0; j < i; j++ {
			ba.Add(roachpb.NewPut(roachpb.Key("a"), roachpb.MakeValueFromString("foo")).(*roachpb.PutRequest))
		}
		if _, pErr := txn.Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}
	}
}

// TestConcurrentTxnRequests verifies that multiple requests cannot be executed
// on a transaction at the same time from multiple goroutines.
func TestConcurrentTxnRequestsProhibited(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	putSync := make(chan struct{})
	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if _, ok := ba.GetArg(roachpb.Put); ok {
			// Block the Put until the Get runs.
			putSync <- struct{}{}
			<-putSync
		}
		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		if _, ok := ba.GetArg(roachpb.EndTxn); ok {
			br.Txn.Status = roachpb.COMMITTED
		}
		return br, nil
	})

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)
	db := kv.NewDB(ambient, factory, clock)

	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		g, gCtx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return txn.Put(gCtx, "test_put", "val")
		})
		g.Go(func() error {
			// Wait for the Put to be blocked.
			<-putSync
			_, err := txn.Get(gCtx, "test_get")
			// Unblock the Put.
			putSync <- struct{}{}
			return err
		})
		return g.Wait()
	})
	require.Regexp(t, "concurrent txn use detected", err)
}

// TestTxnRequestTxnTimestamp verifies response txn timestamp is
// always upgraded on successive requests.
func TestTxnRequestTxnTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)
	db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)

	curReq := 0
	requests := []struct {
		expRequestTS, responseTS hlc.Timestamp
	}{
		{hlc.Timestamp{WallTime: 5, Logical: 0}, hlc.Timestamp{WallTime: 10, Logical: 0}},
		{hlc.Timestamp{WallTime: 10, Logical: 0}, hlc.Timestamp{WallTime: 10, Logical: 1}},
		{hlc.Timestamp{WallTime: 10, Logical: 1}, hlc.Timestamp{WallTime: 10, Logical: 0}},
		{hlc.Timestamp{WallTime: 10, Logical: 1}, hlc.Timestamp{WallTime: 20, Logical: 1}},
		{hlc.Timestamp{WallTime: 20, Logical: 1}, hlc.Timestamp{WallTime: 20, Logical: 1}},
		{hlc.Timestamp{WallTime: 20, Logical: 1}, hlc.Timestamp{WallTime: 19, Logical: 0}},
		{hlc.Timestamp{WallTime: 20, Logical: 1}, hlc.Timestamp{WallTime: 20, Logical: 1}},
	}

	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		req := requests[curReq]
		if req.expRequestTS != ba.Txn.WriteTimestamp {
			return nil, roachpb.NewErrorf("%d: expected ts %s got %s",
				curReq, req.expRequestTS, ba.Txn.WriteTimestamp)
		}

		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		br.Txn.WriteTimestamp.Forward(requests[curReq].responseTS)
		return br, nil
	})

	manual.Set(requests[0].expRequestTS.WallTime)

	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		for curReq = range requests {
			if _, err := txn.Get(ctx, "k"); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestReadOnlyTxnObeysDeadline tests that read-only transactions obey the
// deadline. Read-only transactions have their EndTxn elided, so the enforcement
// of the deadline is done in the client.
func TestReadOnlyTxnObeysDeadline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	sender := &mockSender{}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	sender.match(func(ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		if _, ok := ba.GetArg(roachpb.Get); ok {
			manual.Increment(100)
			br := ba.CreateReply()
			br.Txn = ba.Txn.Clone()
			br.Txn.WriteTimestamp.Forward(clock.Now())
			return br, nil
		}
		return nil, nil
	})

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		sender,
	)
	db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)

	// We're going to run two tests: one where the EndTxn is by itself in a
	// batch, one where it is not. As of June 2018, the EndTxn is elided in
	// different ways in the two cases.

	t.Run("standalone commit", func(t *testing.T) {
		txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
		// Set a deadline. We'll generate a retriable error with a higher timestamp.
		txn.UpdateDeadlineMaybe(ctx, clock.Now())
		if _, err := txn.Get(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		err := txn.Commit(ctx)
		assertTransactionRetryError(t, err)
		if !testutils.IsError(err, "RETRY_COMMIT_DEADLINE_EXCEEDED") {
			t.Fatalf("expected deadline exceeded, got: %s", err)
		}
	})

	t.Run("commit in batch", func(t *testing.T) {
		txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
		// Set a deadline. We'll generate a retriable error with a higher timestamp.
		txn.UpdateDeadlineMaybe(ctx, clock.Now())
		b := txn.NewBatch()
		b.Get("k")
		err := txn.CommitInBatch(ctx, b)
		assertTransactionRetryError(t, err)
		if !testutils.IsError(err, "RETRY_COMMIT_DEADLINE_EXCEEDED") {
			t.Fatalf("expected deadline exceeded, got: %s", err)
		}
	})
}

// TestTxnCoordSenderPipelining verifies that transactional pipelining of writes
// is enabled by default in a transaction and is disabled after
// DisablePipelining is called. It also verifies that DisablePipelining returns
// an error if the transaction has already performed an operation.
func TestTxnCoordSenderPipelining(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := createTestDB(t)
	defer s.Stop()
	distSender := s.DB.GetFactory().(*TxnCoordSenderFactory).NonTransactionalSender()

	var calls []roachpb.Method
	var senderFn kv.SenderFunc = func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		calls = append(calls, ba.Methods()...)
		if et, ok := ba.GetArg(roachpb.EndTxn); ok {
			// Ensure that no transactions enter a STAGING state.
			et.(*roachpb.EndTxnRequest).InFlightWrites = nil
		}
		return distSender.Send(ctx, ba)
	}

	ambientCtx := log.AmbientContext{Tracer: tracing.NewTracer()}
	tsf := NewTxnCoordSenderFactory(TxnCoordSenderFactoryConfig{
		AmbientCtx: ambientCtx,
		Settings:   s.Cfg.Settings,
		Clock:      s.Clock,
		Stopper:    s.Stopper,
	}, senderFn)
	db := kv.NewDB(ambientCtx, tsf, s.Clock)

	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return txn.Put(ctx, "key", "val")
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.DisablePipelining(); err != nil {
			return err
		}
		return txn.Put(ctx, "key", "val")
	})
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, []roachpb.Method{
		roachpb.Put, roachpb.QueryIntent, roachpb.EndTxn,
		roachpb.Put, roachpb.EndTxn,
	}, calls)

	for _, action := range []func(ctx context.Context, txn *kv.Txn) error{
		func(ctx context.Context, txn *kv.Txn) error { return txn.Put(ctx, "key", "val") },
		func(ctx context.Context, txn *kv.Txn) error { _, err := txn.Get(ctx, "key"); return err },
	} {
		err = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if err := action(ctx, txn); err != nil {
				t.Fatal(err)
			}
			return txn.DisablePipelining()
		})
		if exp := "cannot disable pipelining on a running transaction"; !testutils.IsError(err, exp) {
			t.Fatalf("expected %q error, but got %v", exp, err)
		}
	}
}

// Test that a txn's anchor is set to the first write key in batches mixing
// reads with writes.
func TestAnchorKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	manual := hlc.NewManualClock(123)
	clock := hlc.NewClock(manual.UnixNano, time.Nanosecond)
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	key1 := roachpb.Key("a")
	key2 := roachpb.Key("b")

	var senderFn kv.SenderFunc = func(
		ctx context.Context, ba roachpb.BatchRequest,
	) (*roachpb.BatchResponse, *roachpb.Error) {
		if !roachpb.Key(ba.Txn.Key).Equal(key2) {
			t.Fatalf("expected anchor %q, got %q", key2, ba.Txn.Key)
		}
		br := ba.CreateReply()
		br.Txn = ba.Txn.Clone()
		if _, ok := ba.GetArg(roachpb.EndTxn); ok {
			br.Txn.Status = roachpb.COMMITTED
		}
		return br, nil
	}

	factory := NewTxnCoordSenderFactory(
		TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Clock:      clock,
			Stopper:    stopper,
			Settings:   cluster.MakeTestingClusterSettings(),
		},
		senderFn,
	)
	db := kv.NewDB(testutils.MakeAmbientCtx(), factory, clock)

	if err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		ba := txn.NewBatch()
		ba.Get(key1)
		ba.Put(key2, "val")
		return txn.Run(ctx, ba)
	}); err != nil {
		t.Fatal(err)
	}
}

// Test that a leaf txn returns a raw error when "rejecting a client" (a client
// sending something after the txn is known to be aborted), not a
// TransactionRetryWithProtoRefreshError. This is important as leaves are not supposed to create
// "handled" errors; instead the DistSQL infra knows to recognize raw retryable
// errors and feed them to the root txn.
func TestLeafTxnClientRejectError(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We're going to inject an error so that a leaf txn is "poisoned". This can
	// happen, for example, if the leaf is used concurrently by multiple requests,
	// where the first one gets a TransactionAbortedError.
	errKey := roachpb.Key("a")
	knobs := &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(_ context.Context, ba roachpb.BatchRequest) *roachpb.Error {
			if g, ok := ba.GetArg(roachpb.Get); ok && g.(*roachpb.GetRequest).Key.Equal(errKey) {
				txn := ba.Txn.Clone()
				txn.Status = roachpb.ABORTED
				return roachpb.NewErrorWithTxn(
					roachpb.NewTransactionAbortedError(roachpb.ABORT_REASON_UNKNOWN), txn,
				)
			}
			return nil
		},
	}

	s := createTestDBWithContextAndKnobs(t, kv.DefaultDBContext(), knobs)
	defer s.Stop()

	ctx := context.Background()
	rootTxn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
	leafInputState := rootTxn.GetLeafTxnInputState(ctx)

	// New create a second, leaf coordinator.
	leafTxn := kv.NewLeafTxn(ctx, s.DB, 0 /* gatewayNodeID */, &leafInputState)

	if _, err := leafTxn.Get(ctx, errKey); !testutils.IsError(err, "TransactionAbortedError") {
		t.Fatalf("expected injected err, got: %v", err)
	}

	// Now use the leaf and check the error. At the TxnCoordSender level, the
	// pErr will be TransactionAbortedError. When pErr.GoError() is called, that's
	// transformed into an UnhandledRetryableError. For our purposes, what this
	// test is interested in demonstrating is that it's not a
	// TransactionRetryWithProtoRefreshError.
	_, err := leafTxn.Get(ctx, roachpb.Key("a"))
	if !errors.HasType(err, (*roachpb.UnhandledRetryableError)(nil)) {
		t.Fatalf("expected UnhandledRetryableError(TransactionAbortedError), got: (%T) %v", err, err)
	}
}

// Check that ingesting an Aborted txn record is a no-op. The TxnCoordSender is
// supposed to reject such updates because they risk putting it into an
// inconsistent state. See comments in TxnCoordSender.UpdateRootWithLeafFinalState().
func TestUpdateRoootWithLeafFinalStateInAbortedTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDBWithContextAndKnobs(t, kv.DefaultDBContext(), nil /* knobs */)
	defer s.Stop()
	ctx := context.Background()

	txn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
	leafInputState := txn.GetLeafTxnInputState(ctx)
	leafTxn := kv.NewLeafTxn(ctx, s.DB, 0, &leafInputState)

	finalState, err := leafTxn.GetLeafTxnFinalState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	finalState.Txn.Status = roachpb.ABORTED
	if err := txn.UpdateRootWithLeafFinalState(ctx, &finalState); err != nil {
		t.Fatal(err)
	}

	// Check that the transaction was not updated.
	leafInputState2 := txn.GetLeafTxnInputState(ctx)
	if leafInputState2.Txn.Status != roachpb.PENDING {
		t.Fatalf("expected PENDING txn, got: %s", leafInputState2.Txn.Status)
	}
}
