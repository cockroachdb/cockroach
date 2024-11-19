// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestTxnCoordSenderCondenseLockSpans verifies that lock spans are condensed
// along range boundaries when they exceed the maximum intent bytes threshold.
//
// TODO(andrei): Merge this test into TestTxnPipelinerCondenseLockSpans2, which
// uses a txnPipeliner instead of a full TxnCoordSender.
func TestTxnPipelinerCondenseLockSpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	a := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key(nil)}
	b := roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key(nil)}
	c := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key(nil)}
	d := roachpb.Span{Key: roachpb.Key("ddddddd"), EndKey: roachpb.Key(nil)}
	e := roachpb.Span{Key: roachpb.Key("e"), EndKey: roachpb.Key(nil)}
	aToBClosed := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b").Next()}
	cToEClosed := roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e").Next()}
	fTof0 := roachpb.Span{Key: roachpb.Key("f"), EndKey: roachpb.Key("f0")}
	g := roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key(nil)}
	g0Tog1 := roachpb.Span{Key: roachpb.Key("g0"), EndKey: roachpb.Key("g1")}
	fTog1 := roachpb.Span{Key: roachpb.Key("f"), EndKey: roachpb.Key("g1")}
	testCases := []struct {
		span         roachpb.Span
		expLocks     []roachpb.Span
		expLocksSize int64 // doesn't include the span overhead
	}{
		{span: a, expLocks: []roachpb.Span{a}, expLocksSize: 1},
		{span: b, expLocks: []roachpb.Span{a, b}, expLocksSize: 2},
		{span: c, expLocks: []roachpb.Span{a, b, c}, expLocksSize: 3},
		{span: d, expLocks: []roachpb.Span{a, b, c, d}, expLocksSize: 10},
		// Note that c-e condenses and then lists first, we proceed to condense
		// a-b too to get under half of the threshold.
		{span: e, expLocks: []roachpb.Span{cToEClosed, aToBClosed}, expLocksSize: 6},
		{span: fTof0, expLocks: []roachpb.Span{cToEClosed, aToBClosed, fTof0}, expLocksSize: 9},
		{span: g, expLocks: []roachpb.Span{cToEClosed, aToBClosed, fTof0, g}, expLocksSize: 10},
		// f-g1 condenses and then aToBClosed gets reordered with cToEClosed.
		{span: g0Tog1, expLocks: []roachpb.Span{fTog1, aToBClosed, cToEClosed}, expLocksSize: 9},
		// Add a key in the middle of a span, which will get merged on commit.
		{span: c, expLocks: []roachpb.Span{fTog1, aToBClosed, cToEClosed, c}, expLocksSize: 10},
	}
	splits := []roachpb.Span{
		{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")},
		{Key: roachpb.Key("c"), EndKey: roachpb.Key("f")},
		{Key: roachpb.Key("f"), EndKey: roachpb.Key("j")},
	}
	descs := []roachpb.RangeDescriptor{kvcoord.TestMetaRangeDescriptor}
	for i, s := range splits {
		descs = append(descs, roachpb.RangeDescriptor{
			RangeID:          roachpb.RangeID(2 + i),
			StartKey:         roachpb.RKey(s.Key),
			EndKey:           roachpb.RKey(s.EndKey),
			InternalReplicas: []roachpb.ReplicaDescriptor{{NodeID: 1, StoreID: 1}},
		})
	}
	descDB := kvcoord.TestingMockRangeDescriptorDBForDescs(descs...)
	s := createTestDB(t)
	st := s.Store.ClusterSettings()
	// 10 bytes for the keys and 192 bytes for the span overhead, and then it
	// will condense.
	kvcoord.TrackedWritesMaxSize.Override(ctx, &st.SV, 10+4*roachpb.SpanOverhead)
	defer s.Stop()

	// Check end transaction locks, which should be condensed and split
	// at range boundaries.
	expLocks := []roachpb.Span{aToBClosed, cToEClosed, fTog1}
	sendFn := func(_ context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
		resp := ba.CreateReply()
		resp.Txn = ba.Txn
		if req, ok := ba.GetArg(kvpb.EndTxn); ok {
			if !req.(*kvpb.EndTxnRequest).Commit {
				t.Errorf("expected commit to be true")
			}
			et := req.(*kvpb.EndTxnRequest)
			if a, e := et.LockSpans, expLocks; !reflect.DeepEqual(a, e) {
				t.Errorf("expected end transaction to have locks %+v; got %+v", e, a)
			}
			resp.Txn.Status = roachpb.COMMITTED
		}
		return resp, nil
	}
	ambient := log.MakeTestingAmbientCtxWithNewTracer()
	ds := kvcoord.NewDistSender(kvcoord.DistSenderConfig{
		AmbientCtx:        ambient,
		Clock:             s.Clock,
		NodeDescs:         s.Gossip,
		Stopper:           s.Stopper(),
		TransportFactory:  kvcoord.TestingAdaptSimpleTransport(sendFn),
		RangeDescriptorDB: descDB,
		Settings:          cluster.MakeTestingClusterSettings(),
	})
	tsf := kvcoord.NewTxnCoordSenderFactory(
		kvcoord.TxnCoordSenderFactoryConfig{
			AmbientCtx: ambient,
			Settings:   st,
			Clock:      s.Clock,
			Stopper:    s.Stopper(),
		},
		ds,
	)
	db := kv.NewDB(ambient, tsf, s.Clock, s.Stopper())

	txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
	// Disable txn pipelining so that all write spans are immediately
	// added to the transaction's lock footprint.
	if err := txn.DisablePipelining(); err != nil {
		t.Fatal(err)
	}
	for i, tc := range testCases {
		if tc.span.EndKey != nil {
			if _, err := txn.DelRange(ctx, tc.span.Key, tc.span.EndKey, false /* returnKeys */); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := txn.Put(ctx, tc.span.Key, []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		tcs := txn.Sender().(*kvcoord.TxnCoordSender)
		locks := tcs.TestingGetLockFootprint(false /* mergeAndSort */)
		if a, e := locks, tc.expLocks; !reflect.DeepEqual(a, e) {
			t.Errorf("%d: expected keys %+v; got %+v", i, e, a)
		}
		locksSize := int64(0)
		for _, i := range locks {
			locksSize += int64(len(i.Key) + len(i.EndKey)) // ignoring the span overhead
		}
		if a, e := locksSize, tc.expLocksSize; a != e {
			t.Errorf("%d: keys size expected %d; got %d", i, e, a)
		}
	}

	metrics := txn.Sender().(*kvcoord.TxnCoordSender).Metrics()
	require.Equal(t, int64(1), metrics.TxnsWithCondensedIntents.Count())
	require.Equal(t, int64(1), metrics.TxnsWithCondensedIntentsGauge.Value())

	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	require.Zero(t, metrics.TxnsWithCondensedIntentsGauge.Value())
}

// TestTxnPipelinerLockValidationConsultsIntentHistory is a regression test for
// #121920. The unit test verifies that an intent's intent history is consulted
// during pipelined lock validation.
func TestTxnPipelinerLockValidationConsultsIntentHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	db := tc.Server(0).DB()

	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		require.NoError(t, txn.SetIsoLevel(isolation.ReadCommitted))
		key := roachpb.Key("a")
		require.NoError(t, txn.Put(ctx, key, "val"))
		sp, err := txn.CreateSavepoint(ctx)
		require.NoError(t, err)
		_, err = txn.Del(ctx, key)
		require.NoError(t, err)
		require.NoError(t, txn.RollbackToSavepoint(ctx, sp))
		_, err = txn.GetForUpdate(ctx, key, kvpb.GuaranteedDurability)
		require.NoError(t, err)
		_, err = txn.Get(ctx, key)
		return err
	}))
}
