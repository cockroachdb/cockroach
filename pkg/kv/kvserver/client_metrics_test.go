// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type gaugeValuer interface {
	GetName() string
	Value() int64
}

func checkGauge(t *testing.T, id string, g gaugeValuer, e int64) {
	t.Helper()
	if a := g.Value(); a != e {
		t.Error(errors.Errorf("%s for store %s: gauge %d != computed %d", g.GetName(), id, a, e))
	}
}

// verifyStats checks a sets of stats on the specified list of servers. This method
// may produce false negatives when executed against a running server that has
// live traffic on it.
func verifyStats(t *testing.T, tc *testcluster.TestCluster, storeIdxSlice ...int) {
	t.Helper()
	var stores []*kvserver.Store
	var wg sync.WaitGroup

	for _, storeIdx := range storeIdxSlice {
		stores = append(stores, tc.GetFirstStoreFromServer(t, storeIdx))
	}

	// Sanity regression check for bug #4624: ensure intent count is zero.
	// This may not be true immediately due to the asynchronous nature of
	// non-local intent resolution.
	for _, s := range stores {
		m := s.Metrics()
		testutils.SucceedsSoon(t, func() error {
			if a := m.IntentCount.Value(); a != 0 {
				return fmt.Errorf("expected intent count to be zero, was %d", a)
			}
			return nil
		})
	}

	wg.Add(len(storeIdxSlice))
	// We actually stop *all* of the Servers. Stopping only a few is riddled
	// with deadlocks since operations can span nodes, but stoppers don't
	// know about this - taking all of them down at the same time is the
	// only sane way of guaranteeing that nothing interesting happens, at
	// least when bringing down the nodes jeopardizes majorities.
	for _, storeIdx := range storeIdxSlice {
		go func(i int) {
			defer wg.Done()
			tc.StopServer(i)
		}(storeIdx)
	}
	wg.Wait()

	for _, s := range stores {
		idString := s.Ident.String()
		m := s.Metrics()

		// Sanity check: LiveBytes is not zero (ensures we don't have
		// zeroed out structures.)
		if liveBytes := m.LiveBytes.Value(); liveBytes == 0 {
			t.Errorf("store %s; got zero live bytes, expected non-zero", idString)
		}

		// Compute real total MVCC statistics from store.
		realStats, err := s.ComputeMVCCStats()
		if err != nil {
			t.Fatal(err)
		}

		// Ensure that real MVCC stats match computed stats.
		checkGauge(t, idString, m.LiveBytes, realStats.LiveBytes)
		checkGauge(t, idString, m.KeyBytes, realStats.KeyBytes)
		checkGauge(t, idString, m.ValBytes, realStats.ValBytes)
		checkGauge(t, idString, m.IntentBytes, realStats.IntentBytes)
		checkGauge(t, idString, m.LiveCount, realStats.LiveCount)
		checkGauge(t, idString, m.KeyCount, realStats.KeyCount)
		checkGauge(t, idString, m.ValCount, realStats.ValCount)
		checkGauge(t, idString, m.IntentCount, realStats.IntentCount)
		checkGauge(t, idString, m.SysBytes, realStats.SysBytes)
		checkGauge(t, idString, m.SysCount, realStats.SysCount)
		checkGauge(t, idString, m.AbortSpanBytes, realStats.AbortSpanBytes)
		// "Ages" will be different depending on how much time has passed. Even with
		// a manual clock, this can be an issue in tests. Therefore, we do not
		// verify them in this test.
	}

	if t.Failed() {
		t.Fatalf("verifyStats failed, aborting test.")
	}

	// Restart all Stores.
	for _, storeIdx := range storeIdxSlice {
		require.NoError(t, tc.RestartServer(storeIdx))
	}
}

func verifyRocksDBStats(t *testing.T, s *kvserver.Store) {
	if err := s.ComputeMetrics(context.Background(), 0); err != nil {
		t.Fatal(err)
	}

	m := s.Metrics()
	testcases := []struct {
		gauge *metric.Gauge
		min   int64
	}{
		{m.RdbBlockCacheHits, 10},
		{m.RdbBlockCacheMisses, 0},
		{m.RdbBlockCacheUsage, 0},
		{m.RdbBlockCachePinnedUsage, 0},
		{m.RdbBloomFilterPrefixChecked, 20},
		{m.RdbBloomFilterPrefixUseful, 20},
		{m.RdbMemtableTotalSize, 5000},
		{m.RdbFlushes, 1},
		{m.RdbCompactions, 0},
		{m.RdbTableReadersMemEstimate, 50},
	}
	for _, tc := range testcases {
		if a := tc.gauge.Value(); a < tc.min {
			t.Errorf("gauge %s = %d < min %d", tc.gauge.GetName(), a, tc.min)
		}
	}
}

// TestStoreResolveMetrics verifies that metrics related to intent resolution
// are tracked properly.
func TestStoreResolveMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// First prevent rot that would result from adding fields without handling
	// them everywhere.
	{
		act := fmt.Sprintf("%+v", result.Metrics{})
		exp := "{LeaseRequestSuccess:0 LeaseRequestError:0 LeaseTransferSuccess:0 LeaseTransferError:0 ResolveCommit:0 ResolveAbort:0 ResolvePoison:0 AddSSTableAsWrites:0}"
		if act != exp {
			t.Errorf("need to update this test due to added fields: %v", act)
		}
	}

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	key, err := s.ScratchRange()
	require.NoError(t, err)
	span := roachpb.Span{Key: key, EndKey: key.Next()}

	txn := roachpb.MakeTransaction("foo", span.Key, roachpb.MinUserPriority, hlc.Timestamp{WallTime: 123}, 999, int32(s.NodeID()))

	const resolveCommitCount = int64(200)
	const resolveAbortCount = int64(800)
	const resolvePoisonCount = int64(2400)

	var ba roachpb.BatchRequest
	{
		repl := store.LookupReplica(keys.MustAddr(span.Key))
		var err error
		if ba.Replica, err = repl.GetReplicaDescriptor(); err != nil {
			t.Fatal(err)
		}
		ba.RangeID = repl.RangeID
	}

	add := func(status roachpb.TransactionStatus, poison bool, n int64) {
		for i := int64(0); i < n; i++ {
			key := span.Key
			endKey := span.EndKey
			if i > n/2 {
				req := &roachpb.ResolveIntentRangeRequest{
					IntentTxn: txn.TxnMeta,
					Status:    status,
					Poison:    poison,
				}
				req.Key, req.EndKey = key, endKey
				ba.Add(req)
				continue
			}
			req := &roachpb.ResolveIntentRequest{
				IntentTxn: txn.TxnMeta,
				Status:    status,
				Poison:    poison,
			}
			req.Key = key
			ba.Add(req)
		}
	}

	add(roachpb.COMMITTED, false, resolveCommitCount)
	add(roachpb.ABORTED, false, resolveAbortCount)
	add(roachpb.ABORTED, true, resolvePoisonCount)

	if _, pErr := store.TestSender().Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	}

	if exp, act := resolveCommitCount, store.Metrics().ResolveCommitCount.Count(); act < exp || act > exp+50 {
		t.Errorf("expected around %d intent commits, saw %d", exp, act)
	}
	if exp, act := resolveAbortCount, store.Metrics().ResolveAbortCount.Count(); act < exp || act > exp+50 {
		t.Errorf("expected around %d intent aborts, saw %d", exp, act)
	}
	if exp, act := resolvePoisonCount, store.Metrics().ResolvePoisonCount.Count(); act < exp || act > exp+50 {
		t.Errorf("expected arounc %d abort span poisonings, saw %d", exp, act)
	}
}

func TestStoreMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()
	const numServers int = 3
	stickyServerArgs := make(map[int]base.TestServerArgs)
	for i := 0; i < numServers; i++ {
		stickyServerArgs[i] = base.TestServerArgs{
			CacheSize: 1 << 20, /* 1 MiB */
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10),
					// Specify a size to trigger the BlockCache in Pebble.
					Size: base.SizeSpec{
						InBytes: 512 << 20, /* 512 MiB */
					},
				},
			},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
				},
				Store: &kvserver.StoreTestingKnobs{
					DisableRaftLogQueue: true,
				},
			},
		}
	}
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)

	// Flush Pebble memtables, so that Pebble begins using block-based tables.
	// This is useful, because most of the stats we track don't apply to
	// memtables.
	for i := range tc.Servers {
		if err := tc.GetFirstStoreFromServer(t, i).Engine().Flush(); err != nil {
			t.Fatal(err)
		}
	}

	initialCount := tc.GetFirstStoreFromServer(t, 0).Metrics().ReplicaCount.Value()
	key := tc.ScratchRange(t)
	if _, err := tc.GetFirstStoreFromServer(t, 0).DB().Inc(ctx, key, 10); err != nil {
		t.Fatal(err)
	}
	// Verify range count is as expected
	checkGauge(t, "store 0", tc.GetFirstStoreFromServer(t, 0).Metrics().ReplicaCount, initialCount+1)

	// Replicate the "right" range to the other stores.
	desc := tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	require.NoError(t, tc.WaitForVoters(key, tc.Targets(1, 2)...))

	// Verify stats on store1 after replication.
	verifyStats(t, tc, 1)

	// Add some data to the "right" range.
	dataKey := key.Next()
	if _, err := tc.GetFirstStoreFromServer(t, 0).DB().Inc(ctx, dataKey, 5); err != nil {
		t.Fatal(err)
	}
	tc.WaitForValues(t, dataKey, []int64{5, 5, 5})

	// Verify all stats on stores after addition.
	// We skip verifying stats on Server[0] because there is no reliable way to
	// do that given all if the system table activity generated by the TestCluster.
	// We use Servers[1] and Servers[2] instead, since we can control the traffic
	// on those servers.
	verifyStats(t, tc, 1, 2)

	// Create a transaction statement that fails. Regression test for #4969.
	if err := tc.GetFirstStoreFromServer(t, 0).DB().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		b := txn.NewBatch()
		var expVal roachpb.Value
		expVal.SetInt(6)
		b.CPut(dataKey, 7, expVal.TagAndDataBytes())
		return txn.Run(ctx, b)
	}); err == nil {
		t.Fatal("Expected transaction error, but none received")
	}

	// Verify stats after addition.
	verifyStats(t, tc, 1, 2)
	checkGauge(t, "store 0", tc.GetFirstStoreFromServer(t, 0).Metrics().ReplicaCount, initialCount+1)
	tc.RemoveLeaseHolderOrFatal(t, desc, tc.Target(0))
	testutils.SucceedsSoon(t, func() error {
		_, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(desc.RangeID)
		if err == nil {
			return fmt.Errorf("replica still exists on dest 0")
		} else if errors.HasType(err, (*roachpb.RangeNotFoundError)(nil)) {
			return nil
		}
		return err
	})
	tc.WaitForValues(t, dataKey, []int64{0, 5, 5})

	// Verify range count is as expected.
	checkGauge(t, "store 0", tc.GetFirstStoreFromServer(t, 0).Metrics().ReplicaCount, initialCount)
	checkGauge(t, "store 1", tc.GetFirstStoreFromServer(t, 1).Metrics().ReplicaCount, 1)

	// Verify all stats on all stores after range is removed.
	verifyStats(t, tc, 1, 2)

	verifyRocksDBStats(t, tc.GetFirstStoreFromServer(t, 1))
	verifyRocksDBStats(t, tc.GetFirstStoreFromServer(t, 2))
}

// TestStoreMaxBehindNanosOnlyTracksEpochBasedLeases ensures that the metric
// ClosedTimestampMaxBehindNanos does not follow the start time of expiration
// based leases. Expiration based leases don't publish closed timestamps.
func TestStoreMaxBehindNanosOnlyTracksEpochBasedLeases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Set a long timeout so that no lease or liveness ever times out.
			RaftConfig: base.RaftConfig{RaftElectionTimeoutTicks: 100},
		},
	})
	defer tc.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	// We want to choose setting values such that this test doesn't take too long
	// with the caveat that under extreme stress, we need to make sure that the
	// subsystem remains live.
	const closedTimestampDuration = 15 * time.Millisecond
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = $1",
		closedTimestampDuration.String())
	tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = $1",
		closedTimestampDuration.String())

	// Let's get to a point where we know that we have an expiration based lease
	// with a start time more than some time ago and then we have a max closed
	// value more recent.
	_, meta2Repl1 := getFirstStoreReplica(t, tc.Server(0), keys.Meta2Prefix)

	// Transfer the lease for the meta range to ensure that it has a non-zero
	// start time.
	require.NoError(t, tc.TransferRangeLease(*meta2Repl1.Desc(), tc.Target(1)))

	testutils.SucceedsSoon(t, func() error {
		_, metaRepl := getFirstStoreReplica(t, tc.Server(1), keys.Meta2Prefix)
		l, _ := metaRepl.GetLease()
		if l.Start.IsEmpty() {
			return errors.Errorf("don't have a lease for meta1 yet: %v %v", l, meta2Repl1)
		}
		sinceExpBasedLeaseStart := timeutil.Since(timeutil.Unix(0, l.Start.WallTime))
		for i := 0; i < tc.NumServers(); i++ {
			s, _ := getFirstStoreReplica(t, tc.Server(i), keys.Meta1Prefix)
			require.NoError(t, s.ComputeMetrics(ctx, 0))
			maxBehind := time.Duration(s.Metrics().ClosedTimestampMaxBehindNanos.Value())
			// We want to make sure that maxBehind ends up being much smaller than the
			// start of an expiration based lease.
			const behindMultiple = 5
			if maxBehind*behindMultiple > sinceExpBasedLeaseStart {
				return errors.Errorf("store %v has a ClosedTimestampMaxBehindNanos"+
					" of %v which is not way less than the an expiration-based lease start, %v",
					s.StoreID(), maxBehind, sinceExpBasedLeaseStart)
			}
		}
		return nil
	})
}
