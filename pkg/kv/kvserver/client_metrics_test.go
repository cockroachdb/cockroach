// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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

// verifyStatsOnServers checks a sets of stats on the specified
// list of servers.
func verifyStatsOnServers(
	t *testing.T,
	tc *testcluster.TestCluster,
	specs map[int]base.StoreSpec,
	stickyRegistry fs.StickyRegistry,
	storeIdxSlice ...int,
) {
	t.Helper()
	ctx := context.Background()

	// Sanity regression check for bug #4624: ensure intent count is zero.
	// This may not be true immediately due to the asynchronous nature of
	// non-local intent resolution.
	for i := 0; i < tc.NumServers(); i++ {
		s := tc.GetFirstStoreFromServer(t, i)
		m := s.Metrics()
		testutils.SucceedsSoon(t, func() error {
			if a := m.IntentCount.Value(); a != 0 {
				return fmt.Errorf("expected intent count to be zero, was %d", a)
			}
			return nil
		})
	}

	// Stop all servers.
	//
	// Stopping the servers stabilizes the metrics, ensuring
	// background operations don't mutate metrics between reading and
	// recomputing them.
	for _, i := range storeIdxSlice {
		tc.StopServer(i)
	}

	for _, storeIdx := range storeIdxSlice {
		// The server is currently stopped and the engine is closed,
		// but we can still retrieve the existing metrics off the
		// Store.
		s := tc.GetFirstStoreFromServer(t, storeIdx)
		idString := s.Ident.String()
		m := s.Metrics()

		// Sanity check: LiveBytes is not zero (ensures we don't have
		// zeroed out structures.)
		if liveBytes := m.LiveBytes.Value(); liveBytes == 0 {
			t.Errorf("store %s; got zero live bytes, expected non-zero", idString)
		}

		// Compute real total MVCC statistics from store.
		// To recompute the metrics, we need an open engine. Open the
		// Engine again in read-only mode (leaving the rest of the
		// Server stopped) to compute MVCC stats.
		env, err := fs.InitEnvFromStoreSpec(ctx, specs[storeIdx], fs.ReadOnly, stickyRegistry, nil /* statsCollector */)
		if err != nil {
			t.Fatal(err)
		}
		e, err := storage.Open(ctx, env, s.GetStoreConfig().Settings, storage.MustExist)
		if err != nil {
			t.Fatal(err)
		}
		realStats, err := s.ComputeMVCCStats(e)
		e.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Ensure that real MVCC stats match computed stats.
		checkGauge(t, idString, m.LiveBytes, realStats.LiveBytes)
		checkGauge(t, idString, m.KeyBytes, realStats.KeyBytes)
		checkGauge(t, idString, m.ValBytes, realStats.ValBytes)
		checkGauge(t, idString, m.RangeKeyBytes, realStats.RangeKeyBytes)
		checkGauge(t, idString, m.RangeValBytes, realStats.RangeValBytes)
		checkGauge(t, idString, m.IntentBytes, realStats.IntentBytes)
		checkGauge(t, idString, m.LockBytes, realStats.LockBytes)
		checkGauge(t, idString, m.LiveCount, realStats.LiveCount)
		checkGauge(t, idString, m.KeyCount, realStats.KeyCount)
		checkGauge(t, idString, m.ValCount, realStats.ValCount)
		checkGauge(t, idString, m.RangeKeyCount, realStats.RangeKeyCount)
		checkGauge(t, idString, m.RangeValCount, realStats.RangeValCount)
		checkGauge(t, idString, m.IntentCount, realStats.IntentCount)
		checkGauge(t, idString, m.LockCount, realStats.LockCount)
		checkGauge(t, idString, m.SysBytes, realStats.SysBytes)
		checkGauge(t, idString, m.SysCount, realStats.SysCount)
		checkGauge(t, idString, m.AbortSpanBytes, realStats.AbortSpanBytes)
		// "Ages" will be different depending on how much time has passed. Even with
		// a manual clock, this can be an issue in tests. Therefore, we do not
		// verify them in this test.
	}

	if t.Failed() {
		t.Fatalf("verifyStatsOnServers failed, aborting test.")
	}
	// Restart all servers.
	for _, i := range storeIdxSlice {
		require.NoError(t, tc.RestartServer(i))
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
		exp := "{LeaseRequestSuccess:0 LeaseRequestError:0 LeaseTransferSuccess:0 LeaseTransferError:0 LeaseTransferLocksWritten:0 ResolveCommit:0 ResolveAbort:0 ResolvePoison:0 AddSSTableAsWrites:0 SplitsWithEstimatedStats:0 SplitEstimatedTotalBytesDiff:0}"
		if act != exp {
			t.Errorf("need to update this test due to added fields: %v", act)
		}
	}

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	key, err := s.ScratchRange()
	require.NoError(t, err)
	span := roachpb.Span{Key: key, EndKey: key.Next()}

	// Clear the metrics before starting the test so that we don't count intent
	// resolutions from server startup.
	store.Metrics().ResolveCommitCount.Clear()
	store.Metrics().ResolveAbortCount.Clear()
	store.Metrics().ResolvePoisonCount.Clear()

	txn := roachpb.MakeTransaction("foo", span.Key, isolation.Serializable, roachpb.MinUserPriority, hlc.Timestamp{WallTime: 123}, 999, int32(s.NodeID()), 0, false /* omitInRangefeeds */)

	const resolveCommitCount = int64(200)
	const resolveAbortCount = int64(800)
	const resolvePoisonCount = int64(2400)

	ba := &kvpb.BatchRequest{}
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
				req := &kvpb.ResolveIntentRangeRequest{
					IntentTxn: txn.TxnMeta,
					Status:    status,
					Poison:    poison,
				}
				req.Key, req.EndKey = key, endKey
				ba.Add(req)
				continue
			}
			req := &kvpb.ResolveIntentRequest{
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

	skip.UnderRace(t, "does not complete under race")

	ctx := context.Background()
	const numServers int = 3
	stickyVFSRegistry := fs.NewStickyRegistry()
	stickyServerArgs := make(map[int]base.TestServerArgs)
	specs := make(map[int]base.StoreSpec)
	for i := 0; i < numServers; i++ {
		spec := base.StoreSpec{
			InMemory:    true,
			StickyVFSID: strconv.FormatInt(int64(i), 10),
			// Specify a size to trigger the BlockCache in Pebble.
			Size: storagepb.SizeSpec{
				Capacity: 512 << 20, /* 512 MiB */
			},
		}
		stickyServerArgs[i] = base.TestServerArgs{
			CacheSize:  2 << 20, /* 2 MiB */
			StoreSpecs: []base.StoreSpec{spec},
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					StickyVFSRegistry: stickyVFSRegistry,
				},
				Store: &kvserver.StoreTestingKnobs{
					DisableRaftLogQueue: true,
					EngineKnobs:         []storage.ConfigOption{storage.DisableAutomaticCompactions},
				},
			},
		}
		specs[i] = spec
	}
	tc := testcluster.StartTestCluster(t, numServers,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationManual,
			ServerArgsPerNode: stickyServerArgs,
		})
	defer tc.Stopper().Stop(ctx)

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
	verifyStatsOnServers(t, tc, specs, stickyVFSRegistry, 1)

	// Add some data to the "right" range.
	rangeKeyStart, rangeKeyEnd := key, key.Next()
	err := tc.GetFirstStoreFromServer(t, 0).DB().DelRangeUsingTombstone(ctx, rangeKeyStart, rangeKeyEnd)
	require.NoError(t, err)

	dataKey := rangeKeyEnd.Next()
	_, err = tc.GetFirstStoreFromServer(t, 0).DB().Inc(ctx, dataKey, 5)
	require.NoError(t, err)

	tc.WaitForValues(t, dataKey, []int64{5, 5, 5})

	// Verify all stats on stores after addition.
	// We skip verifying stats on Server[0] because there is no reliable way to
	// do that given all if the system table activity generated by the TestCluster.
	// We use Servers[1] and Servers[2] instead, since we can control the traffic
	// on those servers.
	verifyStatsOnServers(t, tc, specs, stickyVFSRegistry, 1, 2)

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
	verifyStatsOnServers(t, tc, specs, stickyVFSRegistry, 1, 2)
	checkGauge(t, "store 0", tc.GetFirstStoreFromServer(t, 0).Metrics().ReplicaCount, initialCount+1)
	tc.RemoveLeaseHolderOrFatal(t, desc, tc.Target(0), tc.Target(1))
	testutils.SucceedsSoon(t, func() error {
		_, err := tc.GetFirstStoreFromServer(t, 0).GetReplica(desc.RangeID)
		if err == nil {
			return fmt.Errorf("replica still exists on dest 0")
		} else if errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)) {
			return nil
		}
		return err
	})
	tc.WaitForValues(t, dataKey, []int64{0, 5, 5})

	// Verify range count is as expected.
	checkGauge(t, "store 0", tc.GetFirstStoreFromServer(t, 0).Metrics().ReplicaCount, initialCount)
	checkGauge(t, "store 1", tc.GetFirstStoreFromServer(t, 1).Metrics().ReplicaCount, 1)

	// Verify all stats on all stores after range is removed.
	verifyStatsOnServers(t, tc, specs, stickyVFSRegistry, 1, 2)

	for _, serverIdx := range []int{1, 2} {
		s := tc.GetFirstStoreFromServer(t, serverIdx)
		if err := s.ComputeMetrics(context.Background()); err != nil {
			t.Fatal(err)
		}

		// TODO(jackson): Adjust this test to reliably construct multiple levels
		// within the LSM so that we can assert non-zero bloom filter
		// statistics. At the time of writing, the engines in TestStoreMetrics
		// sometimes only contain files only in L6, which do not use bloom
		// filters except when explicitly opted into.

		m := s.Metrics()

		for _, tc := range []struct {
			gauge *metric.Gauge
			min   int64
		}{
			{m.RdbBlockCacheUsage, 0},
			{m.RdbMemtableTotalSize, 5000},
			{m.RdbTableReadersMemEstimate, 50},
		} {
			if a := tc.gauge.Value(); a < tc.min {
				t.Errorf("gauge %s = %d < min %d", tc.gauge.GetName(), a, tc.min)
			}
		}
		for _, tc := range []struct {
			counter *metric.Counter
			min     int64
		}{
			{m.RdbBlockCacheHits, 10},
			{m.RdbBlockCacheMisses, 0},
			{m.RdbBloomFilterPrefixChecked, 0},
			{m.RdbBloomFilterPrefixUseful, 0},
			{m.RdbCompactions, 0},
		} {
			if a := tc.counter.Count(); a < tc.min {
				t.Errorf("counter %s = %d < min %d", tc.counter.GetName(), a, tc.min)
			}
		}
	}
}
