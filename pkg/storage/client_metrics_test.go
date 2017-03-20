// Copyright 2016 The Cockroach Authors.
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
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
)

func checkGauge(t *testing.T, g *metric.Gauge, e int64) {
	if a := g.Value(); a != e {
		t.Error(errors.Errorf("%s for store: actual %d != expected %d", g.GetName(), a, e))
	}
}

func checkCounter(t *testing.T, c *metric.Counter, e int64) {
	if a := c.Count(); a != e {
		t.Error(errors.Errorf("%s for store: actual %d != expected %d", c.GetName(), a, e))
	}
}

func verifyStats(t *testing.T, mtc *multiTestContext, storeIdxSlice ...int) {
	var stores []*storage.Store
	var wg sync.WaitGroup

	mtc.mu.RLock()
	numStores := len(mtc.stores)
	// We need to stop the stores at the given indexes, while keeping the reference to the
	// store objects. ComputeMVCCStats() still works on a stopped store (it needs
	// only the engine, which is still open), and the most recent stats are still
	// available on the stopped store object; however, no further information can
	// be committed to the store while it is stopped, preventing any races during
	// verification.
	for _, storeIdx := range storeIdxSlice {
		stores = append(stores, mtc.stores[storeIdx])
	}
	mtc.mu.RUnlock()

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

	wg.Add(numStores)
	// We actually stop *all* of the Stores. Stopping only a few is riddled
	// with deadlocks since operations can span nodes, but stoppers don't
	// know about this - taking all of them down at the same time is the
	// only sane way of guaranteeing that nothing interesting happens, at
	// least when bringing down the nodes jeopardizes majorities.
	for i := 0; i < numStores; i++ {
		go func(i int) {
			defer wg.Done()
			mtc.stopStore(i)
		}(i)
	}
	wg.Wait()

	for _, s := range stores {
		fatalf := func(msg string, args ...interface{}) {
			prefix := s.Ident.String() + ": "
			t.Fatalf(prefix+msg, args...)
		}

		m := s.Metrics()

		// Sanity check: LiveBytes is not zero (ensures we don't have
		// zeroed out structures.)
		if liveBytes := m.LiveBytes.Value(); liveBytes == 0 {
			fatalf("expected livebytes to be non-zero, was zero")
		}

		// Compute real total MVCC statistics from store.
		realStats, err := s.ComputeMVCCStats()
		if err != nil {
			t.Fatal(err)
		}

		// Ensure that real MVCC stats match computed stats.
		checkGauge(t, m.LiveBytes, realStats.LiveBytes)
		checkGauge(t, m.KeyBytes, realStats.KeyBytes)
		checkGauge(t, m.ValBytes, realStats.ValBytes)
		checkGauge(t, m.IntentBytes, realStats.IntentBytes)
		checkGauge(t, m.LiveCount, realStats.LiveCount)
		checkGauge(t, m.KeyCount, realStats.KeyCount)
		checkGauge(t, m.ValCount, realStats.ValCount)
		checkGauge(t, m.IntentCount, realStats.IntentCount)
		checkGauge(t, m.SysBytes, realStats.SysBytes)
		checkGauge(t, m.SysCount, realStats.SysCount)
		// "Ages" will be different depending on how much time has passed. Even with
		// a manual clock, this can be an issue in tests. Therefore, we do not
		// verify them in this test.

		if t.Failed() {
			fatalf("verifyStats failed, aborting test.")
		}
	}

	// Restart all Stores.
	for i := 0; i < numStores; i++ {
		mtc.restartStore(i)
	}
}

func verifyRocksDBStats(t *testing.T, s *storage.Store) {
	if err := s.ComputeMetrics(context.TODO(), 0); err != nil {
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
		{m.RdbMemtableHits, 0},
		{m.RdbMemtableMisses, 0},
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

func TestStoreMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("TODO(mrtracy): #9204")

	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Flush RocksDB memtables, so that RocksDB begins using block-based tables.
	// This is useful, because most of the stats we track don't apply to
	// memtables.
	if err := mtc.stores[0].Engine().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := mtc.stores[1].Engine().Flush(); err != nil {
		t.Fatal(err)
	}

	// Disable the raft log truncation which confuses this test.
	for _, s := range mtc.stores {
		s.SetRaftLogQueueActive(false)
	}

	// Perform a split, which has special metrics handling.
	splitArgs := adminSplitArgs(roachpb.KeyMin, roachpb.Key("m"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), splitArgs); err != nil {
		t.Fatal(err)
	}

	// Verify range count is as expected
	checkCounter(t, mtc.stores[0].Metrics().ReplicaCount, 2)

	// Verify all stats on store0 after split.
	verifyStats(t, mtc, 0)

	// Replicate the "right" range to the other stores.
	replica := mtc.stores[0].LookupReplica(roachpb.RKey("z"), nil)
	mtc.replicateRange(replica.RangeID, 1, 2)

	// Verify stats on store1 after replication.
	verifyStats(t, mtc, 1)

	// Add some data to the "right" range.
	dataKey := []byte("z")
	if _, err := mtc.dbs[0].Inc(context.TODO(), dataKey, 5); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("z"), []int64{5, 5, 5})

	// Verify all stats on store 0 and 1 after addition.
	verifyStats(t, mtc, 0, 1)

	// Create a transaction statement that fails, but will add an entry to the
	// sequence cache. Regression test for #4969.
	if err := mtc.dbs[0].Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		b.CPut(dataKey, 7, 6)
		return txn.Run(ctx, b)
	}); err == nil {
		t.Fatal("Expected transaction error, but none received")
	}

	// Verify stats after sequence cache addition.
	verifyStats(t, mtc, 0)
	checkCounter(t, mtc.stores[0].Metrics().ReplicaCount, 2)

	// Unreplicate range from the first store.
	mtc.unreplicateRange(replica.RangeID, 0)

	// Force GC Scan on store 0 in order to fully remove range.
	mtc.stores[1].ForceReplicaGCScanAndProcess()
	mtc.waitForValues(roachpb.Key("z"), []int64{0, 5, 5})

	// Verify range count is as expected.
	checkCounter(t, mtc.stores[0].Metrics().ReplicaCount, 1)
	checkCounter(t, mtc.stores[1].Metrics().ReplicaCount, 1)

	// Verify all stats on store0 and store1 after range is removed.
	verifyStats(t, mtc, 0, 1)

	verifyRocksDBStats(t, mtc.stores[0])
	verifyRocksDBStats(t, mtc.stores[1])
}
