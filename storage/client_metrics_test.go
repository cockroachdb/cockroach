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
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/pkg/errors"
)

func getGauge(t *testing.T, s *storage.Store, key string) int64 {
	gauge := s.Registry().GetGauge(key)
	if gauge == nil {
		t.Fatal(errors.Errorf("store did not contain gauge %s", key))
	}
	return gauge.Value()
}

func getCounter(t *testing.T, s *storage.Store, key string) int64 {
	counter := s.Registry().GetCounter(key)
	if counter == nil {
		t.Fatal(errors.Errorf("store did not contain counter %s", key))
	}
	return counter.Count()
}

func checkGauge(t *testing.T, s *storage.Store, key string, e int64) {
	if a := getGauge(t, s, key); a != e {
		t.Error(errors.Errorf("%s for store: actual %d != expected %d", key, a, e))
	}
}

func checkCounter(t *testing.T, s *storage.Store, key string, e int64) {
	if a := getCounter(t, s, key); a != e {
		t.Error(errors.Errorf("%s for store: actual %d != expected %d", key, a, e))
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
		// Compute real total MVCC statistics from store.
		realStats, err := s.ComputeMVCCStats()
		if err != nil {
			t.Fatal(err)
		}

		// Sanity regression check for bug #4624: ensure intent count is zero.
		if a := realStats.IntentCount; a != 0 {
			fatalf("expected intent count to be zero, was %d", a)
		}

		// Sanity check: LiveBytes is not zero (ensures we don't have
		// zeroed out structures.)
		if liveBytes := getGauge(t, s, "livebytes"); liveBytes == 0 {
			fatalf("expected livebytes to be non-zero, was zero")
		}

		// Ensure that real MVCC stats match computed stats.
		checkGauge(t, s, "livebytes", realStats.LiveBytes)
		checkGauge(t, s, "keybytes", realStats.KeyBytes)
		checkGauge(t, s, "valbytes", realStats.ValBytes)
		checkGauge(t, s, "intentbytes", realStats.IntentBytes)
		checkGauge(t, s, "livecount", realStats.LiveCount)
		checkGauge(t, s, "keycount", realStats.KeyCount)
		checkGauge(t, s, "valcount", realStats.ValCount)
		checkGauge(t, s, "intentcount", realStats.IntentCount)
		checkGauge(t, s, "sysbytes", realStats.SysBytes)
		checkGauge(t, s, "syscount", realStats.SysCount)
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
	if err := s.ComputeMetrics(); err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		gaugeName string
		min       int64
	}{
		{"rocksdb.block.cache.hits", 10},
		{"rocksdb.block.cache.misses", 0},
		{"rocksdb.block.cache.usage", 0},
		{"rocksdb.block.cache.pinned-usage", 0},
		{"rocksdb.bloom.filter.prefix.checked", 20},
		{"rocksdb.bloom.filter.prefix.useful", 20},
		{"rocksdb.memtable.hits", 0},
		{"rocksdb.memtable.misses", 0},
		{"rocksdb.memtable.total-size", 5000},
		{"rocksdb.flushes", 1},
		{"rocksdb.compactions", 0},
		{"rocksdb.table-readers-mem-estimate", 50},
	}
	for _, tc := range testcases {
		if a := getGauge(t, s, tc.gaugeName); a < tc.min {
			t.Errorf("gauge %s = %d < min %d", tc.gaugeName, a, tc.min)
		}
	}
}

func TestStoreMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

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
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &splitArgs); err != nil {
		t.Fatal(err)
	}

	// Verify range count is as expected
	checkCounter(t, mtc.stores[0], "replicas", 2)

	// Verify all stats on store0 after split.
	verifyStats(t, mtc, 0)

	// Replicate the "right" range to the other stores.
	replica := mtc.stores[0].LookupReplica(roachpb.RKey("z"), nil)
	mtc.replicateRange(replica.RangeID, 1, 2)

	// Verify stats on store1 after replication.
	verifyStats(t, mtc, 1)

	// Add some data to the "right" range.
	dataKey := []byte("z")
	if _, err := mtc.dbs[0].Inc(dataKey, 5); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("z"), []int64{5, 5, 5})

	// Verify all stats on store 0 and 1 after addition.
	verifyStats(t, mtc, 0, 1)

	// Create a transaction statement that fails, but will add an entry to the
	// sequence cache. Regression test for #4969.
	if err := mtc.dbs[0].Txn(func(txn *client.Txn) error {
		b := &client.Batch{}
		b.CPut(dataKey, 7, 6)
		return txn.Run(b)
	}); err == nil {
		t.Fatal("Expected transaction error, but none received")
	}

	// Verify stats after sequence cache addition.
	verifyStats(t, mtc, 0)
	checkCounter(t, mtc.stores[0], "replicas", 2)

	// Unreplicate range from the first store.
	mtc.unreplicateRange(replica.RangeID, 0)

	// Force GC Scan on store 0 in order to fully remove range.
	mtc.stores[1].ForceReplicaGCScanAndProcess()
	mtc.waitForValues(roachpb.Key("z"), []int64{0, 5, 5})

	// Verify range count is as expected.
	checkCounter(t, mtc.stores[0], "replicas", 1)
	checkCounter(t, mtc.stores[1], "replicas", 1)

	// Verify all stats on store0 and store1 after range is removed.
	verifyStats(t, mtc, 0, 1)

	verifyRocksDBStats(t, mtc.stores[0])
	verifyRocksDBStats(t, mtc.stores[1])
}
