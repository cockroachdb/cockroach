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
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func getGauge(t *testing.T, s *storage.Store, key string) int64 {
	gauge := s.Registry().GetGauge(key)
	if gauge == nil {
		t.Fatal(util.ErrorfSkipFrames(1, "store did not contain gauge %s", key))
	}
	return gauge.Value()
}

func getCounter(t *testing.T, s *storage.Store, key string) int64 {
	counter := s.Registry().GetCounter(key)
	if counter == nil {
		t.Fatal(util.ErrorfSkipFrames(1, "store did not contain counter %s", key))
	}
	return counter.Count()
}

func checkGauge(t *testing.T, s *storage.Store, key string, e int64) {
	if a := getGauge(t, s, key); a != e {
		t.Error(util.ErrorfSkipFrames(1, "%s for store: actual %d != expected %d", key, a, e))
	}
}

func checkCounter(t *testing.T, s *storage.Store, key string, e int64) {
	if a := getCounter(t, s, key); a != e {
		t.Error(util.ErrorfSkipFrames(1, "%s for store: actual %d != expected %d", key, a, e))
	}
}

func verifyStats(t *testing.T, mtc *multiTestContext, storeIdx int) {
	// Get the current store at storeIdx.
	s := mtc.stores[storeIdx]
	// Stop the store at the given index, while keeping the the reference to the
	// store object. Method ComputeMVCCStatsTest() still works on a stopped
	// store (it needs only the engine, which is still open), and the most
	// recent stats are still available on the stopped store object; however, no
	// further information can be committed to the store while it is stopped,
	// preventing any races during verification.
	mtc.stopStore(storeIdx)

	// Compute real total MVCC statistics from store.
	realStats, err := s.ComputeMVCCStatsTest()
	if err != nil {
		t.Fatal(err)
	}

	// Sanity regression check for bug #4624: ensure intent count is zero.
	if a := realStats.IntentCount; a != 0 {
		t.Fatalf("Expected intent count to be zero, was %d", a)
	}

	// Sanity check: LiveBytes is not zero (ensures we don't have
	// zeroed out structures.)
	if liveBytes := getGauge(t, s, "livebytes"); liveBytes == 0 {
		t.Fatal("Expected livebytes to be non-zero, was zero")
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
		t.Log(util.ErrorfSkipFrames(1, "verifyStats failed, aborting test."))
		t.FailNow()
	}

	// Restart the store at the provided index.
	mtc.restartStore(storeIdx)
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
	if _, pErr := mtc.dbs[0].Inc(dataKey, 5); pErr != nil {
		t.Fatal(pErr)
	}
	mtc.waitForValues(roachpb.Key("z"), []int64{5, 5, 5})

	// Verify all stats on store 0 and 1 after addition.
	verifyStats(t, mtc, 0)
	verifyStats(t, mtc, 1)

	// Create a transaction statement that fails, but will add an entry to the
	// sequence cache. Regression test for #4969.
	if pErr := mtc.dbs[0].Txn(func(txn *client.Txn) *roachpb.Error {
		b := &client.Batch{}
		b.CPut(dataKey, 7, 6)
		return txn.Run(b)
	}); pErr == nil {
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
	verifyStats(t, mtc, 0)
	verifyStats(t, mtc, 1)

	verifyRocksDBStats(t, mtc.stores[0])
	verifyRocksDBStats(t, mtc.stores[1])
}
