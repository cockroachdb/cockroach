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

func verifyStats(t *testing.T, s *storage.Store) {
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
		t.Fatal("Expected livebytes to be non-nero, was zero")
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
	// "Ages" will be different depending on how much time has passed. Even with
	// a manual clock, this can be an issue in tests. Therefore, we do not
	// verify them in this test.

	if t.Failed() {
		t.Log(util.ErrorfSkipFrames(1, "verifyStats failed, aborting test."))
		t.FailNow()
	}
}

func TestStoreMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	store0 := mtc.stores[0]

	// Perform a split, which has special metrics handling.
	splitArgs := adminSplitArgs(roachpb.KeyMin, roachpb.Key("m"))
	if _, err := client.SendWrapped(rg1(store0), nil, &splitArgs); err != nil {
		t.Fatal(err)
	}

	// Verify range count is as expected
	checkCounter(t, store0, "ranges", 2)

	// Verify all stats on store0 after split.
	verifyStats(t, store0)

	// Replicate the "right" range to the other stores.
	replica := store0.LookupReplica(roachpb.RKey("z"), nil)
	mtc.replicateRange(replica.RangeID, 1, 2)

	// Add some data to the "right" range.
	incArgs := incrementArgs([]byte("z"), 5)
	if _, err := client.SendWrappedWith(store0, nil, roachpb.Header{
		RangeID: replica.RangeID,
	}, &incArgs); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("z"), []int64{5, 5, 5})

	// Verify all stats on store0 after addition.
	verifyStats(t, store0)

	// Unreplicate range from the first store.
	mtc.unreplicateRange(replica.RangeID, 0)

	// Force GC Scan on store 0 in order to fully remove range.
	mtc.stores[1].ForceReplicaGCScanAndProcess()
	mtc.waitForValues(roachpb.Key("z"), []int64{0, 5, 5})

	// Verify range count is as expected.
	checkCounter(t, store0, "ranges", 1)

	// Verify all stats on store0 after range is removed.
	verifyStats(t, store0)
}
