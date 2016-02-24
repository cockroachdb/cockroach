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
// Author: Matt Tracy (matt@cockroachlabs.com)

package storage_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func getGauge(t *testing.T, s *storage.Store, key string) int64 {
	gauge := s.Registry().GetGauge(key)
	if gauge == nil {
		t.Fatalf("store did not contain gauge %s", key)
		return 0
	}
	return gauge.Value()
}

func getCounter(t *testing.T, s *storage.Store, key string) int64 {
	counter := s.Registry().GetCounter(key)
	if counter == nil {
		t.Fatalf("store did not contain counter %s", key)
		return 0
	}
	return counter.Count()
}

func checkGauge(t *testing.T, s *storage.Store, key string, e int64) {
	if a := getGauge(t, s, key); a != e {
		t.Errorf("%s for store: actual %d != expected %d", key, a, e)
	}
}

func checkCounter(t *testing.T, s *storage.Store, key string, e int64) {
	if a := getCounter(t, s, key); a != e {
		t.Errorf("%s for store: actual %d != expected %d", key, a, e)
	}
}

func TestStoreStatsSplit(t *testing.T) {
	defer leaktest.AfterTest(t)
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

	// Compute real total MVCC statistics from store.
	realStats, err := store0.ComputeMVCCStatsTest()
	if err != nil {
		t.Fatal(err)
	}

	// Sanity regression check for bug #4624: ensure intent count is zero.
	if a := realStats.IntentCount; a != 0 {
		t.Fatalf("Expected intent count to be zero, was %d", a)
	}

	// Sanity check: LiveBytes is greater than zero (ensures we don't have
	// zeroed out structures.)
	if liveBytes := getGauge(t, store0, "livebytes"); liveBytes <= 0 {
		t.Fatalf("Expected livebytes to be greater than zero, was %d", liveBytes)
	}

	// Ensure that real MVCC stats match computed stats.
	checkGauge(t, store0, "livebytes", realStats.LiveBytes)
	checkGauge(t, store0, "keybytes", realStats.KeyBytes)
	checkGauge(t, store0, "valbytes", realStats.ValBytes)
	checkGauge(t, store0, "intentbytes", realStats.IntentBytes)
	checkGauge(t, store0, "livecount", realStats.LiveCount)
	checkGauge(t, store0, "keycount", realStats.KeyCount)
	checkGauge(t, store0, "valcount", realStats.ValCount)
	checkGauge(t, store0, "intentcount", realStats.IntentCount)
	checkGauge(t, store0, "intentage", realStats.IntentAge)
	checkGauge(t, store0, "gcbytesage", realStats.GCBytesAge)
	checkGauge(t, store0, "lastupdatenanos", realStats.LastUpdateNanos)
}
