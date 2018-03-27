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

package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func newStores(ambientCtx log.AmbientContext, clock *hlc.Clock) *Stores {
	return NewStores(ambientCtx, clock, cluster.BinaryMinimumSupportedVersion, cluster.BinaryServerVersion)
}

func TestStoresAddStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))
	store := Store{}
	ls.AddStore(&store)
	if !ls.HasStore(store.Ident.StoreID) {
		t.Errorf("expected local sender to contain storeID=%d", store.Ident.StoreID)
	}
	if ls.HasStore(store.Ident.StoreID + 1) {
		t.Errorf("expected local sender to not contain storeID=%d", store.Ident.StoreID+1)
	}
}

func TestStoresRemoveStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))

	storeID := roachpb.StoreID(89)

	ls.AddStore(&Store{Ident: roachpb.StoreIdent{StoreID: storeID}})

	ls.RemoveStore(&Store{Ident: roachpb.StoreIdent{StoreID: storeID}})

	if ls.HasStore(storeID) {
		t.Errorf("expted local sender to remove storeID=%d", storeID)
	}
}

func TestStoresGetStoreCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))
	if ls.GetStoreCount() != 0 {
		t.Errorf("expected 0 stores in new local sender")
	}

	expectedCount := 10
	for i := 0; i < expectedCount; i++ {
		ls.AddStore(&Store{Ident: roachpb.StoreIdent{StoreID: roachpb.StoreID(i)}})
	}
	if count := ls.GetStoreCount(); count != expectedCount {
		t.Errorf("expected store count to be %d but was %d", expectedCount, count)
	}
}

func TestStoresVisitStores(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))
	numStores := 10
	for i := 0; i < numStores; i++ {
		ls.AddStore(&Store{Ident: roachpb.StoreIdent{StoreID: roachpb.StoreID(i)}})
	}

	visit := make([]bool, numStores)
	err := ls.VisitStores(func(s *Store) error { visit[s.Ident.StoreID] = true; return nil })
	if err != nil {
		t.Errorf("unexpected error on visit: %s", err.Error())
	}

	for i, visited := range visit {
		if !visited {
			t.Errorf("store %d was not visited", i)
		}
	}

	errBoom := errors.New("boom")
	if err := ls.VisitStores(func(s *Store) error {
		return errBoom
	}); err != errBoom {
		t.Errorf("got unexpected error %v", err)
	}
}

func TestStoresGetReplicaForRangeID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	ls := newStores(log.AmbientContext{}, clock)
	numStores := 10
	for i := 0; i < numStores; i++ {
		storeID := roachpb.StoreID(i)
		rangeID := roachpb.RangeID(i)

		memEngine := engine.NewInMem(roachpb.Attributes{}, 1<<20)
		stopper.AddCloser(memEngine)

		cfg := TestStoreConfig(clock)
		cfg.Transport = NewDummyRaftTransport(cfg.Settings)

		store := NewStore(cfg, memEngine, &roachpb.NodeDescriptor{NodeID: 1})
		store.Ident.StoreID = storeID
		ls.AddStore(store)

		desc := &roachpb.RangeDescriptor{
			RangeID:  rangeID,
			StartKey: roachpb.RKey("a"),
			EndKey:   roachpb.RKey("b"),
			Replicas: []roachpb.ReplicaDescriptor{{StoreID: storeID}},
		}

		replica, err := NewReplica(desc, store, 0)
		if err != nil {
			t.Fatalf("unexpected error when creating replica: %v", err)
		}
		err2 := store.AddReplica(replica)
		if err2 != nil {
			t.Fatalf("unexpected error when adding replica: %v", err2)
		}
	}

	// Test the case where the replica we're looking for exists.
	rangeID1 := roachpb.RangeID(5)
	replica1, err1 := ls.GetReplicaForRangeID(rangeID1)
	if replica1 == nil {
		t.Fatal("expected replica to be found; was nil")
	}
	if err1 != nil {
		t.Fatalf("expected err to be nil; was %v", err1)
	}
	if replica1.RangeID != rangeID1 {
		t.Fatalf("expected replica's range id to be %v; got %v", rangeID1, replica1.RangeID)
	}

	// Test the case where the replica we're looking for doesn't exist.
	rangeID2 := roachpb.RangeID(1000)
	replica2, err2 := ls.GetReplicaForRangeID(rangeID2)
	if replica2 != nil {
		t.Fatalf("expected replica to be nil; was %v", replica2)
	}
	expectedError := roachpb.NewRangeNotFoundError(rangeID2)
	if err2.Error() != expectedError.Error() {
		t.Fatalf("expected err to be %v; was %v", expectedError, err2)
	}
}

func TestStoresGetStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))
	store := Store{}
	replica := roachpb.ReplicaDescriptor{StoreID: store.Ident.StoreID}
	s, pErr := ls.GetStore(replica.StoreID)
	if s != nil || pErr == nil {
		t.Errorf("expected no stores in new local sender")
	}

	ls.AddStore(&store)
	s, pErr = ls.GetStore(replica.StoreID)
	if s == nil {
		t.Errorf("expected store")
	} else if s.Ident.StoreID != store.Ident.StoreID {
		t.Errorf("expected storeID to be %d but was %d",
			s.Ident.StoreID, store.Ident.StoreID)
	} else if pErr != nil {
		t.Errorf("expected no error, instead had err=%s", pErr)
	}
}

func TestStoresLookupReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	cfg := TestStoreConfig(nil)
	ls := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, cfg.Clock)

	// Create two new stores with ranges we care about.
	var e [2]engine.Engine
	var s [2]*Store
	var d [2]*roachpb.RangeDescriptor
	ranges := []struct {
		storeID    roachpb.StoreID
		start, end roachpb.RKey
	}{
		{2, roachpb.RKeyMin, roachpb.RKey("c")},
		{3, roachpb.RKey("x"), roachpb.RKey("z")},
	}
	for i, rng := range ranges {
		e[i] = engine.NewInMem(roachpb.Attributes{}, 1<<20)
		stopper.AddCloser(e[i])
		cfg.Transport = NewDummyRaftTransport(cfg.Settings)
		s[i] = NewStore(cfg, e[i], &roachpb.NodeDescriptor{NodeID: 1})
		s[i].Ident.StoreID = rng.storeID

		d[i] = &roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(i),
			StartKey: rng.start,
			EndKey:   rng.end,
			Replicas: []roachpb.ReplicaDescriptor{{StoreID: rng.storeID}},
		}
		newRng, err := NewReplica(d[i], s[i], 0)
		if err != nil {
			t.Fatal(err)
		}
		if err := s[i].AddReplica(newRng); err != nil {
			t.Error(err)
		}
		ls.AddStore(s[i])
	}

	testCases := []struct {
		start, end roachpb.RKey
		expStoreID roachpb.StoreID
		expError   string
	}{
		{
			start:      roachpb.RKey("a"),
			end:        roachpb.RKey("c"),
			expStoreID: s[0].Ident.StoreID,
		},
		{
			start:      roachpb.RKey("b"),
			end:        nil,
			expStoreID: s[0].Ident.StoreID,
		},
		{
			start:    roachpb.RKey("b"),
			end:      roachpb.RKey("d"),
			expError: "outside of bounds of range",
		},
		{
			start:      roachpb.RKey("x"),
			end:        roachpb.RKey("z"),
			expStoreID: s[1].Ident.StoreID,
		},
		{
			start:      roachpb.RKey("y"),
			end:        nil,
			expStoreID: s[1].Ident.StoreID,
		},
		{
			start:    roachpb.RKey("z1"),
			end:      roachpb.RKey("z2"),
			expError: "r0 was not found",
		},
	}
	for testIdx, tc := range testCases {
		_, r, err := ls.LookupReplica(tc.start, tc.end)
		if tc.expError != "" {
			if !testutils.IsError(err, tc.expError) {
				t.Errorf("%d: got error %v (expected %s)", testIdx, err, tc.expError)
			}
		} else if err != nil {
			t.Errorf("%d: %s", testIdx, err)
		} else if r.StoreID != tc.expStoreID {
			t.Errorf("%d: expected store %d; got %d", testIdx, tc.expStoreID, r.StoreID)
		}
	}

	if desc, err := ls.FirstRange(); err != nil {
		t.Error(err)
	} else if !reflect.DeepEqual(desc, d[0]) {
		t.Fatalf("expected first range %+v; got %+v", desc, d[0])
	}
}

var storeIDAlloc roachpb.StoreID

// createStores creates a slice of count stores.
func createStores(count int, t *testing.T) (*hlc.ManualClock, []*Store, *Stores, *stop.Stopper) {
	stopper := stop.NewStopper()
	manual := hlc.NewManualClock(123)
	cfg := TestStoreConfig(hlc.NewClock(manual.UnixNano, time.Nanosecond))
	ls := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, cfg.Clock)

	// Create two stores with ranges we care about.
	stores := []*Store{}
	for i := 0; i < count; i++ {
		cfg.Transport = NewDummyRaftTransport(cfg.Settings)
		eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
		stopper.AddCloser(eng)
		s := NewStore(cfg, eng, &roachpb.NodeDescriptor{NodeID: 1})
		storeIDAlloc++
		s.Ident.StoreID = storeIDAlloc
		stores = append(stores, s)
	}

	return manual, stores, ls, stopper
}

// TestStoresGossipStorage verifies reading and writing of bootstrap info.
func TestStoresGossipStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual, stores, ls, stopper := createStores(2, t)
	defer stopper.Stop(context.TODO())
	ls.AddStore(stores[0])

	// Verify initial read is empty.
	var bi gossip.BootstrapInfo
	if err := ls.ReadBootstrapInfo(&bi); err != nil {
		t.Fatal(err)
	}
	if len(bi.Addresses) != 0 {
		t.Errorf("expected empty bootstrap info: %+v", bi)
	}

	// Add a fake address and write.
	manual.Increment(1)
	bi.Addresses = append(bi.Addresses, util.MakeUnresolvedAddr("tcp", "127.0.0.1:8001"))
	if err := ls.WriteBootstrapInfo(&bi); err != nil {
		t.Fatal(err)
	}

	// Verify on read.
	manual.Increment(1)
	var newBI gossip.BootstrapInfo
	if err := ls.ReadBootstrapInfo(&newBI); err != nil {
		t.Fatal(err)
	}
	if len(newBI.Addresses) != 1 {
		t.Errorf("expected single bootstrap info address: %+v", newBI)
	}

	// Add another store and verify it has bootstrap info written.
	ls.AddStore(stores[1])

	// Create a new stores object to verify read.
	ls2 := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, ls.clock)
	ls2.AddStore(stores[1])
	var verifyBI gossip.BootstrapInfo
	if err := ls2.ReadBootstrapInfo(&verifyBI); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bi, verifyBI) {
		t.Errorf("bootstrap info %+v not equal to expected %+v", verifyBI, bi)
	}
}

// TestStoresGossipStorageReadLatest verifies that the latest
// bootstrap info from multiple stores is returned on Read.
func TestStoresGossipStorageReadLatest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual, stores, ls, stopper := createStores(2, t)
	defer stopper.Stop(context.TODO())
	ls.AddStore(stores[0])

	// Add a fake address and write.
	var bi gossip.BootstrapInfo
	bi.Addresses = append(bi.Addresses, util.MakeUnresolvedAddr("tcp", "127.0.0.1:8001"))
	if err := ls.WriteBootstrapInfo(&bi); err != nil {
		t.Fatal(err)
	}

	// Now remove store 0 and add store 1.
	ls.RemoveStore(stores[0])
	ls.AddStore(stores[1])

	// Increment clock, add another address and write.
	manual.Increment(1)
	bi.Addresses = append(bi.Addresses, util.MakeUnresolvedAddr("tcp", "127.0.0.1:8002"))
	if err := ls.WriteBootstrapInfo(&bi); err != nil {
		t.Fatal(err)
	}

	// Create a new stores object to freshly read. Should get latest
	// version from store 1.
	manual.Increment(1)
	ls2 := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, ls.clock)
	ls2.AddStore(stores[0])
	ls2.AddStore(stores[1])
	var verifyBI gossip.BootstrapInfo
	if err := ls2.ReadBootstrapInfo(&verifyBI); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bi, verifyBI) {
		t.Errorf("bootstrap info %+v not equal to expected %+v", verifyBI, bi)
	}

	// Verify that stores[0], which had old info, was updated with
	// latest bootstrap info during the read.
	ls3 := newStores(log.AmbientContext{Tracer: tracing.NewTracer()}, ls.clock)
	ls3.AddStore(stores[0])
	verifyBI.Reset()
	if err := ls3.ReadBootstrapInfo(&verifyBI); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bi, verifyBI) {
		t.Errorf("bootstrap info %+v not equal to expected %+v", verifyBI, bi)
	}
}

// TestStoresClusterVersionWriteSynthesize verifies that the cluster version is
// written to all stores and that missing versions are filled in appropriately.
func TestStoresClusterVersionWriteSynthesize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	_, stores, _, stopper := createStores(3, t)
	ctx := context.Background()
	defer stopper.Stop(ctx)

	makeStores := func() *Stores {
		// Hard-code ServerVersion of 1.1 for this test.
		// Hard-code MinSupportedVersion of 1.0 for this test.
		ls := NewStores(log.AmbientContext{}, stores[0].Clock(), cluster.VersionByKey(cluster.VersionBase), roachpb.Version{Major: 1, Minor: 1})
		return ls
	}

	ls0 := makeStores()

	// If there are no stores, default to minSupportedVersion
	// (VersionBase in this test)
	if initialCV, err := ls0.SynthesizeClusterVersion(ctx); err != nil {
		t.Fatal(err)
	} else {
		expCV := cluster.ClusterVersion{
			MinimumVersion: cluster.VersionByKey(cluster.VersionBase),
			UseVersion:     cluster.VersionByKey(cluster.VersionBase),
		}
		if !reflect.DeepEqual(initialCV, expCV) {
			t.Fatalf("expected %+v; got %+v", expCV, initialCV)
		}
	}

	ls0.AddStore(stores[0])

	versionA := roachpb.Version{Major: 1, Minor: 0, Unstable: 1} // 1.0-1
	versionB := roachpb.Version{Major: 1, Minor: 0, Unstable: 2} // 1.0-2
	versionC := roachpb.Version{Major: 1, Minor: 1}              // 1.1-0

	// Verify that the initial read of an empty store synthesizes v1.0-0. This
	// is the code path that runs after starting the 1.1 binary for the first
	// time after the rolling upgrade from 1.0.
	//
	// Do it twice because after the first iteration, we have written these
	// values to storage, so the second time around the synthesis does not
	// happen.
	for i := 0; i < 2; i++ {
		if initialCV, err := ls0.SynthesizeClusterVersion(ctx); err != nil {
			t.Fatal(err)
		} else {
			expCV := cluster.ClusterVersion{
				MinimumVersion: cluster.VersionByKey(cluster.VersionBase),
				UseVersion:     cluster.VersionByKey(cluster.VersionBase),
			}
			if !reflect.DeepEqual(initialCV, expCV) {
				t.Fatalf("expected %+v; got %+v", expCV, initialCV)
			}
		}
	}

	// Bump a version to something more modern (but supported by this binary).
	// Note that there's still only one store.
	{
		cv := cluster.ClusterVersion{
			MinimumVersion: versionA,
			UseVersion:     versionB,
		}
		if err := ls0.WriteClusterVersion(ctx, cv); err != nil {
			t.Fatal(err)
		}

		// Verify the same thing comes back on read.
		if newCV, err := ls0.SynthesizeClusterVersion(ctx); err != nil {
			t.Fatal(err)
		} else {
			expCV := cv
			if !reflect.DeepEqual(newCV, cv) {
				t.Fatalf("expected %+v; got %+v", expCV, newCV)
			}
		}
	}

	// Make a stores with store0 and store1. It reads as v1.0 because store1 has
	// no entry, lowering the use version to v1.0 (but not the min version).
	{
		ls01 := makeStores()
		ls01.AddStore(stores[0])
		ls01.AddStore(stores[1])

		expCV := cluster.ClusterVersion{
			MinimumVersion: versionA,
			UseVersion:     cluster.VersionByKey(cluster.VersionBase),
		}
		if cv, err := ls01.SynthesizeClusterVersion(ctx); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(cv, expCV) {
			t.Fatalf("expected %+v, got %+v", expCV, cv)
		}

		// Verify the same thing comes back on read of either of the stores (i.e.,
		// we wrote the updated version to both).
		for _, store := range stores[:2] {
			ls := makeStores()
			ls.AddStore(store)
			if cv, err := ls.SynthesizeClusterVersion(ctx); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(cv, expCV) {
				t.Fatalf("expected %+v; got %+v", expCV, cv)
			}
		}

		// Write an updated UseVersion to both stores.
		cv := cluster.ClusterVersion{
			MinimumVersion: versionA,
			UseVersion:     versionB,
		}
		if err := ls01.WriteClusterVersion(ctx, cv); err != nil {
			t.Fatal(err)
		}
	}

	// Third node comes along, for now it's alone. It has a higher (but
	// compatible) MinimumVersion, and a lower use version.
	cv := cluster.ClusterVersion{
		MinimumVersion: versionC,
		UseVersion:     versionA,
	}

	{
		ls3 := makeStores()
		ls3.AddStore(stores[2])
		if err := ls3.WriteClusterVersion(ctx, cv); err != nil {
			t.Fatal(err)
		}
	}

	ls012 := makeStores()
	for _, store := range stores {
		ls012.AddStore(store)
	}

	// Reading across all stores, we expect to pick up the highest min version
	// and the lowest useVersion both from the third store.
	expCV := cluster.ClusterVersion{
		MinimumVersion: versionC,
		UseVersion:     versionA,
	}
	if cv, err := ls012.SynthesizeClusterVersion(ctx); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(cv, expCV) {
		t.Fatalf("expected %+v, got %+v", expCV, cv)
	}
}

// TestStoresClusterVersionIncompatible verifies an error occurs when
// setting up the cluster version from stores that are incompatible with the
// running binary.
func TestStoresClusterVersionIncompatible(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	vOneDashOne := roachpb.Version{Major: 1, Unstable: 1}
	vOne := roachpb.Version{Major: 1}

	type testFn func(*cluster.ClusterVersion, *Stores) string
	for name, setter := range map[string]testFn{
		"StoreTooNewUseVersion": func(cv *cluster.ClusterVersion, ls *Stores) string {
			// This is what the running node requires from its stores.
			ls.minSupportedVersion = vOne
			// This is what the node is running.
			ls.serverVersion = vOneDashOne
			// MinimumVersion is harmless, it's the "running" version.
			cv.MinimumVersion = ls.serverVersion
			// UseVersion is way too high for this node.
			cv.UseVersion = roachpb.Version{Major: 9}
			return `cockroach version v1\.0-1 is incompatible with data in store <no-attributes>=<in-mem>; use version v9\.0 or later`
		},
		"StoreTooNewMinVersion": func(cv *cluster.ClusterVersion, ls *Stores) string {
			ls.minSupportedVersion = vOne
			ls.serverVersion = vOneDashOne
			// These two are switched compared to previous test case to make
			// sure it doesn't change the outcome.
			cv.UseVersion = ls.serverVersion
			cv.MinimumVersion = roachpb.Version{Major: 9}
			return `cockroach version v1\.0-1 is incompatible with data in store <no-attributes>=<in-mem>; use version v9\.0 or later`
		},
		"StoreTooOldUseVersion": func(cv *cluster.ClusterVersion, ls *Stores) string {
			// This is what the running node requires from its stores.
			ls.minSupportedVersion = roachpb.Version{Major: 5}
			// This is what the node is running.
			ls.serverVersion = roachpb.Version{Major: 9}
			// MinimumVersion is compatible in this test case.
			cv.MinimumVersion = ls.serverVersion
			// UseVersion is way too low.
			cv.UseVersion = roachpb.Version{Major: 4}
			return `store <no-attributes>=<in-mem>, last used with cockroach version v4\.0, is too old for running version v9\.0 \(which requires data from v5\.0 or later\)`
		},
		"StoreTooOldMinVersion": func(cv *cluster.ClusterVersion, ls *Stores) string {
			// Like the previous test case, but this time cv.MinimumVersion is the culprit.
			ls.minSupportedVersion = roachpb.Version{Major: 5}
			ls.serverVersion = roachpb.Version{Major: 9}
			cv.MinimumVersion = ls.serverVersion
			cv.UseVersion = roachpb.Version{Major: 4}
			return `store <no-attributes>=<in-mem>, last used with cockroach version v4\.0, is too old for running version v9\.0 \(which requires data from v5\.0 or later\)`
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, stores, ls, stopper := createStores(1, t)
			defer stopper.Stop(ctx)
			ls.AddStore(stores[0])
			// Configure versions and write.
			var cv cluster.ClusterVersion
			expErr := setter(&cv, ls)
			if err := ls.WriteClusterVersion(ctx, cv); err != nil {
				t.Fatal(err)
			}
			if _, err := ls.SynthesizeClusterVersion(ctx); !testutils.IsError(err, expErr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
