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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestStoresAddStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := NewStores(log.AmbientContext{}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))
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
	ls := NewStores(log.AmbientContext{}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))

	storeID := roachpb.StoreID(89)

	ls.AddStore(&Store{Ident: roachpb.StoreIdent{StoreID: storeID}})

	ls.RemoveStore(&Store{Ident: roachpb.StoreIdent{StoreID: storeID}})

	if ls.HasStore(storeID) {
		t.Errorf("expted local sender to remove storeID=%d", storeID)
	}
}

func TestStoresGetStoreCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := NewStores(log.AmbientContext{}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))
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
	ls := NewStores(log.AmbientContext{}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))
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

func TestStoresGetStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := NewStores(log.AmbientContext{}, hlc.NewClock(hlc.UnixNano, time.Nanosecond))
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
	ls := NewStores(log.AmbientContext{}, cfg.Clock)

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
		cfg.Transport = NewDummyRaftTransport()
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
	ls := NewStores(log.AmbientContext{}, cfg.Clock)

	// Create two stores with ranges we care about.
	stores := []*Store{}
	for i := 0; i < 2; i++ {
		cfg.Transport = NewDummyRaftTransport()
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
	ls2 := NewStores(log.AmbientContext{}, ls.clock)
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
	ls2 := NewStores(log.AmbientContext{}, ls.clock)
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
	ls3 := NewStores(log.AmbientContext{}, ls.clock)
	ls3.AddStore(stores[0])
	verifyBI.Reset()
	if err := ls2.ReadBootstrapInfo(&verifyBI); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bi, verifyBI) {
		t.Errorf("bootstrap info %+v not equal to expected %+v", verifyBI, bi)
	}
}
