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

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestStoresAddStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := NewStores(hlc.NewClock(hlc.UnixNano))
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
	ls := NewStores(hlc.NewClock(hlc.UnixNano))

	storeID := roachpb.StoreID(89)

	ls.AddStore(&Store{Ident: roachpb.StoreIdent{StoreID: storeID}})

	ls.RemoveStore(&Store{Ident: roachpb.StoreIdent{StoreID: storeID}})

	if ls.HasStore(storeID) {
		t.Errorf("expted local sender to remove storeID=%d", storeID)
	}
}

func TestStoresGetStoreCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := NewStores(hlc.NewClock(hlc.UnixNano))
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
	ls := NewStores(hlc.NewClock(hlc.UnixNano))
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

	err = ls.VisitStores(func(s *Store) error { return errors.New("") })
	if err == nil {
		t.Errorf("expected visit error")
	}
}

func TestStoresGetStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ls := NewStores(hlc.NewClock(hlc.UnixNano))
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
	defer stopper.Stop()
	ctx := TestStoreContext()
	manualClock := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manualClock.UnixNano)
	ls := NewStores(ctx.Clock)

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
		e[i] = engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)
		ctx.Transport = NewDummyRaftTransport()
		s[i] = NewStore(ctx, e[i], &roachpb.NodeDescriptor{NodeID: 1})
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
		if err := s[i].AddReplicaTest(newRng); err != nil {
			t.Error(err)
		}
		ls.AddStore(s[i])
	}

	if _, r, err := ls.lookupReplica(roachpb.RKey("a"), roachpb.RKey("c")); r.StoreID != s[0].Ident.StoreID || err != nil {
		t.Errorf("expected store %d; got %d: %v", s[0].Ident.StoreID, r.StoreID, err)
	}
	if _, r, err := ls.lookupReplica(roachpb.RKey("b"), nil); r.StoreID != s[0].Ident.StoreID || err != nil {
		t.Errorf("expected store %d; got %d: %v", s[0].Ident.StoreID, r.StoreID, err)
	}
	if _, r, err := ls.lookupReplica(roachpb.RKey("b"), roachpb.RKey("d")); r != nil || err == nil {
		t.Errorf("expected store 0 and error got %d", r.StoreID)
	}
	if _, r, err := ls.lookupReplica(roachpb.RKey("x"), roachpb.RKey("z")); r.StoreID != s[1].Ident.StoreID {
		t.Errorf("expected store %d; got %d: %v", s[1].Ident.StoreID, r.StoreID, err)
	}
	if _, r, err := ls.lookupReplica(roachpb.RKey("y"), nil); r.StoreID != s[1].Ident.StoreID || err != nil {
		t.Errorf("expected store %d; got %d: %v", s[1].Ident.StoreID, r.StoreID, err)
	}

	if desc, err := ls.FirstRange(); err != nil || !reflect.DeepEqual(desc, d[0]) {
		t.Fatalf("first range not as expected: error=%v, desc=%+v", err, desc)
	}
}

var storeIDAlloc roachpb.StoreID

// createStores creates a slice of count stores.
func createStores(count int, t *testing.T) (*hlc.ManualClock, []*Store, *Stores, *stop.Stopper) {
	stopper := stop.NewStopper()
	ctx := TestStoreContext()
	manualClock := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manualClock.UnixNano)
	ls := NewStores(ctx.Clock)

	// Create two stores with ranges we care about.
	stores := []*Store{}
	for i := 0; i < 2; i++ {
		ctx.Transport = NewDummyRaftTransport()
		s := NewStore(ctx, engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper), &roachpb.NodeDescriptor{NodeID: 1})
		storeIDAlloc++
		s.Ident.StoreID = storeIDAlloc
		stores = append(stores, s)
	}

	return manualClock, stores, ls, stopper
}

// TestStoresGossipStorage verifies reading and writing of bootstrap info.
func TestStoresGossipStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	manual, stores, ls, stopper := createStores(2, t)
	defer stopper.Stop()
	ls.AddStore(stores[0])

	manual.Set(1)

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
	ls2 := NewStores(ls.clock)
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
	defer stopper.Stop()
	ls.AddStore(stores[0])

	// Set clock to 1.
	manual.Set(1)

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
	ls2 := NewStores(ls.clock)
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
	ls3 := NewStores(ls.clock)
	ls3.AddStore(stores[0])
	verifyBI.Reset()
	if err := ls2.ReadBootstrapInfo(&verifyBI); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bi, verifyBI) {
		t.Errorf("bootstrap info %+v not equal to expected %+v", verifyBI, bi)
	}
}
