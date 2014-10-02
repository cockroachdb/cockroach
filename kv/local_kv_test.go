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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
)

func TestLocalKVAddStore(t *testing.T) {
	kv := NewLocalKV()
	store := storage.Store{}
	kv.AddStore(&store)
	if !kv.HasStore(store.Ident.StoreID) {
		t.Errorf("expected kv to contain storeID=%d", store.Ident.StoreID)
	}
	if kv.HasStore(store.Ident.StoreID + 1) {
		t.Errorf("expected kv to not contain storeID=%d", store.Ident.StoreID+1)
	}
}

func TesLocalKVtGetStoreCount(t *testing.T) {
	kv := NewLocalKV()
	if kv.GetStoreCount() != 0 {
		t.Errorf("expected 0 stores in new kv")
	}

	expectedCount := 10
	for i := 0; i < expectedCount; i++ {
		kv.AddStore(&storage.Store{Ident: proto.StoreIdent{StoreID: int32(i)}})
	}
	if count := kv.GetStoreCount(); count != expectedCount {
		t.Errorf("expected store count to be %d but was %d", expectedCount, count)
	}
}

func TestLocalKVVisitStores(t *testing.T) {
	kv := NewLocalKV()
	numStores := 10
	for i := 0; i < numStores; i++ {
		kv.AddStore(&storage.Store{Ident: proto.StoreIdent{StoreID: int32(i)}})
	}

	visit := make([]bool, numStores)
	err := kv.VisitStores(func(s *storage.Store) error { visit[s.Ident.StoreID] = true; return nil })
	if err != nil {
		t.Errorf("unexpected error on visit: %s", err.Error())
	}

	for i, visited := range visit {
		if !visited {
			t.Errorf("store %d was not visited", i)
		}
	}

	err = kv.VisitStores(func(s *storage.Store) error { return errors.New("") })
	if err == nil {
		t.Errorf("expected visit error")
	}
}

func TestLocalKVGetStore(t *testing.T) {
	kv := NewLocalKV()
	store := storage.Store{}
	replica := proto.Replica{StoreID: store.Ident.StoreID}
	s, err := kv.GetStore(replica.StoreID)
	if s != nil || err == nil {
		t.Errorf("expected no stores in new local kv.")
	}

	kv.AddStore(&store)
	s, err = kv.GetStore(replica.StoreID)
	if s == nil {
		t.Errorf("expected store")
	} else if s.Ident.StoreID != store.Ident.StoreID {
		t.Errorf("expected storeID to be %d but was %d",
			s.Ident.StoreID, store.Ident.StoreID)
	} else if err != nil {
		t.Errorf("expected no error, instead had err=%s", err.Error())
	}
}

func TestLocalKVLookupReplica(t *testing.T) {
	manual := hlc.ManualClock(0)
	clock := hlc.NewClock(manual.UnixNano)
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	kv := NewLocalKV()
	db := NewDB(kv, clock)
	store := storage.NewStore(clock, eng, db, nil)
	if err := store.Bootstrap(proto.StoreIdent{StoreID: 1}); err != nil {
		t.Fatal(err)
	}
	kv.AddStore(store)
	meta := store.BootstrapRangeMetadata()
	meta.StartKey = engine.KeySystemPrefix
	meta.EndKey = engine.KeySystemPrefix.PrefixEnd()
	if _, err := store.CreateRange(meta); err != nil {
		t.Fatal(err)
	}
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	// Create two new stores with ranges we care about.
	var s [2]*storage.Store
	ranges := []struct {
		storeID    int32
		start, end engine.Key
	}{
		{2, engine.Key("a"), engine.Key("c")},
		{3, engine.Key("x"), engine.Key("z")},
	}
	for i, rng := range ranges {
		s[i] = storage.NewStore(clock, eng, db, nil)
		s[i].Ident.StoreID = rng.storeID
		replica := proto.Replica{StoreID: rng.storeID}
		_, err := s[i].CreateRange(store.NewRangeMetadata(rng.start, rng.end, []proto.Replica{replica}))
		if err != nil {
			t.Fatal(err)
		}
		kv.AddStore(s[i])
	}

	if r, err := kv.lookupReplica(engine.Key("a"), engine.Key("c")); r.StoreID != s[0].Ident.StoreID || err != nil {
		t.Errorf("expected store %d; got %d: %v", s[0].Ident.StoreID, r.StoreID, err)
	}
	if r, err := kv.lookupReplica(engine.Key("b"), nil); r.StoreID != s[0].Ident.StoreID || err != nil {
		t.Errorf("expected store %d; got %d: %v", s[0].Ident.StoreID, r.StoreID, err)
	}
	if r, err := kv.lookupReplica(engine.Key("b"), engine.Key("d")); r != nil || err == nil {
		t.Errorf("expected store 0 and error got %d", r.StoreID)
	}
	if r, err := kv.lookupReplica(engine.Key("x"), engine.Key("z")); r.StoreID != s[1].Ident.StoreID {
		t.Errorf("expected store %d; got %d: %v", s[1].Ident.StoreID, r.StoreID, err)
	}
	if r, err := kv.lookupReplica(engine.Key("y"), nil); r.StoreID != s[1].Ident.StoreID || err != nil {
		t.Errorf("expected store %d; got %d: %v", s[1].Ident.StoreID, r.StoreID, err)
	}
}
