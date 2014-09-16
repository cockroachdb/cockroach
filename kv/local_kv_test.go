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
// Author: Matthew O'Connor (matthew.t.oconnor@gmail.com)
// Author: Zach Brock (zbrock@gmail.com)

package kv

import (
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
)

func TestAddStore(t *testing.T) {
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

func TestGetStoreCount(t *testing.T) {
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

func TestVisitStores(t *testing.T) {
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

func TestGetStore(t *testing.T) {
	kv := NewLocalKV()
	store := storage.Store{}
	replica := proto.Replica{StoreID: store.Ident.StoreID}
	s, err := kv.GetStore(&replica)
	if s != nil || err == nil {
		t.Errorf("expected no stores in new local kv.")
	}

	kv.AddStore(&store)
	s, err = kv.GetStore(&replica)
	if s == nil {
		t.Errorf("expected store")
	} else if s.Ident.StoreID != store.Ident.StoreID {
		t.Errorf("expected storeID to be %d but was %d",
			s.Ident.StoreID, store.Ident.StoreID)
	} else if err != nil {
		t.Errorf("expected no error, instead had err=%s", err.Error())
	}
}

func TestReplicaLookup(t *testing.T) {
	kv := NewLocalKV()
	r1 := addTestRange(kv, engine.KeyMin, engine.Key("C"))
	r2 := addTestRange(kv, engine.Key("C"), engine.Key("X"))
	r3 := addTestRange(kv, engine.Key("X"), engine.KeyMax)
	if len(kv.ranges) != 3 {
		t.Errorf("pre-condition failed-expected ranges to be size 3, got %d", len(kv.ranges))
	}

	assertReplicaForRange(t, kv.lookupReplica(engine.KeyMin), r1)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("B")), r1)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("C")), r2)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("M")), r2)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("X")), r3)
	assertReplicaForRange(t, kv.lookupReplica(engine.Key("Z")), r3)
	if kv.lookupReplica(engine.KeyMax) != nil {
		t.Errorf("expected engine.KeyMax to not have an associated Replica")
	}
}

func assertReplicaForRange(t *testing.T, repl *proto.Replica, rng *storage.Range) {
	if repl == nil {
		t.Errorf("no replica returned")
	} else if repl.RangeID != rng.Meta.RangeID {
		t.Errorf("wrong replica returned-expected %+v and %+v to have the same RangeID",
			rng.Meta, repl)
	}
}

func addTestRange(kv *LocalKV, start, end engine.Key) *storage.Range {
	r := storage.Range{}
	rep := proto.Replica{NodeID: 1, StoreID: 1, RangeID: int64(len(kv.ranges) + 1)}
	r.Meta = &proto.RangeMetadata{
		ClusterID:       "some-cluster",
		RangeID:         rep.RangeID,
		RangeDescriptor: proto.RangeDescriptor{StartKey: start, EndKey: end, Replicas: []proto.Replica{rep}},
	}
	kv.ranges = append(kv.ranges, &r)
	return &r
}
