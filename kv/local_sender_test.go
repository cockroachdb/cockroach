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

	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestLocalSenderAddStore(t *testing.T) {
	defer leaktest.AfterTest(t)
	ls := NewLocalSender()
	store := storage.Store{}
	ls.AddStore(&store)
	if !ls.HasStore(store.Ident.StoreID) {
		t.Errorf("expected local sender to contain storeID=%d", store.Ident.StoreID)
	}
	if ls.HasStore(store.Ident.StoreID + 1) {
		t.Errorf("expected local sender to not contain storeID=%d", store.Ident.StoreID+1)
	}
}

func TestLocalSenderGetStoreCount(t *testing.T) {
	defer leaktest.AfterTest(t)
	ls := NewLocalSender()
	if ls.GetStoreCount() != 0 {
		t.Errorf("expected 0 stores in new local sender")
	}

	expectedCount := 10
	for i := 0; i < expectedCount; i++ {
		ls.AddStore(&storage.Store{Ident: proto.StoreIdent{StoreID: proto.StoreID(i)}})
	}
	if count := ls.GetStoreCount(); count != expectedCount {
		t.Errorf("expected store count to be %d but was %d", expectedCount, count)
	}
}

func TestLocalSenderVisitStores(t *testing.T) {
	defer leaktest.AfterTest(t)
	ls := NewLocalSender()
	numStores := 10
	for i := 0; i < numStores; i++ {
		ls.AddStore(&storage.Store{Ident: proto.StoreIdent{StoreID: proto.StoreID(i)}})
	}

	visit := make([]bool, numStores)
	err := ls.VisitStores(func(s *storage.Store) error { visit[s.Ident.StoreID] = true; return nil })
	if err != nil {
		t.Errorf("unexpected error on visit: %s", err.Error())
	}

	for i, visited := range visit {
		if !visited {
			t.Errorf("store %d was not visited", i)
		}
	}

	err = ls.VisitStores(func(s *storage.Store) error { return errors.New("") })
	if err == nil {
		t.Errorf("expected visit error")
	}
}

func TestLocalSenderGetStore(t *testing.T) {
	defer leaktest.AfterTest(t)
	ls := NewLocalSender()
	store := storage.Store{}
	replica := proto.Replica{StoreID: store.Ident.StoreID}
	s, err := ls.GetStore(replica.StoreID)
	if s != nil || err == nil {
		t.Errorf("expected no stores in new local sender")
	}

	ls.AddStore(&store)
	s, err = ls.GetStore(replica.StoreID)
	if s == nil {
		t.Errorf("expected store")
	} else if s.Ident.StoreID != store.Ident.StoreID {
		t.Errorf("expected storeID to be %d but was %d",
			s.Ident.StoreID, store.Ident.StoreID)
	} else if err != nil {
		t.Errorf("expected no error, instead had err=%s", err.Error())
	}
}

func TestLocalSenderLookupReplica(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	ctx := storage.TestStoreContext
	manualClock := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manualClock.UnixNano)
	ls := NewLocalSender()

	// Create two new stores with ranges we care about.
	var e [2]engine.Engine
	var s [2]*storage.Store
	ranges := []struct {
		storeID    proto.StoreID
		start, end proto.Key
	}{
		{2, proto.Key("a"), proto.Key("c")},
		{3, proto.Key("x"), proto.Key("z")},
	}
	for i, rng := range ranges {
		e[i] = engine.NewInMem(proto.Attributes{}, 1<<20)
		ctx.Transport = multiraft.NewLocalRPCTransport(stopper)
		defer ctx.Transport.Close()
		s[i] = storage.NewStore(ctx, e[i], &proto.NodeDescriptor{NodeID: 1})
		s[i].Ident.StoreID = rng.storeID

		desc := &proto.RangeDescriptor{
			RangeID:  proto.RangeID(i),
			StartKey: rng.start,
			EndKey:   rng.end,
			Replicas: []proto.Replica{{StoreID: rng.storeID}},
		}
		newRng, err := storage.NewReplica(desc, s[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := s[i].AddReplicaTest(newRng); err != nil {
			t.Error(err)
		}
		ls.AddStore(s[i])
	}

	if _, r, err := ls.lookupReplica(proto.Key("a"), proto.Key("c")); r.StoreID != s[0].Ident.StoreID || err != nil {
		t.Errorf("expected store %d; got %d: %v", s[0].Ident.StoreID, r.StoreID, err)
	}
	if _, r, err := ls.lookupReplica(proto.Key("b"), nil); r.StoreID != s[0].Ident.StoreID || err != nil {
		t.Errorf("expected store %d; got %d: %v", s[0].Ident.StoreID, r.StoreID, err)
	}
	if _, r, err := ls.lookupReplica(proto.Key("b"), proto.Key("d")); r != nil || err == nil {
		t.Errorf("expected store 0 and error got %d", r.StoreID)
	}
	if _, r, err := ls.lookupReplica(proto.Key("x"), proto.Key("z")); r.StoreID != s[1].Ident.StoreID {
		t.Errorf("expected store %d; got %d: %v", s[1].Ident.StoreID, r.StoreID, err)
	}
	if _, r, err := ls.lookupReplica(proto.Key("y"), nil); r.StoreID != s[1].Ident.StoreID || err != nil {
		t.Errorf("expected store %d; got %d: %v", s[1].Ident.StoreID, r.StoreID, err)
	}
}
