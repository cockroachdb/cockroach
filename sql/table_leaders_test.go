// Copyright 2015 The Cockroach Authors.
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
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql

import (
	"testing"

	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

func TestTableLeaders(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	ctx := storage.TestStoreContext
	manualClock := hlc.NewManualClock(0)
	ctx.Clock = hlc.NewClock(manualClock.UnixNano)
	stores := storage.NewStores()

	// Create two new stores with ranges we care about.
	var e [2]engine.Engine
	var s [2]*storage.Store
	var d [2]*roachpb.RangeDescriptor
	ranges := []struct {
		storeID    roachpb.StoreID
		start, end roachpb.RKey
	}{
		{2, roachpb.RKeyMin, MakeIndexKeyPrefix(1000, 1)},
		{3, MakeIndexKeyPrefix(1002, 1), MakeIndexKeyPrefix(1005, 1)},
	}
	for i, rng := range ranges {
		e[i] = engine.NewInMem(roachpb.Attributes{}, 1<<20, stopper)
		ctx.Transport = multiraft.NewLocalRPCTransport(stopper)
		defer ctx.Transport.Close()
		s[i] = storage.NewStore(ctx, e[i], &roachpb.NodeDescriptor{NodeID: 1})
		s[i].Ident.StoreID = rng.storeID

		d[i] = &roachpb.RangeDescriptor{
			RangeID:  roachpb.RangeID(i),
			StartKey: rng.start,
			EndKey:   rng.end,
			Replicas: []roachpb.ReplicaDescriptor{{StoreID: rng.storeID}},
		}
		newRng, err := storage.NewReplica(d[i], s[i])
		if err != nil {
			t.Fatal(err)
		}
		if err := s[i].AddReplicaTest(newRng); err != nil {
			t.Error(err)
		}
		stores.AddStore(s[i])
	}

	tl := newTableLeaders(stores, stopper)
	report := make(chan tableLeaderNotification, 10)

	testIsReplicaLeader = true
	// Register a table for which this node doesn't have a replica.
	tl.register(1001, 1, report)
	// Register a table for which this node has a replica.
	tl.register(1003, 1, report)
	n := <-report
	if !n.isLeader || n.tableID != 1003 {
		t.Fatalf("is not leader notification: %v", n)
	}
	testIsReplicaLeader = false
	n = <-report
	if n.isLeader || n.tableID != 1003 {
		t.Fatalf("is leader notification: %v", n)
	}
	tl.unregister(1001)
	tl.unregister(1003)
	if !tl.done() {
		t.Fatal("there are still some registered tables")
	}
}
