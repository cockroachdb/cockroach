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
// Author: Ben Darnell

package storage

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

type multiTestContext struct {
	testContext
	engines []engine.Engine
	stores  []*Store
}

func (m *multiTestContext) Start(t *testing.T, numStores int) {
	m.testContext.Start(t)
	m.engines = []engine.Engine{m.engine}
	m.stores = []*Store{m.store}

	for i := 1; i < numStores; i++ {
		m.addStore(t)
	}
}

func (m *multiTestContext) Stop() {
	// store 0 will be closed by testContext.Stop.
	for _, store := range m.stores[1:] {
		store.Stop()
	}
	m.testContext.Stop()
}

// AddStore creates a new store on the same Transport but doesn't create any ranges.
func (m *multiTestContext) addStore(t *testing.T) {
	eng := engine.NewInMem(proto.Attributes{}, 1<<20)
	store := NewStore(m.clock, eng, nil, m.gossip, m.transport)
	err := store.Bootstrap(proto.StoreIdent{
		NodeID:  proto.NodeID(len(m.stores) + 1),
		StoreID: proto.StoreID(len(m.stores) + 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	// We use testSenders instead of real clients so we can address the two stores
	// separately.
	store.db = client.NewKV(&testSender{store: store}, nil)
	if err := store.Start(); err != nil {
		t.Fatal(err)
	}
	m.engines = append(m.engines, eng)
	m.stores = append(m.stores, store)
}

// StopStore stops a store but leaves the engine intact.
// All stopped stores must be restarted before multiTestContext.Stop is called.
func (m *multiTestContext) StopStore(i int) {
	m.stores[i].Stop()
	m.stores[i] = nil
	if i == 0 {
		m.store = nil
	}
}

// RetartStore restarts a store previously stopped with StopStore.
func (m *multiTestContext) RestartStore(i int, t *testing.T) {
	m.stores[i] = NewStore(m.clock, m.engines[i], nil, m.gossip, m.transport)
	m.stores[i].db = client.NewKV(&testSender{store: m.stores[i]}, nil)
	if err := m.stores[i].Start(); err != nil {
		t.Fatal(err)
	}
	if i == 0 {
		m.store = m.stores[i]
	}
}

func internalChangeReplicasArgs(newIdent proto.StoreIdent,
	changeType proto.ReplicaChangeType, raftID int64, storeID proto.StoreID) (
	*proto.InternalChangeReplicasRequest, *proto.InternalChangeReplicasResponse) {
	args := &proto.InternalChangeReplicasRequest{
		RequestHeader: proto.RequestHeader{
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		NodeID:     newIdent.NodeID,
		StoreID:    newIdent.StoreID,
		ChangeType: changeType,
	}
	reply := &proto.InternalChangeReplicasResponse{}
	return args, reply
}

// TestReplicateRange verifies basic replication functionality by creating two stores
// and a range, replicating the range to the second store, and reading its data there.
func TestReplicateRange(t *testing.T) {
	mtc := multiTestContext{}
	mtc.Start(t, 2)
	defer mtc.Stop()

	// Issue a command on the first node before replicating.
	incArgs, incResp := incrementArgs([]byte("a"), 5, mtc.rangeID, mtc.store.StoreID())
	if err := mtc.rng.AddCmd(proto.Increment, incArgs, incResp, true); err != nil {
		t.Fatal(err)
	}

	args, resp := internalChangeReplicasArgs(mtc.stores[1].Ident, proto.ADD_REPLICA,
		mtc.rangeID, mtc.store.StoreID())
	if err := mtc.rng.AddCmd(proto.InternalChangeReplicas, args, resp, true); err != nil {
		t.Fatal(err)
	}

	// Verify that the same data is available on the replica.
	// TODO(bdarnell): relies on the fact that we allow reads from followers.
	// When we enforce reads from leader/quorum leases, we'll need to introduce a
	// non-transactional local read for tests like this.
	// Also applies to other tests in this file.
	if err := util.IsTrueWithin(func() bool {
		getArgs, getResp := getArgs([]byte("a"), mtc.rangeID, mtc.stores[1].StoreID())
		if err := mtc.stores[1].ExecuteCmd(proto.Get, getArgs, getResp); err != nil {
			return false
		}
		return getResp.Value.GetInteger() == 5
	}, 1*time.Second); err != nil {
		t.Fatal(err)
	}
}

// TestRestoreReplicas ensures that consensus group membership is properly
// persisted to disk and restored when a node is stopped and restarted.
func TestRestoreReplicas(t *testing.T) {
	mtc := multiTestContext{}
	mtc.Start(t, 2)
	defer mtc.Stop()

	args, resp := internalChangeReplicasArgs(mtc.stores[1].Ident, proto.ADD_REPLICA,
		mtc.rangeID, mtc.store.StoreID())
	if err := mtc.stores[0].ExecuteCmd(proto.InternalChangeReplicas, args, resp); err != nil {
		t.Fatal(err)
	}

	// Let the range propagate to the second replica.
	// TODO(bdarnell): we need an observable indication that this has completed.
	// TODO(bdarnell): initial creation and replication needs to be atomic;
	// cutting off the process too soon currently results in a corrupted range.
	// We need 500ms to be safe in -race tests.
	time.Sleep(500 * time.Millisecond)

	mtc.StopStore(0)
	mtc.StopStore(1)
	mtc.RestartStore(0, t)
	mtc.RestartStore(1, t)

	// Send a command on each store. The follower will forward to the leader and both
	// commands will eventually commit.
	incArgs, incResp := incrementArgs([]byte("a"), 5, mtc.rangeID, mtc.stores[0].StoreID())
	if err := mtc.stores[0].ExecuteCmd(proto.Increment, incArgs, incResp); err != nil {
		t.Fatal(err)
	}
	incArgs, incResp = incrementArgs([]byte("a"), 11, mtc.rangeID, mtc.stores[1].StoreID())
	if err := mtc.stores[1].ExecuteCmd(proto.Increment, incArgs, incResp); err != nil {
		t.Fatal(err)
	}

	if err := util.IsTrueWithin(func() bool {
		getArgs, getResp := getArgs([]byte("a"), mtc.rangeID, mtc.stores[1].StoreID())
		if err := mtc.stores[1].ExecuteCmd(proto.Get, getArgs, getResp); err != nil {
			return false
		}
		return getResp.Value.GetInteger() == 16
	}, 1*time.Second); err != nil {
		t.Fatal(err)
	}

	// Both replicas have a complete list in Desc.Replicas
	for i, store := range mtc.stores {
		rng, err := store.GetRange(mtc.rangeID)
		if err != nil {
			t.Fatal(err)
		}
		rng.RLock()
		if len(rng.Desc.Replicas) != 2 {
			t.Fatalf("store %d: expected 2 replicas, found %d", i, len(rng.Desc.Replicas))
		}
		if rng.Desc.Replicas[0].NodeID != mtc.stores[0].Ident.NodeID {
			t.Errorf("store %d: expected replica[0].NodeID == %d, was %d",
				i, mtc.stores[0].Ident.NodeID, rng.Desc.Replicas[0].NodeID)
		}
		rng.RUnlock()
	}
}
