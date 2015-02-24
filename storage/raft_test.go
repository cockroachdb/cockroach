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

func internalChangeReplicasArgs(newNodeID proto.NodeID, newStoreID proto.StoreID,
	changeType proto.ReplicaChangeType, raftID int64, storeID proto.StoreID) (
	*proto.InternalChangeReplicasRequest, *proto.InternalChangeReplicasResponse) {
	args := &proto.InternalChangeReplicasRequest{
		RequestHeader: proto.RequestHeader{
			RaftID:  raftID,
			Replica: proto.Replica{StoreID: storeID},
		},
		NodeID:     newNodeID,
		StoreID:    newStoreID,
		ChangeType: changeType,
	}
	reply := &proto.InternalChangeReplicasResponse{}
	return args, reply
}

func TestReplicateRange(t *testing.T) {
	tc := testContext{}
	tc.Start(t)
	defer tc.Stop()

	const (
		rangeID  = 1
		nodeID1  = 1
		storeID1 = 1
		nodeID2  = 2
		storeID2 = 3
	)

	if tc.store.Ident.NodeID != nodeID1 || tc.store.Ident.StoreID != storeID1 {
		t.Fatalf("unexpected store ident %+v", tc.store.Ident)
	}

	// Start a second store on the same transport, but don't create any ranges.
	// We use testSenders instead of real clients so we can address the two stores
	// separately.
	engine2 := engine.NewInMem(proto.Attributes{}, 1<<20)
	store2 := NewStore(tc.clock, engine2, nil, tc.gossip, tc.transport)
	err := store2.Bootstrap(proto.StoreIdent{NodeID: nodeID2, StoreID: storeID2})
	if err != nil {
		t.Fatal(err)
	}
	store2.db = client.NewKV(&testSender{store: store2}, nil)
	if err := store2.Start(); err != nil {
		t.Fatal(err)
	}
	defer store2.Stop()

	// Issue a command on the first node before replicating.
	incArgs, incResp := incrementArgs([]byte("a"), 5, rangeID, tc.store.StoreID())
	if err := tc.rng.AddCmd(proto.Increment, incArgs, incResp, true); err != nil {
		t.Fatal(err)
	}

	args, resp := internalChangeReplicasArgs(nodeID2, storeID2, proto.ADD_REPLICA,
		rangeID, tc.store.StoreID())
	if err := tc.rng.AddCmd(proto.InternalChangeReplicas, args, resp, true); err != nil {
		t.Fatal(err)
	}

	// Verify that the same data is available on the replica.
	if err := util.IsTrueWithin(func() bool {
		getArgs, getResp := getArgs([]byte("a"), rangeID, store2.StoreID())
		if err := store2.ExecuteCmd(proto.Get, getArgs, getResp); err != nil {
			return false
		}
		return getResp.Value.GetInteger() == 5
	}, 1*time.Second); err != nil {
		t.Fatal(err)
	}
}
