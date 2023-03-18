// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package nodelivenesstest provides a mock implementation of NodeLiveness
// to facilitate testing of upgrade infrastructure.
package upgradecluster

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"time"
)

// NodeLiveness is a testing-only implementation of the NodeLiveness. It
// lets tests mock out restarting, killing, decommissioning and adding Nodes to
// the cluster.
type NodeLivenessMap struct {
	ls map[roachpb.NodeID]*livenesspb.NodeStatusEntry
}

// New constructs a new NodeLiveness with the specified number of nodes.
func NewLivenessMap(numNodes int) *NodeLivenessMap {
	nl := &NodeLivenessMap{
		ls: make(map[roachpb.NodeID]*livenesspb.NodeStatusEntry),
	}
	for i := 1; i <= numNodes; i++ {
		nodeId := roachpb.NodeID(i)
		nse :=
			livenesspb.Liveness{
				NodeID:     roachpb.NodeID(i),
				Epoch:      0,
				Expiration: hlc.LegacyTimestamp{},
				Draining:   false,
				Membership: livenesspb.MembershipStatus_ACTIVE,
			}.CreateNodeStatusEntry(timeutil.Now(), time.Hour)
		nl.ls[nodeId] = &nse
	}
	return nl
}

func (t *NodeLivenessMap) NotDecommissionedList() []roachpb.NodeID {
	var nl []roachpb.NodeID
	for id, entry := range t.ls {
		if entry.IsAvailableNotDraining() {
			nl = append(nl, id)
		}
	}
	return nl
}

func (t *NodeLivenessMap) GetMutableNodeStatus(id roachpb.NodeID) *livenesspb.NodeStatusEntry {
	return t.ls[id]
}

func (t *NodeLivenessMap) GetNodeStatus(id roachpb.NodeID) livenesspb.NodeStatusEntry {
	return *t.ls[id]
}

// AddNode adds a new node with the specified ID.
func (t *NodeLivenessMap) AddNode(id roachpb.NodeID) {
	nse := livenesspb.Liveness{
		NodeID:     id,
		Epoch:      1,
		Expiration: hlc.LegacyTimestamp{WallTime: 1},
		Draining:   false,
		Membership: livenesspb.MembershipStatus_ACTIVE,
	}.CreateNodeStatusEntry(time.Time{}, time.Hour)
	t.ls[id] = &nse
}
