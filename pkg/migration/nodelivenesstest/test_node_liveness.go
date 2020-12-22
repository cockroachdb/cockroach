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
// to facilitate testing of migration infrastructure.
package nodelivenesstest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// NodeLiveness is a testing-only implementation of the NodeLiveness. It
// lets tests mock out restarting, killing, decommissioning and adding Nodes to
// the cluster.
type NodeLiveness struct {
	ls   []livenesspb.Liveness
	dead map[roachpb.NodeID]struct{}
}

// New constructs a new NodeLiveness with the specified number of nodes.
func New(numNodes int) *NodeLiveness {
	nl := &NodeLiveness{
		ls:   make([]livenesspb.Liveness, numNodes),
		dead: make(map[roachpb.NodeID]struct{}),
	}
	for i := 0; i < numNodes; i++ {
		nl.ls[i] = livenesspb.Liveness{
			NodeID: roachpb.NodeID(i + 1), Epoch: 1,
			Membership: livenesspb.MembershipStatus_ACTIVE,
		}
	}
	return nl
}

// GetLivenessesFromKV implements the NodeLiveness interface.
func (t *NodeLiveness) GetLivenessesFromKV(context.Context) ([]livenesspb.Liveness, error) {
	return t.ls, nil
}

// IsLive implements the NodeLiveness interface.
func (t *NodeLiveness) IsLive(id roachpb.NodeID) (bool, error) {
	_, dead := t.dead[id]
	return !dead, nil
}

// Decommission marks the specified node as decommissioned.
func (t *NodeLiveness) Decommission(id roachpb.NodeID) {
	for i := range t.ls {
		if t.ls[i].NodeID == id {
			t.ls[i].Membership = livenesspb.MembershipStatus_DECOMMISSIONED
			break
		}
	}
}

// AddNewNode adds a new node with an ID greater than all other nodes.
func (t *NodeLiveness) AddNewNode() {
	t.AddNode(roachpb.NodeID(len(t.ls) + 1))
}

// AddNode adds a new node with the specified ID.
func (t *NodeLiveness) AddNode(id roachpb.NodeID) {
	t.ls = append(t.ls, livenesspb.Liveness{
		NodeID:     id,
		Epoch:      1,
		Membership: livenesspb.MembershipStatus_ACTIVE,
	})
}

// DownNode marks a given node as down.
func (t *NodeLiveness) DownNode(id roachpb.NodeID) {
	t.dead[id] = struct{}{}
}

// RestartNode increments the epoch for a given node and marks it as
// alive.
func (t *NodeLiveness) RestartNode(id roachpb.NodeID) {
	for i := range t.ls {
		if t.ls[i].NodeID == id {
			t.ls[i].Epoch++
			break
		}
	}

	delete(t.dead, id)
}
