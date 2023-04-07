// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
)

// RaftTransportDisconnectedListener observes every instance of the raft
// transport disconnecting replication traffic to a given (remote) store.
type RaftTransportDisconnectedListener interface {
	OnRaftTransportDisconnected(context.Context, ...roachpb.StoreID)
}

// raftTransportForFlowControl abstracts the node-level raft transport, and is
// used by the canonical replicaFlowControlIntegration implementation. It
// exposes the set of (remote) stores the raft transport is connected to. If the
// underlying gRPC streams break, they
type raftTransportForFlowControl interface {
	IsConnected(storeID roachpb.StoreID) bool
}

// RaftTransportConnectedStores tracks the set of (remote) stores the raft
// transport is connected to.
type RaftTransportConnectedStores interface {
	// MarkConnected is used to inform the tracker that we've received raft
	// messages at the given time from nodes with the given set of stores.
	MarkConnected(storeIDs []roachpb.StoreID, now time.Time)
	// IsConnected returns whether we're connected to the given store.
	IsConnected(storeID roachpb.StoreID) bool
	// ShouldDisconnect returns the set of stores we've not heard from in a
	// given duration.
	ShouldDisconnect(now time.Time, exp time.Duration) []roachpb.StoreID
	// MarkDisconnected marks the given set of stores as disconnected.
	MarkDisconnected(storeIDs []roachpb.StoreID)
}

// RaftTransportConnectedNodes tracks the set of nodes the raft transport is
// connected to.
type RaftTransportConnectedNodes interface {
	// MarkConnected informs the tracker that we're connected to the given node
	// using the given RPC connection class.
	MarkConnected(nodeID roachpb.NodeID, class rpc.ConnectionClass)
	// MarkDisconnected informs the tracker that a previous connection to the
	// given node along the given RPC connection class is now broken.
	MarkDisconnected(nodeID roachpb.NodeID, class rpc.ConnectionClass)
	// IsConnected returns whether we're connected to the given node,
	// independent of the specific RPC connection class.
	IsConnected(nodeID roachpb.NodeID) bool
}

type raftTransportFlowControl RaftTransport

var _ raftTransportForFlowControl = &raftTransportFlowControl{}

// IsConnected implements the raftTransportForFlowControl interface.
func (rf *raftTransportFlowControl) IsConnected(storeID roachpb.StoreID) bool {
	r := (*RaftTransport)(rf)
	rf.kvflowControl.mu.Lock()
	defer rf.kvflowControl.mu.Unlock()
	return r.kvflowControl.mu.connectedStores.IsConnected(storeID)
}

type raftTransportConnectedStores struct {
	m map[roachpb.StoreID]time.Time
}

var _ raftTransportForFlowControl = &raftTransportConnectedStores{}
var _ RaftTransportConnectedStores = &raftTransportConnectedStores{}

// NewRaftTransportConnectedStores returns the canonical implementation of the
// RaftTransportConnectedStores interface.
func NewRaftTransportConnectedStores() RaftTransportConnectedStores {
	return &raftTransportConnectedStores{
		m: make(map[roachpb.StoreID]time.Time),
	}
}

// IsConnected implements the raftTransportForFlowControl interface.
func (c *raftTransportConnectedStores) IsConnected(storeID roachpb.StoreID) bool {
	_, found := c.m[storeID]
	return found
}

// MarkConnected implements the RaftTransportConnectedStores interface.
func (c *raftTransportConnectedStores) MarkConnected(storeIDs []roachpb.StoreID, now time.Time) {
	for _, storeID := range storeIDs {
		c.m[storeID] = now
	}
}

// ShouldDisconnect implements the RaftTransportConnectedStores interface.
func (c *raftTransportConnectedStores) ShouldDisconnect(
	now time.Time, exp time.Duration,
) []roachpb.StoreID {
	var storeIDs []roachpb.StoreID
	for storeID, lastConnected := range c.m {
		if now.Sub(lastConnected) > exp {
			storeIDs = append(storeIDs, storeID)
		}
	}
	return storeIDs
}

// MarkDisconnected implements the RaftTransportConnectedStores interface.
func (c *raftTransportConnectedStores) MarkDisconnected(storeIDs []roachpb.StoreID) {
	for _, storeID := range storeIDs {
		delete(c.m, storeID)
	}
}

type raftTransportConnectedNodes struct {
	m map[roachpb.NodeID]map[rpc.ConnectionClass]struct{}
}

var _ RaftTransportConnectedNodes = &raftTransportConnectedNodes{}

// NewRaftTransportConnectedNodes returns the canonical implementation of the
// RaftTransportConnectedNodes interface.
func NewRaftTransportConnectedNodes() RaftTransportConnectedNodes {
	return &raftTransportConnectedNodes{
		m: make(map[roachpb.NodeID]map[rpc.ConnectionClass]struct{}),
	}
}

// MarkConnected implements the RaftTransportConnectedNodes interface.
func (q *raftTransportConnectedNodes) MarkConnected(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) {
	if len(q.m[nodeID]) == 0 {
		q.m[nodeID] = map[rpc.ConnectionClass]struct{}{}
	}
	q.m[nodeID][class] = struct{}{}
}

// MarkDisconnected implements the RaftTransportConnectedNodes interface.
func (q *raftTransportConnectedNodes) MarkDisconnected(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) {
	delete(q.m[nodeID], class)
	if len(q.m[nodeID]) == 0 {
		delete(q.m, nodeID)
	}
}

// IsConnected implements the RaftTransportConnectedNodes interface.
func (q *raftTransportConnectedNodes) IsConnected(nodeID roachpb.NodeID) bool {
	_, found := q.m[nodeID]
	return found
}

// NoopRaftTransportConnectedStores is a no-op implementation of the
// RaftTransportConnectedStores interface.
type NoopRaftTransportConnectedStores struct{}

var _ RaftTransportConnectedStores = NoopRaftTransportConnectedStores{}

// MarkConnected implements the RaftTransportConnectedStores interface.
func (NoopRaftTransportConnectedStores) MarkConnected(storeIDs []roachpb.StoreID, now time.Time) {
}

// IsConnected implements the RaftTransportConnectedStores interface.
func (NoopRaftTransportConnectedStores) IsConnected(storeID roachpb.StoreID) bool {
	return true
}

// ShouldDisconnect implements the RaftTransportConnectedStores interface.
func (NoopRaftTransportConnectedStores) ShouldDisconnect(
	now time.Time, exp time.Duration,
) []roachpb.StoreID {
	return nil
}

// MarkDisconnected implements the RaftTransportConnectedStores interface.
func (NoopRaftTransportConnectedStores) MarkDisconnected(storeIDs []roachpb.StoreID) {
}

// NoopRaftTransportConnectedNodes is a no-op implementation of the
// RaftTransportConnectedNodes interface.
type NoopRaftTransportConnectedNodes struct{}

var _ RaftTransportConnectedNodes = NoopRaftTransportConnectedNodes{}

// MarkConnected implements the RaftTransportConnectedNodes interface.
func (NoopRaftTransportConnectedNodes) MarkConnected(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) {
}

// MarkDisconnected implements the RaftTransportConnectedNodes interface.
func (NoopRaftTransportConnectedNodes) MarkDisconnected(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) {
}

// IsConnected implements the RaftTransportConnectedNodes interface.
func (NoopRaftTransportConnectedNodes) IsConnected(nodeID roachpb.NodeID) bool {
	return true
}
