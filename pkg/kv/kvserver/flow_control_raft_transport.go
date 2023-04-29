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
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
)

// raftTransportForFlowControl abstracts the node-level raft transport, and is
// used by the canonical replicaFlowControlIntegration implementation. It
// exposes the set of (remote) stores the raft transport is connected to. If the
// underlying gRPC streams break and don't reconnect, this indicates as much.
// Ditto if they're reconnected to. Also see RaftTransportDisconnectListener,
// which is used to observe every instance of gRPC streams breaking.
type raftTransportForFlowControl interface {
	isConnectedTo(storeID roachpb.StoreID) bool
}

var _ raftTransportForFlowControl = &RaftTransport{}

// isConnected implements the raftTransportForFlowControl interface.
func (r *RaftTransport) isConnectedTo(storeID roachpb.StoreID) bool {
	r.kvflowControl.mu.Lock()
	defer r.kvflowControl.mu.Unlock()
	return r.kvflowControl.mu.connectionTracker.isStoreConnected(storeID)
}

// RaftTransportDisconnectListener observes every instance of the raft
// transport disconnecting replication traffic to the given (remote) stores.
type RaftTransportDisconnectListener interface {
	OnRaftTransportDisconnected(context.Context, ...roachpb.StoreID)
}

// connectionTrackerForFlowControl tracks the set of client-side stores and
// server-side nodes the raft transport is connected to.
type connectionTrackerForFlowControl struct {
	stores map[roachpb.StoreID]struct{}
	nodes  map[roachpb.NodeID]map[rpc.ConnectionClass]struct{}
}

func newConnectionTrackerForFlowControl() *connectionTrackerForFlowControl {
	return &connectionTrackerForFlowControl{
		stores: make(map[roachpb.StoreID]struct{}),
		nodes:  make(map[roachpb.NodeID]map[rpc.ConnectionClass]struct{}),
	}
}

// isStoreConnected returns whether we're connected to the given store.
func (c *connectionTrackerForFlowControl) isStoreConnected(storeID roachpb.StoreID) bool {
	_, found := c.stores[storeID]
	return found
}

// markStoresConnected is used to inform the tracker that we've received
// raft messages from nodes with the given set of stores.
func (c *connectionTrackerForFlowControl) markStoresConnected(storeIDs []roachpb.StoreID) {
	for _, storeID := range storeIDs {
		c.stores[storeID] = struct{}{}
	}
}

// markStoresDisconnected marks the given set of stores as disconnected.
func (c *connectionTrackerForFlowControl) markStoresDisconnected(storeIDs []roachpb.StoreID) {
	for _, storeID := range storeIDs {
		delete(c.stores, storeID)
	}
}

// isNodeConnected returns whether we're connected to the given node,
// independent of the specific RPC connection class.
func (q *connectionTrackerForFlowControl) isNodeConnected(nodeID roachpb.NodeID) bool {
	_, found := q.nodes[nodeID]
	return found
}

// markNodeConnected informs the tracker that we're connected to the given
// node using the given RPC connection class.
func (q *connectionTrackerForFlowControl) markNodeConnected(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) {
	if len(q.nodes[nodeID]) == 0 {
		q.nodes[nodeID] = map[rpc.ConnectionClass]struct{}{}
	}
	q.nodes[nodeID][class] = struct{}{}
}

// markNodeDisconnected informs the tracker that a previous connection to
// the given node along the given RPC connection class is now broken.
func (q *connectionTrackerForFlowControl) markNodeDisconnected(
	nodeID roachpb.NodeID, class rpc.ConnectionClass,
) {
	delete(q.nodes[nodeID], class)
	if len(q.nodes[nodeID]) == 0 {
		delete(q.nodes, nodeID)
	}
}

func (c *connectionTrackerForFlowControl) testingPrint() string {
	var storeIDs []roachpb.StoreID
	var nodeIDs []roachpb.NodeID
	for storeID := range c.stores {
		storeIDs = append(storeIDs, storeID)
	}
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}

	var buf strings.Builder
	sort.Sort(roachpb.StoreIDSlice(storeIDs))
	sort.Sort(roachpb.NodeIDSlice(nodeIDs))
	if len(storeIDs) > 0 {
		buf.WriteString(fmt.Sprintf("connected-stores: %s\n", roachpb.StoreIDSlice(storeIDs)))
	}
	if len(nodeIDs) > 0 {
		buf.WriteString(fmt.Sprintf("connected-nodes: %s\n", roachpb.NodeIDSlice(nodeIDs)))
	}
	return buf.String()
}

// NoopRaftTransportDisconnectListener is a no-op implementation of the
// RaftTransportDisconnectListener interface.
type NoopRaftTransportDisconnectListener struct{}

var _ RaftTransportDisconnectListener = NoopRaftTransportDisconnectListener{}

// OnRaftTransportDisconnected implements the RaftTransportDisconnectListener
// interface.
func (n NoopRaftTransportDisconnectListener) OnRaftTransportDisconnected(
	ctx context.Context, storeIDs ...roachpb.StoreID,
) {
}
