// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
)

var _ raftTransportForFlowControl = &RaftTransport{}

// isConnectedTo implements the raftTransportForFlowControl interface.
func (r *RaftTransport) isConnectedTo(storeID roachpb.StoreID) bool {
	r.kvflowControl.mu.RLock()
	defer r.kvflowControl.mu.RUnlock()
	return r.kvflowControl.mu.connectionTracker.isStoreConnected(storeID)
}

// connectionTrackerForFlowControl tracks the set of client-side stores and
// server-side nodes the raft transport is connected to. The "client" and
// "server" refer to the client and server side of the RaftTransport stream (see
// flow_control_integration.go). Client-side stores return tokens, it's where
// work gets admitted. Server-side nodes are where tokens were originally
// deducted.
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

// isStoreConnected returns whether we're connected to the given (client-side)
// store.
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

// isNodeConnected returns whether we're connected to the given (server-side)
// node, independent of the specific RPC connection class.
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
	storeIDs := make([]roachpb.StoreID, 0, len(c.stores))
	nodeIDs := make([]roachpb.NodeID, 0, len(c.nodes))
	for storeID := range c.stores {
		storeIDs = append(storeIDs, storeID)
	}
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}

	var buf strings.Builder
	slices.Sort(storeIDs)
	slices.Sort(nodeIDs)
	buf.WriteString(fmt.Sprintf("connected-stores (server POV): %s\n", roachpb.StoreIDSlice(storeIDs)))
	buf.WriteString(fmt.Sprintf("connected-nodes  (client POV): %s\n", roachpb.NodeIDSlice(nodeIDs)))
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
