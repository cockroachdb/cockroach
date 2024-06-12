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
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

// RACv2

// AdmittedPiggybackStateManager manages the state for piggybacking
// MsgAppResps that specifically try to advance Admitted.
type AdmittedPiggybackStateManager interface {
	// AddMsgForRange is called with a MsgAppResp that should be piggybacked on
	// RaftMessageRequests flowing to leaderNodeID. The leaderStoreID and
	// rangeID are for demultiplexing once the message reached the leaderNodeID.
	//
	// m must be a MsgAppResp.
	//
	// Keeps for each rangeID the latest m. There shouldn't be multiple replicas
	// for a range at this node, so that is sufficient. Also, keys these by leaderNodeID,
	// so can be grabbed to piggyback to that range.
	AddMsgForRange(
		rangeID roachpb.RangeID, leaderNodeID roachpb.NodeID, leaderStoreID roachpb.StoreID, m raftpb.Message)
	// PopMsgsForNode is used by RaftTransport to grab messages to piggyback
	// when it is sending a message to nodeID.
	PopMsgsForNode(nodeID roachpb.NodeID, maxBytes int64) (msgs []kvflowcontrolpb.AdmittedForRangeRACv2, remainingMsgs int)
	// NodesWithMsgs is used to periodically drop msgs from disconnected nodes.
	// See RaftTransport.dropFlowTokensForDisconnectedNodes.
	NodesWithMsgs() []roachpb.NodeID
}

func NewAdmittedPiggybackStateManager() AdmittedPiggybackStateManager {
	return &admittedPiggybackStateManager{}
}

type admittedPiggybackStateManager struct {
	mu struct {
		syncutil.Mutex
		msgsForNode map[roachpb.NodeID]map[roachpb.RangeID]kvflowcontrolpb.AdmittedForRangeRACv2
	}
}

func (ap *admittedPiggybackStateManager) AddMsgForRange(
	rangeID roachpb.RangeID,
	leaderNodeID roachpb.NodeID,
	leaderStoreID roachpb.StoreID,
	m raftpb.Message,
) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	perRangeMap, ok := ap.mu.msgsForNode[leaderNodeID]
	if !ok {
		perRangeMap = map[roachpb.RangeID]kvflowcontrolpb.AdmittedForRangeRACv2{}
		ap.mu.msgsForNode[leaderNodeID] = perRangeMap
	}
	perRangeMap[rangeID] = kvflowcontrolpb.AdmittedForRangeRACv2{
		LeaderStoreID: leaderStoreID,
		RangeID:       rangeID,
		Msg:           m,
	}
}

// Made-up number.
const admittedForRangeRACv2SizeBytes = 60

func (ap *admittedPiggybackStateManager) PopMsgsForNode(
	nodeID roachpb.NodeID, maxBytes int64,
) (msgs []kvflowcontrolpb.AdmittedForRangeRACv2, remainingMsgs int) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	perRangeMap, ok := ap.mu.msgsForNode[nodeID]
	if !ok || len(perRangeMap) == 0 {
		return nil, 0
	}
	maxEntries := maxBytes / admittedForRangeRACv2SizeBytes
	for rangeID, msg := range perRangeMap {
		msgs = append(msgs, msg)
		delete(perRangeMap, rangeID)
		if int64(len(msgs)) > maxEntries {
			return msgs, len(perRangeMap)
		}
	}
	return msgs, 0
}

func (ap *admittedPiggybackStateManager) NodesWithMsgs() []roachpb.NodeID {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	var nodes []roachpb.NodeID
	for nodeID, perRangeMap := range ap.mu.msgsForNode {
		if len(perRangeMap) > 0 {
			nodes = append(nodes, nodeID)
		}
	}
	return nodes
}
