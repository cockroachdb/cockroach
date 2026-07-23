// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
)

// connectionTrackerForFlowControl tracks the set of server-side nodes the
// raft transport is connected to. Client-side stores return flow tokens, so
// if a server-side node is no longer connected, we can choose to drop the
// token return messages.
type connectionTrackerForFlowControl struct {
	nodes map[roachpb.NodeID]map[rpcbase.ConnectionClass]struct{}
}

func newConnectionTrackerForFlowControl() *connectionTrackerForFlowControl {
	return &connectionTrackerForFlowControl{
		nodes: make(map[roachpb.NodeID]map[rpcbase.ConnectionClass]struct{}),
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
	nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) {
	if len(q.nodes[nodeID]) == 0 {
		q.nodes[nodeID] = map[rpcbase.ConnectionClass]struct{}{}
	}
	q.nodes[nodeID][class] = struct{}{}
}

// markNodeDisconnected informs the tracker that a previous connection to
// the given node along the given RPC connection class is now broken.
func (q *connectionTrackerForFlowControl) markNodeDisconnected(
	nodeID roachpb.NodeID, class rpcbase.ConnectionClass,
) {
	delete(q.nodes[nodeID], class)
	if len(q.nodes[nodeID]) == 0 {
		delete(q.nodes, nodeID)
	}
}

func (c *connectionTrackerForFlowControl) testingPrint() string {
	nodeIDs := make([]roachpb.NodeID, 0, len(c.nodes))
	for nodeID := range c.nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}

	var buf strings.Builder
	slices.Sort(nodeIDs)
	buf.WriteString(fmt.Sprintf("connected-nodes  (client POV): %s\n", roachpb.NodeIDSlice(nodeIDs)))
	return buf.String()
}
