// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package node_rac2

import (
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// PiggybackMsgReader is used to retrieve messages that need to be
// piggybacked.
type PiggybackMsgReader interface {
	// PopMsgsForNode is used by RaftTransport to grab messages to piggyback
	// when it is sending a message to nodeID. Even if maxBytes is zero, at
	// least one message will be popped.
	PopMsgsForNode(
		now time.Time, nodeID roachpb.NodeID, maxBytes int64,
	) (_ []kvflowcontrolpb.PiggybackedAdmittedState, remainingMsgs int)
	// NodesWithMsgs is used to periodically drop msgs from disconnected nodes.
	// See RaftTransport.dropFlowTokensForDisconnectedNodes.
	NodesWithMsgs(now time.Time) []roachpb.NodeID
}

// AdmittedPiggybacker implements PiggybackMsgReader and
// replica_rac2.AdmittedPiggybacker. A nil value is safe to use as a
// PiggybackMsgReader, and will return nothing.
type AdmittedPiggybacker struct {
	mu struct {
		syncutil.Mutex
		msgsForNode map[roachpb.NodeID]*rangeMap
	}
}

type rangeMap struct {
	rangeMap              map[roachpb.RangeID]kvflowcontrolpb.PiggybackedAdmittedState
	transitionToEmptyTime time.Time
}

func NewAdmittedPiggybacker() *AdmittedPiggybacker {
	ap := &AdmittedPiggybacker{}
	ap.mu.msgsForNode = map[roachpb.NodeID]*rangeMap{}
	return ap
}

var _ PiggybackMsgReader = &AdmittedPiggybacker{}
var _ replica_rac2.AdmittedPiggybacker = &AdmittedPiggybacker{}

// Add implements replica_rac2.AdmittedPiggybacker.
func (ap *AdmittedPiggybacker) Add(
	nodeID roachpb.NodeID, msg kvflowcontrolpb.PiggybackedAdmittedState,
) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	rm, ok := ap.mu.msgsForNode[nodeID]
	if !ok {
		rm = &rangeMap{rangeMap: map[roachpb.RangeID]kvflowcontrolpb.PiggybackedAdmittedState{}}
		ap.mu.msgsForNode[nodeID] = rm
	}
	rm.rangeMap[msg.RangeID] = msg
}

// Made-up number. There are < 10 integers, all varint encoded, many of which
// like nodeID, storeID, replicaIDs etc. will be small.
const admittedForRangeRACv2SizeBytes = 40

// PopMsgsForNode implements PiggybackMsgReader.
func (ap *AdmittedPiggybacker) PopMsgsForNode(
	now time.Time, nodeID roachpb.NodeID, maxBytes int64,
) (_ []kvflowcontrolpb.PiggybackedAdmittedState, remainingMsgs int) {
	if ap == nil {
		return nil, 0
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	rm, ok := ap.mu.msgsForNode[nodeID]
	if !ok || len(rm.rangeMap) == 0 {
		return nil, 0
	}
	// NB: +1 to include at least one entry.
	maxEntries := maxBytes/admittedForRangeRACv2SizeBytes + 1
	msgs := make([]kvflowcontrolpb.PiggybackedAdmittedState, 0,
		min(int64(len(rm.rangeMap)), maxEntries))
	for rangeID, msg := range rm.rangeMap {
		if len(msgs) == cap(msgs) {
			break
		}
		msgs = append(msgs, msg)
		delete(rm.rangeMap, rangeID)
	}
	n := len(rm.rangeMap)
	if n == 0 {
		rm.transitionToEmptyTime = now
	}
	return msgs, n
}

// gcTimeDuration is used to garbage collect a per-node map if it has been
// empty for this duration. The value is somewhat arbitrary -- creating a new
// map for a node at this frequency is completely acceptable, and nodes don't
// change in a cluster at a high frequency.
const gcTimeDuration time.Duration = time.Minute

// NodesWithMsgs implements PiggybackMsgReader.
func (ap *AdmittedPiggybacker) NodesWithMsgs(now time.Time) []roachpb.NodeID {
	if ap == nil {
		return nil
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	var nodes []roachpb.NodeID
	for nodeID, rm := range ap.mu.msgsForNode {
		if len(rm.rangeMap) > 0 {
			nodes = append(nodes, nodeID)
		} else if now.Sub(rm.transitionToEmptyTime) > gcTimeDuration {
			delete(ap.mu.msgsForNode, nodeID)
		}
	}
	return nodes
}

func (ap *AdmittedPiggybacker) RangesWithMsgsForTesting(nodeID roachpb.NodeID) []roachpb.RangeID {
	if ap == nil {
		return nil
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	rm, ok := ap.mu.msgsForNode[nodeID]
	if !ok || len(rm.rangeMap) == 0 {
		return nil
	}
	var ranges []roachpb.RangeID
	for rangeID := range rm.rangeMap {
		ranges = append(ranges, rangeID)
	}
	// Make the output deterministic, since this is a testing-only method.
	slices.Sort(ranges)
	return ranges
}
