// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package node_rac2

import (
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
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
		t time.Time, nodeID roachpb.NodeID, maxBytes int64,
	) (msgs []kvflowcontrolpb.AdmittedResponseForRange, remainingMsgs int)
	// NodesWithMsgs is used to periodically drop msgs from disconnected nodes.
	// See RaftTransport.dropFlowTokensForDisconnectedNodes.
	NodesWithMsgs(t time.Time) []roachpb.NodeID
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
	rangeMap              map[roachpb.RangeID]kvflowcontrolpb.AdmittedResponseForRange
	transitionToEmptyTime time.Time
}

func NewAdmittedPiggybacker() *AdmittedPiggybacker {
	ap := &AdmittedPiggybacker{}
	ap.mu.msgsForNode = map[roachpb.NodeID]*rangeMap{}
	return ap
}

var _ PiggybackMsgReader = &AdmittedPiggybacker{}
var _ replica_rac2.AdmittedPiggybacker = &AdmittedPiggybacker{}

func (ap *AdmittedPiggybacker) AddMsgAppRespForLeader(
	nodeID roachpb.NodeID, storeID roachpb.StoreID, rangeID roachpb.RangeID, msg raftpb.Message,
) {
	ap.mu.Lock()
	defer ap.mu.Unlock()
	rm, ok := ap.mu.msgsForNode[nodeID]
	if !ok {
		rm = &rangeMap{rangeMap: map[roachpb.RangeID]kvflowcontrolpb.AdmittedResponseForRange{}}
		ap.mu.msgsForNode[nodeID] = rm
	}
	rm.rangeMap[rangeID] = kvflowcontrolpb.AdmittedResponseForRange{
		LeaderStoreID: storeID,
		RangeID:       rangeID,
		Msg:           msg,
	}
}

// Made-up number. There are 10+ integers, all varint encoded, many of which
// like nodeID, storeID, replicaIDs etc. will be small.
const admittedForRangeRACv2SizeBytes = 50

func (ap *AdmittedPiggybacker) PopMsgsForNode(
	t time.Time, nodeID roachpb.NodeID, maxBytes int64,
) (msgs []kvflowcontrolpb.AdmittedResponseForRange, remainingMsgs int) {
	if ap == nil {
		return nil, 0
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	rm, ok := ap.mu.msgsForNode[nodeID]
	if !ok || len(rm.rangeMap) == 0 {
		return nil, 0
	}
	maxEntries := maxBytes / admittedForRangeRACv2SizeBytes
	for rangeID, msg := range rm.rangeMap {
		msgs = append(msgs, msg)
		delete(rm.rangeMap, rangeID)
		if int64(len(msgs)) > maxEntries {
			break
		}
	}
	n := len(rm.rangeMap)
	if n == 0 {
		rm.transitionToEmptyTime = t
	}
	return msgs, n
}

// gcTimeDuration is used to garbage collect a per-node map if it has been
// empty for this duration. The value is somewhat arbitrary -- creating a new
// map for a node at this frequency is completely acceptable, and nodes don't
// change in a cluster at a high frequency.
const gcTimeDuration time.Duration = time.Minute

func (ap *AdmittedPiggybacker) NodesWithMsgs(t time.Time) []roachpb.NodeID {
	if ap == nil {
		return nil
	}
	ap.mu.Lock()
	defer ap.mu.Unlock()
	var nodes []roachpb.NodeID
	for nodeID, rm := range ap.mu.msgsForNode {
		if len(rm.rangeMap) > 0 {
			nodes = append(nodes, nodeID)
		} else if t.Sub(rm.transitionToEmptyTime) > gcTimeDuration {
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
	slices.Sort(ranges)
	return ranges
}
