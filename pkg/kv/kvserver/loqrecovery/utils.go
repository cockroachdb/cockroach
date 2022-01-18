// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type storeIDSet map[roachpb.StoreID]struct{}

// asSlice unwraps map to a sorted list of StoreIDs.
func (s storeIDSet) asSlice() []roachpb.StoreID {
	storeIDs := make([]roachpb.StoreID, 0, len(s))
	for k := range s {
		storeIDs = append(storeIDs, k)
	}
	sort.Slice(storeIDs, func(i, j int) bool {
		return storeIDs[i] < storeIDs[j]
	})
	return storeIDs
}

// Make a string of stores 'set' in ascending order.
func joinStoreIDs(storeIDs storeIDSet) string {
	storeNames := make([]string, 0, len(storeIDs))
	for _, id := range storeIDs.asSlice() {
		storeNames = append(storeNames, fmt.Sprintf("s%d", id))
	}
	return strings.Join(storeNames, ", ")
}

func keyMax(key1 roachpb.RKey, key2 roachpb.RKey) roachpb.RKey {
	if key1.Less(key2) {
		return key2
	}
	return key1
}

func keyMin(key1 roachpb.RKey, key2 roachpb.RKey) roachpb.RKey {
	if key2.Less(key1) {
		return key2
	}
	return key1
}

// keyspaceCoverageAnomaly records errors found when checking keyspace coverage.
// Anomaly is a key span where there's no coverage or there are more than one
// range that covers the span.
// Anomaly also contains additional information about ranges that either
// bordering the gap or overlap over the anomaly span.
type keyspaceCoverageAnomaly struct {
	span    roachpb.Span
	overlap bool

	range1     roachpb.RangeID
	range1Span roachpb.Span

	range2     roachpb.RangeID
	range2Span roachpb.Span
}

func (i keyspaceCoverageAnomaly) String() string {
	if i.overlap {
		return fmt.Sprintf("range overlap %v\n  r%d: %v\n  r%d: %v",
			i.span, i.range1, i.range1Span, i.range2, i.range2Span)
	}
	return fmt.Sprintf("range gap %v\n  r%d: %v\n  r%d: %v",
		i.span, i.range1, i.range1Span, i.range2, i.range2Span)
}

// KeyspaceCoverageError is returned by replica planner when it detects problems
// with key coverage. Error contains all anomalies found. It also provides a
// convenience function to print report.
type KeyspaceCoverageError struct {
	anomalies []keyspaceCoverageAnomaly
}

func (e *KeyspaceCoverageError) Error() string {
	return "keyspace coverage error"
}

// ErrorDetail returns a properly formatted report that could be presented
// to user.
func (e *KeyspaceCoverageError) ErrorDetail() string {
	descriptions := make([]string, 0, len(e.anomalies))
	for _, id := range e.anomalies {
		descriptions = append(descriptions, fmt.Sprintf("%v", id))
	}
	return fmt.Sprintf(
		"Key space covering is not complete. Discovered following inconsistencies:\n%s\n",
		strings.Join(descriptions, "\n"))
}

// nodeIDSet helper type to remove clutter from node set manipulation
type nodeIDSet map[roachpb.NodeID]struct{}

func (s nodeIDSet) asSlice() []roachpb.NodeID {
	var nodeIDs []roachpb.NodeID
	for nodeID := range s {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})
	return nodeIDs
}

func (s nodeIDSet) removeSet(other nodeIDSet) {
	for nodeID := range other {
		delete(s, nodeID)
	}
}

func (s nodeIDSet) removeSlice(other []roachpb.NodeID) {
	for _, nodeID := range other {
		delete(s, nodeID)
	}
}

type clusterReplicaInfos []loqrecoverypb.NodeReplicaInfo

func (c clusterReplicaInfos) visit(visitor func(roachpb.ReplicaDescriptor)) {
	for _, nodeReplicas := range c {
		for _, replicaInfo := range nodeReplicas.Replicas {
			for _, replica := range replicaInfo.Desc.InternalReplicas {
				visitor(replica)
			}
		}
	}
}
