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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type storeIDSet map[roachpb.StoreID]struct{}

func idSetFromSlice(ids []roachpb.StoreID) storeIDSet {
	set := make(storeIDSet)
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

// storeSliceFromSet unwraps map to a sorted list of StoreIDs.
func (s storeIDSet) storeSliceFromSet() []roachpb.StoreID {
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
func (s storeIDSet) joinStoreIDs() string {
	storeNames := make([]string, 0, len(s))
	for _, id := range s.storeSliceFromSet() {
		storeNames = append(storeNames, fmt.Sprintf("s%d", id))
	}
	return strings.Join(storeNames, ", ")
}

func (s storeIDSet) intersect(other storeIDSet) storeIDSet {
	set := make(storeIDSet)
	for k := range s {
		if _, ok := other[k]; ok {
			set[k] = struct{}{}
		}
	}
	return set
}

func (s storeIDSet) diff(other storeIDSet) storeIDSet {
	set := make(storeIDSet)
	for k := range s {
		if _, ok := other[k]; !ok {
			set[k] = struct{}{}
		}
	}
	return set
}

type locationsMap map[roachpb.NodeID]storeIDSet

// locationsFromSlice create a node to storages locations map with each node
// having an empty storage set. Resulting map is useful when performing set
// operations on nodes.
func locationsFromSlice(deadNodeIDs []roachpb.NodeID) locationsMap {
	locations := make(locationsMap)
	for _, id := range deadNodeIDs {
		locations[id] = make(storeIDSet)
	}
	return locations
}

// add inserts store into node entry. if node is missing it is added.
func (m locationsMap) add(node roachpb.NodeID, store roachpb.StoreID) {
	var set storeIDSet
	var ok bool
	if set, ok = m[node]; !ok {
		set = make(storeIDSet)
		m[node] = set
	}
	set[store] = struct{}{}
}

func (m locationsMap) asSortedSlice() []NodeStores {
	nodes := make([]NodeStores, 0, len(m))
	for k, v := range m {
		nodes = append(nodes, NodeStores{NodeID: k, StoreIDs: v.storeSliceFromSet()})
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].NodeID < nodes[j].NodeID
	})
	return nodes
}

// stores returns a flatted set of stores from all nodes.
func (m locationsMap) stores() storeIDSet {
	s := make(storeIDSet)
	for _, ss := range m {
		for k := range ss {
			s[k] = struct{}{}
		}
	}
	return s
}

// diff is a set operation on nodes. Resulting map will contain values from
// left hand side (receiver) and ignore stores in argument set.
func (m locationsMap) diff(nodes locationsMap) locationsMap {
	l := make(locationsMap)
	for n, ss := range m {
		if _, ok := nodes[n]; !ok {
			l[n] = ss
		}
	}
	return l
}

// intersect is a set operation on nodes. Resulting map will contain values from
// left hand side (receiver) and ignore stores in argument set.
func (m locationsMap) intersect(nodes locationsMap) locationsMap {
	l := make(locationsMap)
	for n, ss := range m {
		if _, ok := nodes[n]; ok {
			l[n] = ss
		}
	}
	return l
}

func (m locationsMap) joinNodeIDs() string {
	nodeIDs := make([]roachpb.NodeID, 0, len(m))
	for k := range m {
		nodeIDs = append(nodeIDs, k)
	}
	sort.Slice(nodeIDs, func(i, j int) bool {
		return nodeIDs[i] < nodeIDs[j]
	})
	nodeNames := make([]string, 0, len(m))
	for _, id := range nodeIDs {
		nodeNames = append(nodeNames, fmt.Sprintf("n%d", id))
	}
	return strings.Join(nodeNames, ", ")
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

// Problem records errors found when checking keyspace coverage and
// health of survivor replicas. Problem covers a key span that is either not
// covered by any ranges, covered multiple times or correspond to replicas data
// in which could not act as a source of truth in absence of other replicas.
// Main goal of this interface to provide a human-readable representations of
// problem discovered during planning process.
// Problems contain span so that they could be ordered for user presentations.
// Problem also contains additional information about ranges that either
// bordering the gap or overlap over the problematic span.
type Problem interface {
	fmt.Stringer
	// Span returns span for detected problem. Problems should report consistent
	// span for the sake of ordered data presentation.
	Span() roachpb.Span
}

type keyspaceGap struct {
	span roachpb.Span

	range1     roachpb.RangeID
	range1Span roachpb.Span

	range2     roachpb.RangeID
	range2Span roachpb.Span
}

func (i keyspaceGap) String() string {
	return fmt.Sprintf("range gap %v\n  r%d: %v\n  r%d: %v",
		i.span, i.range1, i.range1Span, i.range2, i.range2Span)
}

func (i keyspaceGap) Span() roachpb.Span {
	return i.span
}

type keyspaceOverlap struct {
	span roachpb.Span

	range1     roachpb.RangeID
	range1Span roachpb.Span

	range2     roachpb.RangeID
	range2Span roachpb.Span
}

func (i keyspaceOverlap) String() string {
	return fmt.Sprintf("range overlap %v\n  r%d: %v\n  r%d: %v",
		i.span, i.range1, i.range1Span, i.range2, i.range2Span)
}

func (i keyspaceOverlap) Span() roachpb.Span {
	return i.span
}

type rangeSplit struct {
	rangeID roachpb.RangeID
	span    roachpb.Span

	rHSRangeID   roachpb.RangeID
	rHSRangeSpan roachpb.Span
}

func (i rangeSplit) String() string {
	return fmt.Sprintf("range has unapplied split operation\n  r%d, %v rhs r%d, %v",
		i.rangeID, i.span, i.rHSRangeID, i.rHSRangeSpan)
}

func (i rangeSplit) Span() roachpb.Span {
	return i.span
}

type rangeMerge rangeSplit

func (i rangeMerge) String() string {
	return fmt.Sprintf("range has unapplied merge operation\n  r%d, %v with r%d, %v",
		i.rangeID, i.span, i.rHSRangeID, i.rHSRangeSpan)
}

func (i rangeMerge) Span() roachpb.Span {
	return i.span
}

type rangeReplicaChange struct {
	rangeID roachpb.RangeID
	span    roachpb.Span
}

func (i rangeReplicaChange) String() string {
	return fmt.Sprintf("range has unapplied descriptor change\n  r%d: %v",
		i.rangeID,
		i.span)
}

func (i rangeReplicaChange) Span() roachpb.Span {
	return i.span
}

// allReplicasLost is a type of recovery problem which is raised when there are
// no replicas remain for a range. Recovery would not do anything about such
// ranges and reads would stall. This problem is a more specific than
// keyspaceGap which would be generated for the same problem in absence of
// range metadata info.
type allReplicasLost struct {
	rangeID roachpb.RangeID
	span    roachpb.Span
}

func (i allReplicasLost) String() string {
	return fmt.Sprintf("range does not have any replicas remaining\n  r%d %v",
		i.rangeID,
		i.span)
}

func (i allReplicasLost) Span() roachpb.Span {
	return i.span
}

// rangeMetaMismatch is a type of recovery problem which is raised when
// remaining replica descriptor doesn't match descriptor read from range
// metadata which means only stale replica of range remains which can't be
// used.
type rangeMetaMismatch struct {
	rangeID  roachpb.RangeID
	span     roachpb.Span
	metaSpan roachpb.Span
}

func (i rangeMetaMismatch) String() string {
	return fmt.Sprintf("range doesn't match to the descriptor in the metadata\n  r%d %v metadata %v",
		i.rangeID,
		i.span,
		i.metaSpan)
}

func (i rangeMetaMismatch) Span() roachpb.Span {
	return i.span
}

// RecoveryError is returned by replica planner when it detects problems
// with replicas in key space. Error contains all problems found in keyspace.
// RecoveryError implements ErrorDetailer to integrate into cli commands.
type RecoveryError struct {
	problems []Problem
}

func (e *RecoveryError) Error() string {
	return "loss of quorum recovery error"
}

// ErrorDetail returns a properly formatted report that could be presented
// to user.
func (e *RecoveryError) ErrorDetail() string {
	descriptions := make([]string, 0, len(e.problems))
	for _, id := range e.problems {
		descriptions = append(descriptions, fmt.Sprintf("%v", id))
	}
	return strings.Join(descriptions, "\n")
}
