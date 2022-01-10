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

// storeSliceFromSet unwraps map to a sorted list of StoreIDs.
func storeSliceFromSet(set storeIDSet) []roachpb.StoreID {
	storeIDs := make([]roachpb.StoreID, 0, len(set))
	for k := range set {
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
	for _, id := range storeSliceFromSet(storeIDs) {
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

// KeyspaceAnomaly records errors found when checking keyspace coverage and
// health of survivor replicas. Anomaly covers a key span that is either not
// covered by any ranges, covered multiple times or correspond to replicas data
// in which could not act as a source of truth in absence of other replicas.
// Main goal of this interface to provide a human-readable representations of
// problem discovered during planning process.
// Anomalies contain span so that they could be ordered for user presentations.
// Anomaly also contains additional information about ranges that either
// bordering the gap or overlap over the anomaly span.
type KeyspaceAnomaly interface {
	fmt.Stringer
	// Span returns span for detected problem. Anomalies should report consistent
	// span for the sake of ordered data presentation.
	Span() roachpb.Span
}

type keyspaceGapAnomaly struct {
	span roachpb.Span

	range1     roachpb.RangeID
	range1Span roachpb.Span

	range2     roachpb.RangeID
	range2Span roachpb.Span
}

func (i keyspaceGapAnomaly) String() string {
	return fmt.Sprintf("range gap %v\n  r%d: %v\n  r%d: %v",
		i.span, i.range1, i.range1Span, i.range2, i.range2Span)
}

func (i keyspaceGapAnomaly) Span() roachpb.Span {
	return i.span
}

type keyspaceOverlapAnomaly struct {
	span roachpb.Span

	range1     roachpb.RangeID
	range1Span roachpb.Span

	range2     roachpb.RangeID
	range2Span roachpb.Span
}

func (i keyspaceOverlapAnomaly) String() string {
	return fmt.Sprintf("range overlap %v\n  r%d: %v\n  r%d: %v",
		i.span, i.range1, i.range1Span, i.range2, i.range2Span)
}

func (i keyspaceOverlapAnomaly) Span() roachpb.Span {
	return i.span
}

type rangeSplitAnomaly struct {
	rangeID roachpb.RangeID
	span    roachpb.Span

	rHSRangeID   roachpb.RangeID
	rHSRangeSpan roachpb.Span
}

func (i rangeSplitAnomaly) String() string {
	return fmt.Sprintf("range has unapplied split operation\n  r%d, %v rhs r%d, %v",
		i.rangeID, i.span, i.rHSRangeID, i.rHSRangeSpan)
}

func (i rangeSplitAnomaly) Span() roachpb.Span {
	return i.span
}

type rangeMergeAnomaly rangeSplitAnomaly

func (i rangeMergeAnomaly) String() string {
	return fmt.Sprintf("range has unapplied merge operation\n  r%d, %v with r%d, %v",
		i.rangeID, i.span, i.rHSRangeID, i.rHSRangeSpan)
}

func (i rangeMergeAnomaly) Span() roachpb.Span {
	return i.span
}

type rangeReplicaRemovalAnomaly struct {
	rangeID roachpb.RangeID
	span    roachpb.Span
}

func (i rangeReplicaRemovalAnomaly) String() string {
	return fmt.Sprintf("range has unapplied descriptor change that removes current replica\n  r%d: %v",
		i.rangeID,
		i.span)
}

func (i rangeReplicaRemovalAnomaly) Span() roachpb.Span {
	return i.span
}

// KeyspaceError is returned by replica planner when it detects problems
// with replicas in key space. Error contains all anomalies found.
// KeyspaceError implements ErrorDetailer to integrate into cli commands.
type KeyspaceError struct {
	anomalies []KeyspaceAnomaly
}

func (e *KeyspaceError) Error() string {
	return "keyspace error"
}

// ErrorDetail returns a properly formatted report that could be presented
// to user.
func (e *KeyspaceError) ErrorDetail() string {
	descriptions := make([]string, 0, len(e.anomalies))
	for _, id := range e.anomalies {
		descriptions = append(descriptions, fmt.Sprintf("%v", id))
	}
	return strings.Join(descriptions, "\n")
}
