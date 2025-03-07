// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// PolicyLocalityKey identifies a closed timestamp entry by combining a policy with
// a locality comparison type (UNDEFINED or specific locality like CROSS_REGION).
type PolicyLocalityKey struct {
	Policy   roachpb.RangeClosedTimestampPolicy
	Locality roachpb.LocalityComparisonType
}

// ToIndex maps a PolicyLocalityKey to an array index:
// - [0, numOfPolicies): (Policy, UNDEFINED) pairs
// - [numOfPolicies, numOfEntries): (LEAD_FOR_GLOBAL_READS, Locality) pairs
//
// Examples:
// - (LAG_BY_LEASEHOLDER, UNDEFINED) -> 0
// - (LEAD_FOR_GLOBAL_READS, UNDEFINED) -> 1
// - (LEAD_FOR_GLOBAL_READS, CROSS_REGION) -> 2
// - (LEAD_FOR_GLOBAL_READS, CROSS_ZONE) -> 3
// - (LEAD_FOR_GLOBAL_READS, SAME_ZONE) -> 4
func (k PolicyLocalityKey) ToIndex() int {
	if k.Locality == roachpb.LocalityComparisonType_UNDEFINED {
		// For UNDEFINED locality, index is just the policy value
		return int(k.Policy)
	}
	// For non-UNDEFINED localities:
	// - Start after all policy indices
	// - Subtract 1 from locality value to skip UNDEFINED in the enum
	return int(roachpb.MAX_CLOSED_TIMESTAMP_POLICY) + int(k.Locality) - 1
}

func (k PolicyLocalityKey) String() string {
	return fmt.Sprintf("(%s,%s)", k.Policy, k.Locality)
}

// Constants for array sizes and index boundaries
const (
	// Number of possible closed timestamp policies
	numOfPolicies = int(roachpb.MAX_CLOSED_TIMESTAMP_POLICY)

	// Number of locality comparison types, excluding UNDEFINED
	numOfLocalities = int(roachpb.LocalityComparisonType_MAX_LOCALITY_COMPARISON_TYPE - 1)

	// Total size for closed timestamp array (policy-only + policy+locality entries)
	numOfEntries = numOfPolicies + numOfLocalities
)

// EntryIdx represents an index into the closed timestamp array [0, numOfEntries).
type EntryIdx int

// valid returns true if index is within [0, numOfEntries).
func (idx EntryIdx) valid() bool {
	return idx >= 0 && idx < EntryIdx(numOfEntries)
}

// ToPolicyLocalityKey converts array index to PolicyLocalityKey (inverse of ToIndex).
func (idx EntryIdx) ToPolicyLocalityKey() PolicyLocalityKey {
	if !idx.valid() {
		log.Errorf(context.Background(), "programming error: invalid index %d", idx)
	}

	if idx < EntryIdx(numOfPolicies) {
		// First numOfPolicies indices are (Policy, UNDEFINED) pairs
		return PolicyLocalityKey{
			Policy:   roachpb.RangeClosedTimestampPolicy(idx),
			Locality: roachpb.LocalityComparisonType_UNDEFINED,
		}
	}

	// Remaining indices are (LEAD_FOR_GLOBAL_READS, Locality) pairs
	// Add 1 to skip UNDEFINED in LocalityComparisonType enum
	localityVal := roachpb.LocalityComparisonType(idx - EntryIdx(numOfPolicies) + 1)
	return PolicyLocalityKey{
		Policy:   roachpb.LEAD_FOR_GLOBAL_READS,
		Locality: localityVal,
	}
}

// PolicyLocalityToTimestampMap maps policy+locality combinations to timestamps using
// a fixed-size array with indices computed from PolicyLocalityKey values.
type PolicyLocalityToTimestampMap struct {
	// Timestamps indexed by PolicyLocalityKey combinations
	Timestamps [numOfEntries]hlc.Timestamp
}

// Get returns the timestamp for a policy and locality combination.
func (m *PolicyLocalityToTimestampMap) Get(key PolicyLocalityKey) hlc.Timestamp {
	return m.Timestamps[key.ToIndex()]
}

// Len returns total number of possible policy+locality combinations.
func (m *PolicyLocalityToTimestampMap) Len() int {
	return numOfEntries
}

// Reset clears all timestamps to zero values.
func (m *PolicyLocalityToTimestampMap) Reset() {
	m.Timestamps = [numOfEntries]hlc.Timestamp{}
}

// Set stores a timestamp for a policy and locality combination.
func (m *PolicyLocalityToTimestampMap) Set(
	key PolicyLocalityKey, ts hlc.Timestamp,
) {
	m.Timestamps[key.ToIndex()] = ts
}
