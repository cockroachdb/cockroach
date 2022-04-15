// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/constraint"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// MaxCapacityCheck returns true if the store has room for a new replica.
func MaxCapacityCheck(store roachpb.StoreDescriptor) bool {
	return store.Capacity.FractionUsed() < MaxFractionUsedThreshold
}

const (
	// MaxFractionUsedThreshold controls the point at which the store cedes having
	// room for new replicas. If the fraction used of a store descriptor capacity
	// is greater than this value, it will never be used as a rebalance or
	// allocate target and we will actively try to move replicas off of it.
	MaxFractionUsedThreshold = 0.95
)

// IsStoreValid returns true iff the provided store would be a valid in a
// range with the provided constraints.
func IsStoreValid(
	store roachpb.StoreDescriptor, constraints []roachpb.ConstraintsConjunction,
) bool {
	if len(constraints) == 0 {
		return true
	}

	for _, subConstraints := range constraints {
		if constraintsOK := constraint.ConjunctionsCheck(
			store, subConstraints.Constraints,
		); constraintsOK {
			return true
		}
	}
	return false
}
