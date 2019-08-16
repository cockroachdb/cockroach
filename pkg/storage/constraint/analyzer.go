// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package constraint

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type AnalyzedConstraints struct {
	Constraints []config.Constraints
	// True if the per-replica constraints don't fully cover all the desired
	// replicas in the range (sum(constraints.NumReplicas) < zone.NumReplicas).
	// In such cases, we allow replicas that don't match any of the per-replica
	// constraints, but never mark them as necessary.
	UnconstrainedReplicas bool
	// For each set of constraints in the above slice, track which StoreIDs
	// satisfy them. This field is unused if there are no constraints.
	SatisfiedBy [][]roachpb.StoreID
	// Maps from StoreID to the indices in the constraints slice of which
	// constraints the store satisfies. This field is unused if there are no
	// constraints.
	Satisfies map[roachpb.StoreID][]int
}

// analyzeConstraints processes the zone config constraints that apply to a
// range along with the current replicas for a range, spitting back out
// information about which constraints are satisfied by which replicas and
// which replicas satisfy which constraints, aiding in allocation decisions.
func AnalyzeConstraints(
	ctx context.Context,
	getStoreDescFn func(roachpb.StoreID) (roachpb.StoreDescriptor, bool),
	existing []roachpb.ReplicaDescriptor,
	zone *config.ZoneConfig,
) AnalyzedConstraints {
	result := AnalyzedConstraints{
		Constraints: zone.Constraints,
	}

	if len(zone.Constraints) > 0 {
		result.SatisfiedBy = make([][]roachpb.StoreID, len(zone.Constraints))
		result.Satisfies = make(map[roachpb.StoreID][]int)
	}

	var constrainedReplicas int32
	for i, subConstraints := range zone.Constraints {
		constrainedReplicas += subConstraints.NumReplicas
		for _, repl := range existing {
			// If for some reason we don't have the store descriptor (which shouldn't
			// happen once a node is hooked into gossip), trust that it's valid. This
			// is a much more stable failure state than frantically moving everything
			// off such a node.
			store, ok := getStoreDescFn(repl.StoreID)
			if !ok || SubConstraintsCheck(store, subConstraints.Constraints) {
				result.SatisfiedBy[i] = append(result.SatisfiedBy[i], store.StoreID)
				result.Satisfies[store.StoreID] = append(result.Satisfies[store.StoreID], i)
			}
		}
	}
	if constrainedReplicas > 0 && constrainedReplicas < *zone.NumReplicas {
		result.UnconstrainedReplicas = true
	}
	return result
}

// subConstraintsCheck checks a store against a single set of constraints (out
// of the possibly numerous sets that apply to a range), returning true iff the
// store matches the constraints.
func SubConstraintsCheck(store roachpb.StoreDescriptor, constraints []config.Constraint) bool {
	for _, constraint := range constraints {
		// StoreMatchesConstraint returns whether a store matches the given constraint.
		hasConstraint := config.StoreHasConstraint(store, constraint)
		if (constraint.Type == config.Constraint_REQUIRED && !hasConstraint) ||
			(constraint.Type == config.Constraint_PROHIBITED && hasConstraint) {
			return false
		} else {
			return true
		}
	}
	return true
}
