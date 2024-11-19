// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package constraint

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// AnalyzedConstraints represents the result of AnalyzeConstraints(). It
// combines a span config's constraints with information about which stores
// satisfy what term of the constraints disjunction.
type AnalyzedConstraints struct {
	Constraints []roachpb.ConstraintsConjunction
	// True if the per-replica constraints don't fully cover all the desired
	// replicas in the range (sum(constraints.NumReplicas) < config.NumReplicas).
	// In such cases, we allow replicas that don't match any of the per-replica
	// constraints, but never mark them as necessary.
	UnconstrainedReplicas bool
	// For each conjunction of constraints in the above slice, track which
	// StoreIDs satisfy them. This field is unused if there are no constraints.
	SatisfiedBy [][]roachpb.StoreID
	// Maps from StoreID to the indices in the constraints slice of which
	// constraints the store satisfies. This field is unused if there are no
	// constraints.
	Satisfies map[roachpb.StoreID][]int
}

// EmptyAnalyzedConstraints represents an empty set of constraints that are
// satisfied by any given configuration of replicas.
var EmptyAnalyzedConstraints = AnalyzedConstraints{}

// StoreResolver resolves a store descriptor by a given ID.
type StoreResolver interface {
	GetStoreDescriptor(storeID roachpb.StoreID) (roachpb.StoreDescriptor, bool)
}

// AnalyzeConstraints processes the span config constraints that apply to a
// range along with the current replicas for a range, spitting back out
// information about which constraints are satisfied by which replicas and
// which replicas satisfy which constraints, aiding in allocation decisions.
func AnalyzeConstraints(
	storeResolver StoreResolver,
	existing []roachpb.ReplicaDescriptor,
	numReplicas int32,
	constraints []roachpb.ConstraintsConjunction,
) AnalyzedConstraints {
	result := AnalyzedConstraints{
		Constraints: constraints,
	}

	if len(constraints) > 0 {
		result.SatisfiedBy = make([][]roachpb.StoreID, len(constraints))
		result.Satisfies = make(map[roachpb.StoreID][]int)
	}

	var constrainedReplicas int32
	for i, subConstraints := range constraints {
		constrainedReplicas += subConstraints.NumReplicas
		for _, repl := range existing {
			// If for some reason we don't have the store descriptor (which shouldn't
			// happen once a node is hooked into gossip), trust that it's valid. This
			// is a much more stable failure state than frantically moving everything
			// off such a node.
			store, ok := storeResolver.GetStoreDescriptor(repl.StoreID)
			if !ok || CheckStoreConjunction(store, subConstraints.Constraints) {
				result.SatisfiedBy[i] = append(result.SatisfiedBy[i], store.StoreID)
				result.Satisfies[store.StoreID] = append(result.Satisfies[store.StoreID], i)
			}
		}
	}
	if constrainedReplicas > 0 && constrainedReplicas < numReplicas {
		result.UnconstrainedReplicas = true
	}
	return result
}

// CheckConjunction checks the given attributes and locality tags against all
// the given constraints. Every constraint must be satisfied by any
// attribute/tier, i.e. they are ANDed together.
func CheckConjunction(
	storeAttrs, nodeAttrs roachpb.Attributes,
	nodeLocality roachpb.Locality,
	constraints []roachpb.Constraint,
) bool {
	for _, constraint := range constraints {
		matchesConstraint := roachpb.MatchesConstraint(storeAttrs, nodeAttrs, nodeLocality, constraint)
		if (constraint.Type == roachpb.Constraint_REQUIRED && !matchesConstraint) ||
			(constraint.Type == roachpb.Constraint_PROHIBITED && matchesConstraint) {
			return false
		}
	}
	return true
}

// CheckStoreConjunction checks a store against a single set of constraints (out of
// the possibly numerous sets that apply to a range), returning true iff the
// store matches the constraints. The constraints are AND'ed together; a store
// matches the conjunction if it matches all of them.
func CheckStoreConjunction(
	storeDesc roachpb.StoreDescriptor, constraints []roachpb.Constraint,
) bool {
	return CheckConjunction(
		storeDesc.Attrs, storeDesc.Node.Attrs, storeDesc.Node.Locality, constraints)
}
