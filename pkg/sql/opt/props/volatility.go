// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// VolatilitySet tracks the set of operator volatilities contained inside an
// expression. See tree.Volatility for more info on volatility values.
//
// TODO(radu): transfer the comment for CanHaveSideEffects here clarifying the
// optimizer policy around volatility and side-effects.
type VolatilitySet uint8

// Add a volatility to the set.
func (vs *VolatilitySet) Add(v tree.Volatility) {
	*vs |= volatilityBit(v)
}

// AddImmutable is a convenience shorthand for adding VolatilityImmutable.
func (vs *VolatilitySet) AddImmutable() {
	vs.Add(tree.VolatilityImmutable)
}

// AddVolatile is a convenience shorthand for adding VolatilityVolatile.
func (vs *VolatilitySet) AddVolatile() {
	vs.Add(tree.VolatilityVolatile)
}

// UnionWith sets the receiver to the union of the two volatility sets.
func (vs *VolatilitySet) UnionWith(other VolatilitySet) {
	*vs = *vs | other
}

// IsLeakProof returns true if the set is empty or only contains
// VolatilityLeakProof.
func (vs VolatilitySet) IsLeakProof() bool {
	return vs == 0 || vs == volatilityBit(tree.VolatilityLeakProof)
}

// HasStable returns true if the set contains VolatilityStable.
func (vs VolatilitySet) HasStable() bool {
	return (vs & volatilityBit(tree.VolatilityStable)) != 0
}

// HasVolatile returns true if the set contains VolatilityVolatile.
func (vs VolatilitySet) HasVolatile() bool {
	return (vs & volatilityBit(tree.VolatilityVolatile)) != 0
}

func (vs VolatilitySet) String() string {
	// There only properties we care about are IsLeakProof(), HasStable() and
	// HasVolatile(). We print one of the strings below:
	//
	//    String            | IsLeakProof | HasStable | HasVolatile
	//   -------------------+-------------+-----------+-------------
	//    "leak-proof"      | true        | false     | false
	//    "immutable"       | false       | false     | false
	//    "stable"          | false       | true      | false
	//    "volatile"        | false       | false     | true
	//    "stable+volatile" | false       | true      | true
	//
	// These are the only valid combinations for these properties.
	//
	if vs.IsLeakProof() {
		return "leak-proof"
	}
	hasStable := vs.HasStable()
	hasVolatile := vs.HasVolatile()
	switch {
	case !hasStable && !hasVolatile:
		return "immutable"
	case hasStable && !hasVolatile:
		return "stable"
	case hasVolatile && !hasStable:
		return "volatile"
	default:
		return "volatile+stable"
	}
}

func volatilityBit(v tree.Volatility) VolatilitySet {
	return 1 << VolatilitySet(v)
}
