// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build race

package props

import (
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Verify runs consistency checks against the shared properties, in order to
// ensure that they conform to several invariants:
//
//   1. The properties must have been built.
//   2. If HasCorrelatedSubquery is true, then HasSubquery must be true as well.
//   3. If Mutate is true, then CanHaveSideEffects must also be true.
//
func (s *Shared) Verify() {
	if !s.Populated {
		panic(errors.AssertionFailedf("properties are not populated"))
	}
	if s.HasCorrelatedSubquery && !s.HasSubquery {
		panic(errors.AssertionFailedf("HasSubquery cannot be false if HasCorrelatedSubquery is true"))
	}
	if s.CanMutate && !s.CanHaveSideEffects {
		panic(errors.AssertionFailedf("CanHaveSideEffects cannot be false if CanMutate is true"))
	}
}

// Verify runs consistency checks against the relational properties, in order to
// ensure that they conform to several invariants:
//
//   1. Functional dependencies are internally consistent.
//   2. Not null columns are a subset of output columns.
//   3. Outer columns do not intersect output columns.
//   4. If functional dependencies indicate that the relation can have at most
//      one row, then the cardinality reflects that as well.
//
func (r *Relational) Verify() {
	r.Shared.Verify()
	r.FuncDeps.Verify()

	if !r.NotNullCols.SubsetOf(r.OutputCols) {
		panic(errors.AssertionFailedf("not null cols %s not a subset of output cols %s",
			log.Safe(r.NotNullCols), log.Safe(r.OutputCols)))
	}
	if r.OuterCols.Intersects(r.OutputCols) {
		panic(errors.AssertionFailedf("outer cols %s intersect output cols %s",
			log.Safe(r.OuterCols), log.Safe(r.OutputCols)))
	}
	if r.FuncDeps.HasMax1Row() {
		if r.Cardinality.Max > 1 {
			panic(errors.AssertionFailedf(
				"max cardinality must be <= 1 if FDs have max 1 row: %s", r.Cardinality))
		}
	}
	if r.IsAvailable(PruneCols) {
		if !r.Rule.PruneCols.SubsetOf(r.OutputCols) {
			panic(errors.AssertionFailedf("prune cols %s must be a subset of output cols %s",
				log.Safe(r.Rule.PruneCols), log.Safe(r.OutputCols)))
		}
	}
}

// VerifyAgainst checks that the two properties don't contradict each other.
// Used for testing (e.g. to cross-check derived properties from expressions in
// the same group).
func (r *Relational) VerifyAgainst(other *Relational) {
	if !r.OutputCols.Equals(other.OutputCols) {
		panic(errors.AssertionFailedf("output cols mismatch: %s vs %s", log.Safe(r.OutputCols), log.Safe(other.OutputCols)))
	}

	if r.Cardinality.Max < other.Cardinality.Min ||
		r.Cardinality.Min > other.Cardinality.Max {
		panic(errors.AssertionFailedf("cardinality mismatch: %s vs %s", log.Safe(r.Cardinality), log.Safe(other.Cardinality)))
	}

	// NotNullCols, FuncDeps are best effort, so they might differ.
	// OuterCols, CanHaveSideEffects, and HasPlaceholder might differ if a
	// subexpression containing them was elided.
}

// Verify runs consistency checks against the relational properties, in order to
// ensure that they conform to several invariants:
//
//   1. Functional dependencies are internally consistent.
//
func (s *Scalar) Verify() {
	s.Shared.Verify()
	s.FuncDeps.Verify()
}
