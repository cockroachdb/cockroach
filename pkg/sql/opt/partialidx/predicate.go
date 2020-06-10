// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package partialidx

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// FiltersImplyPredicate attempts to prove that a partial index predicate is
// implied by the given filters. If implication is proven, the function returns
// a simplified filters expression and true. If implication cannot be proven,
// nil and false are returned. Note that this "proof" is not mathematically
// formal or rigorous. For the sake of efficiency and reduced complexity this
// proof is a best-effort attempt and false-negatives are possible.
//
// When implication is proven, the filters expression returned may be
// simplified with parts of the original filters removed.
//
// The logic for testing implication of ScalarExpr with conjunctions,
// disjunctions, and atoms (anything that is not an AND or OR) is directly
// influenced by Postgres's predtest library.
//
// The logic is as follows, where "=>" means "implies":
//
//   atom A => atom B iff:          A contains B
//   atom A => AND-expr B iff:      A => each of B's children
//   atom A => OR-expr B iff:       A => any of B's children
//
//   AND-expr A => atom B iff:      any of A's children => B
//   AND-expr A => AND-expr B iff:  A => each of B's children
//   AND-expr A => OR-expr B iff:   A => any of B's children OR
//                                   any of A's children => B
//
//   OR-expr A => atom B iff:       each of A's children => B
//   OR-expr A => AND-expr B iff:   A => each of B's children
//   OR-expr A => OR-expr B iff:    each of A's children => any of B's children
//
// TODO(mgartner): Implement more advanced proofs for implication. We only
// support simple "atom => atom" and "AND-expr => atom" currently.
func FiltersImplyPredicate(filters memo.FiltersExpr, pred opt.ScalarExpr) (memo.FiltersExpr, bool) {
	for i := range filters {
		if pred == filters[i].Condition {
			return filters.RemoveFiltersItem(&filters[i]), true
		}
	}
	return nil, false
}
