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
// the remaining filters that don't match the predicate expression and true. If
// implication cannot be proven, nil and false are returned. Note that this
// "proof" of implication is not mathematically formal or rigorous. For the sake
// of efficiency and reduced complexity this proof is a best-effort attempt and
// false-negatives are possible.

// The remaining filters are identical to the input filters except that any
// parts that exactly match the predicate expression are removed. When the
// remaining filters are applied on top of a scan of a partial index with the
// given predicate, the resulting expression is equivalent to the original
// expression. This reduces the complexity of the filters and allows any columns
// that are referenced only in the filters to be pruned from the query plan.
//
// If implication is proven, the input filters cannot always be reduced. As an
// example, the filter "a > 10" implies the predicate "a > 5", but the filter
// must remain in order to further filter out values between 6 and 10 that would
// be returned from a partial index scan.
//
// The logic for testing implication of ScalarExprs with conjunctions,
// disjunctions, and atoms (anything that is not an AND or OR) is the same as
// Postgres's predtest library.
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
