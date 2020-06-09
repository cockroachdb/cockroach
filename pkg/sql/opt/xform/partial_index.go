// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// predicateImpliedByFilters attempts to prove that a partial index predicate is
// implied by the given filters. If implication is proven, the function returns
// a simplified filters expression and true. If implication cannot be proven,
// nil and false are returned. Note that this "proof" is not mathematically
// formal or rigorous. For the sake of efficiency and reduced complexity this
// proof is a best-effort attempt and false-negatives are possible.
//
// When implication is proven, the filters expression returned may be simpified
// by having parts of the original filters removed. This happens when the
// predicate exactly matches a part of the filters, meaning that a scan
// over the corresponding partial index implicitly applies that part of the
// filters. Eliminating parts of the filter when possible allows constrained
// scans to be generated in more cases.
//
// Implication can be proven if any of the following are true:
//
//   - Any FilterItem in the filters is an exact match (via pointer equality)
//     to the predicate.
//
// TODO(mgartner): Implement more advanced proofs for implication.
// TODO(mgartner): remove c
func (c *CustomFuncs) predicateImpliedByFilters(
	pred opt.ScalarExpr, filters memo.FiltersExpr,
) (memo.FiltersExpr, bool) {
	fmt.Printf("pred: %p, %v\n", pred, pred)
	for i := range filters {
		fmt.Printf("filters[%v].Condition: %p, %v\n", i, filters[i].Condition, filters[i].Condition)

		if pred == filters[i].Condition {
			return c.RemoveFiltersItem(filters, &filters[i]), true
		}
	}
	return nil, false
}
