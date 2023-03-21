// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lookupjoin

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// HasJoinFilterConstants returns true if the filter constrains the given column
// to a constant, non-NULL value or set of constant, non-NULL values.
func HasJoinFilterConstants(
	filters memo.FiltersExpr, col opt.ColumnID, evalCtx *eval.Context,
) bool {
	for filterIdx := range filters {
		props := filters[filterIdx].ScalarProps()
		if props.TightConstraints {
			if ok := props.Constraints.HasSingleColumnNonNullConstValues(evalCtx, col); ok {
				return true
			}
		}
	}
	return false
}

// FindJoinFilterConstants tries to find a filter that is exactly equivalent to
// constraining the given column to a constant value or a set of constant
// values. If successful, the constant values and the index of the constraining
// FiltersItem are returned. If multiple filters match, the one that minimizes
// the number of returned values is chosen. Note that the returned constant
// values do not contain NULL.
func FindJoinFilterConstants(
	filters memo.FiltersExpr, col opt.ColumnID, evalCtx *eval.Context,
) (values tree.Datums, filterIdx int, ok bool) {
	var bestValues tree.Datums
	var bestFilterIdx int
	for filterIdx := range filters {
		props := filters[filterIdx].ScalarProps()
		if props.TightConstraints {
			constVals, ok := props.Constraints.ExtractSingleColumnNonNullConstValues(evalCtx, col)
			if !ok {
				continue
			}
			if bestValues == nil || len(bestValues) > len(constVals) {
				bestValues = constVals
				bestFilterIdx = filterIdx
			}
		}
	}
	if bestValues == nil {
		return nil, -1, false
	}
	return bestValues, bestFilterIdx, true
}
