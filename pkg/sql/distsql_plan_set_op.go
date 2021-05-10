// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// mergeResultTypesForSetOp reconciles the ResultTypes between two plans. It
// enforces that each pair of ColumnTypes must either match or be null, in which
// case the non-null type is used. This logic is necessary for cases like SELECT
// NULL UNION SELECT 1.
//
// This method is intended to be used only for planning of set operations.
func mergeResultTypesForSetOp(leftPlan, rightPlan *PhysicalPlan) ([]*types.T, error) {
	// getTypesIgnoreMergeOrdering returns the schema of the rows produced by
	// plan removing all columns that are propagated for the sole purpose of
	// maintaining the ordering during stream merges.
	getTypesIgnoreMergeOrdering := func(plan *PhysicalPlan) []*types.T {
		typs := plan.GetResultTypes()
		if len(plan.MergeOrdering.Columns) == 0 {
			// There is no merge ordering, so we return the schema as is.
			return typs
		}
		res := make([]*types.T, 0, len(plan.PlanToStreamColMap))
		// Iterate over PlanToStreamColMap and include all columns that are
		// needed by the consumer.
		for _, colIdx := range plan.PlanToStreamColMap {
			if colIdx != -1 {
				res = append(res, typs[colIdx])
			}
		}
		return res
	}
	left, right := getTypesIgnoreMergeOrdering(leftPlan), getTypesIgnoreMergeOrdering(rightPlan)
	if len(left) != len(right) {
		return nil, errors.Errorf("ResultTypes length mismatch: %d and %d", len(left), len(right))
	}
	merged := make([]*types.T, len(left))
	for i := range left {
		leftType, rightType := left[i], right[i]
		if rightType.Family() == types.UnknownFamily {
			merged[i] = leftType
		} else if leftType.Family() == types.UnknownFamily {
			merged[i] = rightType
		} else if leftType.Equivalent(rightType) {
			// The types are equivalent for the purpose of UNION. Precision,
			// Width, Oid, etc. do not affect the merging of values.
			merged[i] = leftType
		} else {
			return nil, errors.Errorf(
				"conflicting ColumnTypes: %s and %s", leftType.DebugString(), rightType.DebugString())
		}
	}
	updateUnknownTypesForSetOp(leftPlan, merged)
	updateUnknownTypesForSetOp(rightPlan, merged)
	return merged, nil
}

// updateUnknownTypesForSetOp modifies plan's output types of the
// types.UnknownFamily type family to be of the corresponding Tuple type coming
// from the merged types. This is needed because at the moment the execbuilder
// is not able to plan casts to tuples.
//
// This method is intended to be used only for planning of set operations.
// TODO(yuzefovich): remove this once the execbuilder plans casts to tuples.
func updateUnknownTypesForSetOp(plan *PhysicalPlan, merged []*types.T) {
	currentTypes := plan.GetResultTypes()
	for i := range merged {
		if merged[i].Family() == types.TupleFamily && currentTypes[i].Family() == types.UnknownFamily {
			for _, procIdx := range plan.ResultRouters {
				plan.Processors[procIdx].Spec.ResultTypes[i] = merged[i]
			}
		}
	}
}
