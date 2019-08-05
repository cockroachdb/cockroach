// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

// CanProvidePhysicalProps returns true if the given expression can provide the
// required physical properties. The optimizer uses this to determine whether an
// expression provides a required physical property. If it does not, then the
// optimizer inserts an enforcer operator that is able to provide it.
//
// Some operators, like Select and Project, may not directly provide a required
// physical property, but do "pass through" the requirement to their input.
// Operators that do this should return true from the appropriate canProvide
// method and then pass through that property in the buildChildPhysicalProps
// method.
func CanProvidePhysicalProps(e memo.RelExpr, required *physical.Required) bool {
	// All operators can provide the Presentation property, so no need to check
	// for that.
	return e.Op() == opt.SortOp || ordering.CanProvide(e, &required.Ordering)
}

// BuildChildPhysicalProps returns the set of physical properties required of
// the nth child, based upon the properties required of the parent. For example,
// the Project operator passes through any ordering requirement to its child,
// but provides any presentation requirement.
//
// The childProps argument is allocated once by the caller and can be reused
// repeatedly as physical properties are derived for each child. On each call,
// buildChildPhysicalProps updates the childProps argument.
func BuildChildPhysicalProps(
	mem *memo.Memo, parent memo.RelExpr, nth int, parentProps *physical.Required,
) *physical.Required {
	var childProps physical.Required

	// Most operations don't require a presentation of their input; these are the
	// exceptions.
	switch parent.Op() {
	case opt.ExplainOp:
		childProps.Presentation = parent.(*memo.ExplainExpr).Props.Presentation
	case opt.AlterTableSplitOp:
		childProps.Presentation = parent.(*memo.AlterTableSplitExpr).Props.Presentation
	case opt.AlterTableUnsplitOp:
		childProps.Presentation = parent.(*memo.AlterTableUnsplitExpr).Props.Presentation
	case opt.AlterTableRelocateOp:
		childProps.Presentation = parent.(*memo.AlterTableRelocateExpr).Props.Presentation
	case opt.ControlJobsOp:
		childProps.Presentation = parent.(*memo.ControlJobsExpr).Props.Presentation
	case opt.CancelQueriesOp:
		childProps.Presentation = parent.(*memo.CancelQueriesExpr).Props.Presentation
	case opt.CancelSessionsOp:
		childProps.Presentation = parent.(*memo.CancelSessionsExpr).Props.Presentation
	case opt.ExportOp:
		childProps.Presentation = parent.(*memo.ExportExpr).Props.Presentation
	}

	childProps.Ordering = ordering.BuildChildRequired(parent, &parentProps.Ordering, nth)

	// If properties haven't changed, no need to re-intern them.
	if childProps.Equals(parentProps) {
		return parentProps
	}

	return mem.InternPhysicalProps(&childProps)
}

// BuildChildPhysicalPropsScalar is like BuildChildPhysicalProps, but for
// when the parent is a scalar expression.
func BuildChildPhysicalPropsScalar(mem *memo.Memo, parent opt.Expr, nth int) *physical.Required {
	var childProps physical.Required
	switch parent.Op() {
	case opt.ArrayFlattenOp:
		if nth == 0 {
			af := parent.(*memo.ArrayFlattenExpr)
			childProps.Ordering.FromOrdering(af.Ordering)
			// ArrayFlatten might have extra ordering columns. Use the Presentation property
			// to get rid of them.
			childProps.Presentation = physical.Presentation{
				opt.AliasedColumn{
					// Keep the existing label for the column.
					Alias: mem.Metadata().ColumnMeta(af.RequestedCol).Alias,
					ID:    af.RequestedCol,
				},
			}
		}
	default:
		return physical.MinRequired
	}
	return mem.InternPhysicalProps(&childProps)
}
