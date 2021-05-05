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
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
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
	// All operators can provide the Presentation and LimitHint properties, so no
	// need to check for that.
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

	// ScalarExprs don't support required physical properties; don't build
	// physical properties for them.
	if _, ok := parent.Child(nth).(opt.ScalarExpr); ok {
		return mem.InternPhysicalProps(&childProps)
	}

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

	switch parent.Op() {
	case opt.LimitOp:
		if constLimit, ok := parent.(*memo.LimitExpr).Limit.(*memo.ConstExpr); ok {
			childProps.LimitHint = float64(*constLimit.Value.(*tree.DInt))
			if childProps.LimitHint <= 0 {
				childProps.LimitHint = 1
			}
		}
	case opt.OffsetOp:
		if parentProps.LimitHint == 0 {
			break
		}
		if constOffset, ok := parent.(*memo.OffsetExpr).Offset.(*memo.ConstExpr); ok {
			childProps.LimitHint = parentProps.LimitHint + float64(*constOffset.Value.(*tree.DInt))
			if childProps.LimitHint <= 0 {
				childProps.LimitHint = 1
			}
		}

	case opt.IndexJoinOp:
		// For an index join, every input row results in exactly one output row.
		childProps.LimitHint = parentProps.LimitHint

	case opt.ExceptOp, opt.ExceptAllOp, opt.IntersectOp, opt.IntersectAllOp,
		opt.UnionOp, opt.UnionAllOp, opt.LocalityOptimizedSearchOp:
		// TODO(celine): Set operation limits need further thought; for example,
		// the right child of an ExceptOp should not be limited.
		childProps.LimitHint = parentProps.LimitHint

	case opt.DistinctOnOp:
		distinctCount := parent.(memo.RelExpr).Relational().Stats.RowCount
		if parentProps.LimitHint > 0 {
			childProps.LimitHint = distinctOnLimitHint(distinctCount, parentProps.LimitHint)
		}

	case opt.SelectOp, opt.LookupJoinOp:
		// These operations are assumed to produce a constant number of output rows
		// for each input row, independent of already-processed rows.
		outputRows := parent.(memo.RelExpr).Relational().Stats.RowCount
		if outputRows == 0 || outputRows < parentProps.LimitHint {
			break
		}
		if input, ok := parent.Child(nth).(memo.RelExpr); ok {
			inputRows := input.Relational().Stats.RowCount
			switch parent.Op() {
			case opt.SelectOp:
				// outputRows / inputRows is roughly the number of output rows produced
				// for each input row. Reduce the number of required input rows so that
				// the expected number of output rows is equal to the parent limit hint.
				childProps.LimitHint = parentProps.LimitHint * inputRows / outputRows
			case opt.LookupJoinOp:
				childProps.LimitHint = lookupJoinInputLimitHint(inputRows, outputRows, parentProps.LimitHint)
			}
		}

	case opt.OrdinalityOp, opt.ProjectOp, opt.ProjectSetOp:
		childProps.LimitHint = parentProps.LimitHint
	}

	if childProps.LimitHint < 0 {
		panic(errors.AssertionFailedf("negative limit hint"))
	}

	// If properties haven't changed, no need to re-intern them.
	if childProps.Equals(parentProps) {
		return parentProps
	}

	return mem.InternPhysicalProps(&childProps)
}

// distinctOnLimitHint returns a limit hint for the distinct operation. Given a
// table with distinctCount distinct rows, distinctOnLimitHint will return an
// estimated number of rows to scan that in most cases will yield at least
// neededRows distinct rows while still substantially reducing the number of
// unnecessarily scanned rows.
//
// Assume that when examining a row, each of the distinctCount possible values
// has an equal probability of appearing. The expected number of rows that must
// be examined to collect neededRows distinct rows is
//
// E[examined rows] = distinctCount * (H_{distinctCount} - H_{distinctCount-neededRows})
//
// where distinctCount > neededRows and H_{i} is the ith harmonic number. This
// is a variation on the coupon collector's problem:
// https://en.wikipedia.org/wiki/Coupon_collector%27s_problem
//
// Since values are not uniformly distributed in practice, the limit hint is
// calculated by multiplying E[examined rows] by an experimentally-chosen factor
// to provide a small overestimate of the actual number of rows needed in most
// cases.
//
// This method is least accurate when attempting to return all or nearly all the
// distinct values in the table, since the actual distribution of values becomes
// the primary factor in how long it takes to "collect" the least-likely values.
// As a result, cases where this limit hint may be poor (too low or more than
// twice as high as needed) tend to occur when distinctCount is very close to
// neededRows.
func distinctOnLimitHint(distinctCount, neededRows float64) float64 {
	// The harmonic function below is not intended for values under 1 (for one,
	// it's not monotonic until 0.5); make sure we never return negative results.
	if neededRows >= distinctCount-1.0 {
		return 0
	}

	// Return an approximation of the nth harmonic number.
	H := func(n float64) float64 {
		// Euler–Mascheroni constant; this is included for clarity but is canceled
		// out in our formula below.
		const gamma = 0.5772156649
		return math.Log(n) + gamma + 1/(2*n)
	}

	// Coupon collector's estimate, for a uniformly-distributed table.
	uniformPrediction := distinctCount * (H(distinctCount) - H(distinctCount-neededRows))

	// This multiplier was chosen based on simulating the distinct operation on
	// hundreds of thousands of nonuniformly distributed tables with values of
	// neededRows and distinctCount ranging between 1 and 1000.
	multiplier := 0.15*neededRows/(distinctCount-neededRows) + 1.2

	// In 91.6% of trials, this scaled estimate was between a 0% and 30%
	// overestimate, and in 97.5% it was between a 0% and 100% overestimate.
	//
	// In 1.8% of tests, the prediction was for an insufficient number of rows, and
	// in 0.7% of tests, the predicted number of rows was more than twice the actual
	// number required.
	return uniformPrediction * multiplier
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
