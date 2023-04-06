// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// CanProvide returns true if the given operator returns rows that can
// satisfy the given required ordering.
func CanProvide(expr memo.RelExpr, required *props.OrderingChoice) bool {
	if required.Any() {
		return true
	}
	if buildutil.CrdbTestBuild {
		checkRequired(expr, required)
	}
	return funcMap[expr.Op()].canProvideOrdering(expr, required)
}

// CanEnforce returns true if the output of the given operator can be sorted
// in order to satisfy the given required ordering.
func CanEnforce(expr memo.RelExpr, required *props.OrderingChoice) bool {
	if required.Any() {
		return false
	}
	if buildutil.CrdbTestBuild {
		checkRequired(expr, required)
	}
	switch t := expr.(type) {
	case *memo.ExplainExpr:
		return false
	case *memo.LookupJoinExpr:
		// For paired joins we use a boolean continuation column to handle false
		// positive matches in the first join. This relies on the ordering being
		// unchanged between the first and second joins, so adding a sort on top
		// of this expression could lead to incorrect results.
		return !t.IsFirstJoinInPairedJoiner
	case *memo.InvertedJoinExpr:
		return !t.IsFirstJoinInPairedJoiner
	}
	return true
}

// BuildChildRequired returns the ordering that must be required of its
// given child in order to satisfy a required ordering. Can only be called if
// CanProvide is true for the required ordering.
func BuildChildRequired(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	result := funcMap[parent.Op()].buildChildReqOrdering(parent, required, childIdx)
	if buildutil.CrdbTestBuild && !result.Any() {
		checkRequired(parent.Child(childIdx).(memo.RelExpr), &result)
	}
	return result
}

// BuildProvided returns a specific ordering that the operator provides (and which
// must be maintained on the results during distributed execution).
//
// The returned ordering, in conjunction with the operator's functional
// dependencies, must intersect the required ordering.
//
// A best-effort attempt is made to make the provided orderings as simple as
// possible (while still satisfying the required ordering).
//
// For example, if we scan an index on x,y,z with required ordering "+y opt(x)",
// the provided ordering is "+x,+y". If we scan the same index with constraint
// x=1, the provided ordering is "+y".
//
// This function assumes that the provided orderings have already been set in
// the children of the expression.
func BuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	if required.Any() {
		return nil
	}
	provided := funcMap[expr.Op()].buildProvidedOrdering(expr, required)
	return finalizeProvided(provided, required, expr.Relational().OutputCols)
}

type funcs struct {
	canProvideOrdering func(expr memo.RelExpr, required *props.OrderingChoice) bool

	buildChildReqOrdering func(
		parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
	) props.OrderingChoice

	buildProvidedOrdering func(
		expr memo.RelExpr, required *props.OrderingChoice,
	) opt.Ordering
}

var funcMap [opt.NumOperators]funcs

func init() {
	for _, op := range opt.RelationalOperators {
		funcMap[op] = funcs{
			canProvideOrdering:    canNeverProvideOrdering,
			buildChildReqOrdering: noChildReqOrdering,
			buildProvidedOrdering: noProvidedOrdering,
		}
	}
	funcMap[opt.ScanOp] = funcs{
		canProvideOrdering:    scanCanProvideOrdering,
		buildChildReqOrdering: noChildReqOrdering,
		buildProvidedOrdering: scanBuildProvided,
	}
	funcMap[opt.SelectOp] = funcs{
		canProvideOrdering:    selectCanProvideOrdering,
		buildChildReqOrdering: selectBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.ProjectOp] = funcs{
		canProvideOrdering:    projectCanProvideOrdering,
		buildChildReqOrdering: projectBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.UnionOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: useRequiredForProvidedOrdering,
	}
	funcMap[opt.UnionAllOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: useRequiredForProvidedOrdering,
	}
	funcMap[opt.IntersectOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: useRequiredForProvidedOrdering,
	}
	funcMap[opt.IntersectAllOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: useRequiredForProvidedOrdering,
	}
	funcMap[opt.ExceptOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: useRequiredForProvidedOrdering,
	}
	funcMap[opt.ExceptAllOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: useRequiredForProvidedOrdering,
	}
	funcMap[opt.IndexJoinOp] = funcs{
		canProvideOrdering:    indexJoinCanProvideOrdering,
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.LookupJoinOp] = funcs{
		canProvideOrdering:    lookupJoinCanProvideOrdering,
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
		buildProvidedOrdering: lookupJoinBuildProvided,
	}
	funcMap[opt.InvertedJoinOp] = funcs{
		canProvideOrdering:    invertedJoinCanProvideOrdering,
		buildChildReqOrdering: invertedJoinBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.OrdinalityOp] = funcs{
		canProvideOrdering:    ordinalityCanProvideOrdering,
		buildChildReqOrdering: ordinalityBuildChildReqOrdering,
		buildProvidedOrdering: ordinalityBuildProvided,
	}
	funcMap[opt.MergeJoinOp] = funcs{
		canProvideOrdering:    mergeJoinCanProvideOrdering,
		buildChildReqOrdering: mergeJoinBuildChildReqOrdering,
		buildProvidedOrdering: mergeJoinBuildProvided,
	}
	funcMap[opt.LimitOp] = funcs{
		canProvideOrdering:    limitOrOffsetCanProvideOrdering,
		buildChildReqOrdering: limitOrOffsetBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.OffsetOp] = funcs{
		canProvideOrdering:    limitOrOffsetCanProvideOrdering,
		buildChildReqOrdering: limitOrOffsetBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.TopKOp] = funcs{
		canProvideOrdering:    topKCanProvideOrdering,
		buildChildReqOrdering: topKBuildChildReqOrdering,
		buildProvidedOrdering: topKBuildProvided,
	}
	funcMap[opt.ScalarGroupByOp] = funcs{
		// ScalarGroupBy always has exactly one result; any required ordering should
		// have been simplified to Any (unless normalization rules are disabled).
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: scalarGroupByBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.GroupByOp] = funcs{
		canProvideOrdering:    groupByCanProvideOrdering,
		buildChildReqOrdering: groupByBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.DistinctOnOp] = funcs{
		canProvideOrdering:    distinctOnCanProvideOrdering,
		buildChildReqOrdering: distinctOnBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.EnsureDistinctOnOp] = funcs{
		canProvideOrdering:    distinctOnCanProvideOrdering,
		buildChildReqOrdering: distinctOnBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.UpsertDistinctOnOp] = funcs{
		canProvideOrdering:    distinctOnCanProvideOrdering,
		buildChildReqOrdering: distinctOnBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.EnsureUpsertDistinctOnOp] = funcs{
		canProvideOrdering:    distinctOnCanProvideOrdering,
		buildChildReqOrdering: distinctOnBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.SortOp] = funcs{
		canProvideOrdering:    nil, // should never get called
		buildChildReqOrdering: sortBuildChildReqOrdering,
		buildProvidedOrdering: useRequiredForProvidedOrdering,
	}
	funcMap[opt.DistributeOp] = funcs{
		canProvideOrdering:    distributeCanProvideOrdering,
		buildChildReqOrdering: distributeBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.InsertOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.UpdateOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.UpsertOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.DeleteOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: firstChildProvidedOrdering,
	}
	funcMap[opt.ExplainOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: explainBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.AlterTableSplitOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: alterTableSplitBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.AlterTableUnsplitOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: alterTableUnsplitBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.AlterTableRelocateOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: alterTableRelocateBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.AlterRangeRelocateOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: alterRangeRelocateBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.ControlJobsOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: controlJobsBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.CancelQueriesOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: cancelQueriesBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.CancelSessionsOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: cancelSessionsBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.ExportOp] = funcs{
		canProvideOrdering:    canNeverProvideOrdering,
		buildChildReqOrdering: exportBuildChildReqOrdering,
		buildProvidedOrdering: noProvidedOrdering,
	}
	funcMap[opt.WithOp] = funcs{
		canProvideOrdering:    withCanProvideOrdering,
		buildChildReqOrdering: withBuildChildReqOrdering,
		buildProvidedOrdering: withBuildProvided,
	}
}

func canNeverProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	return false
}

func noChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return props.OrderingChoice{}
}

func noProvidedOrdering(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	return nil
}

func firstChildProvidedOrdering(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	return expr.Child(0).(memo.RelExpr).ProvidedPhysical().Ordering
}

func useRequiredForProvidedOrdering(
	expr memo.RelExpr, required *props.OrderingChoice,
) opt.Ordering {
	return required.ToOrdering()
}

// finalizeProvided ensures that the provided ordering satisfies the following
// properties:
//  1. The provided ordering can be proven to satisfy the required ordering
//     without the use of additional (e.g. functional dependency) information.
//  2. The provided ordering is simplified, such that it does not contain any
//     columns from the required ordering optional set.
//  3. The provided ordering only refers to output columns for the operator.
//
// Step (1) is necessary because it is possible for child operators to have
// different functional dependency information than their parents as well as
// different output columns. We have to protect against the case where a parent
// operator cannot prove that its child's provided ordering satisfies its
// required ordering. The other steps allow the provided-building logic for each
// operator to ignore concerns of remapping and simplifying a provided ordering.
//
// finalizeProvided makes the assumption that the given provided ordering is
// already known to satisfy the required ordering (even if this can only be
// proven using extra functional dependencies). finalizeProvided does not
// attempt to prove that this is the case - that is the responsibility of the
// logic that builds the original provided ordering.
func finalizeProvided(
	provided opt.Ordering, required *props.OrderingChoice, outCols opt.ColSet,
) (newProvided opt.Ordering) {
	// First check if the given provided is already suitable.
	providedCols := provided.ColSet()
	if len(provided) == len(required.Columns) && providedCols.SubsetOf(outCols) {
		needsRemap := false
		for i := range provided {
			choice, ordCol := required.Columns[i], provided[i]
			if !choice.Group.Contains(ordCol.ID()) || choice.Descending != ordCol.Descending() {
				needsRemap = true
				break
			}
		}
		if !needsRemap {
			return provided
		}
	}
	// Build a new provided ordering that satisfies the required properties.
	newProvided = make(opt.Ordering, len(required.Columns))
	for i, choice := range required.Columns {
		group := choice.Group.Intersection(outCols)
		if group.Intersects(providedCols) {
			// Prefer using columns from the provided ordering if possible. It's ok to
			// use a different column from the OrderingChoice if not because the
			// provided ordering is already known to satisfy the required ordering,
			// even if we cannot prove it here.
			group.IntersectionWith(providedCols)
		}
		col, ok := group.Next(0)
		if !ok {
			panic(errors.AssertionFailedf("no output column equivalent to %d", redact.Safe(col)))
		}
		newProvided[i] = opt.MakeOrderingColumn(col, choice.Descending)
	}
	return newProvided
}

// checkRequired runs sanity checks on the ordering required of an operator.
func checkRequired(expr memo.RelExpr, required *props.OrderingChoice) {
	rel := expr.Relational()

	// Verify that the ordering only refers to output columns.
	if !required.SubsetOfCols(rel.OutputCols) {
		panic(errors.AssertionFailedf("required ordering refers to non-output columns (op %s)", redact.Safe(expr.Op())))
	}

	// Verify that columns in a column group are equivalent.
	for i := range required.Columns {
		c := &required.Columns[i]
		if !c.Group.SubsetOf(rel.FuncDeps.ComputeEquivGroup(c.AnyID())) && !rel.Cardinality.IsZero() {
			panic(errors.AssertionFailedf(
				"ordering column group %s contains non-equivalent columns (op %s)",
				c.Group, expr.Op(),
			))
		}
	}
}
