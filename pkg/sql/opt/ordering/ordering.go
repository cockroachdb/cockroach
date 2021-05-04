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
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// CanProvide returns true if the given operator returns rows that can
// satisfy the given required ordering.
func CanProvide(expr memo.RelExpr, required *props.OrderingChoice) bool {
	if required.Any() {
		return true
	}
	if util.CrdbTestBuild {
		checkRequired(expr, required)
	}
	return funcMap[expr.Op()].canProvideOrdering(expr, required)
}

// BuildChildRequired returns the ordering that must be required of its
// given child in order to satisfy a required ordering. Can only be called if
// CanProvide is true for the required ordering.
func BuildChildRequired(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	result := funcMap[parent.Op()].buildChildReqOrdering(parent, required, childIdx)
	if util.CrdbTestBuild && !result.Any() {
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

	if util.CrdbTestBuild {
		checkProvided(expr, required, provided)
	}

	return provided
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
		buildProvidedOrdering: selectBuildProvided,
	}
	funcMap[opt.ProjectOp] = funcs{
		canProvideOrdering:    projectCanProvideOrdering,
		buildChildReqOrdering: projectBuildChildReqOrdering,
		buildProvidedOrdering: projectBuildProvided,
	}
	funcMap[opt.UnionOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: setOpBuildProvided,
	}
	funcMap[opt.UnionAllOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: setOpBuildProvided,
	}
	funcMap[opt.IntersectOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: setOpBuildProvided,
	}
	funcMap[opt.IntersectAllOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: setOpBuildProvided,
	}
	funcMap[opt.ExceptOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: setOpBuildProvided,
	}
	funcMap[opt.ExceptAllOp] = funcs{
		canProvideOrdering:    setOpCanProvideOrdering,
		buildChildReqOrdering: setOpBuildChildReqOrdering,
		buildProvidedOrdering: setOpBuildProvided,
	}
	funcMap[opt.IndexJoinOp] = funcs{
		canProvideOrdering:    lookupOrIndexJoinCanProvideOrdering,
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
		buildProvidedOrdering: indexJoinBuildProvided,
	}
	funcMap[opt.LookupJoinOp] = funcs{
		canProvideOrdering:    lookupOrIndexJoinCanProvideOrdering,
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
		buildProvidedOrdering: lookupJoinBuildProvided,
	}
	funcMap[opt.InvertedJoinOp] = funcs{
		canProvideOrdering:    invertedJoinCanProvideOrdering,
		buildChildReqOrdering: invertedJoinBuildChildReqOrdering,
		buildProvidedOrdering: invertedJoinBuildProvided,
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
		buildProvidedOrdering: limitOrOffsetBuildProvided,
	}
	funcMap[opt.OffsetOp] = funcs{
		canProvideOrdering:    limitOrOffsetCanProvideOrdering,
		buildChildReqOrdering: limitOrOffsetBuildChildReqOrdering,
		buildProvidedOrdering: limitOrOffsetBuildProvided,
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
		buildProvidedOrdering: groupByBuildProvided,
	}
	funcMap[opt.DistinctOnOp] = funcs{
		canProvideOrdering:    distinctOnCanProvideOrdering,
		buildChildReqOrdering: distinctOnBuildChildReqOrdering,
		buildProvidedOrdering: distinctOnBuildProvided,
	}
	funcMap[opt.EnsureDistinctOnOp] = funcs{
		canProvideOrdering:    distinctOnCanProvideOrdering,
		buildChildReqOrdering: distinctOnBuildChildReqOrdering,
		buildProvidedOrdering: distinctOnBuildProvided,
	}
	funcMap[opt.UpsertDistinctOnOp] = funcs{
		canProvideOrdering:    distinctOnCanProvideOrdering,
		buildChildReqOrdering: distinctOnBuildChildReqOrdering,
		buildProvidedOrdering: distinctOnBuildProvided,
	}
	funcMap[opt.EnsureUpsertDistinctOnOp] = funcs{
		canProvideOrdering:    distinctOnCanProvideOrdering,
		buildChildReqOrdering: distinctOnBuildChildReqOrdering,
		buildProvidedOrdering: distinctOnBuildProvided,
	}
	funcMap[opt.SortOp] = funcs{
		canProvideOrdering:    nil, // should never get called
		buildChildReqOrdering: sortBuildChildReqOrdering,
		buildProvidedOrdering: sortBuildProvided,
	}
	funcMap[opt.InsertOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: mutationBuildProvided,
	}
	funcMap[opt.UpdateOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: mutationBuildProvided,
	}
	funcMap[opt.UpsertOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: mutationBuildProvided,
	}
	funcMap[opt.DeleteOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: mutationBuildProvided,
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

// remapProvided remaps columns in a provided ordering (according to the given
// FDs) so that it only refers to columns in the given outCols set. It also
// removes any columns that are redundant according to the FDs.
//
// Can only be called if the provided ordering can be remapped.
//
// Does not modify <provided> in place, but it can return the same slice.
func remapProvided(provided opt.Ordering, fds *props.FuncDepSet, outCols opt.ColSet) opt.Ordering {
	if len(provided) == 0 {
		return nil
	}

	// result is nil until we determine that we need to make a copy.
	var result opt.Ordering

	// closure is the set of columns that are functionally determined by the
	// columns in provided[:i].
	closure := fds.ComputeClosure(opt.ColSet{})
	for i := range provided {
		col := provided[i].ID()
		if closure.Contains(col) {
			// At the level of the new operator, this column is redundant.
			if result == nil {
				result = make(opt.Ordering, i, len(provided))
				copy(result, provided)
			}
			continue
		}
		if outCols.Contains(col) {
			if result != nil {
				result = append(result, provided[i])
			}
		} else {
			equivCols := fds.ComputeEquivClosure(opt.MakeColSet(col))
			remappedCol, ok := equivCols.Intersection(outCols).Next(0)
			if !ok {
				panic(errors.AssertionFailedf("no output column equivalent to %d", log.Safe(col)))
			}
			if result == nil {
				result = make(opt.Ordering, i, len(provided))
				copy(result, provided)
			}
			result = append(result, opt.MakeOrderingColumn(
				remappedCol, provided[i].Descending(),
			))
		}
		closure.Add(col)
		closure = fds.ComputeClosure(closure)
	}
	if result == nil {
		return provided
	}
	return result
}

// trimProvided returns the smallest prefix of <provided> that is sufficient to
// satisfy <required> (in conjunction with the FDs).
//
// This is useful because in a distributed setting execution is configured to
// maintain the provided ordering when merging results from multiple nodes, and
// we don't want to make needless comparisons.
func trimProvided(
	provided opt.Ordering, required *props.OrderingChoice, fds *props.FuncDepSet,
) opt.Ordering {
	if len(provided) == 0 {
		return nil
	}
	// closure is the set of columns that are functionally determined by the
	// columns in provided[:provIdx].
	closure := fds.ComputeClosure(opt.ColSet{})
	provIdx := 0
	for reqIdx := range required.Columns {
		c := &required.Columns[reqIdx]
		// Consume columns from the provided ordering until their closure intersects
		// the required group.
		for !closure.Intersects(c.Group) {
			closure.Add(provided[provIdx].ID())
			closure = fds.ComputeClosure(closure)
			provIdx++
			if provIdx == len(provided) {
				return provided
			}
		}
	}
	return provided[:provIdx]
}

// checkRequired runs sanity checks on the ordering required of an operator.
func checkRequired(expr memo.RelExpr, required *props.OrderingChoice) {
	rel := expr.Relational()

	// Verify that the ordering only refers to output columns.
	if !required.SubsetOfCols(rel.OutputCols) {
		panic(errors.AssertionFailedf("required ordering refers to non-output columns (op %s)", log.Safe(expr.Op())))
	}

	// Verify that columns in a column group are equivalent.
	for i := range required.Columns {
		c := &required.Columns[i]
		if !c.Group.SubsetOf(rel.FuncDeps.ComputeEquivGroup(c.AnyID())) {
			panic(errors.AssertionFailedf(
				"ordering column group %s contains non-equivalent columns (op %s)",
				c.Group, expr.Op(),
			))
		}
	}
}

// checkProvided runs sanity checks on a provided ordering.
func checkProvided(expr memo.RelExpr, required *props.OrderingChoice, provided opt.Ordering) {
	// The provided ordering must refer only to output columns.
	if outCols := expr.Relational().OutputCols; !provided.ColSet().SubsetOf(outCols) {
		panic(errors.AssertionFailedf(
			"provided %s must refer only to output columns %s", provided, outCols,
		))
	}

	// TODO(radu): this check would be nice to have, but it is too strict. In some
	// cases, child expressions created during exploration (like constrained
	// scans) have FDs that are more restricted than what was known when the
	// parent expression was constructed. Related to #32320.
	if false {
		// The provided ordering must intersect the required ordering, after FDs are
		// applied.
		fds := &expr.Relational().FuncDeps
		r := required.Copy()
		r.Simplify(fds)
		var p props.OrderingChoice
		p.FromOrdering(provided)
		p.Simplify(fds)
		if !r.Any() && (p.Any() || !p.Intersects(&r)) {
			panic(errors.AssertionFailedf(
				"provided %s does not intersect required %s (FDs: %s)", provided, required, fds,
			))
		}
	}

	// The provided ordering should not have unnecessary columns.
	fds := &expr.Relational().FuncDeps
	if trimmed := trimProvided(provided, required, fds); len(trimmed) != len(provided) {
		panic(errors.AssertionFailedf(
			"provided %s can be trimmed to %s (FDs: %s)", log.Safe(provided), log.Safe(trimmed), log.Safe(fds),
		))
	}
}
