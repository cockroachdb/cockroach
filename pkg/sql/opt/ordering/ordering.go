// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// CanProvide returns true if the given operator returns rows that can
// satisfy the given required ordering.
func CanProvide(mem *memo.Memo, expr memo.RelExpr, required *props.OrderingChoice) bool {
	if required.Any() {
		return true
	}
	if buildutil.CrdbTestBuild {
		checkRequired(expr, required)
	}
	// Special cases for operators that need to access the memo.
	switch expr.Op() {
	case opt.ScanOp:
		return scanCanProvideOrdering(mem, expr, required)
	case opt.LookupJoinOp:
		return lookupJoinCanProvideOrdering(mem, expr, required)
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
	mem *memo.Memo, parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	var result props.OrderingChoice
	switch parent.Op() {
	// Special cases for operators that need to access the memo.
	case opt.SelectOp:
		result = selectBuildChildReqOrdering(mem, parent, required, childIdx)
	default:
		result = funcMap[parent.Op()].buildChildReqOrdering(parent, required, childIdx)
	}
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
func BuildProvided(
	evalCtx *eval.Context, mem *memo.Memo, expr memo.RelExpr, required *props.OrderingChoice,
) opt.Ordering {
	if required.Any() {
		return nil
	}
	var provided opt.Ordering
	switch expr.Op() {
	// Special cases for operators that need to access the memo.
	case opt.InsertOp, opt.UpdateOp, opt.UpsertOp, opt.DeleteOp:
		provided = mutationBuildProvided(mem, expr)
	case opt.LookupJoinOp:
		provided = lookupJoinBuildProvided(mem, expr, required)
	case opt.ScanOp:
		provided = scanBuildProvided(mem, expr, required)
	default:
		provided = funcMap[expr.Op()].buildProvidedOrdering(expr, required)
	}
	if evalCtx.SessionData().OptimizerUseProvidedOrderingFix {
		provided = finalizeProvided(provided, required, expr.Relational().OutputCols)
	}

	if buildutil.CrdbTestBuild {
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
		canProvideOrdering:    nil, // Called directly in CanProvide.
		buildChildReqOrdering: noChildReqOrdering,
		buildProvidedOrdering: nil, // Called directly in BuildProvided.
	}
	funcMap[opt.SelectOp] = funcs{
		canProvideOrdering:    selectCanProvideOrdering,
		buildChildReqOrdering: nil, // Called directly in BuildChildRequired.
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
		canProvideOrdering:    indexJoinCanProvideOrdering,
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
		buildProvidedOrdering: indexJoinBuildProvided,
	}
	funcMap[opt.LookupJoinOp] = funcs{
		canProvideOrdering:    nil, // Called directly in CanProvide.
		buildChildReqOrdering: lookupOrIndexJoinBuildChildReqOrdering,
		buildProvidedOrdering: nil, // Called directly in BuildProvided.
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
	funcMap[opt.DistributeOp] = funcs{
		canProvideOrdering:    distributeCanProvideOrdering,
		buildChildReqOrdering: distributeBuildChildReqOrdering,
		buildProvidedOrdering: distributeBuildProvided,
	}
	funcMap[opt.InsertOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: nil, // Called directly from BuildProvided.
	}
	funcMap[opt.UpdateOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: nil, // Called directly from BuildProvided.
	}
	funcMap[opt.UpsertOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: nil, // Called directly from BuildProvided.
	}
	funcMap[opt.DeleteOp] = funcs{
		canProvideOrdering:    mutationCanProvideOrdering,
		buildChildReqOrdering: mutationBuildChildReqOrdering,
		buildProvidedOrdering: nil, // Called directly from BuildProvided.
	}
	funcMap[opt.LockOp] = funcs{
		canProvideOrdering:    lockCanProvideOrdering,
		buildChildReqOrdering: lockBuildChildReqOrdering,
		buildProvidedOrdering: lockBuildProvided,
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
			equivCols := fds.ComputeEquivClosureNoCopy(opt.MakeColSet(col))
			remappedCol, ok := equivCols.Next(0)
			if !ok {
				panic(errors.AssertionFailedf("no output column equivalent to %d", redact.Safe(col)))
			}
			// If the column is in the output use that.
			remappedColFromOutput, ok := equivCols.Intersection(outCols).Next(0)
			if ok {
				remappedCol = remappedColFromOutput
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

// finalizeProvided ensures that the provided ordering satisfies the following
// properties:
//  1. The provided ordering can be proven to satisfy the required ordering
//     without the use of additional (e.g. functional dependency) information.
//  2. The provided ordering is simplified, such that it does not contain any
//     columns from the required ordering optional set.
//  3. The provided ordering only refers to output columns for the operator.
//
// This step is necessary because it is possible for child operators to have
// different functional dependency information than their parents as well as
// different output columns. We have to protect against the case where a parent
// operator cannot prove that its child's provided ordering satisfies its
// required ordering.
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
	newProvided = make(opt.Ordering, len(required.Columns))
	for i, choice := range required.Columns {
		group := choice.Group.Intersection(outCols)
		if group.Intersects(providedCols) {
			// Prefer using columns from the provided ordering if possible.
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
}
