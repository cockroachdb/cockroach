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
)

func indexJoinCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// IndexJoin can pass through its ordering if the ordering depends only on
	// columns present in the input.
	inputCols := expr.Child(0).(memo.RelExpr).Relational().OutputCols
	return required.CanProjectCols(inputCols)
}

func lookupJoinCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	lookupJoin := expr.(*memo.LookupJoinExpr)

	// LookupJoin can pass through its ordering if the ordering depends only on
	// columns present in the input.
	inputCols := lookupJoin.Input.Relational().OutputCols
	canProjectCols := required.CanProjectCols(inputCols)

	if canProjectCols && lookupJoin.IsSecondJoinInPairedJoiner {
		// Can only pass through ordering if the ordering can be provided by the
		// child, since we don't want a sort to be interposed between the child
		// and this join.
		//
		// We may need to remove ordering columns that are not output by the input
		// expression. This results in an equivalent ordering, but with fewer
		// options in the OrderingChoice.
		child := lookupJoin.Input
		res := projectOrderingToInput(child, required)
		// It is in principle possible that the lookup join has an ON condition that
		// forces an equality on two columns in the input. In this case we need to
		// trim the column groups to keep the ordering valid w.r.t the child FDs
		// (similar to Select).
		//
		// This case indicates that we didn't do a good job pushing down equalities
		// (see #36219), but it should be handled correctly here nevertheless.
		res = trimColumnGroups(&res, &child.Relational().FuncDeps)
		return CanProvide(child, &res)
	}
	if !canProjectCols {
		// It is not possible to serve the required ordering using only input
		// columns. However, the lookup join may be able to satisfy the required
		// ordering. This is possible if the output ordering consists of a series of
		// input columns that form a key over the input, followed by the index
		// columns in index order. Due to implementation details, currently the
		// index can only contain columns sorted in ascending order. The following
		// is a case where a lookup join could maintain an ordering over both input
		// and index columns:
		//   CREATE TABLE ab (a INT, b INT, PRIMARY KEY(a, b));
		//   CREATE TABLE xy (x INT, y INT, PRIMARY KEY(x, y));
		//   SELECT * FROM ab INNER LOOKUP JOIN xy ON a = x ORDER BY a, b, x, y;
		// Note that in this example the 'a' and 'b' columns form a key over the
		// input of the lookup join. Additionally, the 'x' column alone is not a key
		// for the 'xy' table, so each lookup may return multiple rows (which need
		// to be ordered among themselves). Since the postfix of the ordering that
		// references index columns is in index order (x, y), the lookup join in the
		// example can supply the ordering itself. On the other hand, switching 'b'
		// and 'y' in the ordering or removing 'b' would mean the query would
		// require a sort.
		var remainingRequired props.OrderingChoice
		inputOrdCols := required.Optional.Copy()
		for i := range required.Columns {
			if !required.Columns[i].Group.Intersects(inputCols) {
				// We have reached the end of the prefix of the required ordering that
				// can be projected by input columns. Keep track of the rest of the
				// columns that cannot be satisfied by an ordering on the input.
				remainingRequired.Columns = required.Columns[i:]
				remainingRequired.Optional = required.Optional.Copy()
				break
			}
			inputOrdCols.UnionWith(required.Columns[i].Group)
		}
		if !lookupJoin.Input.Relational().FuncDeps.ColsAreStrictKey(inputOrdCols) {
			// The index ordering is maintained for each input row separately, so the
			// ordering must form a key over the input.
			// TODO(drewk): it is possible to take advantage of the index ordering
			//  when the input ordering does not form a key over the input. In this
			//  case, we would require that the index ordering columns for a given
			//  input row are functionally determined by the input ordering columns.
			//  This would disqualify IN constraints and inequalities.
			return false
		}
		// The columns from the prefix of the required ordering satisfied by the
		// input are considered optional for the index ordering.
		remainingRequired.Optional.UnionWith(inputOrdCols)

		// Build an ordering that represents the index.
		idx := lookupJoin.Memo().Metadata().Table(lookupJoin.Table).Index(lookupJoin.Index)
		indexOrder := make(opt.Ordering, idx.KeyColumnCount())
		for i := 0; i < idx.KeyColumnCount(); i++ {
			if idx.Column(i).Descending {
				// Lookups cannot be served in index order when the index contains
				// descending columns.
				return false
			}
			idxColID := lookupJoin.Table.ColumnID(idx.Column(i).Ordinal())
			indexOrder[i] = opt.MakeOrderingColumn(idxColID, idx.Column(i).Descending)
		}
		// Check if the index ordering satisfies the postfix of the required
		// ordering that cannot be satisfied by the input.
		_, remaining := trySatisfyRequired(&remainingRequired, indexOrder)
		return remaining == nil
	}
	return canProjectCols
}

func lookupOrIndexJoinBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}
	child := parent.Child(0).(memo.RelExpr)

	if _, ok := parent.(*memo.LookupJoinExpr); ok {
		// We need to truncate the ordering columns to the prefix of those that can
		// project input columns without interleaving with index columns (see
		// maybeAddLookupOrdCols in lookupJoinBuildProvided).
		for i := range required.Columns {
			if !required.Columns[i].Group.Intersects(child.Relational().OutputCols) {
				// Shallow-copy the required properties and reslice the copy.
				newRequired := *required
				required = &newRequired
				required.Columns = required.Columns[:i]
				break
			}
		}
	}

	// We may need to remove ordering columns that are not output by the input
	// expression.
	res := projectOrderingToInput(child, required)
	// It is in principle possible that the lookup join has an ON condition that
	// forces an equality on two columns in the input. In this case we need to
	// trim the column groups to keep the ordering valid w.r.t the child FDs
	// (similar to Select).
	//
	// This case indicates that we didn't do a good job pushing down equalities
	// (see #36219), but it should be handled correctly here nevertheless.
	res = trimColumnGroups(&res, &child.Relational().FuncDeps)

	// The propagation of FDs might not be perfect; in that case we need to
	// simplify the required ordering, or risk passing through unnecessary columns
	// in provided orderings.
	if fds := &child.Relational().FuncDeps; res.CanSimplify(fds) {
		res = res.Copy()
		res.Simplify(fds)
	}
	return res
}

func indexJoinBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	// If an index join has a requirement on some input columns, those columns
	// must be output columns (or equivalent to them). We may still need to remap
	// using column equivalencies.
	indexJoin := expr.(*memo.IndexJoinExpr)
	rel := indexJoin.Relational()
	input := indexJoin.Input
	// The index join's FDs may not include all the necessary columns for
	// remapping, so we add the input's FDs as well. See `buildIndexJoinProps`.
	var fds props.FuncDepSet
	fds.CopyFrom(&input.Relational().FuncDeps)
	fds.AddFrom(&rel.FuncDeps)
	return remapProvided(input.ProvidedPhysical().Ordering, &fds, rel.OutputCols)
}

func lookupJoinBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	lookupJoin := expr.(*memo.LookupJoinExpr)
	childProvided := lookupJoin.Input.ProvidedPhysical().Ordering

	provided := maybeAddLookupOrdCols(lookupJoin, required, childProvided)

	// The lookup join includes an implicit projection (lookupJoin.Cols); some of
	// the input columns might not be output columns so we may need to remap them.
	// First check if we need to.
	needsRemap := false
	for i := range provided {
		if !lookupJoin.Cols.Contains(provided[i].ID()) {
			needsRemap = true
			break
		}
	}
	if !needsRemap {
		// Fast path: we don't need to remap any columns.
		return provided
	}

	// Because of the implicit projection, the FDs of the LookupJoin don't include
	// all the columns we care about; we have to recreate the FDs of the join
	// before the projection. These are the FDs of the input plus the equality
	// constraints implied by the lookup join.
	var fds props.FuncDepSet
	fds.CopyFrom(&lookupJoin.Input.Relational().FuncDeps)

	md := lookupJoin.Memo().Metadata()
	index := md.Table(lookupJoin.Table).Index(lookupJoin.Index)
	for i, colID := range lookupJoin.KeyCols {
		indexColID := lookupJoin.Table.ColumnID(index.Column(i).Ordinal())
		fds.AddEquivalency(colID, indexColID)
	}
	for i := range lookupJoin.LookupExpr {
		filterProps := lookupJoin.LookupExpr[i].ScalarProps()
		fds.AddFrom(&filterProps.FuncDeps)
	}

	return remapProvided(provided, &fds, lookupJoin.Cols)
}

// Lookup joins can maintain the index ordering on each lookup, in order to
// return results in index order for each input row. maybeAddLookupOrdCols
// takes advantage of this effect by appending the lookup index columns to the
// input provided ordering. The result may then satisfy the required ordering.
func maybeAddLookupOrdCols(
	lookupJoin *memo.LookupJoinExpr, required *props.OrderingChoice, inputOrd opt.Ordering,
) opt.Ordering {
	var toExtend *props.OrderingChoice
	if inputOrd, toExtend = trySatisfyRequired(required, inputOrd); toExtend == nil {
		// The input provided ordering satisfies the required ordering.
		return inputOrd
	}
	// The input provided ordering does not satisfy the required ordering. Try
	// appending index columns and check the required ordering again.
	if !lookupJoin.Input.Relational().FuncDeps.ColsAreStrictKey(inputOrd.ColSet()) {
		// Ensure that the ordering forms a key over the input columns. Lookup
		// joins can only maintain the index ordering for each individual input
		// row, so we need to disallow cases where different input rows may sort
		// the same on the input ordering.
		return inputOrd
	}
	joinFDs := &lookupJoin.Relational().FuncDeps
	optionalCols := joinFDs.ComputeClosure(inputOrd.ColSet().Union(required.Optional))
	idx := lookupJoin.Memo().Metadata().Table(lookupJoin.Table).Index(lookupJoin.Index)
	withLookup := make(opt.Ordering, len(inputOrd), len(inputOrd)+idx.KeyColumnCount())
	copy(withLookup, inputOrd)
	for i := 0; i < idx.KeyColumnCount(); i++ {
		if idx.Column(i).Descending {
			// The index columns must be ASC in order for lookups to be returned in
			// index order.
			return inputOrd
		}
		idxColID := lookupJoin.Table.ColumnID(idx.Column(i).Ordinal())
		if optionalCols.Contains(idxColID) {
			// Don't try to include optional columns.
			continue
		}
		if i < len(lookupJoin.KeyCols) && optionalCols.Contains(lookupJoin.KeyCols[i]) {
			// The index column is equivalent to an optional column. Add both to the
			// optional columns set and compute its closure.
			optionalCols.Add(lookupJoin.KeyCols[i])
			optionalCols.Add(idxColID)
			optionalCols = joinFDs.ComputeClosure(optionalCols)
			continue
		}
		withLookup = append(withLookup, opt.MakeOrderingColumn(idxColID, idx.Column(i).Descending))
	}
	if withLookup, toExtend = trySatisfyRequired(required, withLookup); toExtend == nil {
		// The input ordering with index columns appended satisfies the required
		// ordering.
		return withLookup
	}
	// The required ordering is not satisfied by appending index columns to the
	// input ordering, so just return the input ordering.
	return inputOrd
}

// trySatisfyRequired returns a prefix of the given provided Ordering that is
// compatible with the required ordering (e.g. all columns either line up with
// the required columns or are optional), as well as an OrderingChoice that
// indicates how the prefix needs to be extended in order to imply the required
// OrderingChoice. If the prefix satisfies the required props, toExtend will be
// nil. The returned fields reference the slices of the inputs, so they are not
// safe to mutate.
func trySatisfyRequired(
	required *props.OrderingChoice, provided opt.Ordering,
) (prefix opt.Ordering, toExtend *props.OrderingChoice) {
	var choiceIdx, providedIdx int
	for choiceIdx < len(required.Columns) && providedIdx < len(provided) {
		choice, providedCol := required.Columns[choiceIdx], provided[providedIdx]
		if required.Optional.Contains(providedCol.ID()) {
			// Skip optional columns.
			providedIdx++
			continue
		}
		if !choice.Group.Contains(providedCol.ID()) || choice.Descending != providedCol.Descending() {
			// The provided ordering columns must either line up with the
			// OrderingChoice columns, or be part of the optional column set.
			// Additionally, the provided ordering must have the same column
			// directions as the OrderingChoice.
			break
		}
		// The current OrderingChoice and provided columns match up.
		choiceIdx++
		providedIdx++
	}
	if providedIdx > 0 {
		prefix = provided[:providedIdx]
	}
	if choiceIdx < len(required.Columns) {
		toExtend = &props.OrderingChoice{
			Optional: required.Optional,
			Columns:  required.Columns[choiceIdx:],
		}
	}
	return prefix, toExtend
}
