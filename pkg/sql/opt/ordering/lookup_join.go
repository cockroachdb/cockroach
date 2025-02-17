// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

func lookupJoinCanProvideOrdering(
	mem *memo.Memo, expr memo.RelExpr, required *props.OrderingChoice,
) bool {
	lookupJoin := expr.(*memo.LookupJoinExpr)

	// LookupJoin can pass through its ordering if the ordering depends only on
	// columns present in the input.
	inputCols := lookupJoin.Input.Relational().OutputCols
	canProjectUsingOnlyInputCols := required.CanProjectCols(inputCols)

	if canProjectUsingOnlyInputCols && lookupJoin.IsSecondJoinInPairedJoiner {
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
		return CanProvide(mem, child, &res)
	}
	if !canProjectUsingOnlyInputCols {
		// It is not possible to serve the required ordering using only input
		// columns. However, the lookup join may be able to satisfy the required
		// ordering by appending index columns to the input ordering. See the
		// getLookupOrdCols comment for more information.
		//
		// Iterate through the prefix of the required columns that can project input
		// columns, and set up to test whether the input ordering can be extended
		// with index columns.
		var remainingRequired props.OrderingChoice
		canProjectPrefixCols := required.Optional.Copy()
		for i := range required.Columns {
			if !required.Columns[i].Group.Intersects(inputCols) {
				// We have reached the end of the prefix of the required ordering that
				// can be projected by input columns. Keep track of the rest of the
				// columns that cannot be satisfied by an ordering on the input.
				remainingRequired.Columns = required.Columns[i:]
				break
			}
			canProjectPrefixCols.UnionWith(required.Columns[i].Group)
		}
		// Check whether appending index columns to the input ordering would satisfy
		// the required ordering.
		_, ok := getLookupOrdCols(mem, lookupJoin, &remainingRequired, canProjectPrefixCols)
		return ok
	}
	return canProjectUsingOnlyInputCols
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

func lookupJoinBuildProvided(
	mem *memo.Memo, expr memo.RelExpr, required *props.OrderingChoice,
) opt.Ordering {
	lookupJoin := expr.(*memo.LookupJoinExpr)
	provided := lookupJoin.Input.ProvidedPhysical().Ordering

	var toExtend *props.OrderingChoice
	if provided, toExtend = trySatisfyRequired(required, provided); toExtend != nil {
		// The input provided ordering cannot satisfy the required ordering. It may
		// be possible to append lookup columns in order to do so. See the
		// getLookupOrdColumns for more details.
		inputOrderingCols := provided.ColSet()
		inputOrderingCols.UnionWith(required.Optional)
		if lookupProvided, ok := getLookupOrdCols(mem, lookupJoin, toExtend, inputOrderingCols); ok {
			newProvided := make(opt.Ordering, len(provided)+len(lookupProvided))
			copy(newProvided, provided)
			copy(newProvided[len(provided):], lookupProvided)
			provided = newProvided
		}
	}

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

	md := mem.Metadata()
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
// return results in index order for each input row. getLookupOrdCols checks
// whether it is possible to append index columns to the input ordering in order
// to satisfy the required ordering. If it is possible, getLookupOrdCols returns
// the index ordering columns that should be appended to the input ordering and
// ok is true. Otherwise, ok is false.
//
// It is possible for a lookup join to supply an ordering that references index
// columns if the ordering consists of a series of input columns that form a key
// over the input, followed by the index columns in index order. The following
// is a case where a lookup join could maintain an ordering over both input and
// index columns:
//
//	CREATE TABLE ab (a INT, b INT, PRIMARY KEY(a, b));
//	CREATE TABLE xy (x INT, y INT, PRIMARY KEY(x, y DESC));
//	SELECT * FROM ab INNER LOOKUP JOIN xy ON a = x ORDER BY a, b, x, y DESC;
//
// Note that in this example the 'a' and 'b' columns form a key over the
// input of the lookup join. Additionally, the 'x' column alone is not a key
// for the 'xy' table, so each lookup may return multiple rows (which need
// to be ordered among themselves). Since the suffix of the ordering that
// references index columns is in index order (x, y DESC), the lookup join in
// the example can supply the ordering itself. On the other hand, switching
// 'b' and 'y' in the ordering, removing 'b', or changing the ordering on 'y' to
// ASC would mean the query would require a sort.
//
// Note that the Columns field of the required OrderingChoice should reflect the
// postfix of the required ordering that cannot be satisfied by input columns,
// rather than the entire required ordering. The inputOrderingColumns argument
// is the set of columns referenced by the input ordering. getLookupOrdCols can
// mutate the inputOrderingCols argument.
func getLookupOrdCols(
	mem *memo.Memo,
	lookupJoin *memo.LookupJoinExpr,
	required *props.OrderingChoice,
	inputOrderingCols opt.ColSet,
) (lookupOrdering opt.Ordering, ok bool) {
	if !lookupJoin.Input.Relational().FuncDeps.ColsAreStrictKey(inputOrderingCols) {
		// Ensure that the ordering forms a key over the input columns. Lookup
		// joins can only maintain the index ordering for each individual input
		// row, so we need to disallow cases where different input rows may sort
		// the same on the input ordering.
		//
		// Note that it would be technically correct to use the index ordering when
		// the input ordering does not form a key over the input iff the input
		// ordering columns functionally determined the index ordering columns.
		// However, in this case the addition of the index ordering columns would be
		// trivial, since the ordering could be simplified to just include the input
		// ordering columns (see OrderingChoice.Simplify).
		return nil, false
	}
	// The columns from the prefix of the required ordering satisfied by the
	// input are considered optional for the index ordering.
	optionalCols := inputOrderingCols

	// Build an ordering that represents the index.
	joinFDs := &lookupJoin.Relational().FuncDeps
	idx := mem.Metadata().Table(lookupJoin.Table).Index(lookupJoin.Index)
	indexOrder := make(opt.Ordering, 0, idx.KeyColumnCount())
	requiredCols := required.ColSet()
	for i := 0; i < idx.KeyColumnCount(); i++ {
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
		if !requiredCols.Contains(idxColID) {
			// This index column is not part of the required ordering. It is possible
			// that the prefix of the index ordering we have reached so far can
			// satisfy the required ordering, so break instead of returning.
			break
		}
		indexOrder = append(indexOrder, opt.MakeOrderingColumn(idxColID, idx.Column(i).Descending))
	}
	// Check if the index ordering satisfies the postfix of the required
	// ordering that cannot be satisfied by the input.
	indexOrder, remaining := trySatisfyRequired(required, indexOrder)
	return indexOrder, remaining == nil
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
	var requiredIdx, providedIdx int
	for requiredIdx < len(required.Columns) && providedIdx < len(provided) {
		requiredCol, providedCol := required.Columns[requiredIdx], provided[providedIdx]
		if required.Optional.Contains(providedCol.ID()) {
			// Skip optional columns.
			providedIdx++
			continue
		}
		if !requiredCol.Group.Contains(providedCol.ID()) ||
			requiredCol.Descending != providedCol.Descending() {
			// The provided ordering columns must either line up with the
			// OrderingChoice columns, or be part of the optional column set.
			// Additionally, the provided ordering must have the same column
			// directions as the OrderingChoice.
			break
		}
		// The current OrderingChoice and provided columns match up.
		requiredIdx++
		providedIdx++
	}
	if providedIdx > 0 {
		prefix = provided[:providedIdx]
	}
	if requiredIdx < len(required.Columns) {
		toExtend = &props.OrderingChoice{
			Optional: required.Optional,
			Columns:  required.Columns[requiredIdx:],
		}
	}
	return prefix, toExtend
}
