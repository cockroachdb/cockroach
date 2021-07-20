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

func lookupOrIndexJoinCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// LookupJoin and IndexJoin can pass through their ordering if the ordering
	// depends only on columns present in the input.
	inputCols := expr.Child(0).(memo.RelExpr).Relational().OutputCols
	canProjectCols := required.CanProjectCols(inputCols)

	if lookupJoin, ok := expr.(*memo.LookupJoinExpr); ok &&
		canProjectCols && lookupJoin.IsSecondJoinInPairedJoiner {
		// Can only pass through ordering if the ordering can be provided by the
		// child, since we don't want a sort to be interposed between the child
		// and this join.
		//
		// We may need to remove ordering columns that are not output by the input
		// expression. This results in an equivalent ordering, but with fewer
		// options in the OrderingChoice.
		child := expr.Child(0).(memo.RelExpr)
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
	return canProjectCols
}

func lookupOrIndexJoinBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}

	// We may need to remove ordering columns that are not output by the input
	// expression.
	child := parent.Child(0).(memo.RelExpr)
	res := projectOrderingToInput(child, required)
	// It is in principle possible that the lookup join has an ON condition that
	// forces an equality on two columns in the input. In this case we need to
	// trim the column groups to keep the ordering valid w.r.t the child FDs
	// (similar to Select).
	//
	// This case indicates that we didn't do a good job pushing down equalities
	// (see #36219), but it should be handled correctly here nevertheless.
	return trimColumnGroups(&res, &child.Relational().FuncDeps)
}

func indexJoinBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	// If an index join has a requirement on some input columns, those columns
	// must be output columns (or equivalent to them). We may still need to remap
	// using column equivalencies.
	indexJoin := expr.(*memo.IndexJoinExpr)
	rel := indexJoin.Relational()
	return remapProvided(indexJoin.Input.ProvidedPhysical().Ordering, &rel.FuncDeps, rel.OutputCols)
}

func lookupJoinBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	lookupJoin := expr.(*memo.LookupJoinExpr)
	childProvided := lookupJoin.Input.ProvidedPhysical().Ordering

	// The lookup join includes an implicit projection (lookupJoin.Cols); some of
	// the input columns might not be output columns so we may need to remap them.
	// First check if we need to.
	needsRemap := false
	for i := range childProvided {
		if !lookupJoin.Cols.Contains(childProvided[i].ID()) {
			needsRemap = true
			break
		}
	}
	if !needsRemap {
		// Fast path: we don't need to remap any columns.
		return childProvided
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

	return remapProvided(childProvided, &fds, lookupJoin.Cols)
}
