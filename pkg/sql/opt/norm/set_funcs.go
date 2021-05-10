// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/errors"
)

// ProjectColMapLeft returns a Projections operator that maps the left side
// columns in a SetPrivate to the output columns in it. Useful for replacing set
// operations with simpler constructs.
func (c *CustomFuncs) ProjectColMapLeft(set *memo.SetPrivate) memo.ProjectionsExpr {
	return c.projectColMapSide(set.OutCols, set.LeftCols)
}

// ProjectColMapRight returns a Project operator that maps the right side
// columns in a SetPrivate to the output columns in it. Useful for replacing set
// operations with simpler constructs.
func (c *CustomFuncs) ProjectColMapRight(set *memo.SetPrivate) memo.ProjectionsExpr {
	return c.projectColMapSide(set.OutCols, set.RightCols)
}

// projectColMapSide implements the side-agnostic logic from ProjectColMapLeft
// and ProjectColMapRight.
func (c *CustomFuncs) projectColMapSide(toList, fromList opt.ColList) memo.ProjectionsExpr {
	items := make(memo.ProjectionsExpr, 0, len(toList))
	for idx, fromCol := range fromList {
		toCol := toList[idx]
		// If the to and from col ids are the same, do not project them. They
		// are passed-through instead. See opt.Metadata for more details about
		// why some col ids could exist on one side and the output of a set
		// operation.
		if fromCol != toCol {
			items = append(items, c.f.ConstructProjectionsItem(c.f.ConstructVariable(fromCol), toCol))
		}
	}
	return items
}

// ProjectPassthroughLeft returns a ColSet that contains the columns that can
// be passed-through from the left side in a SetPrivate when eliminating a set
// operation, like in EliminateUnionAllLeft. Columns in both the output
// ColList and the left ColList should be passed-through.
func (c *CustomFuncs) ProjectPassthroughLeft(set *memo.SetPrivate) opt.ColSet {
	out := set.OutCols.ToSet()
	out.IntersectionWith(set.LeftCols.ToSet())
	return out
}

// ProjectPassthroughRight returns a ColSet that contains the columns that can
// be passed-through from the right side in a SetPrivate when eliminating a set
// operation, like in EliminateUnionAllRight. Columns in both the output
// ColList and the right ColList should be passed-through.
func (c *CustomFuncs) ProjectPassthroughRight(set *memo.SetPrivate) opt.ColSet {
	out := set.OutCols.ToSet()
	out.IntersectionWith(set.RightCols.ToSet())
	return out
}

// PruneSetPrivate returns a SetPrivate based on the given SetPrivate, but with
// unneeded input and output columns discarded.
func (c *CustomFuncs) PruneSetPrivate(needed opt.ColSet, set *memo.SetPrivate) *memo.SetPrivate {
	length := needed.Len()
	prunedSet := memo.SetPrivate{
		LeftCols:  make(opt.ColList, 0, length),
		RightCols: make(opt.ColList, 0, length),
		OutCols:   make(opt.ColList, 0, length),
		Ordering:  set.Ordering,
	}
	for idx, outCol := range set.OutCols {
		if needed.Contains(outCol) {
			prunedSet.LeftCols = append(prunedSet.LeftCols, set.LeftCols[idx])
			prunedSet.RightCols = append(prunedSet.RightCols, set.RightCols[idx])
			prunedSet.OutCols = append(prunedSet.OutCols, outCol)
		}
	}
	if !prunedSet.Ordering.SubsetOfCols(needed) {
		prunedSet.Ordering = prunedSet.Ordering.Copy()
		prunedSet.Ordering.ProjectCols(needed)
	}
	return &prunedSet
}

// CanConvertUnionToDistinctUnionAll checks whether a Union can be replaced
// with a DistinctOn-UnionAll complex. If the replacement is valid, it returns
// a ColSet with the key columns the DistinctOn will need to group on.
// Otherwise, returns ok=false. See the ConvertUnionToDistinctUnionAll rule
// comment for more info on the conditions that allow this transformation.
//
// CanConvertUnionToDistinctUnionAll assumes that both expressions have
// non-empty keys, that all key columns are contained within the given lists,
// and that the lists contain no duplicates.
//
// leftCols and rightCols contain the columns from the left and right inputs of
// the union that will be unioned together. leftColSet contains all columns from
// the left input; it is used to ensure that all columns from the key selected
// from the base table are present in the union.
func (c *CustomFuncs) CanConvertUnionToDistinctUnionAll(
	leftCols, rightCols opt.ColList,
) (keyCols opt.ColSet, ok bool) {
	if len(leftCols) != len(rightCols) || len(leftCols) == 0 {
		panic(errors.AssertionFailedf("invalid union"))
	}
	md := c.mem.Metadata()
	leftTableID, rightTableID := md.ColumnMeta(leftCols[0]).Table, md.ColumnMeta(rightCols[0]).Table
	if leftTableID == opt.TableID(0) || rightTableID == opt.TableID(0) {
		// All columns must come from a base table.
		return opt.ColSet{}, false
	}
	if md.Table(leftTableID) != md.Table(rightTableID) {
		// All columns must come from the same base table.
		return opt.ColSet{}, false
	}
	for i := range leftCols {
		if leftTableID != md.ColumnMeta(leftCols[i]).Table ||
			rightTableID != md.ColumnMeta(rightCols[i]).Table {
			// All columns on a given side must come from the same meta table.
			return opt.ColSet{}, false
		}
		if leftTableID.ColumnOrdinal(leftCols[i]) != rightTableID.ColumnOrdinal(rightCols[i]) {
			// Pairs of equivalent columns must come from the same ordinal position in
			// the original base table.
			return opt.ColSet{}, false
		}
	}

	// Now find a subset of the columns that forms a strict key over the base
	// table.
	leftFDs := memo.MakeTableFuncDep(md, leftTableID)
	leftColSet := leftCols.ToSet()
	if !leftFDs.ColsAreStrictKey(leftColSet) {
		// The columns must form a strict key over the base table.
		return opt.ColSet{}, false
	}
	keyCols = leftFDs.ReduceCols(leftColSet)
	if keyCols.Equals(leftColSet) {
		// The key columns must form a strict subset of the union columns, or the
		// transformation is not worth it.
		return opt.ColSet{}, false
	}
	rightFDs := memo.MakeTableFuncDep(md, rightTableID)
	if !rightFDs.ColsAreStrictKey(c.TranslateColSet(keyCols, leftCols, rightCols)) {
		// The columns must form a strict key over both meta tables. The meta tables
		// can have different functional dependencies when the
		// IgnoreUniqueWithoutIndexKeys flag is set for one table and not the other.
		// A meta table with the flag set will not be able to take advantage of a
		// UNIQUE NOT NULL column in calculating functional dependencies.
		return opt.ColSet{}, false
	}
	return keyCols, true
}

// TranslateColSet is used to translate a ColSet from one set of column IDs
// to an equivalent set. See the opt.TranslateColSet comment for more info.
func (c *CustomFuncs) TranslateColSet(colSetIn opt.ColSet, from, to opt.ColList) opt.ColSet {
	return opt.TranslateColSet(colSetIn, from, to)
}
