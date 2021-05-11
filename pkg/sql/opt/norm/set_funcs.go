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
	}
	for idx, outCol := range set.OutCols {
		if needed.Contains(outCol) {
			prunedSet.LeftCols = append(prunedSet.LeftCols, set.LeftCols[idx])
			prunedSet.RightCols = append(prunedSet.RightCols, set.RightCols[idx])
			prunedSet.OutCols = append(prunedSet.OutCols, outCol)
		}
	}
	return &prunedSet
}

// CanConvertUnionToDistinctUnionAll indicates whether a Union can be replaced
// with a DistinctOn-UnionAll complex. If the replacement is valid, also returns
// a ColSet with the key columns the DistinctOn will need to group on. See the
// ConvertUnionToDistinctUnionAll comment for more info on the conditions that
// allow this transformation.
//
// CanConvertUnionToDistinctUnionAll assumes that both expressions have
// non-empty keys, that all key columns are contained within the given lists,
// and that the lists contain no duplicates.
func (c *CustomFuncs) CanConvertUnionToDistinctUnionAll(
	leftCols, rightCols opt.ColList, leftColSet opt.ColSet,
) (bool, opt.ColSet) {
	var keyCols opt.ColSet
	if len(leftCols) != len(rightCols) || len(leftCols) == 0 {
		return false, keyCols
	}
	md := c.mem.Metadata()
	var leftTableID, rightTableID opt.TableID
	for i := range leftCols {
		if leftTableID == opt.TableID(0) {
			leftTableID = md.ColumnMeta(leftCols[i]).Table
		}
		if rightTableID == opt.TableID(0) {
			rightTableID = md.ColumnMeta(rightCols[i]).Table
		}
		if leftTableID == opt.TableID(0) || rightTableID == opt.TableID(0) {
			// Both columns must come from a table, all columns must come
			// from the same base table, and all columns on a given side must come from the same meta table.
			return false, keyCols
		}
		if leftTableID != md.ColumnMeta(leftCols[i]).Table ||
			rightTableID != md.ColumnMeta(rightCols[i]).Table {
			// All columns on a given side must come from the same meta table.
			return false, keyCols
		}
		if md.Table(leftTableID) != md.Table(rightTableID) {
			// All columns must come from the same base table.m
			return false, keyCols
		}
		if leftTableID.ColumnOrdinal(leftCols[i]) != rightTableID.ColumnOrdinal(rightCols[i]) {
			// Pairs of equivalent columns must come from the same ordinal position in
			// the original base table.
			return false, keyCols
		}
	}

	// Now check that the columns form a strict key over the base table. Iterate
	// over the key columns of each index until we find a strict key that is a
	// subset of the given columns.
	table := md.Table(leftTableID)
	for i := 0; i < table.IndexCount(); i++ {
		index := table.Index(i)
		if index.IsInverted() {
			continue
		}
		if _, isPartial := index.Predicate(); isPartial {
			// Partial constraint keys are only unique for a subset of the rows.
			continue
		}
		foundUsableKey := true
		for j := 0; j < index.KeyColumnCount(); j++ {
			ord := index.Column(j).Ordinal()
			if !leftColSet.Contains(leftTableID.ColumnID(ord)) {
				// The input does not contain all columns that form this key.
				foundUsableKey = false
				break
			}
		}
		if foundUsableKey {
			// Add the key columns to the keyCols set and return.
			for j := 0; j < index.KeyColumnCount(); j++ {
				ord := index.Column(j).Ordinal()
				keyCols.Add(leftTableID.ColumnID(ord))
			}
			return true, keyCols
		}
	}

	// No strict key was found that is encompassed by the input columns.
	return false, keyCols
}

// TranslateColSet is used to translate a ColSet from one set of column IDs
// to an equivalent set. See the opt.TranslateColSet comment for more info.
func (c *CustomFuncs) TranslateColSet(colSetIn opt.ColSet, from, to opt.ColList) opt.ColSet {
	return opt.TranslateColSet(colSetIn, from, to)
}
