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
