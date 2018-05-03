// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

// neededCols returns the set of columns needed by the given group. It is an
// alias for outerCols that's used for clarity with the UnusedCols patterns.
func (f *Factory) neededCols(group memo.GroupID) opt.ColSet {
	return f.outerCols(group)
}

// neededCols2 unions the set of columns needed by either of the given groups.
func (f *Factory) neededCols2(left, right memo.GroupID) opt.ColSet {
	return f.outerCols(left).Union(f.outerCols(right))
}

// neededCols3 unions the set of columns needed by any of the given groups.
func (f *Factory) neededCols3(group1, group2, group3 memo.GroupID) opt.ColSet {
	cols := f.outerCols(group1).Union(f.outerCols(group2))
	cols.UnionWith(f.outerCols(group3))
	return cols
}

// neededColsGroupBy unions the columns needed by either of a GroupBy's
// operands - either aggregations or groupingCols. This case doesn't fit any
// of the neededCols methods because groupingCols is a private, not a group.
func (f *Factory) neededColsGroupBy(aggs memo.GroupID, groupingCols memo.PrivateID) opt.ColSet {
	colSet := f.mem.LookupPrivate(groupingCols).(opt.ColSet)
	return f.outerCols(aggs).Union(colSet)
}

// neededColsLimit unions the columns needed by Projections with the columns in
// the Ordering of a Limit/Offset operator.
func (f *Factory) neededColsLimit(projections memo.GroupID, ordering memo.PrivateID) opt.ColSet {
	return f.outerCols(projections).Union(f.extractOrdering(ordering).ColSet())
}

// hasUnusedCols returns true if the target group has extra columns that are
// not needed at this level of the tree, and can be eliminated by one of the
// UnusedCols rules. A column cannot be eliminated if it is either needed by
// another expression bound in the rule:
//
//   SELECT * FROM (SELECT * FROM a WHERE y=5) AS target WHERE x=1
//                                                       ^^^^^^^^^
//
// or else if it is used in the target group's subtree:
//
//   SELECT * FROM (SELECT * FROM a WHERE y=5) AS target WHERE x=1
//                                  ^^^^^^^^^
//
// In this example, the "x" and "y" columns are needed, but any other
// columns from the table will be eliminated.
func (f *Factory) hasUnusedCols(target memo.GroupID, neededCols opt.ColSet) bool {
	colSet := f.outputCols(target)
	colSet.DifferenceWith(f.usedCols(target))
	colSet.DifferenceWith(neededCols)
	return !colSet.Empty()
}

// usedCols returns the subset of output columns that are in use in the target
// expression's subtree. Projections and Aggregations return an empty set, since
// they are projecting a new set of columns that haven't yet been used.
// Relational expressions consult the UsedCols property to determine if an
// expression in the subtree has referenced one or more of the output columns.
func (f *Factory) usedCols(target memo.GroupID) opt.ColSet {
	switch f.mem.NormExpr(target).Operator() {
	case opt.ProjectionsOp, opt.AggregationsOp:
		return opt.ColSet{}
	}
	return f.lookupLogical(target).Relational.UsedCols
}

// filterUnusedCols creates an expression that discards any outputs columns
// of the given group that are not used. If the target expression type supports
// column filtering (like Scan, Values, Projections, etc.), then create a new
// instance of that operator that does the filtering. Otherwise, construct a
// Project operator that wraps the operator and does the filtering.
func (f *Factory) filterUnusedCols(target memo.GroupID, neededCols opt.ColSet) memo.GroupID {
	targetExpr := f.mem.NormExpr(target)
	switch targetExpr.Operator() {
	case opt.ScanOp:
		return f.filterUnusedScanCols(target, neededCols)

	case opt.ValuesOp:
		return f.filterUnusedValuesCols(target, neededCols)
	}

	// Get the subset of the target group's output columns that are in the used
	// set, including columns used in the subtree and additional columns passed
	// in by the rule. Any columns not in that set will be filtered away.
	colSet := f.usedCols(target).Union(neededCols)
	colSet.IntersectionWith(f.outputCols(target))
	cnt := colSet.Len()

	// Create a new list of groups to project, along with the list of column
	// indexes to be projected. These will become inputs to the construction of
	// the Projections or Aggregations operator.
	groupList := make([]memo.GroupID, 0, cnt)
	colList := make(opt.ColList, 0, cnt)

	switch targetExpr.Operator() {
	case opt.ProjectionsOp, opt.AggregationsOp:
		// Get groups from existing lists.
		var existingList []memo.GroupID
		var existingCols opt.ColList
		if targetExpr.Operator() == opt.ProjectionsOp {
			existingList = f.mem.LookupList(targetExpr.AsProjections().Elems())
			existingCols = f.extractColList(targetExpr.AsProjections().Cols())
		} else {
			existingList = f.mem.LookupList(targetExpr.AsAggregations().Aggs())
			existingCols = f.extractColList(targetExpr.AsAggregations().Cols())
		}

		for i, group := range existingList {
			// Only add groups that are part of the needed columns.
			if neededCols.Contains(int(existingCols[i])) {
				groupList = append(groupList, group)
				colList = append(colList, existingCols[i])
			}
		}

	default:
		// Construct new variable groups for each output column that's needed.
		colSet.ForEach(func(i int) {
			v := f.ConstructVariable(f.InternColumnID(opt.ColumnID(i)))
			groupList = append(groupList, v)
			colList = append(colList, opt.ColumnID(i))
		})
	}

	if targetExpr.Operator() == opt.AggregationsOp {
		return f.ConstructAggregations(f.InternList(groupList), f.InternColList(colList))
	}

	projections := f.ConstructProjections(f.InternList(groupList), f.InternColList(colList))
	if targetExpr.Operator() == opt.ProjectionsOp {
		return projections
	}

	// Else wrap in Project operator.
	return f.ConstructProject(target, projections)
}

// filterUnusedScanCols constructs a new Scan operator based on the given
// existing Scan operator, but projecting only the needed columns.
func (f *Factory) filterUnusedScanCols(scan memo.GroupID, neededCols opt.ColSet) memo.GroupID {
	colSet := f.outputCols(scan).Intersection(neededCols)
	scanExpr := f.mem.NormExpr(scan).AsScan()
	existing := f.mem.LookupPrivate(scanExpr.Def()).(*memo.ScanOpDef)
	new := memo.ScanOpDef{Table: existing.Table, Cols: colSet}
	return f.ConstructScan(f.mem.InternScanOpDef(&new))
}

// filterUnusedValuesCols constructs a new Values operator based on the
// given existing Values operator. The new operator will have the same set of
// rows, but containing only the needed columns. Other columns are discarded.
func (f *Factory) filterUnusedValuesCols(
	values memo.GroupID, neededCols opt.ColSet,
) memo.GroupID {
	valuesExpr := f.mem.NormExpr(values).AsValues()
	existingCols := f.extractColList(valuesExpr.Cols())
	newCols := make(opt.ColList, 0, neededCols.Len())

	existingRows := f.mem.LookupList(valuesExpr.Rows())
	newRows := make([]memo.GroupID, 0, len(existingRows))

	// Create new list of columns that only contains needed columns.
	for _, colID := range existingCols {
		if !neededCols.Contains(int(colID)) {
			continue
		}
		newCols = append(newCols, colID)
	}

	// newElems is used to store tuple values, and can be allocated once and
	// reused repeatedly, since InternList will copy values to memo storage.
	newElems := make([]memo.GroupID, len(newCols))

	for _, row := range existingRows {
		existingElems := f.mem.LookupList(f.mem.NormExpr(row).AsTuple().Elems())

		n := 0
		for i, elem := range existingElems {
			if !neededCols.Contains(int(existingCols[i])) {
				continue
			}

			newElems[n] = elem
			n++
		}

		newRows = append(newRows, f.ConstructTuple(f.InternList(newElems)))
	}

	return f.ConstructValues(f.InternList(newRows), f.InternColList(newCols))
}
