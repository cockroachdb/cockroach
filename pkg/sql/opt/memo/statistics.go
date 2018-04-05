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

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Statistics is a collection of measurements and statistics that is used by
// the coster to estimate the cost of expressions. Statistics are collected
// for tables and indexes and are exposed to the optimizer via opt.Catalog
// interfaces. As logical properties are derived bottom-up for each
// expression, statistics are derived for each relational expression, based
// on the characteristics of the expression's operator type, as well as the
// properties of its operands. For example:
//
//   SELECT * FROM a WHERE a=1
//
// Table and column statistics can be used to estimate the number of rows
// in table a, and then to determine the selectivity of the a=1 filter, in
// order to derive the statistics of the Select expression.
type Statistics struct {
	// RowCount is the estimated number of rows returned by the expression.
	RowCount uint64

	// ColStats contains statistics that pertain to subsets of the columns
	// in this expression.
	ColStats []ColumnStatistics
}

// ColumnStatistics is a collection of statistics that applies to a particular
// set of columns. In theory, a table could have a ColumnStatistics object
// for every possible subset of columns. In practice, it is only worth
// maintaining statistics on a few columns and column sets that are frequently
// used in predicates, group by columns, etc.
type ColumnStatistics struct {
	// Cols is the set of columns whose data are summarized by this
	// ColumnStatistics struct.
	Cols opt.ColSet

	// DistinctCount is the estimated number of distinct values of this
	// set of columns for this expression.
	DistinctCount uint64
}

func (s *Statistics) applyConstraint(
	c *constraint.Constraint, evalCtx *tree.EvalContext,
) {
	if c.IsUnconstrained() {
		return
	}

	if c.IsContradiction() {
		s.updateStatsFromContradiction()
		return
	}

	distinctCounts := make(map[int]uint64, c.Columns.Count())
	distinctCounts = s.getDistinctCountsFromConstraint(c, evalCtx, distinctCounts)

	s.updateStatsFromDistinctCounts(distinctCounts)
}

func (s *Statistics) applyConstraintSet(cs *constraint.Set, evalCtx *tree.EvalContext) {
	if cs.IsUnconstrained() {
		return
	}

	if cs == constraint.Contradiction {
		s.updateStatsFromContradiction()
		return
	}

	distinctCounts := make(map[int]uint64, cs.Length())
	for i := 0; i < cs.Length(); i++ {
		distinctCounts = s.getDistinctCountsFromConstraint(cs.Constraint(i), evalCtx, distinctCounts)
	}

	s.updateStatsFromDistinctCounts(distinctCounts)
}

// updateStatsFromContradiction sets the row count and distinct count to zero,
// since a contradiction results in 0 rows.
func (s *Statistics) updateStatsFromContradiction() {
	s.RowCount = 0
	for i := range s.ColStats {
		s.ColStats[i].DistinctCount = 0
	}
}

// updateStatsFromDistinctCounts updates the column statistics given a map
// from column ID to distinct count.
func (s *Statistics) updateStatsFromDistinctCounts(distinctCounts map[int]uint64) {
	var constrainedCols opt.ColSet
	for k := range distinctCounts {
		constrainedCols.Add(k)
	}

	// Use the new stats to update the old stats.
	for i := range s.ColStats {
		colStat := &s.ColStats[i]
		if colStat.Cols.SubsetOf(constrainedCols) {
			distinctCount := uint64(1)
			colStat.Cols.ForEach(func(i int) {
				distinctCount *= distinctCounts[i]
			})
			colStat.DistinctCount = min(distinctCount, colStat.DistinctCount)
		}
	}
}

// getDistinctCountsFromConstraint gets the distinct count for each column in
// a constraint that can be determined to have a finite number of possible
// values. It updates distinctCounts, which is a map from column ID to distinct
// count, and returns the updated map. distinctCounts may be reused across
// multiple calls to getDistinctCountsFromConstraint in order to determine
// distinct counts from a constraint set. If the same column appears in
// multiple constraints, the returned distinct count is the minimum for that
// column across all constraints.
//
// For example, consider the following constraint set:
//
//   /a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]; /c: [/6 - /6]
//
// After the first constraint is processed, distinctCounts contains the
// following:
//   [a] -> 1
//   [b] -> 1
//   [c] -> 5
//
// After the second constraint is processed, column c is further constrained,
// so distinctCounts contains the following:
//   [a] -> 1
//   [b] -> 1
//   [c] -> 1
//
// Note that getDistinctCountsFromConstraint is pessimistic, and assumes that
// there is at least one row for every possible value provided by the
// constraint. For example, /a: [/1 - /1000000] would return [a] -> 1000000
// even if there are only 10 rows in the table. This discrepancy must be
// resolved by the calling function.
func (s *Statistics) getDistinctCountsFromConstraint(
	c *constraint.Constraint, evalCtx *tree.EvalContext, distinctCounts map[int]uint64,
) map[int]uint64 {
	// All of the columns that are part of the exact prefix have exactly one
	// distinct value.
	exactPrefix := c.ExactPrefix(evalCtx)
	for i := 0; i < exactPrefix; i++ {
		id := int(c.Columns.Get(i).ID())
		distinctCounts[id] = 1
	}

	// If there are no other columns beyond the exact prefix, we are done.
	if exactPrefix >= c.Columns.Count() {
		return distinctCounts
	}

	// If there are any other columns beyond the exact prefix, we may be able to
	// determine the number of distinct values for the first one. For example:
	//   /a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]
	//       -> Columns a and b have DistinctCount = 1.
	//       -> Column c has DistinctCount = 5.
	col := exactPrefix
	// All columns should have at least one distinct value.
	distinctCount := uint64(1)

	var val tree.Datum
	for i := 0; i < c.Spans.Count(); i++ {
		sp := c.Spans.Get(i)
		if sp.StartKey().Length() <= col || sp.EndKey().Length() <= col {
			// We can't determine the distinct count for this column. For example,
			// the number of distinct values for column b in the constraint
			// /a/b: [/1/1 - /1] cannot be determined.
			return distinctCounts
		}
		startVal := sp.StartKey().Value(col)
		if startVal.Compare(evalCtx, sp.EndKey().Value(col)) != 0 {
			// TODO(rytaft): are there other types we should handle here
			// besides int?
			if startVal.ResolvedType() == types.Int {
				start := int(*startVal.(*tree.DInt))
				end := int(*sp.EndKey().Value(col).(*tree.DInt))
				// We assume that both start and end boundaries are inclusive. This
				// should be the case for integer valued columns (due to normalization
				// by constraint.PreferInclusive).
				if c.Columns.Get(col).Ascending() {
					distinctCount += uint64(end - start)
				} else {
					distinctCount += uint64(start - end)
				}
			} else {
				// We can't determine the distinct count for this column. For example,
				// the number of distinct values in the constraint
				// /a: [/'cherry' - /'mango'] cannot be determined.
				return distinctCounts
			}
		}
		if i == 0 {
			val = startVal
		} else if startVal.Compare(evalCtx, val) != 0 {
			distinctCount++
		}
	}

	id := int(c.Columns.Get(col).ID())
	if v, ok := distinctCounts[id]; ok {
		distinctCounts[id] = min(v, distinctCount)
	} else {
		distinctCounts[id] = distinctCount
	}

	return distinctCounts
}

func (s *Statistics) updateScanStats(def *ScanOpDef, evalCtx *tree.EvalContext) {
	// TODO: Need actual number of rows.
	if def.Constraint != nil {
		s.RowCount = 100
	} else {
		s.RowCount = 1000
	}

	// Cap number of rows at limit, if it exists.
	if def.HardLimit > 0 && uint64(def.HardLimit) < s.RowCount {
		s.RowCount = uint64(def.HardLimit)
	}

	// TODO(rytaft): Need actual number of distinct values.
	def.Cols.ForEach(func(i int) {
		cols := util.MakeFastIntSet(i)
		s.ColStats = append(s.ColStats, ColumnStatistics{Cols: cols, DistinctCount: s.RowCount / 4})
	})
	s.ColStats = append(s.ColStats, ColumnStatistics{
		Cols: def.Cols,
		// Cast to int64 and then to uint64 to make the linter happy.
		DistinctCount: uint64(int64(float64(s.RowCount) * 0.9)),
	})

	if def.Constraint != nil {
		s.applyConstraint(def.Constraint, evalCtx)
	}
}

func (s *Statistics) updateSelectStats(
	filter ExprView, inputStats *Statistics, evalCtx *tree.EvalContext,
) {
	// TODO: Need better estimate based on actual filter conditions.
	s.RowCount = inputStats.RowCount / 10

	s.ColStats = make([]ColumnStatistics, len(inputStats.ColStats))
	copy(s.ColStats, inputStats.ColStats)
	for i := range s.ColStats {
		s.ColStats[i].DistinctCount = s.ColStats[i].DistinctCount / 10
	}

	constraintSet := filter.Logical().Scalar.Constraints
	if constraintSet != nil {
		s.applyConstraintSet(constraintSet, evalCtx)
	}
}

func (s *Statistics) updateProjectStats(inputStats *Statistics, outputCols *opt.ColSet) {
	s.RowCount = inputStats.RowCount

	// Inherit column statistics from input, but only for columns that are also
	// output columns.
	s.filterColumnStats(inputStats.ColStats, outputCols)

	// TODO(rytaft): Try to update the distinct count for computed columns based
	// on the distinct count of the input columns.
}

func (s *Statistics) updateJoinStats(
	op opt.Operator, leftStats, rightStats *Statistics, on ExprView,
) {
	// TODO: Need better estimate based on actual on conditions.
	s.RowCount = leftStats.RowCount * rightStats.RowCount
	if on.Operator() != opt.TrueOp {
		s.RowCount /= 10
	}

	// The number of distinct values for the column subsets doesn't change
	// significantly unless the column subsets are part of the ON conditions.
	// For now, add them all unchanged.
	switch op {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Inherit column stats from left side of join.
		s.ColStats = make([]ColumnStatistics, len(leftStats.ColStats))
		copy(s.ColStats, leftStats.ColStats)

	default:
		// Union column stats from both sides of join.
		cs := len(leftStats.ColStats)
		s.ColStats = append(leftStats.ColStats[:cs:cs], rightStats.ColStats...)
	}
}

func (s *Statistics) updateGroupByStats(
	inputStats *Statistics, groupingColSet, outputCols *opt.ColSet,
) {
	if groupingColSet.Empty() {
		// Scalar group by.
		s.RowCount = 1

		s.ColStats = make([]ColumnStatistics, 0, outputCols.Len())
		outputCols.ForEach(func(i int) {
			cols := util.MakeFastIntSet(i)
			s.ColStats = append(s.ColStats, ColumnStatistics{Cols: cols, DistinctCount: 1})
		})
	} else {
		// Inherit column statistics from input, but only for columns that are also
		// output columns.
		s.filterColumnStats(inputStats.ColStats, outputCols)

		// See if we can determine the number of rows from the distinct counts.
		var distinctRows, maxDistinct uint64
		for _, colStats := range s.ColStats {
			if colStats.Cols.Equals(*groupingColSet) {
				distinctRows = colStats.DistinctCount
			} else if colStats.DistinctCount > maxDistinct {
				maxDistinct = colStats.DistinctCount
			}
		}

		if distinctRows > 0 {
			// We can estimate the row count based on the distinct count of the
			// grouping columns.
			s.RowCount = distinctRows
		} else {
			// We can't determine the row count exactly based on the distinct count
			// of the grouping columns, so make a rough guess.
			// TODO: Need better estimate.
			s.RowCount = max(maxDistinct, inputStats.RowCount/10)
		}
	}
}

func (s *Statistics) updateSetOpStats(
	op opt.Operator, leftStats, rightStats *Statistics, colMap *SetOpColMap, outputCols *opt.ColSet,
) {
	//
	// Update row statistics.
	//

	// These calculations are an upper bound on the row count. It's likely that
	// there is some overlap between the two sets, but not full overlap.
	switch op {
	case opt.UnionOp, opt.UnionAllOp:
		s.RowCount = leftStats.RowCount + rightStats.RowCount

	case opt.IntersectOp, opt.IntersectAllOp:
		s.RowCount = min(leftStats.RowCount, rightStats.RowCount)

	case opt.ExceptOp, opt.ExceptAllOp:
		s.RowCount = leftStats.RowCount
	}

	//
	// Update column statistics.
	//

	// Determine which of the two input relations has the larger set of column
	// statistics.
	var lgStats, smStats []ColumnStatistics
	var lgColList, smColList opt.ColList
	leftIsLarger := len(leftStats.ColStats) > len(rightStats.ColStats)
	if leftIsLarger {
		lgStats, smStats = leftStats.ColStats, rightStats.ColStats
		lgColList, smColList = colMap.Left, colMap.Right
	} else {
		lgStats, smStats = rightStats.ColStats, leftStats.ColStats
		lgColList, smColList = colMap.Right, colMap.Left
	}

	// Build a map on the smaller of the two sets of statistics, keyed by
	// opt.ColSet.
	colStatsMap := make(map[opt.ColSet]*ColumnStatistics, len(smStats))
	for i := range smStats {
		smColStats := &smStats[i]

		// Create a key using column IDs from the relation with the larger set
		// of statistics.
		key := translateColSet(smColStats.Cols, smColList, lgColList)
		colStatsMap[key] = smColStats
	}

	// Iterate through the larger set of statistics, probing into the map to
	// see if there is a match with the smaller set.
	for i := range lgStats {
		lgColStats := &lgStats[i]
		if smColStats, ok := colStatsMap[lgColStats.Cols]; ok {
			// We found a match! Update the column statistics for the output
			// relation.
			var leftStats, rightStats *ColumnStatistics
			if leftIsLarger {
				leftStats, rightStats = lgColStats, smColStats
			} else {
				leftStats, rightStats = smColStats, lgColStats
			}

			s.updateSetOpColumnStats(op, leftStats, rightStats, colMap, outputCols)
		}
	}
}

// updateSetOpColumnStats adds column statistics for the set of output columns
// produced by merging the columns from leftStats and rightStats.
func (s *Statistics) updateSetOpColumnStats(
	op opt.Operator,
	leftStats, rightStats *ColumnStatistics,
	colMap *SetOpColMap,
	outputCols *opt.ColSet,
) {
	// Translate the column set to use the column IDs of the output relation.
	cols := translateColSet(leftStats.Cols, colMap.Left, colMap.Out)

	var distinctCount uint64
	// These calculations are an upper bound on the distinct count. It's likely
	// that there is some overlap between the two sets, but not full overlap.
	switch op {
	case opt.UnionOp, opt.UnionAllOp:
		distinctCount = leftStats.DistinctCount + rightStats.DistinctCount

	case opt.IntersectOp, opt.IntersectAllOp:
		distinctCount = min(leftStats.DistinctCount, rightStats.DistinctCount)

	case opt.ExceptOp, opt.ExceptAllOp:
		distinctCount = leftStats.DistinctCount
	}

	switch op {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		// Since UNION, INTERSECT and EXCEPT eliminate duplicate rows, the row
		// count will equal the distinct count if this column set matches the
		// output columns.
		//
		// TODO(rytaft): we may be able to update the row count even if cols
		// doesn't exactly match outputCols. For example, if the output cols are
		// (a, b), and we have stats for a and b individually, we can estimate
		// the row count as a.DistinctCount * b.DistinctCount.
		if cols.Equals(*outputCols) {
			s.RowCount = distinctCount
		}
	}

	s.ColStats = append(s.ColStats, ColumnStatistics{
		Cols:          cols,
		DistinctCount: distinctCount,
	})
}

func min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// translateColSet is used to translate a ColSet from one set of column IDs
// to an equivalent set. This is relevant for set operations such as UNION,
// INTERSECT and EXCEPT, and can be used to map a ColSet defined on the left
// relation to an equivalent ColSet on the right relation (or between any two
// relations with a defined column mapping).
//
// For example, suppose we have a UNION with the following column mapping:
//   Left:  1, 2, 3
//   Right: 4, 5, 6
//   Out:   7, 8, 9
//
// Here are some possible calls to translateColSet and their results:
//   translateColSet(ColSet{1, 2}, Left, Right) -> ColSet{4, 5}
//   translateColSet(ColSet{5, 6}, Right, Out)  -> ColSet{8, 9}
//   translateColSet(ColSet{9}, Out, Right)     -> ColSet{6}
//
func translateColSet(colSetIn opt.ColSet, from opt.ColList, to opt.ColList) opt.ColSet {
	var colSetOut opt.ColSet
	for i := range from {
		if colSetIn.Contains(int(from[i])) {
			colSetOut.Add(int(to[i]))
		}
	}

	return colSetOut
}

func (s *Statistics) updateValuesStats(ev ExprView, outputCols *opt.ColSet) {
	s.RowCount = uint64(ev.ChildCount())

	// Determine distinct count from the number of distinct memo groups.
	// Get the distinct count per column and for all columns.
	if ev.ChildCount() > 0 {
		colStatsCount := ev.Child(0).ChildCount()
		multiCol := colStatsCount > 1
		if multiCol {
			colStatsCount++
		}

		// Initialize the array of maps.
		distinct := make([]map[GroupID]struct{}, colStatsCount)
		for i := 0; i < colStatsCount; i++ {
			distinct[i] = make(map[GroupID]struct{})
		}

		// Use array of maps to find the exact count of distinct values.
		var exists = struct{}{}
		for i := 0; i < ev.ChildCount(); i++ {
			for j := 0; j < ev.Child(i).ChildCount(); j++ {
				distinct[j][ev.Child(i).ChildGroup(j)] = exists
			}

			if multiCol {
				distinct[colStatsCount-1][ev.ChildGroup(i)] = exists
			}
		}

		// Update the column statistics.
		s.ColStats = make([]ColumnStatistics, colStatsCount)
		colList := ev.Private().(opt.ColList)
		for i := 0; i < len(colList); i++ {
			s.ColStats[i].Cols = util.MakeFastIntSet(int(colList[i]))
			s.ColStats[i].DistinctCount = uint64(len(distinct[i]))
		}

		if multiCol {
			s.ColStats[colStatsCount-1].Cols = *outputCols
			s.ColStats[colStatsCount-1].DistinctCount = uint64(len(distinct[colStatsCount-1]))
		}
	}
}

func (s *Statistics) updateLimitStats(limit ExprView, inputStats *Statistics) {
	s.ColStats = make([]ColumnStatistics, len(inputStats.ColStats))
	copy(s.ColStats, inputStats.ColStats)

	// Update row count and distinct count if limit is a constant.
	if limit.Operator() == opt.ConstOp {
		hardLimit := *limit.Private().(*tree.DInt)
		if hardLimit > 0 {
			s.RowCount = uint64(hardLimit)

			for i := range s.ColStats {
				colStats := &s.ColStats[i]
				colStats.DistinctCount = min(uint64(hardLimit), colStats.DistinctCount)
			}
		}
	}
}

func (s *Statistics) updateMax1RowStats(inputStats *Statistics) {
	s.ColStats = make([]ColumnStatistics, len(inputStats.ColStats))
	copy(s.ColStats, inputStats.ColStats)

	// Update row count and distinct count.
	s.RowCount = 1

	for i := range s.ColStats {
		s.ColStats[i].DistinctCount = 1
	}
}

// filterColumnStats adds column statistics from inputColStats to s.ColStats,
// but only for column sets that are fully contained in outputCols.
func (s *Statistics) filterColumnStats(inputColStats []ColumnStatistics, outputCols *opt.ColSet) {
	s.ColStats = make([]ColumnStatistics, 0, len(inputColStats))
	for i := range inputColStats {
		colStats := &inputColStats[i]
		if colStats.Cols.SubsetOf(*outputCols) {
			s.ColStats = append(s.ColStats, *colStats)
		}
	}
}
