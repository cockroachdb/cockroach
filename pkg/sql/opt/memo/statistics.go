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
	"math"

	"bytes"
	"encoding/binary"

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
	ColStats ColumnStatistics
}

// ColumnStatistic is a collection of statistics that applies to a particular
// set of columns. In theory, a table could have a ColumnStatistic object
// for every possible subset of columns. In practice, it is only worth
// maintaining statistics on a few columns and column sets that are frequently
// used in predicates, group by columns, etc.
type ColumnStatistic struct {
	// Cols is the set of columns whose data are summarized by this
	// ColumnStatistic struct.
	Cols opt.ColSet

	// DistinctCount is the estimated number of distinct values of this
	// set of columns for this expression.
	DistinctCount uint64
}

// ColumnStatistics is a slice of ColumnStatistic values.
type ColumnStatistics []ColumnStatistic

// Len returns the number of ColumnStatistic values.
func (c ColumnStatistics) Len() int { return len(c) }

// Less is part of the Sorter interface.
func (c ColumnStatistics) Less(i, j int) bool {
	if c[i].Cols.Len() != c[j].Cols.Len() {
		return c[i].Cols.Len() < c[j].Cols.Len()
	}

	prev := 0
	for {
		nextI, ok := c[i].Cols.Next(prev)
		if !ok {
			return false
		}

		// No need to check if ok since both ColSets are the same length and
		// so far have had the same elements.
		nextJ, _ := c[j].Cols.Next(prev)

		if nextI != nextJ {
			return nextI < nextJ
		}

		prev = nextI
	}
}

// Swap is part of the Sorter interface.
func (c ColumnStatistics) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (s *Statistics) applyFilterNoStats() {
	// We don't have any info about the data distribution, so just make a rough
	// estimate.
	s.RowCount /= 10
}

func (s *Statistics) applyConstraint(evalCtx *tree.EvalContext, c *constraint.Constraint) {
	if c.IsUnconstrained() {
		return
	}

	if c.IsContradiction() {
		s.updateStatsFromContradiction()
		return
	}

	if len(s.ColStats) == 0 {
		s.applyFilterNoStats()
		return
	}

	distinctCounts := make(map[int]uint64, c.Columns.Count())
	applied := s.updateDistinctCountsFromConstraint(evalCtx, c, distinctCounts)

	// If applied is false, it means the distinct counts do not represent the
	// selectivity of the filter.
	s.updateFromDistinctCounts(distinctCounts, applied)
}

func (s *Statistics) applyConstraintSet(evalCtx *tree.EvalContext, cs *constraint.Set, tight bool) {
	if cs.IsUnconstrained() && tight {
		return
	}

	if cs == constraint.Contradiction {
		s.updateStatsFromContradiction()
		return
	}

	if len(s.ColStats) == 0 {
		s.applyFilterNoStats()
		return
	}

	distinctCounts := make(map[int]uint64, cs.Length())
	for i := 0; i < cs.Length(); i++ {
		applied := s.updateDistinctCountsFromConstraint(evalCtx, cs.Constraint(i), distinctCounts)

		// If a constraint could not be applied, it means the distinct counts do
		// not fully represent the selectivity of the constraint set.
		tight = tight && applied
	}

	s.updateFromDistinctCounts(distinctCounts, tight)
}

// updateStatsFromContradiction sets the row count and distinct count to zero,
// since a contradiction results in 0 rows.
func (s *Statistics) updateStatsFromContradiction() {
	s.RowCount = 0
	for i := range s.ColStats {
		s.ColStats[i].DistinctCount = 0
	}
}

// updateFromDistinctCounts updates the column statistics given a map from
// column ID to distinct count.
func (s *Statistics) updateFromDistinctCounts(distinctCounts map[int]uint64, tight bool) {
	var constrainedCols opt.ColSet
	for k := range distinctCounts {
		constrainedCols.Add(k)
	}

	// Use the new stats to update the old stats as follows:
	//
	// For each column statistic containing a subset of the columns constrained
	// by the filter, update the distinct count to be the product of the distinct
	// counts of each of its columns (if less than the existing distinct count).
	//
	// For example, consider the following query:
	//
	//   SELECT * FROM t WHERE x BETWEEN 5 AND 10 AND y BETWEEN 10 AND 20
	//
	// From this query we can determine that distinct(x)<=5, distinct(y)<=10,
	// and distinct(x,y)<=50. All three corresponding statistics (if they exist)
	// will be updated with these values, unless doing so would overwrite a
	// lower value.
	//
	// We also calculate the selectivity of the entire constraint set by taking
	// the product of selectivities of each constrained column. This can be
	// represented by the formula:
	//
	//                  ┬-┬ ⎛ new distinct(i) ⎞
	//   selectivity =  │ │ ⎜ --------------- ⎟
	//                  ┴ ┴ ⎝ old distinct(i) ⎠
	//                 i in
	//              {constrained
	//                columns}
	//
	// If the constraint is not tight, we also multiply by a "fudge factor"
	// of 1/3.
	//
	// This selectivity will be used to update the row count and the distinct
	// count for the unconstrained columns in applySelectivity.
	selectivity := 1.0
	for i := range s.ColStats {
		colStat := &s.ColStats[i]
		if colStat.Cols.SubsetOf(constrainedCols) {
			distinctCount := uint64(1)
			colStat.Cols.ForEach(func(i int) {
				distinctCount *= distinctCounts[i]
			})
			distinctCount = min(distinctCount, colStat.DistinctCount)

			// Update selectivity for single column stats.
			if colStat.Cols.Len() == 1 {
				selectivity *= float64(distinctCount) / float64(colStat.DistinctCount)
			}
			colStat.DistinctCount = distinctCount
		}
	}

	// If the constraints are not tight, our calculations are pessimistic (they
	// are based only on part of the filter). Adjust the selectivity to account
	// for the remaining filter.
	if !tight {
		// TODO(rytaft): This is arbitrary, but is similar to what other optimizers
		// have done (e.g., Spark Catalyst). See if we can improve this estimate.
		selectivity /= 3
	}

	// Using the newly calculated selectivity, update the row count and
	// other distinct counts.
	s.applySelectivity(selectivity, constrainedCols)
}

// applySelectivity updates the row and column statistics given a filter
// selectivity. It skips over the column statistics for constrainedCols, since
// those columns are constrained by the filter and their statistics were
// already updated.
func (s *Statistics) applySelectivity(selectivity float64, constrainedCols opt.ColSet) {
	n := float64(s.RowCount)

	// Cast to int64 and then to uint64 to make the linter happy.
	s.RowCount = max(1, uint64(int64(n*selectivity)))
	for i := range s.ColStats {
		colStat := &s.ColStats[i]

		// Only update the stats that weren't already updated.
		if colStat.DistinctCount != 0 && !colStat.Cols.SubsetOf(constrainedCols) {
			d := float64(colStat.DistinctCount)

			// If each distinct value appears n/d times, and the probability of a
			// row being filtered out is (1 - selectivity), the probability that all
			// n/d rows are filtered out is (1 - selectivity)^(n/d). So the expected
			// number of values that are filtered out is d*(1 - selectivity)^(n/d).
			//
			// This formula returns d * selectivity when d=n but is closer to d
			// when d << n.
			d = d - d*math.Pow(1-selectivity, n/d)
			colStat.DistinctCount = max(1, uint64(int64(d)))
		}
	}
}

// updateDistinctCountsFromConstraint gets the distinct count for each column
// in a constraint that can be determined to have a finite number of possible
// values. It updates distinctCounts, which is a map from column ID to distinct
// count, and returns a boolean indicating if the constraint was applied to the
// map (i.e., the distinctCount for at least one column could be inferred from
// the constraint). distinctCounts may be reused across multiple calls to
// updateDistinctCountsFromConstraint in order to determine distinct counts
// from a constraint set. If the same column appears in multiple constraints,
// the returned distinct count is the minimum for that column across all
// constraints.
//
// For example, consider the following constraint set:
//
//   /a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]
//   /c: [/6 - /6]
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
// Note that updateDistinctCountsFromConstraint is pessimistic, and assumes
// that there is at least one row for every possible value provided by the
// constraint. For example, /a: [/1 - /1000000] would return [a] -> 1000000
// even if there are only 10 rows in the table. This discrepancy must be
// resolved by the calling function.
func (s *Statistics) updateDistinctCountsFromConstraint(
	evalCtx *tree.EvalContext, c *constraint.Constraint, distinctCounts map[int]uint64,
) (applied bool) {
	// All of the columns that are part of the exact prefix have exactly one
	// distinct value.
	exactPrefix := c.ExactPrefix(evalCtx)
	for i := 0; i < exactPrefix; i++ {
		id := int(c.Columns.Get(i).ID())
		distinctCounts[id], applied = 1, true
	}

	// If there are no other columns beyond the exact prefix, we are done.
	if exactPrefix >= c.Columns.Count() {
		return applied
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
			return applied
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
				return applied
			}
		}
		if i == 0 {
			val = startVal
		} else if startVal.Compare(evalCtx, val) != 0 {
			// This check is needed to ensure that we calculate the correct distinct
			// value count for constraints such as:
			//   /a/b: [/1/2 - /1/2] [/1/4 - /1/4] [/2 - /2]
			// We should only increment the distinct count for column a once we reach
			// the third span.
			distinctCount++
		}
	}

	id := int(c.Columns.Get(col).ID())
	if v, ok := distinctCounts[id]; ok {
		distinctCounts[id] = min(v, distinctCount)
	} else {
		distinctCounts[id] = distinctCount
	}

	return true /* applied */
}

// writeColSet writes a series of varints, one for each column in the set, in
// column id order. Can be used as a key in a map.
func writeColSet(colSet opt.ColSet) string {
	var buf [10]byte
	var res bytes.Buffer
	colSet.ForEach(func(i int) {
		cnt := binary.PutUvarint(buf[:], uint64(i))
		res.Write(buf[:cnt])
	})
	return res.String()
}

func (s *Statistics) colSetFromTableStatistic(
	md *opt.Metadata, stat opt.TableStatistic, tableID opt.TableID,
) (cols opt.ColSet) {
	for i := 0; i < stat.ColumnCount(); i++ {
		cols.Add(int(md.TableColumn(tableID, stat.ColumnOrdinal(i))))
	}
	return cols
}

func (s *Statistics) initScan(
	evalCtx *tree.EvalContext, md *opt.Metadata, def *ScanOpDef, tab opt.Table,
) {
	if tab.StatisticCount() == 0 {
		// No statistics.
		s.RowCount = 1000
	} else {
		// Get the RowCount from the most recent statistic.
		mostRecent, mostRecentTime := 0, tab.Statistic(0).CreatedAt()
		for i := 1; i < tab.StatisticCount(); i++ {
			if t := tab.Statistic(i).CreatedAt(); mostRecentTime.Before(t) {
				mostRecent, mostRecentTime = i, t
			}
		}
		s.RowCount = tab.Statistic(mostRecent).RowCount()

		// Add all the column statistics, using the most recent statistic for each
		// column set.
		mostRecentColStats := make(map[string]opt.TableStatistic, tab.StatisticCount())
		for i := 0; i < tab.StatisticCount(); i++ {
			stat := tab.Statistic(i)
			cols := s.colSetFromTableStatistic(md, stat, def.Table)

			// Get a unique key for this column set.
			key := writeColSet(cols)

			if val, ok := mostRecentColStats[key]; !ok || val.CreatedAt().Before(stat.CreatedAt()) {
				mostRecentColStats[key] = stat
			}
		}

		s.ColStats = make(ColumnStatistics, 0, len(mostRecentColStats))
		for _, stat := range mostRecentColStats {
			// TODO(rytaft): for columns that are keys, get the distinct count from
			// the row count.
			cols := s.colSetFromTableStatistic(md, stat, def.Table)
			s.ColStats = append(s.ColStats, ColumnStatistic{Cols: cols, DistinctCount: stat.DistinctCount()})
		}

		s.filterColumnStats(s.ColStats, &def.Cols)
	}

	if def.Constraint != nil {
		s.applyConstraint(evalCtx, def.Constraint)
	}

	// Cap number of rows at limit, if it exists.
	if def.HardLimit > 0 && uint64(def.HardLimit) < s.RowCount {
		s.RowCount = uint64(def.HardLimit)

		for i := range s.ColStats {
			colStat := &s.ColStats[i]
			colStat.DistinctCount = min(colStat.DistinctCount, uint64(def.HardLimit))
		}
	}
}

func (s *Statistics) initSelect(
	evalCtx *tree.EvalContext, filter ExprView, inputStats *Statistics,
) {
	// Copy stats from input.
	s.copyColStats(inputStats)

	// Update stats based on filter conditions.
	constraintSet := filter.Logical().Scalar.Constraints
	if constraintSet != nil {
		s.applyConstraintSet(evalCtx, constraintSet, filter.Logical().Scalar.TightConstraints)
	} else {
		// TODO: Need better estimate based on actual filter conditions.
		selectivity := 1.0 / 10.0
		s.applySelectivity(selectivity, opt.ColSet{} /* constrainedCols */)
	}
}

func (s *Statistics) initProject(inputStats *Statistics, outputCols *opt.ColSet) {
	s.RowCount = inputStats.RowCount

	// Inherit column statistics from input, but only for columns that are also
	// output columns.
	s.filterColumnStats(inputStats.ColStats, outputCols)

	// TODO(rytaft): Try to update the distinct count for computed columns based
	// on the distinct count of the input columns.
}

func (s *Statistics) initJoin(op opt.Operator, leftStats, rightStats *Statistics, on ExprView) {
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
		s.ColStats = make(ColumnStatistics, len(leftStats.ColStats))
		copy(s.ColStats, leftStats.ColStats)

	default:
		// Union column stats from both sides of join.
		cs := len(leftStats.ColStats)
		s.ColStats = append(leftStats.ColStats[:cs:cs], rightStats.ColStats...)
	}
}

func (s *Statistics) initGroupBy(inputStats *Statistics, groupingColSet, outputCols *opt.ColSet) {
	if groupingColSet.Empty() {
		// Scalar group by.
		s.RowCount = 1
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

func (s *Statistics) initSetOp(
	op opt.Operator, leftStats, rightStats *Statistics, colMap *SetOpColMap,
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
	var lgStats, smStats ColumnStatistics
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
	// column ID.
	colStatsMap := make(map[opt.ColumnID][]*ColumnStatistic, len(smStats))
	for i := range smStats {
		smColStat := &smStats[i]

		// Only use single-column statistics.
		if smColStat.Cols.Len() != 1 {
			continue
		}

		// Create key(s) using column IDs from the relation with the larger set
		// of statistics.
		key := translateColSet(smColStat.Cols, smColList, lgColList)
		key.ForEach(func(i int) {
			colStatsMap[opt.ColumnID(i)] = append(colStatsMap[opt.ColumnID(i)], smColStat)
		})
	}

	// Iterate through the larger set of statistics, probing into the map to
	// see if there is a match with the smaller set.
	distinctCounts := make(map[opt.ColumnID]uint64, len(colStatsMap))
	for i := range lgStats {
		lgColStat := &lgStats[i]

		// Only use single-column statistics.
		if lgColStat.Cols.Len() != 1 {
			continue
		}
		val, _ := lgColStat.Cols.Next(0)

		if smColStats, ok := colStatsMap[opt.ColumnID(val)]; ok {
			for _, smColStat := range smColStats {
				// We found a match! Update the distinct counts for the output relation.
				var leftStat, rightStat *ColumnStatistic
				if leftIsLarger {
					leftStat, rightStat = lgColStat, smColStat
				} else {
					leftStat, rightStat = smColStat, lgColStat
				}

				s.updateDistinctCountsForSetOp(op, leftStat, rightStat, colMap, distinctCounts)
			}
		}
	}

	// Update the column stats based on the distinct counts.
	s.ColStats = make(ColumnStatistics, 0, len(distinctCounts))
	for col, distinctCount := range distinctCounts {
		s.ColStats = append(s.ColStats, ColumnStatistic{DistinctCount: distinctCount})
		s.ColStats[len(s.ColStats)-1].Cols.Add(int(col))
	}

	switch op {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		// Since UNION, INTERSECT and EXCEPT eliminate duplicate rows, the row
		// count will equal the distinct count of the set of output columns.
		// This is approximately equal to the product of the distinct counts
		// of the individual columns.
		if len(s.ColStats) != 0 {
			distinctCount := uint64(1)
			for _, c := range s.ColStats {
				distinctCount *= c.DistinctCount
			}
			s.RowCount = min(s.RowCount, distinctCount)
		}
	}
}

// updateDistinctCountsForSetOp updates the distinct counts for the set of
// output columns produced by merging the columns from leftStats and
// rightStats.
func (s *Statistics) updateDistinctCountsForSetOp(
	op opt.Operator,
	leftStat, rightStat *ColumnStatistic,
	colMap *SetOpColMap,
	distinctCounts map[opt.ColumnID]uint64,
) {
	var distinctCount uint64
	// These calculations are an upper bound on the distinct count. It's likely
	// that there is some overlap between the two sets, but not full overlap.
	switch op {
	case opt.UnionOp, opt.UnionAllOp:
		distinctCount = leftStat.DistinctCount + rightStat.DistinctCount

	case opt.IntersectOp, opt.IntersectAllOp:
		distinctCount = min(leftStat.DistinctCount, rightStat.DistinctCount)

	case opt.ExceptOp, opt.ExceptAllOp:
		distinctCount = leftStat.DistinctCount
	}

	// Translate the column set to use the column IDs of the output relation.
	cols := translateColSet(leftStat.Cols, colMap.Left, colMap.Out)
	cols.IntersectionWith(translateColSet(rightStat.Cols, colMap.Right, colMap.Out))
	cols.ForEach(func(i int) {
		// Use the maximum distinct count for the output column.
		if val, ok := distinctCounts[opt.ColumnID(i)]; !ok || val < distinctCount {
			distinctCounts[opt.ColumnID(i)] = distinctCount
		}
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
// Note that for the output of translateColSet to be correct, colSetIn must be
// a subset of the columns in `from`. translateColSet does not check that this
// is the case, because that would require building a ColSet from `from`, and
// checking that colSetIn.SubsetOf(fromColSet) is true -- a lot of computation
// for a validation check. It is not correct or sufficient to check that
// colSetIn.Len() == colSetOut.Len(), because it is possible that colSetIn and
// colSetOut could have different lengths and still be valid. Consider the
// following case:
//
//   SELECT x, x, y FROM xyz UNION SELECT a, b, c FROM abc
//
// translateColSet(ColSet{x, y}, Left, Right) correctly returns
// ColSet{a, b, c}, even though ColSet{x, y}.Len() != ColSet{a, b, c}.Len().
func translateColSet(colSetIn opt.ColSet, from opt.ColList, to opt.ColList) opt.ColSet {
	var colSetOut opt.ColSet
	for i := range from {
		if colSetIn.Contains(int(from[i])) {
			colSetOut.Add(int(to[i]))
		}
	}

	return colSetOut
}

// initValues initializes the statistics for a VALUES expression.
func (s *Statistics) initValues(ev ExprView, outputCols *opt.ColSet) {
	s.RowCount = uint64(ev.ChildCount())

	// Determine distinct count from the number of distinct memo groups.
	// Get the distinct count per column and for all columns.
	if ev.ChildCount() > 0 {
		multiCol := ev.Child(0).ChildCount() > 1
		multiColRowGroups := util.FastIntSet{}

		// Use array of sets to find the exact count of distinct values.
		distinct := make([]util.FastIntSet, ev.Child(0).ChildCount())
		for i := 0; i < ev.ChildCount(); i++ {
			for j := 0; j < ev.Child(i).ChildCount(); j++ {
				distinct[j].Add(int(ev.Child(i).ChildGroup(j)))
			}

			if multiCol {
				multiColRowGroups.Add(int(ev.ChildGroup(i)))
			}
		}

		// Update the column statistics.
		s.ColStats = make(ColumnStatistics, 0, ev.Child(0).ChildCount()+1)
		colList := ev.Private().(opt.ColList)
		for i := 0; i < len(colList); i++ {
			s.ColStats = append(s.ColStats, ColumnStatistic{DistinctCount: uint64(distinct[i].Len())})
			s.ColStats[i].Cols.Add(int(colList[i]))
		}

		if multiCol {
			s.ColStats = append(s.ColStats, ColumnStatistic{
				Cols:          *outputCols,
				DistinctCount: uint64(multiColRowGroups.Len()),
			})
		}
	}
}

func (s *Statistics) initLimit(limit ExprView, inputStats *Statistics) {
	// Copy stats from input.
	s.copyColStats(inputStats)

	// Update row count and distinct count if limit is a constant.
	if limit.Operator() == opt.ConstOp {
		hardLimit := *limit.Private().(*tree.DInt)
		if hardLimit > 0 {
			s.RowCount = min(uint64(hardLimit), inputStats.RowCount)

			for i := range s.ColStats {
				colStats := &s.ColStats[i]
				colStats.DistinctCount = min(uint64(hardLimit), colStats.DistinctCount)
			}
		}
	}
}

func (s *Statistics) initMax1Row(inputStats *Statistics) {
	// Update row count.
	s.RowCount = 1
	s.ColStats = nil
}

// filterColumnStats adds column statistics from inputColStats to s.ColStats,
// but only for column sets that are fully contained in outputCols.
func (s *Statistics) filterColumnStats(inputColStats ColumnStatistics, outputCols *opt.ColSet) {
	s.ColStats = nil
	for i := range inputColStats {
		colStats := &inputColStats[i]
		if colStats.Cols.SubsetOf(*outputCols) {
			if s.ColStats == nil {
				s.ColStats = make(ColumnStatistics, 0, len(inputColStats)-i)
			}
			s.ColStats = append(s.ColStats, *colStats)
		}
	}
}

func (s *Statistics) copyColStats(inputStats *Statistics) {
	s.RowCount = inputStats.RowCount
	s.ColStats = make(ColumnStatistics, len(inputStats.ColStats))
	copy(s.ColStats, inputStats.ColStats)
}
