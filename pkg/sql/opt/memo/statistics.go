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

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Statistics is a collection of measurements and statistics that is used by
// the coster to estimate the cost of expressions. See the comment above
// opt.Statistics in sql/opt/metadata.go for more details.
type Statistics struct {
	*opt.Statistics

	// ev is the ExprView for which these statistics are valid.
	ev ExprView

	// selectivity is used by the scan, select, and join operators to store
	// the selectivity of the filter.
	selectivity float64
	evalCtx     *tree.EvalContext
}

func (s *Statistics) init(ev ExprView, evalCtx *tree.EvalContext) {
	s.Statistics = &opt.Statistics{}
	s.ev = ev
	s.selectivity = 1
	s.evalCtx = evalCtx
	s.ColStats = make(map[opt.ColumnID]*opt.ColumnStatistic)
	s.MultiColStats = make(map[string]*opt.ColumnStatistic)
}

// colStat gets a column statistic for the given set of columns if it exists.
// If the column statistic is not available in the current Statistics object,
// colStat recursively tries to find it in the children of the expression,
// lazily populating either s.ColStats or s.MultiColStats with the statistic
// as it gets passed up the expression tree. If the statistic cannot be
// determined from the children, colStat returns ok=false.
func (s *Statistics) colStat(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	if colSet.Len() == 0 {
		return nil, false
	}

	if colSet.Len() == 1 {
		col, _ := colSet.Next(0)
		return s.singleColStat(opt.ColumnID(col))
	}
	return s.multiColStat(colSet)
}

func (s *Statistics) singleColStat(col opt.ColumnID) (_ *opt.ColumnStatistic, ok bool) {
	if stat, ok := s.ColStats[col]; ok {
		return stat, true
	}

	return s.colStatFromChildren(util.MakeFastIntSet(int(col)))
}

func (s *Statistics) multiColStat(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	key := keyBuffer{}
	key.writeColSet(colSet)
	if stat, ok := s.MultiColStats[key.String()]; ok {
		return stat, true
	}

	return s.colStatFromChildren(colSet)
}

func (s *Statistics) colStatFromChildren(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	switch s.ev.Operator() {
	case opt.UnknownOp:
		return nil, false

	case opt.ScanOp:
		return s.colStatScan(colSet)

	case opt.SelectOp:
		return s.colStatSelect(colSet)

	case opt.ProjectOp:
		return s.colStatProject(colSet)

	case opt.ValuesOp:
		return s.colStatValues(colSet)

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		return s.colStatJoin(colSet)

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		return s.colStatSetOp(colSet)

	case opt.GroupByOp:
		return s.colStatGroupBy(colSet)

	case opt.LimitOp:
		return s.colStatLimit(colSet)

	case opt.OffsetOp:
		return s.colStatOffset(colSet)

	case opt.Max1RowOp:
		return s.colStatMax1Row(colSet)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", s.ev.op))
}

// ensureColStat creates a column statistic for column "col" if it doesn't
// already exist in s.ColStats, and sets the distinct count to the provided
// value. If the statistic already exists, ensureColStat sets the distinct
// count to the minimum of the existing value and the new value.
func (s *Statistics) ensureColStat(col opt.ColumnID, distinctCount uint64) *opt.ColumnStatistic {
	if colStat, ok := s.singleColStat(col); ok {
		colStat.DistinctCount = min(colStat.DistinctCount, distinctCount)
		return colStat
	}

	colStat := &opt.ColumnStatistic{DistinctCount: distinctCount}
	colStat.Cols.Add(int(col))
	s.ColStats[col] = colStat
	return colStat
}

// makeColStat creates a column statistic for the given set of columns, and
// returns a pointer to the newly created statistic.
func (s *Statistics) makeColStat(colSet opt.ColSet) *opt.ColumnStatistic {
	colStat := &opt.ColumnStatistic{Cols: colSet}
	if colSet.Len() == 1 {
		col, _ := colSet.Next(0)
		s.ColStats[opt.ColumnID(col)] = colStat
	} else {
		key := keyBuffer{}
		key.writeColSet(colSet)
		s.MultiColStats[key.String()] = colStat
	}

	return colStat
}

// ColumnStatistics is a slice of ColumnStatistic values.
type ColumnStatistics []opt.ColumnStatistic

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

const (
	// This is the value used for inequality filters such as x < 1 in
	// "Access Path Selection in a Relational Database Management System"
	// by Pat Selinger et al.
	unknownFilterSelectivity = 1.0 / 3.0

	// TODO(rytaft): Add other selectivities for other types of predicates.
)

func (s *Statistics) applyConstraint(
	c *constraint.Constraint, inputStats *Statistics,
) (selectivity float64) {
	if c.IsUnconstrained() {
		return 1 /* selectivity */
	}

	if c.IsContradiction() {
		// A contradiction results in 0 rows.
		return 0 /* selectivity */
	}

	if applied := s.updateDistinctCountsFromConstraint(c); !applied {
		// If a constraint cannot be applied, it probably represents an
		// inequality like x < 1. As a result, distinctCounts does not
		// represent the selectivity of the constraint. Return a
		// rough guess for the selectivity.
		return unknownFilterSelectivity
	}

	return s.selectivityFromDistinctCounts(inputStats)
}

func (s *Statistics) applyConstraintSet(
	cs *constraint.Set, inputStats *Statistics,
) (selectivity float64) {
	if cs.IsUnconstrained() {
		return 1 /* selectivity */
	}

	if cs == constraint.Contradiction {
		// A contradiction results in 0 rows.
		return 0 /* selectivity */
	}

	adjustedSelectivity := 1.0
	for i := 0; i < cs.Length(); i++ {
		applied := s.updateDistinctCountsFromConstraint(cs.Constraint(i))
		if !applied {
			// If a constraint cannot be applied, it probably represents an
			// inequality like x < 1. As a result, distinctCounts does not fully
			// represent the selectivity of the constraint set. Adjust the
			// selectivity to account for this constraint.
			adjustedSelectivity *= unknownFilterSelectivity
		}
	}

	selectivity = s.selectivityFromDistinctCounts(inputStats)
	return selectivity * adjustedSelectivity
}

// updateStatsFromContradiction sets the row count and distinct count to zero,
// since a contradiction results in 0 rows.
func (s *Statistics) updateStatsFromContradiction() {
	s.RowCount = 0
	for i := range s.ColStats {
		s.ColStats[i].DistinctCount = 0
	}
	for i := range s.MultiColStats {
		s.MultiColStats[i].DistinctCount = 0
	}
}

// selectivityFromDistinctCounts calculates the selectivity of a filter by
// taking the product of selectivities of each constrained column. This can be
// represented by the formula:
//
//                  ┬-┬ ⎛ new distinct(i) ⎞
//   selectivity =  │ │ ⎜ --------------- ⎟
//                  ┴ ┴ ⎝ old distinct(i) ⎠
//                 i in
//              {constrained
//                columns}
//
// If we do not have a value for the old distinct count of a column, we instead
// estimate the selectivity of the constrained column as 1/3.
//
// This selectivity will be used later to update the row count and the
// distinct count for the unconstrained columns in applySelectivityToColStat.
//
// TODO(rytaft): This formula assumes that the columns are completely
// independent. Improve this estimate to take functional dependencies and/or
// column correlations into account.
func (s *Statistics) selectivityFromDistinctCounts(inputStats *Statistics) (selectivity float64) {
	selectivity = 1.0
	for col, colStat := range s.ColStats {
		if inputStat, ok := inputStats.singleColStat(col); ok {
			if inputStat.DistinctCount != 0 {
				selectivity *= float64(colStat.DistinctCount) / float64(inputStat.DistinctCount)
			}
		} else {
			// TODO(rytaft): improve this estimate.
			selectivity *= unknownFilterSelectivity
		}
	}

	return selectivity
}

// applySelectivityToColStat updates the given column statistics according to
// the filter selectivity.
func (s *Statistics) applySelectivityToColStat(colStat *opt.ColumnStatistic, inputRows uint64) {
	if s.selectivity == 0 {
		colStat.DistinctCount = 0
		return
	}

	n := float64(inputRows)
	d := float64(colStat.DistinctCount)

	// If each distinct value appears n/d times, and the probability of a
	// row being filtered out is (1 - selectivity), the probability that all
	// n/d rows are filtered out is (1 - selectivity)^(n/d). So the expected
	// number of values that are filtered out is d*(1 - selectivity)^(n/d).
	//
	// This formula returns d * selectivity when d=n but is closer to d
	// when d << n.
	d = d - d*math.Pow(1-s.selectivity, n/d)

	// Cast to int64 and then to uint64 to make the linter happy.
	colStat.DistinctCount = max(1, uint64(int64(d)))
}

// applySelectivity updates the row count according to the filter selectivity,
// and ensures that no distinct counts are larger than the row count.
func (s *Statistics) applySelectivity(inputRows uint64) {
	if s.selectivity == 0 {
		s.updateStatsFromContradiction()
		return
	}

	s.RowCount = max(1, uint64(int64(float64(inputRows)*s.selectivity)))

	// At this point we only have single-column stats on columns that were
	// constrained by the filter. Make sure none of the distinct counts are
	// larger than the row count.
	for _, colStat := range s.ColStats {
		colStat.DistinctCount = min(colStat.DistinctCount, s.RowCount)
	}
}

// updateDistinctCountsFromConstraint updates the distinct count for each
// column in a constraint that can be determined to have a finite number of
// possible values. It returns a boolean indicating if the constraint was
// applied (i.e., the distinct count for at least one column could be inferred
// from the constraint). If the same column appears in multiple constraints,
// the distinct count is the minimum for that column across all constraints.
//
// For example, consider the following constraint set:
//
//   /a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]
//   /c: [/6 - /6]
//
// After the first constraint is processed, s.ColStats contains the
// following:
//   [a] -> { ... DistinctCount: 1 ... }
//   [b] -> { ... DistinctCount: 1 ... }
//   [c] -> { ... DistinctCount: 5 ... }
//
// After the second constraint is processed, column c is further constrained,
// so s.ColStats contains the following:
//   [a] -> { ... DistinctCount: 1 ... }
//   [b] -> { ... DistinctCount: 1 ... }
//   [c] -> { ... DistinctCount: 1 ... }
//
// Note that updateDistinctCountsFromConstraint is pessimistic, and assumes
// that there is at least one row for every possible value provided by the
// constraint. For example, /a: [/1 - /1000000] would find a distinct count of
// 1000000 for column "a" even if there are only 10 rows in the table. This
// discrepancy must be resolved by the calling function.
func (s *Statistics) updateDistinctCountsFromConstraint(c *constraint.Constraint) (applied bool) {
	// All of the columns that are part of the exact prefix have exactly one
	// distinct value.
	exactPrefix := c.ExactPrefix(s.evalCtx)
	for i := 0; i < exactPrefix; i++ {
		s.ensureColStat(c.Columns.Get(i).ID(), 1 /* distinctCount */)
		applied = true
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
		if startVal.Compare(s.evalCtx, sp.EndKey().Value(col)) != 0 {
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
		} else if startVal.Compare(s.evalCtx, val) != 0 {
			// This check is needed to ensure that we calculate the correct distinct
			// value count for constraints such as:
			//   /a/b: [/1/2 - /1/2] [/1/4 - /1/4] [/2 - /2]
			// We should only increment the distinct count for column "a" once we
			// reach the third span.
			distinctCount++
		}
	}

	s.ensureColStat(c.Columns.Get(col).ID(), distinctCount)
	return true /* applied */
}

func (s *Statistics) initScan(def *ScanOpDef) {
	inputStats := Statistics{Statistics: s.ev.Metadata().TableStatistics(def.Table)}

	if def.Constraint != nil {
		s.selectivity = s.applyConstraint(def.Constraint, &inputStats)
	}

	s.applySelectivity(inputStats.RowCount)

	// Cap number of rows at limit, if it exists.
	if def.HardLimit > 0 && uint64(def.HardLimit) < s.RowCount {
		s.RowCount = uint64(def.HardLimit)

		// At this point we only have single-column stats on columns that were
		// constrained by the filter.
		for _, colStat := range s.ColStats {
			colStat.DistinctCount = min(colStat.DistinctCount, uint64(def.HardLimit))
		}
	}
}

func (s *Statistics) colStatScan(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	def := s.ev.Private().(*ScanOpDef)

	inputStats := Statistics{Statistics: s.ev.Metadata().TableStatistics(def.Table)}
	colStat, ok := s.copyColStat(&inputStats, colSet)
	if !ok {
		return nil, false
	}

	s.applySelectivityToColStat(colStat, inputStats.RowCount)

	// Cap distinct count at limit, if it exists.
	if def.HardLimit > 0 && uint64(def.HardLimit) < s.RowCount {
		colStat.DistinctCount = min(colStat.DistinctCount, uint64(def.HardLimit))
	}

	return colStat, true
}

func (s *Statistics) initSelect(filter ExprView, inputStats *Statistics) {
	// Update stats based on filter conditions.
	//
	// Some stats can be determined directly from the constraint set. For
	// example, the constraint `/a: [/1 - /1]` indicates that column `a` has
	// exactly one distinct value. Other stats, such as the row count, must be
	// updated based on the selectivity of the filter.
	//
	// The selectivity of the filter can be calculated as the product of the
	// selectivities of the conjuncts in the filter. For example, the selectivity
	// of <pred1> AND <pred2> is selectivity(pred1) * selectivity(pred2).
	// The selectivity for each conjunct can be calculated in one of three ways:
	//
	// (1) If the predicate can be converted to a tight constraint set,
	//     applyConstraintSet calculates the selectivity of the constraint.
	//     See comments in applyConstraintSet and updateFromDistinctCounts
	//     for more details.
	//
	// (2) If only part of the predicate can be converted to a constraint set
	//     (i.e., it's not tight), the selectivity is calculated as:
	//     min(selectivity from applyConstraintSet, 1/3).
	//
	// (3) If we can't convert the predicate to a constraint set, the predicate
	//     is too complex to easily determine the selectivity, so use 1/3.
	//
	//     TODO(rytaft): we may be able to get a more precise estimate than
	//     1/3 for certain types of filters. For example, the selectivity of
	//     x=y can be estimated as 1/(max(distinct(x), distinct(y)).
	s.selectivity = 1
	sel := func(constraintSet *constraint.Set, tight bool) {
		if constraintSet != nil {
			childSelectivity := s.applyConstraintSet(constraintSet, inputStats)
			if !tight && childSelectivity > unknownFilterSelectivity {
				childSelectivity = unknownFilterSelectivity
			}
			s.selectivity *= childSelectivity
		} else {
			s.selectivity *= unknownFilterSelectivity
		}
	}

	constraintSet := filter.Logical().Scalar.Constraints
	tight := filter.Logical().Scalar.TightConstraints
	if (constraintSet != nil && tight) || (filter.op != opt.FiltersOp && filter.op != opt.AndOp) {
		// Shortcut if the top level constraint is tight or if we only have one
		// conjunct.
		sel(constraintSet, tight)
	} else {
		for i := 0; i < filter.ChildCount(); i++ {
			child := filter.Child(i)
			constraintSet = child.Logical().Scalar.Constraints
			tight = child.Logical().Scalar.TightConstraints
			sel(constraintSet, tight)
		}
	}

	s.applySelectivity(inputStats.RowCount)
}

func (s *Statistics) colStatSelect(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	inputStats := &s.ev.childGroup(0).logical.Relational.Stats

	colStat, ok := s.copyColStat(inputStats, colSet)
	if !ok {
		return nil, false
	}

	s.applySelectivityToColStat(colStat, inputStats.RowCount)
	return colStat, true
}

func (s *Statistics) initProject(inputStats *Statistics) {
	s.RowCount = inputStats.RowCount
}

func (s *Statistics) colStatProject(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	inputStats := &s.ev.childGroup(0).logical.Relational.Stats

	// Inherit column statistics from input.
	return s.copyColStat(inputStats, colSet)

	// TODO(rytaft): Try to update the distinct count for computed columns based
	// on the distinct count of the input columns.
}

func (s *Statistics) initJoin(op opt.Operator, leftStats, rightStats *Statistics, on ExprView) {
	// TODO: Need better estimate based on actual on conditions.
	s.RowCount = leftStats.RowCount * rightStats.RowCount
	if on.Operator() != opt.TrueOp {
		s.RowCount /= 10
	}
}

func (s *Statistics) colStatJoin(colSet opt.ColSet) (colStat *opt.ColumnStatistic, ok bool) {
	leftStats := &s.ev.childGroup(0).logical.Relational.Stats
	rightStats := &s.ev.childGroup(1).logical.Relational.Stats

	// The number of distinct values for the column subsets doesn't change
	// significantly unless the column subsets are part of the ON conditions.
	// For now, add them all unchanged.
	switch s.ev.Operator() {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Column stats come from left side of join.
		return s.copyColStat(leftStats, colSet)

	default:
		// Column stats come from both sides of join.
		leftCols := s.ev.Child(0).Logical().Relational.OutputCols
		leftCols.IntersectionWith(colSet)
		rightCols := s.ev.Child(1).Logical().Relational.OutputCols
		rightCols.IntersectionWith(colSet)

		if rightCols.Len() == 0 {
			return s.copyColStat(leftStats, leftCols)
		}

		if leftCols.Len() == 0 {
			return s.copyColStat(rightStats, rightCols)
		}

		if leftColStat, leftOk := leftStats.colStat(leftCols); leftOk {
			if rightColStat, rightOk := rightStats.colStat(rightCols); rightOk {
				colStat = s.makeColStat(colSet)
				colStat.DistinctCount = leftColStat.DistinctCount * rightColStat.DistinctCount
				return colStat, true
			}
		}
	}

	// TODO(rytaft): Apply selectivity to the distinct counts based on the join
	// condition.

	return nil, false
}

func (s *Statistics) initGroupBy(inputStats *Statistics, groupingColSet opt.ColSet) {
	if groupingColSet.Empty() {
		// Scalar group by.
		s.RowCount = 1
	} else {
		// See if we can determine the number of rows from the distinct counts.
		distinctRows := uint64(1)
		allCols := true
		if colStat, ok := s.copyColStat(inputStats, groupingColSet); ok {
			distinctRows = colStat.DistinctCount
		} else {
			groupingColSet.ForEach(func(i int) {
				if colStat, ok := s.copyColStat(inputStats, util.MakeFastIntSet(i)); ok {
					distinctRows *= colStat.DistinctCount
				} else {
					allCols = false
				}
			})
		}

		if allCols {
			// We can estimate the row count based on the distinct count of the
			// grouping columns.
			s.RowCount = min(distinctRows, inputStats.RowCount)
		} else {
			s.RowCount = inputStats.RowCount
		}
	}
}

func (s *Statistics) colStatGroupBy(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	inputStats := &s.ev.childGroup(0).logical.Relational.Stats
	groupingColSet := s.ev.Private().(opt.ColSet)

	if groupingColSet.Empty() {
		// Scalar group by.
		colStat := s.makeColStat(colSet)
		colStat.DistinctCount = 1
		return colStat, true
	}

	return s.copyColStat(inputStats, colSet)
}

func (s *Statistics) initSetOp(
	op opt.Operator, leftStats, rightStats *Statistics, colMap *SetOpColMap,
) {
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

	switch op {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		// Since UNION, INTERSECT and EXCEPT eliminate duplicate rows, the row
		// count will equal the distinct count of the set of output columns.
		outputCols := opt.ColListToSet(colMap.Out)
		if colStat, ok := s.colStatSetOpImpl(op, leftStats, rightStats, colMap, outputCols); ok {
			s.RowCount = min(s.RowCount, colStat.DistinctCount)
			return
		}

		// If we cannot get the column stats for the set of output columns, estimate
		// the row count as the product of the distinct counts of the individual
		// columns.
		// TODO(rytaft): This is a pessimistic estimate. See if it can be improved.
		distinctCount := uint64(1)
		allCols := true
		outputCols.ForEach(func(i int) {
			colSet := util.MakeFastIntSet(i)
			if colStat, ok := s.colStatSetOpImpl(op, leftStats, rightStats, colMap, colSet); ok {
				distinctCount *= colStat.DistinctCount
			} else {
				allCols = false
			}
		})

		if allCols {
			s.RowCount = min(s.RowCount, distinctCount)
		}
	}
}

func (s *Statistics) colStatSetOp(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	leftStats := s.ev.childGroup(0).logical.Relational.Stats
	rightStats := s.ev.childGroup(1).logical.Relational.Stats
	colMap := s.ev.Private().(*SetOpColMap)
	return s.colStatSetOpImpl(s.ev.Operator(), &leftStats, &rightStats, colMap, colSet)
}

func (s *Statistics) colStatSetOpImpl(
	op opt.Operator, leftStats, rightStats *Statistics, colMap *SetOpColMap, outputCols opt.ColSet,
) (_ *opt.ColumnStatistic, ok bool) {
	leftCols := translateColSet(outputCols, colMap.Out, colMap.Left)
	rightCols := translateColSet(outputCols, colMap.Out, colMap.Right)
	if leftColStat, ok := leftStats.colStat(leftCols); ok {
		if rightColStat, ok := rightStats.colStat(rightCols); ok {
			colStat := s.makeColStat(outputCols)

			// These calculations are an upper bound on the distinct count. It's likely
			// that there is some overlap between the two sets, but not full overlap.
			switch op {
			case opt.UnionOp, opt.UnionAllOp:
				colStat.DistinctCount = leftColStat.DistinctCount + rightColStat.DistinctCount

			case opt.IntersectOp, opt.IntersectAllOp:
				colStat.DistinctCount = min(leftColStat.DistinctCount, rightColStat.DistinctCount)

			case opt.ExceptOp, opt.ExceptAllOp:
				colStat.DistinctCount = leftColStat.DistinctCount
			}

			return colStat, true
		}
	}

	return nil, false
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
func (s *Statistics) initValues() {
	s.RowCount = uint64(s.ev.ChildCount())
}

func (s *Statistics) colStatValues(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	if s.ev.ChildCount() == 0 {
		return nil, false
	}

	colList := s.ev.Private().(opt.ColList)

	// Determine distinct count from the number of distinct memo groups. Use a
	// map to find the exact count of distinct values for the columns in colSet.
	distinct := make(map[string]struct{}, s.ev.Child(0).ChildCount())
	key := keyBuffer{}
	for i := 0; i < s.ev.ChildCount(); i++ {
		groups := make([]GroupID, 0, colSet.Len())
		for j := 0; j < s.ev.Child(i).ChildCount(); j++ {
			if colSet.Contains(int(colList[j])) {
				groups = append(groups, s.ev.Child(i).ChildGroup(j))
			}
		}
		key.Reset()
		key.writeGroupList(groups)
		distinct[key.String()] = struct{}{}
	}

	// Update the column statistics.
	colStat := s.makeColStat(colSet)
	colStat.DistinctCount = uint64(len(distinct))
	return colStat, true
}

func (s *Statistics) initLimit(limit ExprView, inputStats *Statistics) {
	// Copy row count from input.
	s.RowCount = inputStats.RowCount

	// Update row count if limit is a constant.
	if limit.Operator() == opt.ConstOp {
		hardLimit := *limit.Private().(*tree.DInt)
		if hardLimit > 0 {
			s.RowCount = min(uint64(hardLimit), inputStats.RowCount)
		}
	}
}

func (s *Statistics) colStatLimit(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	inputStats := &s.ev.childGroup(0).logical.Relational.Stats
	limit := s.ev.Child(1)

	colStat, ok := s.copyColStat(inputStats, colSet)
	if !ok {
		return nil, false
	}

	// Update distinct count if limit is a constant.
	if limit.Operator() == opt.ConstOp {
		hardLimit := *limit.Private().(*tree.DInt)
		if hardLimit > 0 {
			colStat.DistinctCount = min(uint64(hardLimit), colStat.DistinctCount)
		}
	}

	return colStat, true
}

func (s *Statistics) initOffset(offset ExprView, inputStats *Statistics) {
	// Copy row count from input.
	s.RowCount = inputStats.RowCount

	// Update row count if offset is a constant.
	if offset.Operator() == opt.ConstOp {
		hardOffset := *offset.Private().(*tree.DInt)
		if hardOffset > 0 {
			s.RowCount = max(0, inputStats.RowCount-uint64(hardOffset))
		}
	}
}

func (s *Statistics) colStatOffset(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	inputStats := &s.ev.childGroup(0).logical.Relational.Stats

	colStat, ok := s.copyColStat(inputStats, colSet)
	if !ok {
		return nil, false
	}

	colStat.DistinctCount = min(s.RowCount, colStat.DistinctCount)
	return colStat, true
}

func (s *Statistics) initMax1Row(inputStats *Statistics) {
	// Update row count.
	s.RowCount = 1
}

func (s *Statistics) colStatMax1Row(colSet opt.ColSet) (_ *opt.ColumnStatistic, ok bool) {
	colStat := s.makeColStat(colSet)
	colStat.DistinctCount = 1
	return colStat, true
}

func (s *Statistics) copyColStat(
	inputStats *Statistics, colSet opt.ColSet,
) (_ *opt.ColumnStatistic, ok bool) {
	if inputColStat, ok := inputStats.colStat(colSet); ok {
		colStat := s.makeColStat(colSet)
		*colStat = *inputColStat
		return colStat, true
	}

	return nil, false
}
