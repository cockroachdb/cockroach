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
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// statisticsBuilder is responsible for building the statistics that are
// used by the coster to estimate the cost of expressions.
//
// It builds the statistics lazily, and only calculates column statistics if
// needed to estimate the row count. For example, consider this simple query,
// which consists of a scan followed by an aggregation:
//
//   SELECT count(*), x, y FROM t GROUP BY x, y
//
// The statistics for the scan of t will be calculated first. The cached
// table statistics from the metadata already contain the estimated row count,
// so no column statistics are needed.
//
// The statistics for the group by operator are calculated second. The row
// count for GROUP BY is determined by the distinct count of its grouping
// columns. Therefore, the statisticsBuilder recursively updates the statistics
// for the scan operator to include column stats for x and y, and then uses
// these column stats to update the statistics for GROUP BY.
//
// See the comments in sql/opt/statistics.go for more details.
type statisticsBuilder struct {
	s     *opt.Statistics
	props *props.Relational

	// ev is the ExprView for which these statistics are valid.
	ev      ExprView
	evalCtx *tree.EvalContext

	// keyBuf is temporary "scratch" storage that's used to build keys.
	keyBuf *keyBuffer
}

func (sb *statisticsBuilder) init(
	evalCtx *tree.EvalContext,
	s *opt.Statistics,
	relational *props.Relational,
	ev ExprView,
	keyBuf *keyBuffer,
) {
	sb.s = s
	sb.props = relational
	sb.ev = ev
	sb.evalCtx = evalCtx
	sb.keyBuf = keyBuf
	sb.s.Selectivity = 1
	sb.s.ColStats = make(map[opt.ColumnID]*opt.ColumnStatistic)
	sb.s.MultiColStats = make(map[string]*opt.ColumnStatistic)
}

// colStat gets a column statistic for the given set of columns if it exists.
// If the column statistic is not available in the current statisticsBuilder object,
// colStat recursively tries to find it in the children of the expression,
// lazily populating either s.ColStats or s.MultiColStats with the statistic
// as it gets passed up the expression tree.
func (sb *statisticsBuilder) colStat(colSet opt.ColSet) *opt.ColumnStatistic {
	if colSet.Len() == 0 {
		return nil
	}

	if colSet.Len() == 1 {
		col, _ := colSet.Next(0)
		return sb.singleColStat(opt.ColumnID(col))
	}
	return sb.multiColStat(colSet)
}

func (sb *statisticsBuilder) singleColStat(col opt.ColumnID) *opt.ColumnStatistic {
	if stat, ok := sb.s.ColStats[col]; ok {
		return stat
	}

	return sb.colStatFromChildren(util.MakeFastIntSet(int(col)))
}

func (sb *statisticsBuilder) multiColStat(colSet opt.ColSet) *opt.ColumnStatistic {
	sb.keyBuf.Reset()
	sb.keyBuf.writeColSet(colSet)
	if stat, ok := sb.s.MultiColStats[sb.keyBuf.String()]; ok {
		return stat
	}

	return sb.colStatFromChildren(colSet)
}

func (sb *statisticsBuilder) colStatFromChildren(colSet opt.ColSet) *opt.ColumnStatistic {
	switch sb.ev.Operator() {
	case opt.UnknownOp:
		// The child of the scan operator is an empty ExprView with unknown
		// operator, since there is technically no input to the scan operator.
		// If colStatFromChildren is called on the inputStatsBuilder created in
		// buildScan or colStatScan, it means the statistics in the metadata
		// must be updated. colStatMetadata performs this update.
		return sb.colStatMetadata(colSet)

	case opt.ScanOp:
		return sb.colStatScan(colSet)

	case opt.SelectOp:
		return sb.colStatSelect(colSet)

	case opt.ProjectOp:
		return sb.colStatProject(colSet)

	case opt.ValuesOp:
		return sb.colStatValues(colSet)

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		return sb.colStatJoin(colSet)

	case opt.LookupJoinOp:
		return sb.colStatLookupJoin(colSet)

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		return sb.colStatSetOp(colSet)

	case opt.GroupByOp:
		return sb.colStatGroupBy(colSet)

	case opt.LimitOp:
		return sb.colStatLimit(colSet)

	case opt.OffsetOp:
		return sb.colStatOffset(colSet)

	case opt.Max1RowOp:
		return sb.colStatMax1Row(colSet)

	case opt.RowNumberOp:
		return sb.colStatRowNumber(colSet)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", sb.ev.op))
}

// ensureColStat creates a column statistic for column "col" if it doesn't
// already exist in s.ColStats, copying the statistic from inputStatsBuilder.
// Then, ensureColStat sets the distinct count to the minimum of the existing
// value and the new value.
func (sb *statisticsBuilder) ensureColStat(
	col opt.ColumnID, distinctCount uint64, inputStatsBuilder *statisticsBuilder,
) *opt.ColumnStatistic {
	colStat, ok := sb.s.ColStats[col]
	if !ok {
		colStat = sb.copyColStat(inputStatsBuilder, util.MakeFastIntSet(int(col)))
	}

	colStat.DistinctCount = min(colStat.DistinctCount, distinctCount)
	return colStat
}

// makeColStat creates a column statistic for the given set of columns, and
// returns a pointer to the newly created statistic.
func (sb *statisticsBuilder) makeColStat(colSet opt.ColSet) *opt.ColumnStatistic {
	colStat := &opt.ColumnStatistic{Cols: colSet}
	if colSet.Len() == 1 {
		col, _ := colSet.Next(0)
		sb.s.ColStats[opt.ColumnID(col)] = colStat
	} else {
		sb.keyBuf.Reset()
		sb.keyBuf.writeColSet(colSet)
		sb.s.MultiColStats[sb.keyBuf.String()] = colStat
	}

	return colStat
}

// colStatMetadata updates the statistics in the metadata to include an
// estimated column statistic for the given column set.
func (sb *statisticsBuilder) colStatMetadata(colSet opt.ColSet) *opt.ColumnStatistic {
	if sb.s.ColStats == nil {
		sb.s.ColStats = make(map[opt.ColumnID]*opt.ColumnStatistic)
	}
	if sb.s.MultiColStats == nil {
		sb.s.MultiColStats = make(map[string]*opt.ColumnStatistic)
	}
	colStat := sb.makeColStat(colSet)

	// If some of the columns are a key, the distinct count equals the row count.
	if sb.props.WeakKeys.ContainsSubsetOf(colSet) {
		colStat.DistinctCount = sb.s.RowCount
		return colStat
	}

	if colSet.Len() == 1 {
		// Cast to int64 and then to uint64 to make the linter happy.
		colStat.DistinctCount = max(1, uint64(int64(unknownDistinctCountRatio*float64(sb.s.RowCount))))
	} else {
		distinctCount := uint64(1)
		colSet.ForEach(func(i int) {
			distinctCount *= sb.singleColStat(opt.ColumnID(i)).DistinctCount
		})
		colStat.DistinctCount = min(distinctCount, sb.s.RowCount)
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

	// This is the ratio of distinct column values to number of rows, which is
	// used in the absence of any real statistics for non-key columns.
	// TODO(rytaft): See if there is an industry standard value for this.
	unknownDistinctCountRatio = 0.7
)

func (sb *statisticsBuilder) applyConstraint(
	c *constraint.Constraint, inputStatsBuilder *statisticsBuilder,
) (selectivity float64) {
	if c.IsUnconstrained() {
		return 1 /* selectivity */
	}

	if c.IsContradiction() {
		// A contradiction results in 0 rows.
		return 0 /* selectivity */
	}

	if applied := sb.updateDistinctCountsFromConstraint(c, inputStatsBuilder); !applied {
		// If a constraint cannot be applied, it probably represents an
		// inequality like x < 1. As a result, distinctCounts does not
		// represent the selectivity of the constraint. Return a
		// rough guess for the selectivity.
		return unknownFilterSelectivity
	}

	return sb.selectivityFromDistinctCounts(inputStatsBuilder)
}

func (sb *statisticsBuilder) applyConstraintSet(
	cs *constraint.Set, inputStatsBuilder *statisticsBuilder,
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
		applied := sb.updateDistinctCountsFromConstraint(cs.Constraint(i), inputStatsBuilder)
		if !applied {
			// If a constraint cannot be applied, it probably represents an
			// inequality like x < 1. As a result, distinctCounts does not fully
			// represent the selectivity of the constraint set. Adjust the
			// selectivity to account for this constraint.
			adjustedSelectivity *= unknownFilterSelectivity
		}
	}

	selectivity = sb.selectivityFromDistinctCounts(inputStatsBuilder)
	return selectivity * adjustedSelectivity
}

// updateStatsFromContradiction sets the row count and distinct count to zero,
// since a contradiction results in 0 rows.
func (sb *statisticsBuilder) updateStatsFromContradiction() {
	sb.s.RowCount = 0
	for i := range sb.s.ColStats {
		sb.s.ColStats[i].DistinctCount = 0
	}
	for i := range sb.s.MultiColStats {
		sb.s.MultiColStats[i].DistinctCount = 0
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
// This selectivity will be used later to update the row count and the
// distinct count for the unconstrained columns in applySelectivityToColStat.
//
// TODO(rytaft): This formula assumes that the columns are completely
// independent. Improve this estimate to take functional dependencies and/or
// column correlations into account.
func (sb *statisticsBuilder) selectivityFromDistinctCounts(
	inputStatsBuilder *statisticsBuilder,
) (selectivity float64) {
	selectivity = 1.0
	for col, colStat := range sb.s.ColStats {
		inputStat := inputStatsBuilder.singleColStat(col)
		if inputStat.DistinctCount != 0 && colStat.DistinctCount < inputStat.DistinctCount {
			selectivity *= float64(colStat.DistinctCount) / float64(inputStat.DistinctCount)
		}
	}

	return selectivity
}

// applySelectivityToColStat updates the given column statistics according to
// the filter selectivity.
func (sb *statisticsBuilder) applySelectivityToColStat(
	colStat *opt.ColumnStatistic, inputRows uint64,
) {
	if sb.s.Selectivity == 0 {
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
	d = d - d*math.Pow(1-sb.s.Selectivity, n/d)

	// Cast to int64 and then to uint64 to make the linter happy.
	colStat.DistinctCount = max(1, uint64(int64(d)))
}

// applySelectivity updates the row count according to the filter selectivity,
// and ensures that no distinct counts are larger than the row count.
func (sb *statisticsBuilder) applySelectivity(inputRows uint64) {
	if sb.s.Selectivity == 0 {
		sb.updateStatsFromContradiction()
		return
	}

	sb.s.RowCount = max(1, uint64(int64(float64(inputRows)*sb.s.Selectivity)))

	// At this point we only have single-column stats on columns that were
	// constrained by the filter. Make sure none of the distinct counts are
	// larger than the row count.
	for _, colStat := range sb.s.ColStats {
		colStat.DistinctCount = min(colStat.DistinctCount, sb.s.RowCount)
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
func (sb *statisticsBuilder) updateDistinctCountsFromConstraint(
	c *constraint.Constraint, inputStatsBuilder *statisticsBuilder,
) (applied bool) {
	// All of the columns that are part of the exact prefix have exactly one
	// distinct value.
	exactPrefix := c.ExactPrefix(sb.evalCtx)
	for i := 0; i < exactPrefix; i++ {
		sb.ensureColStat(c.Columns.Get(i).ID(), 1 /* distinctCount */, inputStatsBuilder)
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
		endVal := sp.EndKey().Value(col)
		if startVal.Compare(sb.evalCtx, endVal) != 0 {
			// TODO(rytaft): are there other types we should handle here
			// besides int?
			if startVal.ResolvedType() == types.Int && endVal.ResolvedType() == types.Int {
				start := int(*startVal.(*tree.DInt))
				end := int(*endVal.(*tree.DInt))
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
		} else if startVal.Compare(sb.evalCtx, val) != 0 {
			// This check is needed to ensure that we calculate the correct distinct
			// value count for constraints such as:
			//   /a/b: [/1/2 - /1/2] [/1/4 - /1/4] [/2 - /2]
			// We should only increment the distinct count for column "a" once we
			// reach the third span.
			distinctCount++
		}
	}

	sb.ensureColStat(c.Columns.Get(col).ID(), distinctCount, inputStatsBuilder)
	return true /* applied */
}

func (sb *statisticsBuilder) buildScan(def *ScanOpDef) {
	inputStatsBuilder := statisticsBuilder{
		s:      sb.ev.Metadata().TableStatistics(def.Table),
		props:  sb.props,
		keyBuf: sb.keyBuf,
	}

	if def.Constraint != nil {
		sb.s.Selectivity = sb.applyConstraint(def.Constraint, &inputStatsBuilder)
	}

	sb.applySelectivity(inputStatsBuilder.s.RowCount)

	// Cap number of rows at limit, if it exists.
	if def.HardLimit > 0 && uint64(def.HardLimit) < sb.s.RowCount {
		sb.s.RowCount = uint64(def.HardLimit)

		// At this point we only have single-column stats on columns that were
		// constrained by the filter.
		for _, colStat := range sb.s.ColStats {
			colStat.DistinctCount = min(colStat.DistinctCount, uint64(def.HardLimit))
		}
	}
}

func (sb *statisticsBuilder) colStatScan(colSet opt.ColSet) *opt.ColumnStatistic {
	def := sb.ev.Private().(*ScanOpDef)

	inputStatsBuilder := statisticsBuilder{
		s:      sb.ev.Metadata().TableStatistics(def.Table),
		props:  sb.props,
		keyBuf: sb.keyBuf,
	}
	colStat := sb.copyColStat(&inputStatsBuilder, colSet)
	sb.applySelectivityToColStat(colStat, inputStatsBuilder.s.RowCount)

	// Cap distinct count at limit, if it exists.
	if def.HardLimit > 0 && uint64(def.HardLimit) < sb.s.RowCount {
		colStat.DistinctCount = min(colStat.DistinctCount, uint64(def.HardLimit))
	}

	return colStat
}

func (sb *statisticsBuilder) buildSelect(filter ExprView, inputStats *opt.Statistics) {
	inputStatsBuilder := sb.makeStatisticsBuilder(inputStats, sb.ev.Child(0))

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
	//     (i.e., it'sb not tight), the selectivity is calculated as:
	//     min(selectivity from applyConstraintSet, 1/3).
	//
	// (3) If we can't convert the predicate to a constraint set, the predicate
	//     is too complex to easily determine the selectivity, so use 1/3.
	//
	//     TODO(rytaft): we may be able to get a more precise estimate than
	//     1/3 for certain types of filters. For example, the selectivity of
	//     x=y can be estimated as 1/(max(distinct(x), distinct(y)).
	sb.s.Selectivity = 1
	sel := func(constraintSet *constraint.Set, tight bool) {
		if constraintSet != nil {
			childSelectivity := sb.applyConstraintSet(constraintSet, &inputStatsBuilder)
			if !tight && childSelectivity > unknownFilterSelectivity {
				childSelectivity = unknownFilterSelectivity
			}
			sb.s.Selectivity *= childSelectivity
		} else {
			sb.s.Selectivity *= unknownFilterSelectivity
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

	sb.applySelectivity(inputStats.RowCount)
}

func (sb *statisticsBuilder) colStatSelect(colSet opt.ColSet) *opt.ColumnStatistic {
	inputStats := &sb.ev.childGroup(0).logical.Relational.Stats
	inputStatsBuilder := sb.makeStatisticsBuilder(inputStats, sb.ev.Child(0))

	colStat := sb.copyColStat(&inputStatsBuilder, colSet)
	sb.applySelectivityToColStat(colStat, inputStats.RowCount)
	return colStat
}

func (sb *statisticsBuilder) buildProject(inputStats *opt.Statistics) {
	sb.s.RowCount = inputStats.RowCount
}

func (sb *statisticsBuilder) colStatProject(colSet opt.ColSet) *opt.ColumnStatistic {
	// Columns may be passed through from the input, or they may reference a
	// higher scope (in the case of a correlated subquery), or they
	// may be synthesized by the projection operation.
	inputCols := sb.ev.Child(0).Logical().Relational.OutputCols
	reqInputCols := colSet.Intersection(inputCols)
	if reqSynthCols := colSet.Difference(inputCols); !reqSynthCols.Empty() {
		// Some of the columns in colSet were synthesized or from a higher scope
		// (in the case of a correlated subquery). We assume that the statistics of
		// the synthesized columns are the same as the statistics of their input
		// columns. For example, the distinct count of (x + 2) is the same as the
		// distinct count of x.
		// TODO(rytaft): This assumption breaks down for certain types of
		// expressions, such as (x < y).
		def := sb.ev.Child(1).Private().(*ProjectionsOpDef)
		for i, col := range def.SynthesizedCols {
			if reqSynthCols.Contains(int(col)) {
				reqInputCols.UnionWith(sb.ev.Child(1).Child(i).Logical().Scalar.OuterCols)
			}
		}

		// Intersect with the input columns one more time to remove any columns
		// from higher scopes. Columns from higher scopes are effectively constant
		// in this scope, and therefore have distinct count = 1.
		reqInputCols.IntersectionWith(inputCols)
	}

	colStat := sb.makeColStat(colSet)

	if reqInputCols.Len() > 0 {
		// Inherit column statistics from input, using the reqInputCols identified
		// above.
		inputStats := &sb.ev.childGroup(0).logical.Relational.Stats
		inputStatsBuilder := sb.makeStatisticsBuilder(inputStats, sb.ev.Child(0))
		inputColStat := inputStatsBuilder.colStat(reqInputCols)
		colStat.DistinctCount = inputColStat.DistinctCount
	} else {
		// There are no columns in this expression, so it must be a constant.
		colStat.DistinctCount = uint64(1)
	}
	return colStat
}

func (sb *statisticsBuilder) buildJoin(
	op opt.Operator, leftStats, rightStats *opt.Statistics, on ExprView,
) {
	// TODO: Need better estimate based on actual on conditions.
	sb.s.RowCount = leftStats.RowCount * rightStats.RowCount
	if on.Operator() != opt.TrueOp {
		sb.s.RowCount /= 10
	}
}

func (sb *statisticsBuilder) colStatJoin(colSet opt.ColSet) *opt.ColumnStatistic {
	leftStats := &sb.ev.childGroup(0).logical.Relational.Stats
	rightStats := &sb.ev.childGroup(1).logical.Relational.Stats
	leftBuilder := sb.makeStatisticsBuilder(leftStats, sb.ev.Child(0))
	rightBuilder := sb.makeStatisticsBuilder(rightStats, sb.ev.Child(1))

	// The number of distinct values for the column subsets doesn't change
	// significantly unless the column subsets are part of the ON conditions.
	// For now, add them all unchanged.
	switch sb.ev.Operator() {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Column stats come from left side of join.
		return sb.copyColStat(&leftBuilder, colSet)

	default:
		// Column stats come from both sides of join.
		leftCols := sb.ev.Child(0).Logical().Relational.OutputCols.Copy()
		leftCols.IntersectionWith(colSet)
		rightCols := sb.ev.Child(1).Logical().Relational.OutputCols.Copy()
		rightCols.IntersectionWith(colSet)

		// TODO(rytaft): Apply selectivity to the distinct counts based on the join
		// condition.

		if rightCols.Len() == 0 {
			return sb.copyColStat(&leftBuilder, leftCols)
		}

		if leftCols.Len() == 0 {
			return sb.copyColStat(&rightBuilder, rightCols)
		}

		leftColStat := leftBuilder.colStat(leftCols)
		rightColStat := rightBuilder.colStat(rightCols)
		colStat := sb.makeColStat(colSet)
		colStat.DistinctCount = leftColStat.DistinctCount * rightColStat.DistinctCount
		return colStat
	}
}

func (sb *statisticsBuilder) buildLookupJoin(inputStats *opt.Statistics) {
	sb.s.RowCount = inputStats.RowCount
}

func (sb *statisticsBuilder) colStatLookupJoin(colSet opt.ColSet) *opt.ColumnStatistic {
	inputCols := sb.ev.Child(0).Logical().Relational.OutputCols
	inputStats := &sb.ev.childGroup(0).logical.Relational.Stats

	colStat := sb.makeColStat(colSet)
	colStat.DistinctCount = uint64(1)

	// Some of the requested columns may be from the input index.
	reqInputCols := colSet.Intersection(inputCols)
	if !reqInputCols.Empty() {
		inputStatsBuilder := sb.makeStatisticsBuilder(inputStats, sb.ev.Child(0))
		inputColStat := inputStatsBuilder.colStat(reqInputCols)
		colStat.DistinctCount = inputColStat.DistinctCount
	}

	// Other requested columns may be from the joined table.
	reqJoinedCols := colSet.Difference(inputCols)
	if !reqJoinedCols.Empty() {
		def := sb.ev.Private().(*LookupJoinDef)
		joinedTableStatsBuilder := statisticsBuilder{
			s:      sb.ev.Metadata().TableStatistics(def.Table),
			props:  sb.props,
			keyBuf: sb.keyBuf,
		}
		joinedTableColStat := joinedTableStatsBuilder.colStat(reqJoinedCols)

		// Apply the selectivity from the input index.
		joinedTableStatsBuilder.s.Selectivity = inputStats.Selectivity
		joinedTableStatsBuilder.applySelectivityToColStat(
			joinedTableColStat,
			joinedTableStatsBuilder.s.RowCount,
		)

		// Multiply the distinct counts in case colStat.DistinctCount is
		// already populated with a statistic from the subset of columns
		// provided by the input index. Multiplying the counts gives a worst-case
		// estimate of the joint distinct count.
		colStat.DistinctCount *= joinedTableColStat.DistinctCount
	}

	// The distinct count should be no larger than the row count.
	if colStat.DistinctCount > sb.s.RowCount {
		colStat.DistinctCount = sb.s.RowCount
	}
	return colStat
}

func (sb *statisticsBuilder) buildGroupBy(inputStats *opt.Statistics, groupingColSet opt.ColSet) {
	if groupingColSet.Empty() {
		// Scalar group by.
		sb.s.RowCount = 1
	} else {
		// Estimate the row count based on the distinct count of the grouping
		// columns.
		inputStatsBuilder := sb.makeStatisticsBuilder(inputStats, sb.ev.Child(0))
		colStat := sb.copyColStat(&inputStatsBuilder, groupingColSet)
		sb.s.RowCount = colStat.DistinctCount
	}
}

func (sb *statisticsBuilder) colStatGroupBy(colSet opt.ColSet) *opt.ColumnStatistic {
	inputStats := &sb.ev.childGroup(0).logical.Relational.Stats
	groupingColSet := sb.ev.Private().(*GroupByDef).GroupingCols

	if groupingColSet.Empty() {
		// Scalar group by.
		colStat := sb.makeColStat(colSet)
		colStat.DistinctCount = 1
		return colStat
	}

	inputStatsBuilder := sb.makeStatisticsBuilder(inputStats, sb.ev.Child(0))
	if !colSet.SubsetOf(groupingColSet) {
		// Some of the requested columns are aggregates. Estimate the distinct
		// count to be the same as the grouping columns.
		colStat := sb.makeColStat(colSet)
		inputColStat := inputStatsBuilder.colStat(groupingColSet)
		colStat.DistinctCount = inputColStat.DistinctCount
		return colStat
	}

	return sb.copyColStat(&inputStatsBuilder, colSet)
}

func (sb *statisticsBuilder) buildSetOp(
	op opt.Operator, leftStats, rightStats *opt.Statistics, colMap *SetOpColMap,
) {
	// These calculations are an upper bound on the row count. It'sb likely that
	// there is some overlap between the two sets, but not full overlap.
	switch op {
	case opt.UnionOp, opt.UnionAllOp:
		sb.s.RowCount = leftStats.RowCount + rightStats.RowCount

	case opt.IntersectOp, opt.IntersectAllOp:
		sb.s.RowCount = min(leftStats.RowCount, rightStats.RowCount)

	case opt.ExceptOp, opt.ExceptAllOp:
		sb.s.RowCount = leftStats.RowCount
	}

	switch op {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		leftBuilder := sb.makeStatisticsBuilder(leftStats, sb.ev.Child(0))
		rightBuilder := sb.makeStatisticsBuilder(rightStats, sb.ev.Child(1))

		// Since UNION, INTERSECT and EXCEPT eliminate duplicate rows, the row
		// count will equal the distinct count of the set of output columns.
		outputCols := opt.ColListToSet(colMap.Out)
		colStat := sb.colStatSetOpImpl(op, &leftBuilder, &rightBuilder, colMap, outputCols)
		sb.s.RowCount = colStat.DistinctCount
	}
}

func (sb *statisticsBuilder) colStatSetOp(colSet opt.ColSet) *opt.ColumnStatistic {
	leftStats := &sb.ev.childGroup(0).logical.Relational.Stats
	rightStats := &sb.ev.childGroup(1).logical.Relational.Stats
	leftBuilder := sb.makeStatisticsBuilder(leftStats, sb.ev.Child(0))
	rightBuilder := sb.makeStatisticsBuilder(rightStats, sb.ev.Child(1))
	colMap := sb.ev.Private().(*SetOpColMap)
	return sb.colStatSetOpImpl(sb.ev.Operator(), &leftBuilder, &rightBuilder, colMap, colSet)
}

func (sb *statisticsBuilder) colStatSetOpImpl(
	op opt.Operator,
	leftBuilder, rightBuilder *statisticsBuilder,
	colMap *SetOpColMap,
	outputCols opt.ColSet,
) *opt.ColumnStatistic {
	leftCols := translateColSet(outputCols, colMap.Out, colMap.Left)
	rightCols := translateColSet(outputCols, colMap.Out, colMap.Right)
	leftColStat := leftBuilder.colStat(leftCols)
	rightColStat := rightBuilder.colStat(rightCols)
	colStat := sb.makeColStat(outputCols)

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

	return colStat
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

// buildValues builds the statistics for a VALUES expression.
func (sb *statisticsBuilder) buildValues() {
	sb.s.RowCount = uint64(sb.ev.ChildCount())
}

func (sb *statisticsBuilder) colStatValues(colSet opt.ColSet) *opt.ColumnStatistic {
	if sb.ev.ChildCount() == 0 {
		return nil
	}

	colList := sb.ev.Private().(opt.ColList)

	// Determine distinct count from the number of distinct memo groups. Use a
	// map to find the exact count of distinct values for the columns in colSet.
	distinct := make(map[string]struct{}, sb.ev.Child(0).ChildCount())
	groups := make([]GroupID, 0, colSet.Len())
	for i := 0; i < sb.ev.ChildCount(); i++ {
		groups = groups[:0]
		for j := 0; j < sb.ev.Child(i).ChildCount(); j++ {
			if colSet.Contains(int(colList[j])) {
				groups = append(groups, sb.ev.Child(i).ChildGroup(j))
			}
		}
		sb.keyBuf.Reset()
		sb.keyBuf.writeGroupList(groups)
		distinct[sb.keyBuf.String()] = struct{}{}
	}

	// Update the column statistics.
	colStat := sb.makeColStat(colSet)
	colStat.DistinctCount = uint64(len(distinct))
	return colStat
}

func (sb *statisticsBuilder) buildLimit(limit ExprView, inputStats *opt.Statistics) {
	// Copy row count from input.
	sb.s.RowCount = inputStats.RowCount

	// Update row count if limit is a constant.
	if limit.Operator() == opt.ConstOp {
		hardLimit := *limit.Private().(*tree.DInt)
		if hardLimit > 0 {
			sb.s.RowCount = min(uint64(hardLimit), inputStats.RowCount)
			sb.s.Selectivity = float64(sb.s.RowCount) / float64(inputStats.RowCount)
		}
	}
}

func (sb *statisticsBuilder) colStatLimit(colSet opt.ColSet) *opt.ColumnStatistic {
	inputStats := &sb.ev.childGroup(0).logical.Relational.Stats
	inputStatsBuilder := sb.makeStatisticsBuilder(inputStats, sb.ev.Child(0))

	colStat := sb.copyColStat(&inputStatsBuilder, colSet)

	// Scale distinct count based on the selectivity of the limit operation.
	sb.applySelectivityToColStat(colStat, inputStats.RowCount)
	return colStat
}

func (sb *statisticsBuilder) buildOffset(offset ExprView, inputStats *opt.Statistics) {
	// Copy row count from input.
	sb.s.RowCount = inputStats.RowCount

	// Update row count if offset is a constant.
	if offset.Operator() == opt.ConstOp {
		hardOffset := *offset.Private().(*tree.DInt)
		if uint64(hardOffset) >= inputStats.RowCount {
			sb.s.RowCount = 0
		} else if hardOffset > 0 {
			sb.s.RowCount = inputStats.RowCount - uint64(hardOffset)
		}
		sb.s.Selectivity = float64(sb.s.RowCount) / float64(inputStats.RowCount)
	}
}

func (sb *statisticsBuilder) colStatOffset(colSet opt.ColSet) *opt.ColumnStatistic {
	inputStats := &sb.ev.childGroup(0).logical.Relational.Stats
	inputStatsBuilder := sb.makeStatisticsBuilder(inputStats, sb.ev.Child(0))

	colStat := sb.copyColStat(&inputStatsBuilder, colSet)

	// Scale distinct count based on the selectivity of the offset operation.
	sb.applySelectivityToColStat(colStat, inputStats.RowCount)
	return colStat
}

func (sb *statisticsBuilder) buildMax1Row(inputStats *opt.Statistics) {
	// Update row count.
	sb.s.RowCount = 1
}

func (sb *statisticsBuilder) colStatMax1Row(colSet opt.ColSet) *opt.ColumnStatistic {
	colStat := sb.makeColStat(colSet)
	colStat.DistinctCount = 1
	return colStat
}

func (sb *statisticsBuilder) buildRowNumber(inputStats *opt.Statistics) {
	sb.s.RowCount = inputStats.RowCount
}

func (sb *statisticsBuilder) colStatRowNumber(colSet opt.ColSet) *opt.ColumnStatistic {
	def := sb.ev.Private().(*RowNumberDef)

	colStat := sb.makeColStat(colSet)

	if colSet.Contains(int(def.ColID)) {
		// The ordinality column is a key, so every row is distinct.
		colStat.DistinctCount = sb.ev.Logical().Relational.Stats.RowCount
	} else {
		inputStats := &sb.ev.childGroup(0).logical.Relational.Stats
		inputStatsBuilder := sb.makeStatisticsBuilder(inputStats, sb.ev.Child(0))
		inputColStat := inputStatsBuilder.colStat(colSet)
		colStat.DistinctCount = inputColStat.DistinctCount
	}

	return colStat
}

func (sb *statisticsBuilder) copyColStat(
	inputStatsBuilder *statisticsBuilder, colSet opt.ColSet,
) *opt.ColumnStatistic {
	inputColStat := inputStatsBuilder.colStat(colSet)
	colStat := sb.makeColStat(colSet)
	*colStat = *inputColStat
	return colStat
}

func (sb *statisticsBuilder) makeStatisticsBuilder(
	inputStats *opt.Statistics, inputEv ExprView,
) statisticsBuilder {
	return statisticsBuilder{
		s:      inputStats,
		props:  inputEv.Logical().Relational,
		ev:     inputEv,
		keyBuf: sb.keyBuf,
	}
}
