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

var statsAnnID = opt.NewTableAnnID()

// statisticsBuilder is responsible for building the statistics that are
// used by the coster to estimate the cost of expressions.
//
// Background
// ----------
//
// Conceptually, there are two kinds of statistics: table statistics and
// relational expression statistics.
//
// 1. Table statistics
//
// Table statistics are stats derived from the underlying data in the
// database. These stats are calculated either automatically or on-demand for
// each table, and include the number of rows in the table as well as
// statistics about selected individual columns or sets of columns. The column
// statistics include the number of null values, the number of distinct values,
// and optionally, a histogram of the data distribution (only applicable for
// single columns, not sets of columns). These stats are only collected
// periodically to avoid overloading the database, so they may be stale. They
// are currently persisted in the system.table_statistics table (see sql/stats
// for details). Inside the optimizer, they are cached in a props.Statistics
// object as a table annotation in opt.Metadata.
//
// 2. Relational expression statistics
//
// Relational expression statistics are derived from table statistics, and are
// only valid for a particular memo group. They are used to estimate how the
// underlying table statistics change as different relational operators are
// applied. The same types of statistics are stored for relational expressions
// as for tables (row count, null count, distinct count, etc.). Inside the
// optimizer, they are stored in a props.Statistics object in the logical
// properties of the relational expression's memo group.
//
// For example, here is a query plan with corresponding estimated statistics at
// each level:
//
//        Query:    SELECT y FROM a WHERE x=1
//
//        Plan:            Project y        Row Count: 10, Distinct(x): 1
//                             |
//                         Select x=1       Row Count: 10, Distinct(x): 1
//                             |
//                          Scan a          Row Count: 100, Distinct(x): 10
//
// The statistics for the Scan operator were presumably retrieved from the
// underlying table statistics cached in the metadata. The statistics for
// the Select operator are determined as follows: Since the predicate x=1
// reduces the number of distinct values of x down to 1, and the previous
// distinct count of x was 10, the selectivity of the predicate is 1/10.
// Thus, the estimated number of output rows is 1/10 * 100 = 10. Finally, the
// Project operator passes through the statistics from its child expression.
//
// Statistics for expressions high up in the query tree tend to be quite
// inaccurate since the estimation errors from lower expressions are
// compounded. Still, statistics are useful throughout the query tree to help
// the optimizer choose between multiple alternative, logically equivalent
// plans.
//
// How statisticsBuilder works
// ---------------------------
//
// statisticsBuilder is responsible for building the second type of statistics,
// relational expression statistics. It builds the statistics lazily, and only
// calculates column statistics if needed to estimate the row count of an
// expression (currently, the row count is the only statistic used by the
// coster).
//
// Every relational operator has a buildXXX and a colStatXXX function. For
// example, Scan has buildScan and colStatScan. buildScan is called when the
// logical properties of a Scan expression are built. The goal of each buildXXX
// function is to calculate the number of rows output by the expression so that
// its cost can be estimated by the coster.
//
// In order to determine the row count, column statistics may be required for a
// subset of the columns of the expression. Column statistics are calculated
// recursively from the child expression(s) via calls to the colStatFromInput
// function. colStatFromInput finds the child expression that might contain the
// requested stats, and calls colStat on the child. colStat checks if the
// requested stats are already cached for the child expression, and if not,
// calls colStatXXX (where the XXX corresponds to the operator of the child
// expression). The child expression may need to calculate column statistics
// from its children, and if so, it makes another recursive call to
// colStatFromInput.
//
// The "base case" for colStatFromInput is a Scan, where the "input" is the raw
// table itself; the table statistics are retrieved from the metadata (the
// metadata may in turn need to fetch the stats from the database if they are
// not already cached). If a particular table statistic is not available, a
// best-effort guess is made (see colStatLeaf for details).
//
// To better understand how the statisticsBuilder works, let us consider this
// simple query, which consists of a scan followed by an aggregation:
//
//   SELECT count(*), x, y FROM t GROUP BY x, y
//
// The statistics for the scan of t will be calculated first, since logical
// properties are built bottom-up. The estimated row count is retrieved from
// the table statistics in the metadata, so no column statistics are needed.
//
// The statistics for the group by operator are calculated second. The row
// count for GROUP BY can be determined by the distinct count of its grouping
// columns. Therefore, the statisticsBuilder recursively updates the statistics
// for the scan operator to include column stats for x and y, and then uses
// these column stats to update the statistics for GROUP BY.
//
// At each stage where column statistics are requested, the statisticsBuilder
// makes a call to colStatFromChild, which in turn calls colStat on the child
// to retrieve the cached statistics or calculate them recursively. Assuming
// that no statistics are cached, this is the order of function calls for the
// above example (somewhat simplified):
//
//        +-------------+               +--------------+
//  1.    | buildScan t |           2.  | buildGroupBy |
//        +-------------+               +--------------+
//               |                             |
//     +-----------------------+   +-------------------------+
//     | makeTableStatistics t |   | colStatFromChild (x, y) |
//     +-----------------------+   +-------------------------+
//                                             |
//                                   +--------------------+
//                                   | colStatScan (x, y) |
//                                   +--------------------+
//                                             |
//                                   +---------------------+
//                                   | colStatTable (x, y) |
//                                   +---------------------+
//                                             |
//                                   +--------------------+
//                                   | colStatLeaf (x, y) |
//                                   +--------------------+
//
// See props/statistics.go for more details.
type statisticsBuilder struct {
	evalCtx *tree.EvalContext
	md      *opt.Metadata

	// keyBuf is temporary "scratch" storage that's used to build keys.
	keyBuf *keyBuffer
}

func (sb *statisticsBuilder) init(evalCtx *tree.EvalContext, md *opt.Metadata, keyBuf *keyBuffer) {
	sb.evalCtx = evalCtx
	sb.md = md
	sb.keyBuf = keyBuf
}

// colStatFromChild retrieves a column statistic from a specific child of the
// given expression.
func (sb *statisticsBuilder) colStatFromChild(
	colSet opt.ColSet, ev ExprView, childIdx int,
) *props.ColumnStatistic {
	// Helper function to return the column statistic if the output columns of
	// the child with the given index intersect colSet.
	child := ev.Child(childIdx)
	childProps := child.Logical().Relational
	if !colSet.SubsetOf(childProps.OutputCols) {
		colSet = colSet.Intersection(childProps.OutputCols)
		if colSet.Empty() {
			// All the columns in colSet are outer columns; therefore, we can treat
			// them as a constant.
			return &props.ColumnStatistic{Cols: colSet, DistinctCount: 1}
		}
	}
	return sb.colStat(colSet, child)
}

// colStatFromInput retrieves a column statistic from the input(s) of a Scan,
// Select, or Join. The input to the Scan is the "raw" table.
func (sb *statisticsBuilder) colStatFromInput(
	colSet opt.ColSet, ev ExprView,
) *props.ColumnStatistic {

	switch ev.Operator() {
	case opt.ScanOp:
		return sb.colStatTable(ev.Private().(*ScanOpDef).Table, colSet)

	case opt.SelectOp:
		return sb.colStatFromChild(colSet, ev, 0)

	default:
		if ev.IsJoin() {
			// TODO(radu): what if both sides have columns in colSet?
			intersectsLeft := ev.Child(0).Logical().Relational.OutputCols.Intersects(colSet)
			intersectsRight := ev.Child(1).Logical().Relational.OutputCols.Intersects(colSet)
			if intersectsLeft {
				if intersectsRight {
					panic(fmt.Sprintf("colSet %v contains both left and right columns", colSet))
				}
				return sb.colStatFromChild(colSet, ev, 0)
			}
			if intersectsRight {
				return sb.colStatFromChild(colSet, ev, 1)
			}
			// All columns in colSet are outer columns; therefore, we can treat them
			// as a constant.
			return &props.ColumnStatistic{Cols: colSet, DistinctCount: 1}
		}

		panic(fmt.Sprintf("unsupported operator %s", ev.Operator()))
	}
}

// colStat gets a column statistic for the given set of columns if it exists.
// If the column statistic is not available in the current expression, colStat
// recursively tries to find it in the children of the expression, lazily
// populating either s.ColStats or s.MultiColStats with the statistic as it
// gets passed up the expression tree.
func (sb *statisticsBuilder) colStat(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	if colSet.Empty() {
		panic("column statistics cannot be determined for empty column set")
	}

	// Check if the requested column statistic is already cached.
	if stat, ok := sb.colStatFromCache(colSet, &ev.Logical().Relational.Stats); ok {
		return stat
	}

	// The statistic was not found in the cache, so calculate it based on the
	// type of expression.
	switch ev.Operator() {
	case opt.ScanOp:
		return sb.colStatScan(colSet, ev)

	case opt.VirtualScanOp:
		return sb.colStatVirtualScan(colSet, ev)

	case opt.SelectOp:
		return sb.colStatSelect(colSet, ev)

	case opt.ProjectOp:
		return sb.colStatProject(colSet, ev)

	case opt.ValuesOp:
		return sb.colStatValues(colSet, ev)

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
		return sb.colStatJoin(colSet, ev)

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		return sb.colStatSetOp(colSet, ev)

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		return sb.colStatGroupBy(colSet, ev)

	case opt.LimitOp:
		return sb.colStatLimit(colSet, ev)

	case opt.OffsetOp:
		return sb.colStatOffset(colSet, ev)

	case opt.Max1RowOp:
		return sb.colStatMax1Row(colSet, ev)

	case opt.RowNumberOp:
		return sb.colStatRowNumber(colSet, ev)

	case opt.ZipOp:
		return sb.colStatZip(colSet, ev)

	case opt.ExplainOp, opt.ShowTraceForSessionOp:
		relProps := ev.Logical().Relational
		return sb.colStatLeaf(colSet, &relProps.Stats, &relProps.FuncDeps)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", ev.op))
}

func (sb *statisticsBuilder) colStatFromCache(
	colSet opt.ColSet, s *props.Statistics,
) (colStat *props.ColumnStatistic, ok bool) {
	// Check if the requested column statistic is already cached.
	if colSet.Len() == 1 {
		col, _ := colSet.Next(0)
		colStat, ok = s.ColStats[opt.ColumnID(col)]
	} else {
		sb.keyBuf.Reset()
		sb.keyBuf.writeColSet(colSet)
		colStat, ok = s.MultiColStats[sb.keyBuf.String()]
	}
	return colStat, ok
}

// colStatLeaf creates a column statistic for a given column set (if it doesn't
// already exist in s), by deriving the statistic from the general statistics.
// Used when there is no child expression to retrieve statistics from, typically
// with the Statistics derived for a table.
func (sb *statisticsBuilder) colStatLeaf(
	colSet opt.ColSet, s *props.Statistics, fd *props.FuncDepSet,
) *props.ColumnStatistic {
	// Check if the requested column statistic is already cached.
	if stat, ok := sb.colStatFromCache(colSet, s); ok {
		return stat
	}

	if s.ColStats == nil {
		s.ColStats = make(map[opt.ColumnID]*props.ColumnStatistic)
	}
	if s.MultiColStats == nil {
		s.MultiColStats = make(map[string]*props.ColumnStatistic)
	}
	colStat := sb.makeColStat(colSet, s)

	// If some of the columns are a lax key, the distinct count equals the row
	// count. Note that this doesn't take into account the possibility of
	// duplicates where all columns are NULL.
	if fd.ColsAreLaxKey(colSet) {
		colStat.DistinctCount = s.RowCount
		return colStat
	}

	if colSet.Len() == 1 {
		col, _ := colSet.Next(0)
		colStat.DistinctCount = unknownDistinctCountRatio * s.RowCount
		if sb.md.ColumnType(opt.ColumnID(col)) == types.Bool {
			colStat.DistinctCount = min(colStat.DistinctCount, 2)
		}
	} else {
		distinctCount := 1.0
		colSet.ForEach(func(i int) {
			distinctCount *= sb.colStatLeaf(util.MakeFastIntSet(i), s, fd).DistinctCount
		})
		colStat.DistinctCount = min(distinctCount, s.RowCount)
	}

	return colStat
}

// +-------+
// | Table |
// +-------+

// makeTableStatistics returns the available statistics for the given table.
// Statistics are derived lazily and are cached in the metadata, since they may
// be accessed multiple times during query optimization. For more details, see
// props.Statistics.
func (sb *statisticsBuilder) makeTableStatistics(tabID opt.TableID) *props.Statistics {
	stats, ok := sb.md.TableAnnotation(tabID, statsAnnID).(*props.Statistics)
	if ok {
		// Already made.
		return stats
	}

	// Make now and annotate the metadata table with it for next time.
	tab := sb.md.Table(tabID)
	stats = &props.Statistics{}
	if tab.StatisticCount() == 0 {
		// No statistics.
		stats.RowCount = unknownRowCount
	} else {
		// Get the RowCount from the most recent statistic. Stats are ordered
		// with most recent first.
		stats.RowCount = float64(tab.Statistic(0).RowCount())

		// Add all the column statistics, using the most recent statistic for each
		// column set. Stats are ordered with most recent first.
		stats.ColStats = make(map[opt.ColumnID]*props.ColumnStatistic)
		stats.MultiColStats = make(map[string]*props.ColumnStatistic)
		for i := 0; i < tab.StatisticCount(); i++ {
			stat := tab.Statistic(i)
			var cols opt.ColSet
			for i := 0; i < stat.ColumnCount(); i++ {
				cols.Add(int(tabID.ColumnID(stat.ColumnOrdinal(i))))
			}

			if cols.Len() == 1 {
				col, _ := cols.Next(0)
				key := opt.ColumnID(col)

				if _, ok := stats.ColStats[key]; !ok {
					stats.ColStats[key] = &props.ColumnStatistic{
						Cols:          cols,
						DistinctCount: float64(stat.DistinctCount()),
					}
				}
			} else {
				// Get a unique key for this column set.
				sb.keyBuf.Reset()
				sb.keyBuf.writeColSet(cols)
				key := sb.keyBuf.String()

				if _, ok := stats.MultiColStats[key]; !ok {
					stats.MultiColStats[key] = &props.ColumnStatistic{
						Cols:          cols,
						DistinctCount: float64(stat.DistinctCount()),
					}
				}
			}
		}
	}
	sb.md.SetTableAnnotation(tabID, statsAnnID, stats)
	return stats
}

func (sb *statisticsBuilder) colStatTable(
	tabID opt.TableID, colSet opt.ColSet,
) *props.ColumnStatistic {
	tableStats := sb.makeTableStatistics(tabID)
	tableFD := makeTableFuncDep(sb.md, tabID)
	return sb.colStatLeaf(colSet, tableStats, tableFD)
}

// +------+
// | Scan |
// +------+

func (sb *statisticsBuilder) buildScan(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	def := ev.Private().(*ScanOpDef)
	inputStats := sb.makeTableStatistics(def.Table)
	s.RowCount = inputStats.RowCount

	if def.Constraint != nil {
		// Calculate distinct counts for constrained columns
		// -------------------------------------------------
		applied := sb.applyConstraint(def.Constraint, ev, relProps)

		// Calculate row count and selectivity
		// -----------------------------------
		if applied {
			var cols opt.ColSet
			for i := 0; i < def.Constraint.Columns.Count(); i++ {
				cols.Add(int(def.Constraint.Columns.Get(i).ID()))
			}
			s.ApplySelectivity(sb.selectivityFromDistinctCounts(cols, ev, s))
		} else {
			s.ApplySelectivity(sb.selectivityFromUnappliedConstraints(1 /* numUnappliedConstraints */))
		}
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatScan(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	relProps := ev.Logical().Relational
	s := &relProps.Stats
	def := ev.Private().(*ScanOpDef)

	colStat := sb.copyColStat(colSet, s, sb.colStatTable(def.Table, colSet))
	if s.Selectivity != 1 {
		tableStats := sb.makeTableStatistics(def.Table)
		colStat.ApplySelectivity(s.Selectivity, tableStats.RowCount)
	}

	// Cap distinct count at limit, if it exists.
	if def.HardLimit.IsSet() {
		if limit := float64(def.HardLimit.RowCount()); limit < s.RowCount {
			colStat.DistinctCount = min(colStat.DistinctCount, limit)
		}
	}

	return colStat
}

// +-------------+
// | VirtualScan |
// +-------------+

func (sb *statisticsBuilder) buildVirtualScan(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	def := ev.Private().(*VirtualScanOpDef)
	inputStats := sb.makeTableStatistics(def.Table)

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatVirtualScan(
	colSet opt.ColSet, ev ExprView,
) *props.ColumnStatistic {
	def := ev.Private().(*VirtualScanOpDef)
	s := &ev.Logical().Relational.Stats
	return sb.copyColStat(colSet, s, sb.colStatTable(def.Table, colSet))
}

// +--------+
// | Select |
// +--------+

func (sb *statisticsBuilder) buildSelect(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	// Update stats based on filter conditions.

	filter := ev.Child(1)
	filterFD := &filter.Logical().Scalar.FuncDeps
	equivReps := filterFD.EquivReps()

	// Calculate distinct counts for constrained columns
	// -------------------------------------------------
	numUnappliedConstraints, constrainedCols := sb.applyFilter(filter, ev, relProps)

	// Try to reduce the number of columns used for selectivity
	// calculation based on functional dependencies.
	inputFD := &ev.Child(0).Logical().Relational.FuncDeps
	constrainedCols = sb.tryReduceCols(constrainedCols, s, inputFD)

	// Calculate selectivity and row count
	// -----------------------------------
	inputStats := &ev.childGroup(0).logical.Relational.Stats
	s.RowCount = inputStats.RowCount
	s.ApplySelectivity(sb.selectivityFromDistinctCounts(constrainedCols, ev, s))
	s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, filterFD, ev, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConstraints(numUnappliedConstraints))

	// Update distinct counts based on equivalencies; this should happen after
	// selectivityFromDistinctCounts and selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, filterFD, ev, relProps)

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatSelect(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	relProps := ev.Logical().Relational
	s := &relProps.Stats
	inputStats := &ev.childGroup(0).logical.Relational.Stats
	colStat := sb.copyColStatFromChild(colSet, ev, 0 /* childIdx */, s)
	colStat.ApplySelectivity(s.Selectivity, inputStats.RowCount)
	return colStat
}

// +---------+
// | Project |
// +---------+

func (sb *statisticsBuilder) buildProject(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &ev.childGroup(0).logical.Relational.Stats

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatProject(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	relProps := ev.Logical().Relational
	s := &relProps.Stats

	// Columns may be passed through from the input, or they may reference a
	// higher scope (in the case of a correlated subquery), or they
	// may be synthesized by the projection operation.
	inputCols := ev.Child(0).Logical().Relational.OutputCols
	reqInputCols := colSet.Intersection(inputCols)
	if reqSynthCols := colSet.Difference(inputCols); !reqSynthCols.Empty() {
		// Some of the columns in colSet were synthesized or from a higher scope
		// (in the case of a correlated subquery). We assume that the statistics of
		// the synthesized columns are the same as the statistics of their input
		// columns. For example, the distinct count of (x + 2) is the same as the
		// distinct count of x.
		// TODO(rytaft): This assumption breaks down for certain types of
		// expressions, such as (x < y).
		def := ev.Child(1).Private().(*ProjectionsOpDef)
		for i, col := range def.SynthesizedCols {
			if reqSynthCols.Contains(int(col)) {
				reqInputCols.UnionWith(ev.Child(1).Child(i).Logical().Scalar.OuterCols)
			}
		}

		// Intersect with the input columns one more time to remove any columns
		// from higher scopes. Columns from higher scopes are effectively constant
		// in this scope, and therefore have distinct count = 1.
		reqInputCols.IntersectionWith(inputCols)
	}

	colStat := sb.makeColStat(colSet, s)

	if !reqInputCols.Empty() {
		// Inherit column statistics from input, using the reqInputCols identified
		// above.
		inputColStat := sb.colStatFromChild(reqInputCols, ev, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
	} else {
		// There are no columns in this expression, so it must be a constant.
		colStat.DistinctCount = 1
	}
	return colStat
}

// +------+
// | Join |
// +------+

func (sb *statisticsBuilder) buildJoin(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	leftStats := &ev.childGroup(0).logical.Relational.Stats
	rightStats := &ev.childGroup(1).logical.Relational.Stats
	on := ev.Child(2)

	// Estimating selectivity for semi-join and anti-join is error-prone.
	// For now, just propagate stats from the left side.
	switch ev.Operator() {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		s.RowCount = leftStats.RowCount
		s.Selectivity = 1
		return
	}

	// Shortcut if there are no ON conditions.
	if on.Operator() == opt.TrueOp {
		s.RowCount = leftStats.RowCount * rightStats.RowCount
		s.Selectivity = 1
		return
	}

	// Shortcut if the ON condition is false or there is a contradiction.
	constraintSet := on.Logical().Scalar.Constraints
	if on.Operator() == opt.FalseOp || constraintSet == constraint.Contradiction {
		switch ev.Operator() {
		case opt.InnerJoinOp, opt.InnerJoinApplyOp:
			s.RowCount = 0

		case opt.LeftJoinOp, opt.LeftJoinApplyOp:
			// All rows from left side should be in the result.
			s.RowCount = leftStats.RowCount

		case opt.RightJoinOp, opt.RightJoinApplyOp:
			// All rows from right side should be in the result.
			s.RowCount = rightStats.RowCount

		case opt.FullJoinOp, opt.FullJoinApplyOp:
			// All rows from both sides should be in the result.
			s.RowCount = leftStats.RowCount + rightStats.RowCount
		}
		s.Selectivity = 0
		return
	}

	// Update stats based on ON conditions.

	leftProps := ev.Child(0).Logical().Relational
	rightProps := ev.Child(1).Logical().Relational
	filterFD := &on.Logical().Scalar.FuncDeps
	equivReps := filterFD.EquivReps()

	// Calculate distinct counts for constrained columns in the ON conditions
	// ----------------------------------------------------------------------
	numUnappliedConstraints, constrainedCols := sb.applyFilter(on, ev, relProps)

	// Try to reduce the number of columns used for selectivity
	// calculation based on functional dependencies.
	leftCols := constrainedCols.Intersection(leftProps.OutputCols)
	rightCols := constrainedCols.Intersection(rightProps.OutputCols)
	leftCols = sb.tryReduceCols(leftCols, s, &leftProps.FuncDeps)
	rightCols = sb.tryReduceCols(rightCols, s, &rightProps.FuncDeps)
	constrainedCols = leftCols.Union(rightCols)

	// Calculate selectivity and row count
	// -----------------------------------
	s.RowCount = leftStats.RowCount * rightStats.RowCount
	s.ApplySelectivity(sb.selectivityFromDistinctCounts(constrainedCols, ev, s))
	s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, filterFD, ev, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConstraints(numUnappliedConstraints))

	// Update distinct counts based on equivalencies; this should happen after
	// selectivityFromDistinctCounts and selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, filterFD, ev, relProps)

	// The above calculation is for inner joins. Tweak the row count and
	// distinct counts for other types of joins.
	var colsToRemove opt.ColSet
	for col := range s.ColStats {
		colsToRemove.Add(int(col))
	}

	// Remove invalid distinct counts.
	switch ev.Operator() {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// Keep only column stats from the right side. The stats from the left side
		// are not valid.
		colsToRemove.DifferenceWith(rightProps.OutputCols)

	case opt.RightJoinOp, opt.RightJoinApplyOp:
		// Keep only column stats from the left side. The stats from the right side
		// are not valid.
		colsToRemove.DifferenceWith(leftProps.OutputCols)

	case opt.FullJoinOp, opt.FullJoinApplyOp:
		// Do not keep any column stats.

	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		// Keep all column stats.
		colsToRemove = opt.ColSet{}
	}

	colsToRemove.ForEach(func(i int) {
		delete(s.ColStats, opt.ColumnID(i))
	})

	// Tweak the row count.
	innerJoinRowCount := s.RowCount
	switch ev.Operator() {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// All rows from left side should be in the result.
		s.RowCount = max(innerJoinRowCount, leftStats.RowCount)

	case opt.RightJoinOp, opt.RightJoinApplyOp:
		// All rows from right side should be in the result.
		s.RowCount = max(innerJoinRowCount, rightStats.RowCount)

	case opt.FullJoinOp, opt.FullJoinApplyOp:
		// All rows from both sides should be in the result.
		// T(A FOJ B) = T(A LOJ B) + T(A ROJ B) - T(A IJ B)
		leftJoinRowCount := max(innerJoinRowCount, leftStats.RowCount)
		rightJoinRowCount := max(innerJoinRowCount, rightStats.RowCount)
		s.RowCount = leftJoinRowCount + rightJoinRowCount - innerJoinRowCount
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatJoin(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	relProps := ev.Logical().Relational
	s := &relProps.Stats
	leftStats := &ev.childGroup(0).logical.Relational.Stats
	rightStats := &ev.childGroup(1).logical.Relational.Stats

	switch ev.Operator() {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Column stats come from left side of join.
		colStat := sb.copyColStatFromChild(colSet, ev, 0 /* childIdx */, s)
		colStat.ApplySelectivity(s.Selectivity, leftStats.RowCount)
		return colStat

	default:
		// Column stats come from both sides of join.
		leftCols := ev.Child(0).Logical().Relational.OutputCols.Copy()
		leftCols.IntersectionWith(colSet)
		rightCols := ev.Child(1).Logical().Relational.OutputCols.Copy()
		rightCols.IntersectionWith(colSet)

		// Join selectivity affects the distinct counts for different columns
		// in different ways depending on the type of join.
		//
		// - For FULL OUTER joins, the selectivity has no impact on distinct count;
		//   all rows from the input are included at least once in the output.
		// - For LEFT OUTER joins, the selectivity only impacts the distinct count
		//   of columns from the right side of the join; all rows from the left
		//   side are included at least once in the output.
		// - For RIGHT OUTER joins, the selectivity only impacts the distinct count
		//   of columns from the left side of the join; all rows from the right
		//   side are included at least once in the output.
		// - For INNER joins, the selectivity impacts the distinct count of all
		//   columns.
		var colStat *props.ColumnStatistic
		inputRowCount := leftStats.RowCount * rightStats.RowCount
		if rightCols.Empty() {
			colStat = sb.copyColStatFromChild(leftCols, ev, 0 /* childIdx */, s)
			switch ev.Operator() {
			case opt.InnerJoinOp, opt.InnerJoinApplyOp, opt.RightJoinOp, opt.RightJoinApplyOp:
				colStat.ApplySelectivity(s.Selectivity, inputRowCount)
			}
		} else if leftCols.Empty() {
			colStat = sb.copyColStatFromChild(rightCols, ev, 1 /* childIdx */, s)
			switch ev.Operator() {
			case opt.InnerJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinOp, opt.LeftJoinApplyOp:
				colStat.ApplySelectivity(s.Selectivity, inputRowCount)
			}
		} else {
			// Make a copy of the input column stats so we don't modify the originals.
			leftColStat := *sb.colStatFromChild(leftCols, ev, 0 /* childIdx */)
			rightColStat := *sb.colStatFromChild(rightCols, ev, 1 /* childIdx */)
			switch ev.Operator() {
			case opt.InnerJoinOp, opt.InnerJoinApplyOp:
				leftColStat.ApplySelectivity(s.Selectivity, inputRowCount)
				rightColStat.ApplySelectivity(s.Selectivity, inputRowCount)

			case opt.LeftJoinOp, opt.LeftJoinApplyOp:
				rightColStat.ApplySelectivity(s.Selectivity, inputRowCount)

			case opt.RightJoinOp, opt.RightJoinApplyOp:
				leftColStat.ApplySelectivity(s.Selectivity, inputRowCount)
			}
			colStat = sb.makeColStat(colSet, s)
			colStat.DistinctCount = leftColStat.DistinctCount * rightColStat.DistinctCount
		}

		// The distinct count should be no larger than the row count.
		if colStat.DistinctCount > s.RowCount {
			colStat.DistinctCount = s.RowCount
		}
		return colStat
	}
}

// +------------+
// | Index Join |
// +------------+

func (sb *statisticsBuilder) buildIndexJoin(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &ev.childGroup(0).logical.Relational.Stats

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

// +----------+
// | Group By |
// +----------+

func (sb *statisticsBuilder) buildGroupBy(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	groupingColSet := ev.Private().(*GroupByDef).GroupingCols

	if groupingColSet.Empty() {
		// ScalarGroupBy or GroupBy with empty grouping columns.
		s.RowCount = 1
	} else {
		// Estimate the row count based on the distinct count of the grouping
		// columns.
		colStat := sb.copyColStatFromChild(groupingColSet, ev, 0 /* childIdx */, s)
		s.RowCount = colStat.DistinctCount
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatGroupBy(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	relProps := ev.Logical().Relational
	s := &relProps.Stats

	groupingColSet := ev.Private().(*GroupByDef).GroupingCols
	if groupingColSet.Empty() {
		// ScalarGroupBy or GroupBy with empty grouping columns.
		colStat := sb.makeColStat(colSet, s)
		colStat.DistinctCount = 1
		return colStat
	}

	if !colSet.SubsetOf(groupingColSet) {
		// Some of the requested columns are aggregates. Estimate the distinct
		// count to be the same as the grouping columns.
		colStat := sb.makeColStat(colSet, s)
		inputColStat := sb.colStatFromChild(groupingColSet, ev, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
		return colStat
	}

	return sb.copyColStatFromChild(colSet, ev, 0 /* childIdx */, s)
}

// +--------+
// | Set Op |
// +--------+

func (sb *statisticsBuilder) buildSetOp(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	leftStats := &ev.childGroup(0).logical.Relational.Stats
	rightStats := &ev.childGroup(1).logical.Relational.Stats

	// These calculations are an upper bound on the row count. It's likely that
	// there is some overlap between the two sets, but not full overlap.
	switch ev.Operator() {
	case opt.UnionOp, opt.UnionAllOp:
		s.RowCount = leftStats.RowCount + rightStats.RowCount

	case opt.IntersectOp, opt.IntersectAllOp:
		s.RowCount = min(leftStats.RowCount, rightStats.RowCount)

	case opt.ExceptOp, opt.ExceptAllOp:
		s.RowCount = leftStats.RowCount
	}

	switch ev.Operator() {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		// Since UNION, INTERSECT and EXCEPT eliminate duplicate rows, the row
		// count will equal the distinct count of the set of output columns.
		colMap := ev.Private().(*SetOpColMap)
		outputCols := opt.ColListToSet(colMap.Out)
		colStat := sb.colStatSetOpImpl(outputCols, ev, relProps)
		s.RowCount = colStat.DistinctCount
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatSetOp(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	return sb.colStatSetOpImpl(colSet, ev, ev.Logical().Relational)
}

func (sb *statisticsBuilder) colStatSetOpImpl(
	outputCols opt.ColSet, ev ExprView, relProps *props.Relational,
) *props.ColumnStatistic {
	s := &relProps.Stats
	colMap := ev.Private().(*SetOpColMap)

	leftCols := translateColSet(outputCols, colMap.Out, colMap.Left)
	rightCols := translateColSet(outputCols, colMap.Out, colMap.Right)
	leftColStat := sb.colStatFromChild(leftCols, ev, 0 /* childIdx */)
	rightColStat := sb.colStatFromChild(rightCols, ev, 1 /* childIdx */)

	colStat := sb.makeColStat(outputCols, s)

	// These calculations are an upper bound on the distinct count. It's likely
	// that there is some overlap between the two sets, but not full overlap.
	switch ev.Operator() {
	case opt.UnionOp, opt.UnionAllOp:
		colStat.DistinctCount = leftColStat.DistinctCount + rightColStat.DistinctCount

	case opt.IntersectOp, opt.IntersectAllOp:
		colStat.DistinctCount = min(leftColStat.DistinctCount, rightColStat.DistinctCount)

	case opt.ExceptOp, opt.ExceptAllOp:
		colStat.DistinctCount = leftColStat.DistinctCount
	}

	return colStat
}

// +--------+
// | Values |
// +--------+

// buildValues builds the statistics for a VALUES expression.
func (sb *statisticsBuilder) buildValues(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	s.RowCount = float64(ev.ChildCount())
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatValues(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	s := &ev.Logical().Relational.Stats
	if ev.ChildCount() == 0 {
		return sb.makeColStat(colSet, s)
	}

	colList := ev.Private().(opt.ColList)

	// Determine distinct count from the number of distinct memo groups. Use a
	// map to find the exact count of distinct values for the columns in colSet.
	distinct := make(map[string]struct{}, ev.Child(0).ChildCount())
	groups := make([]GroupID, 0, colSet.Len())
	for i, in := 0, ev.ChildCount(); i < in; i++ {
		groups = groups[:0]
		for j, jn := 0, ev.Child(i).ChildCount(); j < jn; j++ {
			if colSet.Contains(int(colList[j])) {
				groups = append(groups, ev.Child(i).ChildGroup(j))
			}
		}
		sb.keyBuf.Reset()
		sb.keyBuf.writeGroupList(groups)
		distinct[sb.keyBuf.String()] = struct{}{}
	}

	// Update the column statistics.
	colStat := sb.makeColStat(colSet, s)
	colStat.DistinctCount = float64(len(distinct))
	return colStat
}

// +-------+
// | Limit |
// +-------+

func (sb *statisticsBuilder) buildLimit(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &ev.childGroup(0).logical.Relational.Stats
	limit := ev.Child(1)

	// Copy row count from input.
	s.RowCount = inputStats.RowCount

	// Update row count if limit is a constant and row count is non-zero.
	if limit.Operator() == opt.ConstOp && inputStats.RowCount > 0 {
		hardLimit := *limit.Private().(*tree.DInt)
		if hardLimit > 0 {
			s.RowCount = min(float64(hardLimit), inputStats.RowCount)
			s.Selectivity = s.RowCount / inputStats.RowCount
		}
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatLimit(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	relProps := ev.Logical().Relational
	s := &relProps.Stats
	inputStats := &ev.childGroup(0).logical.Relational.Stats
	colStat := sb.copyColStatFromChild(colSet, ev, 0 /* childIdx */, s)

	// Scale distinct count based on the selectivity of the limit operation.
	colStat.ApplySelectivity(s.Selectivity, inputStats.RowCount)
	return colStat
}

// +--------+
// | Offset |
// +--------+

func (sb *statisticsBuilder) buildOffset(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &ev.childGroup(0).logical.Relational.Stats
	offset := ev.Child(1)

	// Copy row count from input.
	s.RowCount = inputStats.RowCount

	// Update row count if offset is a constant and row count is non-zero.
	if offset.Operator() == opt.ConstOp && inputStats.RowCount > 0 {
		hardOffset := *offset.Private().(*tree.DInt)
		if float64(hardOffset) >= inputStats.RowCount {
			s.RowCount = 0
		} else if hardOffset > 0 {
			s.RowCount = inputStats.RowCount - float64(hardOffset)
		}
		s.Selectivity = s.RowCount / inputStats.RowCount
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatOffset(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	relProps := ev.Logical().Relational
	s := &relProps.Stats
	inputStats := &ev.childGroup(0).logical.Relational.Stats
	colStat := sb.copyColStatFromChild(colSet, ev, 0 /* childIdx */, s)

	// Scale distinct count based on the selectivity of the offset operation.
	colStat.ApplySelectivity(s.Selectivity, inputStats.RowCount)
	return colStat
}

// +---------+
// | Max1Row |
// +---------+

func (sb *statisticsBuilder) buildMax1Row(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	s.RowCount = 1
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatMax1Row(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	colStat := sb.makeColStat(colSet, &ev.Logical().Relational.Stats)
	colStat.DistinctCount = 1
	return colStat
}

// +------------+
// | Row Number |
// +------------+

func (sb *statisticsBuilder) buildRowNumber(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &ev.childGroup(0).logical.Relational.Stats

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatRowNumber(
	colSet opt.ColSet, ev ExprView,
) *props.ColumnStatistic {
	relProps := ev.Logical().Relational
	s := &relProps.Stats
	def := ev.Private().(*RowNumberDef)

	colStat := sb.makeColStat(colSet, s)

	if colSet.Contains(int(def.ColID)) {
		// The ordinality column is a key, so every row is distinct.
		colStat.DistinctCount = s.RowCount
	} else {
		inputColStat := sb.colStatFromChild(colSet, ev, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
	}

	return colStat
}

// +-----+
// | Zip |
// +-----+

func (sb *statisticsBuilder) buildZip(ev ExprView, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	// The row count of a zip operation is equal to the maximum row count of its
	// children.
	for i, n := 0, ev.ChildCount(); i < n; i++ {
		child := ev.Child(i)
		if child.Operator() == opt.FunctionOp {
			def := child.Private().(*FuncOpDef)
			if def.Overload.Generator != nil {
				// TODO(rytaft): We may want to estimate the number of rows based on
				// the type of generator function and its parameters.
				s.RowCount = unknownRowCount
				break
			}
		}

		// A scalar function generates one row.
		s.RowCount = 1
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatZip(colSet opt.ColSet, ev ExprView) *props.ColumnStatistic {
	s := &ev.Logical().Relational.Stats

	colStat := sb.makeColStat(colSet, s)
	// TODO(rytaft): We may want to determine which generator function the
	// columns in colSet correspond to, and estimate the distinct count based on
	// the type of generator function and its parameters.
	if s.RowCount == 1 {
		colStat.DistinctCount = 1
	} else {
		colStat.DistinctCount = s.RowCount * unknownDistinctCountRatio
	}
	return colStat
}

/////////////////////////////////////////////////
// General helper functions for building stats //
/////////////////////////////////////////////////

// copyColStatFromChild copies the column statistic for the given colSet from
// a child of ev into ev. If the child expected to contain the column statistic
// for colSet is already known, childIdx contains the index of that child.
// Otherwise, childIdx is -1. colStatFromChild may trigger recursive calls if
// the requested statistic is not already cached in the child.
func (sb *statisticsBuilder) copyColStatFromChild(
	colSet opt.ColSet, ev ExprView, childIdx int, s *props.Statistics,
) *props.ColumnStatistic {
	childColStat := sb.colStatFromChild(colSet, ev, childIdx)
	return sb.copyColStat(colSet, s, childColStat)
}

// ensureColStat creates a column statistic for column "col" if it doesn't
// already exist in s.ColStats, copying the statistic from a child.
// Then, ensureColStat sets the distinct count to the minimum of the existing
// value and the new value.
func (sb *statisticsBuilder) ensureColStat(
	col opt.ColumnID, maxDistinctCount float64, ev ExprView, relProps *props.Relational,
) *props.ColumnStatistic {
	s := &relProps.Stats

	colStat, ok := s.ColStats[col]
	if !ok {
		colSet := util.MakeFastIntSet(int(col))
		colStat = sb.copyColStat(colSet, s, sb.colStatFromInput(colSet, ev))
	}

	colStat.DistinctCount = min(colStat.DistinctCount, maxDistinctCount)
	return colStat
}

// makeColStat creates a column statistic for the given set of columns, and
// returns a pointer to the newly created statistic.
func (sb *statisticsBuilder) makeColStat(
	colSet opt.ColSet, s *props.Statistics,
) *props.ColumnStatistic {
	colStat := &props.ColumnStatistic{Cols: colSet}
	if colSet.Len() == 1 {
		col, _ := colSet.Next(0)
		s.ColStats[opt.ColumnID(col)] = colStat
	} else {
		sb.keyBuf.Reset()
		sb.keyBuf.writeColSet(colSet)
		s.MultiColStats[sb.keyBuf.String()] = colStat
	}

	return colStat
}

// copyColStat creates a column statistic and copies the data from an existing
// column statistic.
func (sb *statisticsBuilder) copyColStat(
	colSet opt.ColSet, s *props.Statistics, inputColStat *props.ColumnStatistic,
) *props.ColumnStatistic {
	if !inputColStat.Cols.SubsetOf(colSet) {
		panic(fmt.Sprintf("copyColStat colSet: %v inputColSet: %v\n", colSet, inputColStat.Cols))
	}
	colStat := sb.makeColStat(colSet, s)
	colStat.DistinctCount = inputColStat.DistinctCount
	return colStat
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

func (sb *statisticsBuilder) finalizeFromCardinality(relProps *props.Relational) {
	s := &relProps.Stats
	// The row count should be between the min and max cardinality.
	if s.RowCount > float64(relProps.Cardinality.Max) && relProps.Cardinality.Max != math.MaxUint32 {
		s.RowCount = float64(relProps.Cardinality.Max)
	} else if s.RowCount < float64(relProps.Cardinality.Min) {
		s.RowCount = float64(relProps.Cardinality.Min)
	}

	// The distinct counts should be no larger than the row count.
	for _, colStat := range s.ColStats {
		colStat.DistinctCount = min(colStat.DistinctCount, s.RowCount)
	}
	for _, colStat := range s.MultiColStats {
		colStat.DistinctCount = min(colStat.DistinctCount, s.RowCount)
	}
}

func min(a float64, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a float64, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

//////////////////////////////////////////////////
// Helper functions for selectivity calculation //
//////////////////////////////////////////////////

const (
	// This is the value used for inequality filters such as x < 1 in
	// "Access Path Selection in a Relational Database Management System"
	// by Pat Selinger et al.
	unknownFilterSelectivity = 1.0 / 3.0

	// TODO(rytaft): Add other selectivities for other types of predicates.

	// This is an arbitrary row count used in the absence of any real statistics.
	unknownRowCount = 1000

	// This is the ratio of distinct column values to number of rows, which is
	// used in the absence of any real statistics for non-key columns.
	// TODO(rytaft): See if there is an industry standard value for this.
	unknownDistinctCountRatio = 0.7
)

// applyFilter uses constraints to update the distinct counts for the
// constrained columns in the filter. The changes in the distinct counts will be
// used later to determine the selectivity of the filter.
//
// Some filters can be translated directly to distinct counts using the
// constraint set. For example, the tight constraint `/a: [/1 - /1]` indicates
// that column `a` has exactly one distinct value.  Other filters may not have
// a tight constraint, or the constraint may be an open inequality such as
// `/a: [/0 - ]`. In this case, it is not possible to determine the distinct
// count for column `a`, so instead we increment numUnappliedConstraints,
// which will be used later for selectivity calculation. See comments in
// applyConstraintSet and updateDistinctCountsFromConstraint for more details
// about how distinct counts are calculated from constraints.
//
// Equalities between two variables (e.g., var1=var2) are handled separately.
// See applyEquivalencies and selectivityFromEquivalencies for details.
//
func (sb *statisticsBuilder) applyFilter(
	filter ExprView, ev ExprView, relProps *props.Relational,
) (numUnappliedConstraints int, constrainedCols opt.ColSet) {
	constraintSet := filter.Logical().Scalar.Constraints
	tight := filter.Logical().Scalar.TightConstraints

	applyConjunct := func(conjunct ExprView, constraintSet *constraint.Set, tight bool) {
		if sb.isEqualityWithTwoVars(conjunct) {
			// We'll handle equalities later.
			return
		}
		// Update constrainedCols after the above check for isEqualityWithTwoVars.
		// We will use constrainedCols later to determine which columns to use for
		// selectivity calculation in selectivityFromDistinctCounts, and we want to
		// make sure that we don't include columns that were only present in
		// equality conjuncts such as var1=var2. The selectivity of these conjuncts
		// will be accounted for in selectivityFromEquivalencies.
		constrainedCols.UnionWith(conjunct.Logical().Scalar.OuterCols)
		if constraintSet != nil {
			n := sb.applyConstraintSet(constraintSet, ev, relProps)
			numUnappliedConstraints += n
			if !tight && n == 0 {
				numUnappliedConstraints++
			}
		} else {
			numUnappliedConstraints++
		}
	}

	if (constraintSet != nil && tight) || (filter.op != opt.FiltersOp && filter.op != opt.AndOp) {
		// Shortcut if the top level constraint is tight or if we only have one
		// conjunct.
		applyConjunct(filter, constraintSet, tight)
	} else {
		for i, n := 0, filter.ChildCount(); i < n; i++ {
			child := filter.Child(i)
			constraintSet = child.Logical().Scalar.Constraints
			tight = child.Logical().Scalar.TightConstraints
			applyConjunct(child, constraintSet, tight)
		}
	}

	return numUnappliedConstraints, constrainedCols
}

func (sb *statisticsBuilder) applyConstraint(
	c *constraint.Constraint, ev ExprView, relProps *props.Relational,
) (applied bool) {
	if c.IsUnconstrained() {
		return true /* applied */
	}

	if c.IsContradiction() {
		panic("applyConstraint called on constraint with contradiction")
	}

	return sb.updateDistinctCountsFromConstraint(c, ev, relProps)
}

func (sb *statisticsBuilder) applyConstraintSet(
	cs *constraint.Set, ev ExprView, relProps *props.Relational,
) (numUnappliedConstraints int) {
	if cs.IsUnconstrained() {
		return 0 /* numUnappliedConstraints */
	}

	if cs == constraint.Contradiction {
		panic("applyConstraintSet called on constraint with contradiction")
	}

	numUnappliedConstraints = 0
	for i := 0; i < cs.Length(); i++ {
		applied := sb.updateDistinctCountsFromConstraint(cs.Constraint(i), ev, relProps)
		if !applied {
			// If a constraint cannot be applied, it may represent an
			// inequality like x < 1. As a result, distinctCounts does not fully
			// represent the selectivity of the constraint set.
			// We return the number of unapplied constraints to the caller
			// function to be used for selectivity calculation.
			numUnappliedConstraints++
		}
	}

	return numUnappliedConstraints
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
	c *constraint.Constraint, ev ExprView, relProps *props.Relational,
) (applied bool) {
	// All of the columns that are part of the prefix have a finite number of
	// distinct values.
	prefix := c.Prefix(sb.evalCtx)

	// If there are any other columns beyond the prefix, we may be able to
	// determine the number of distinct values for the first one. For example:
	//   /a/b/c: [/1/2/3 - /1/2/3] [/1/4/5 - /1/4/8]
	//       -> Column a has DistinctCount = 1.
	//       -> Column b has DistinctCount = 2.
	//       -> Column c has DistinctCount = 5.
	for col := 0; col <= prefix; col++ {
		// All columns should have at least one distinct value.
		distinctCount := 1.0

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
						distinctCount += float64(end - start)
					} else {
						distinctCount += float64(start - end)
					}
				} else {
					// We can't determine the distinct count for this column. For example,
					// the number of distinct values in the constraint
					// /a: [/'cherry' - /'mango'] cannot be determined.
					return applied
				}
			}
			if i != 0 {
				compare := startVal.Compare(sb.evalCtx, val)
				ascending := c.Columns.Get(col).Ascending()
				if (compare > 0 && ascending) || (compare < 0 && !ascending) {
					// This check is needed to ensure that we calculate the correct distinct
					// value count for constraints such as:
					//   /a/b: [/1/2 - /1/2] [/1/4 - /1/4] [/2 - /2]
					// We should only increment the distinct count for column "a" once we
					// reach the third span.
					distinctCount++
				} else if compare != 0 {
					// This can happen if we have a prefix, but not an exact prefix. For
					// example:
					//   /a/b: [/1/2 - /1/4] [/3/2 - /3/5] [/6/0 - /6/0]
					// In this case, /a is a prefix, but not an exact prefix. Trying to
					// figure out the distinct count for column b may be more trouble
					// than it's worth. For now, don't bother trying.
					return applied
				}
			}
			val = endVal
		}

		colID := c.Columns.Get(col).ID()
		sb.ensureColStat(colID, distinctCount, ev, relProps)
		applied = true
	}

	return applied
}

func (sb *statisticsBuilder) applyEquivalencies(
	equivReps opt.ColSet, filterFD *props.FuncDepSet, ev ExprView, relProps *props.Relational,
) {
	equivReps.ForEach(func(i int) {
		equivGroup := filterFD.ComputeEquivGroup(opt.ColumnID(i))
		sb.updateDistinctCountsFromEquivalency(equivGroup, ev, relProps)
	})
}

func (sb *statisticsBuilder) updateDistinctCountsFromEquivalency(
	equivGroup opt.ColSet, ev ExprView, relProps *props.Relational,
) {
	s := &relProps.Stats

	// Find the minimum distinct count for all columns in this equivalency group.
	minDistinctCount := s.RowCount
	equivGroup.ForEach(func(i int) {
		col := opt.ColumnID(i)
		colStat, ok := s.ColStats[col]
		if !ok {
			colSet := util.MakeFastIntSet(i)
			colStat = sb.copyColStat(colSet, s, sb.colStatFromInput(colSet, ev))
		}
		if colStat.DistinctCount < minDistinctCount {
			minDistinctCount = colStat.DistinctCount
		}
	})

	// Set the distinct count to the minimum for all columns in this equivalency
	// group.
	equivGroup.ForEach(func(i int) {
		col := opt.ColumnID(i)
		sb.ensureColStat(col, minDistinctCount, ev, relProps)
	})
}

// selectivityFromDistinctCounts calculates the selectivity of a filter by
// taking the product of selectivities of each constrained column. In the general case,
// this can be represented by the formula:
//
//                  ┬-┬ ⎛ new distinct(i) ⎞
//   selectivity =  │ │ ⎜ --------------- ⎟
//                  ┴ ┴ ⎝ old distinct(i) ⎠
//                 i in
//              {constrained
//                columns}
//
// This selectivity will be used later to update the row count and the
// distinct count for the unconstrained columns.
//
// This algorithm assumes the columns are completely independent.
//
func (sb *statisticsBuilder) selectivityFromDistinctCounts(
	cols opt.ColSet, ev ExprView, s *props.Statistics,
) (selectivity float64) {
	selectivity = 1.0
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colStat, ok := s.ColStats[opt.ColumnID(col)]
		if !ok {
			continue
		}

		inputStat := sb.colStatFromInput(colStat.Cols, ev)
		if inputStat.DistinctCount != 0 && colStat.DistinctCount < inputStat.DistinctCount {
			selectivity *= colStat.DistinctCount / inputStat.DistinctCount
		}
	}

	return selectivity
}

// selectivityFromEquivalencies determines the selectivity of equality
// constraints. It must be called before applyEquivalencies.
func (sb *statisticsBuilder) selectivityFromEquivalencies(
	equivReps opt.ColSet, filterFD *props.FuncDepSet, ev ExprView, s *props.Statistics,
) (selectivity float64) {
	selectivity = 1.0
	equivReps.ForEach(func(i int) {
		equivGroup := filterFD.ComputeEquivGroup(opt.ColumnID(i))
		selectivity *= sb.selectivityFromEquivalency(equivGroup, ev, s)
	})
	return selectivity
}

func (sb *statisticsBuilder) selectivityFromEquivalency(
	equivGroup opt.ColSet, ev ExprView, s *props.Statistics,
) (selectivity float64) {
	// Find the maximum input distinct count for all columns in this equivalency
	// group.
	maxDistinctCount := float64(0)
	equivGroup.ForEach(func(i int) {
		// If any of the distinct counts were updated by the filter, we want to use
		// the updated value.
		colStat, ok := s.ColStats[opt.ColumnID(i)]
		if !ok {
			colStat = sb.colStatFromInput(util.MakeFastIntSet(i), ev)
		}
		if maxDistinctCount < colStat.DistinctCount {
			maxDistinctCount = colStat.DistinctCount
		}
	})
	if maxDistinctCount > s.RowCount {
		maxDistinctCount = s.RowCount
	}

	// The selectivity of an equality condition var1=var2 is
	// 1/max(distinct(var1), distinct(var2)).
	selectivity = 1.0
	if maxDistinctCount > 1 {
		selectivity = 1 / maxDistinctCount
	}
	return selectivity
}

func (sb *statisticsBuilder) selectivityFromUnappliedConstraints(
	numUnappliedConstraints int,
) (selectivity float64) {
	return math.Pow(unknownFilterSelectivity, float64(numUnappliedConstraints))
}

// tryReduceCols is used to determine which columns to use for selectivity
// calculation.
//
// When columns in the colStats are functionally determined by other columns,
// and the determinant columns each have distinctCount = 1, we should consider
// the implied correlations for selectivity calculation. Consider the query:
//
//   SELECT * FROM customer WHERE id = 123 and name = 'John Smith'
//
// If id is the primary key of customer, then name is functionally determined
// by id. We only need to consider the selectivity of id, not name, since id
// and name are fully correlated. To determine if we have a case such as this
// one, we functionally reduce the set of columns which have column statistics,
// eliminating columns that can be functionally determined by other columns.
// If the distinct count on all of these reduced columns is one, then we return
// this reduced column set to be used for selectivity calculation.
//
func (sb *statisticsBuilder) tryReduceCols(
	cols opt.ColSet, s *props.Statistics, fd *props.FuncDepSet,
) opt.ColSet {
	reducedCols := fd.ReduceCols(cols)
	if reducedCols.Empty() {
		// There are no reduced columns so we return the original column set.
		return cols
	}

	for i, ok := reducedCols.Next(0); ok; i, ok = reducedCols.Next(i + 1) {
		colStat, ok := s.ColStats[opt.ColumnID(i)]
		if !ok || colStat.DistinctCount != 1 {
			// The reduced columns are not all constant, so return the original
			// column set.
			return cols
		}
	}

	return reducedCols
}

func (sb *statisticsBuilder) isEqualityWithTwoVars(cond ExprView) bool {
	if cond.Operator() == opt.EqOp {
		left := cond.Child(0)
		right := cond.Child(1)
		if left.Operator() == opt.VariableOp && right.Operator() == opt.VariableOp {
			return true
		}
	}
	return false
}
