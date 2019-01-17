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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/json"
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
}

func (sb *statisticsBuilder) init(evalCtx *tree.EvalContext, md *opt.Metadata) {
	sb.evalCtx = evalCtx
	sb.md = md
}

func (sb *statisticsBuilder) clear() {
	sb.evalCtx = nil
	sb.md = nil
}

// colStatFromChild retrieves a column statistic from a specific child of the
// given expression.
func (sb *statisticsBuilder) colStatFromChild(
	colSet opt.ColSet, e RelExpr, childIdx int,
) *props.ColumnStatistic {
	// Helper function to return the column statistic if the output columns of
	// the child with the given index intersect colSet.
	child := e.Child(childIdx).(RelExpr)
	childProps := child.Relational()
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

// statsFromChild retrieves the main statistics struct from a specific child
// of the given expression.
func (sb *statisticsBuilder) statsFromChild(e RelExpr, childIdx int) *props.Statistics {
	return &e.Child(childIdx).(RelExpr).Relational().Stats
}

// colStatFromInput retrieves a column statistic from the input(s) of a Scan,
// Select, or Join. The input to the Scan is the "raw" table.
func (sb *statisticsBuilder) colStatFromInput(colSet opt.ColSet, e RelExpr) *props.ColumnStatistic {
	var lookupJoin *LookupJoinExpr
	var zigzagJoin *ZigzagJoinExpr

	switch t := e.(type) {
	case *ScanExpr:
		return sb.colStatTable(t.Table, colSet)

	case *SelectExpr:
		return sb.colStatFromChild(colSet, t, 0)

	case *LookupJoinExpr:
		lookupJoin = t

	case *ZigzagJoinExpr:
		zigzagJoin = t
	}

	if lookupJoin != nil || zigzagJoin != nil || opt.IsJoinOp(e) {
		var leftProps *props.Relational
		if zigzagJoin != nil {
			ensureZigzagJoinInputProps(zigzagJoin, sb)
			leftProps = &zigzagJoin.leftProps
		} else {
			leftProps = e.Child(0).(RelExpr).Relational()
		}
		intersectsLeft := leftProps.OutputCols.Intersects(colSet)
		var intersectsRight bool
		if lookupJoin != nil {
			ensureLookupJoinInputProps(lookupJoin, sb)
			intersectsRight = lookupJoin.lookupProps.OutputCols.Intersects(colSet)
		} else if zigzagJoin != nil {
			intersectsRight = zigzagJoin.rightProps.OutputCols.Intersects(colSet)
		} else {
			intersectsRight = e.Child(1).(RelExpr).Relational().OutputCols.Intersects(colSet)
		}
		if intersectsLeft {
			if intersectsRight {
				// TODO(radu): what if both sides have columns in colSet?
				panic(fmt.Sprintf("colSet %v contains both left and right columns", colSet))
			}
			if zigzagJoin != nil {
				return sb.colStatTable(zigzagJoin.LeftTable, colSet)
			}
			return sb.colStatFromChild(colSet, e, 0 /* childIdx */)
		}
		if intersectsRight {
			if lookupJoin != nil {
				return sb.colStatTable(lookupJoin.Table, colSet)
			}
			if zigzagJoin != nil {
				return sb.colStatTable(zigzagJoin.RightTable, colSet)
			}
			return sb.colStatFromChild(colSet, e, 1 /* childIdx */)
		}
		// All columns in colSet are outer columns; therefore, we can treat them
		// as a constant.
		return &props.ColumnStatistic{Cols: colSet, DistinctCount: 1}
	}

	panic(fmt.Sprintf("unsupported operator type %s", e.Op()))
}

// colStat gets a column statistic for the given set of columns if it exists.
// If the column statistic is not available in the current expression, colStat
// recursively tries to find it in the children of the expression, lazily
// populating either s.ColStats or s.MultiColStats with the statistic as it
// gets passed up the expression tree.
func (sb *statisticsBuilder) colStat(colSet opt.ColSet, e RelExpr) *props.ColumnStatistic {
	if colSet.Empty() {
		panic("column statistics cannot be determined for empty column set")
	}

	// Check if the requested column statistic is already cached.
	if stat, ok := e.Relational().Stats.ColStats.Lookup(colSet); ok {
		return stat
	}

	// The statistic was not found in the cache, so calculate it based on the
	// type of expression.
	switch e.Op() {
	case opt.ScanOp:
		return sb.colStatScan(colSet, e.(*ScanExpr))

	case opt.VirtualScanOp:
		return sb.colStatVirtualScan(colSet, e.(*VirtualScanExpr))

	case opt.SelectOp:
		return sb.colStatSelect(colSet, e.(*SelectExpr))

	case opt.ProjectOp:
		return sb.colStatProject(colSet, e.(*ProjectExpr))

	case opt.ValuesOp:
		return sb.colStatValues(colSet, e.(*ValuesExpr))

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.RightJoinApplyOp, opt.FullJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp,
		opt.LookupJoinOp, opt.ZigzagJoinOp:
		return sb.colStatJoin(colSet, e)

	case opt.IndexJoinOp:
		return sb.colStatIndexJoin(colSet, e.(*IndexJoinExpr))

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		return sb.colStatSetNode(colSet, e)

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp:
		return sb.colStatGroupBy(colSet, e)

	case opt.LimitOp:
		return sb.colStatLimit(colSet, e.(*LimitExpr))

	case opt.OffsetOp:
		return sb.colStatOffset(colSet, e.(*OffsetExpr))

	case opt.Max1RowOp:
		return sb.colStatMax1Row(colSet, e.(*Max1RowExpr))

	case opt.RowNumberOp:
		return sb.colStatRowNumber(colSet, e.(*RowNumberExpr))

	case opt.ProjectSetOp:
		return sb.colStatProjectSet(colSet, e.(*ProjectSetExpr))

	case opt.InsertOp, opt.UpdateOp, opt.UpsertOp, opt.DeleteOp:
		return sb.colStatMutation(colSet, e)

	case opt.ExplainOp, opt.ShowTraceForSessionOp:
		relProps := e.Relational()
		return sb.colStatLeaf(colSet, &relProps.Stats, &relProps.FuncDeps, relProps.NotNullCols)
	}

	panic(fmt.Sprintf("unrecognized relational expression type: %v", e.Op()))
}

// colStatLeaf creates a column statistic for a given column set (if it doesn't
// already exist in s), by deriving the statistic from the general statistics.
// Used when there is no child expression to retrieve statistics from, typically
// with the Statistics derived for a table.
func (sb *statisticsBuilder) colStatLeaf(
	colSet opt.ColSet, s *props.Statistics, fd *props.FuncDepSet, notNullCols opt.ColSet,
) *props.ColumnStatistic {
	// Ensure that the requested column statistic is in the cache.
	colStat, added := s.ColStats.Add(colSet)
	if !added {
		// Already in the cache.
		return colStat
	}

	// If some of the columns are a lax key, the distinct count equals the row
	// count. The null count is 0 if these columns are not nullable, otherwise
	// copy the null count from the nullable columns in colSet.
	if fd.ColsAreLaxKey(colSet) {
		if colSet.SubsetOf(notNullCols) {
			colStat.NullCount = 0
		} else {
			nullableCols := colSet.Difference(notNullCols)
			if nullableCols.Equals(colSet) {
				// No column statistics on this colSet - use the unknown
				// null count ratio.
				colStat.NullCount = s.RowCount * unknownNullCountRatio
			} else {
				colStatLeaf := sb.colStatLeaf(nullableCols, s, fd, notNullCols)
				colStat.NullCount = colStatLeaf.NullCount
			}
		}
		colStat.DistinctCount = s.RowCount - colStat.NullCount
		return colStat
	}

	if colSet.Len() == 1 {
		col, _ := colSet.Next(0)
		colStat.DistinctCount = unknownDistinctCountRatio * s.RowCount
		colStat.NullCount = unknownNullCountRatio * s.RowCount
		if sb.md.ColumnMeta(opt.ColumnID(col)).Type == types.Bool {
			colStat.DistinctCount = min(colStat.DistinctCount, 2)
		}
		if notNullCols.Contains(col) {
			colStat.NullCount = 0
		}
	} else {
		distinctCount := 1.0
		nullCount := 0.0
		colSet.ForEach(func(i int) {
			colStatLeaf := sb.colStatLeaf(util.MakeFastIntSet(i), s, fd, notNullCols)
			distinctCount *= colStatLeaf.DistinctCount
			if nullCount < s.RowCount {
				// Subtract the expected chance of collisions with nulls already collected.
				nullCount += colStatLeaf.NullCount * (1 - nullCount/s.RowCount)
			}
		})
		colStat.DistinctCount = min(distinctCount, s.RowCount)
		colStat.NullCount = min(nullCount, s.RowCount)
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
		for i := 0; i < tab.StatisticCount(); i++ {
			stat := tab.Statistic(i)
			var cols opt.ColSet
			for i := 0; i < stat.ColumnCount(); i++ {
				cols.Add(int(tabID.ColumnID(stat.ColumnOrdinal(i))))
			}
			if colStat, ok := stats.ColStats.Add(cols); ok {
				colStat.DistinctCount = float64(stat.DistinctCount())
				colStat.NullCount = float64(stat.NullCount())
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
	tableNotNullCols := tableNotNullCols(sb.md, tabID)
	return sb.colStatLeaf(colSet, tableStats, tableFD, tableNotNullCols)
}

// +------+
// | Scan |
// +------+

func (sb *statisticsBuilder) buildScan(scan *ScanExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := sb.makeTableStatistics(scan.Table)
	s.RowCount = inputStats.RowCount

	if scan.Constraint != nil {
		// Calculate distinct counts for constrained columns
		// -------------------------------------------------
		var numUnappliedConjuncts float64
		var cols opt.ColSet
		// Inverted indexes are a special case; a constraint like:
		// /1: [/'{"a": "b"}' - /'{"a": "b"}']
		// does not necessarily mean there is only going to be one distinct
		// value for column 1, if it is being applied to an inverted index.
		// This is because inverted index keys could correspond to partial
		// column values, such as one path-to-a-leaf through a JSON object.
		//
		// For now, don't apply constraints on inverted index columns.
		if sb.md.Table(scan.Table).Index(scan.Index).IsInverted() {
			for i, n := 0, scan.Constraint.ConstrainedColumns(sb.evalCtx); i < n; i++ {
				numUnappliedConjuncts += sb.numConjunctsInConstraint(scan.Constraint, i)
			}
		} else {
			numUnappliedConjuncts = sb.applyIndexConstraint(scan.Constraint, scan, relProps)
			for i, n := 0, scan.Constraint.ConstrainedColumns(sb.evalCtx); i < n; i++ {
				cols.Add(int(scan.Constraint.Columns.Get(i).ID()))
			}
		}

		// Calculate row count and selectivity
		// -----------------------------------
		inputRowCount := s.RowCount
		s.ApplySelectivity(sb.selectivityFromDistinctCounts(cols, scan, s))
		s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(numUnappliedConjuncts))

		// Set null counts to 0 for non-nullable columns
		// -------------------------------------------------
		sb.updateNullCountsFromProps(scan, relProps, inputStats.RowCount)

		s.ApplySelectivity(sb.selectivityFromNullCounts(cols, scan, s, inputRowCount))
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatScan(colSet opt.ColSet, scan *ScanExpr) *props.ColumnStatistic {
	relProps := scan.Relational()
	s := &relProps.Stats

	colStat := sb.copyColStat(colSet, s, sb.colStatTable(scan.Table, colSet))
	if s.Selectivity != 1 {
		tableStats := sb.makeTableStatistics(scan.Table)
		colStat.ApplySelectivity(s.Selectivity, tableStats.RowCount)
	}

	// Cap distinct and null counts at limit, if it exists.
	if scan.HardLimit.IsSet() {
		if limit := float64(scan.HardLimit.RowCount()); limit < s.RowCount {
			colStat.DistinctCount = min(colStat.DistinctCount, limit)
			colStat.NullCount = min(colStat.NullCount, limit)
		}
	}

	if colSet.SubsetOf(relProps.NotNullCols) {
		colStat.NullCount = 0
	}

	return colStat
}

// +-------------+
// | VirtualScan |
// +-------------+

func (sb *statisticsBuilder) buildVirtualScan(scan *VirtualScanExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := sb.makeTableStatistics(scan.Table)

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatVirtualScan(
	colSet opt.ColSet, scan *VirtualScanExpr,
) *props.ColumnStatistic {
	s := &scan.Relational().Stats
	return sb.copyColStat(colSet, s, sb.colStatTable(scan.Table, colSet))
}

// +--------+
// | Select |
// +--------+

func (sb *statisticsBuilder) buildSelect(sel *SelectExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	// Update stats based on equivalencies in the filter conditions. Note that
	// EquivReps from the Select FD should not be used, as they include
	// equivalencies derived from input expressions.
	var equivFD props.FuncDepSet
	for i := range sel.Filters {
		equivFD.AddEquivFrom(&sel.Filters[i].ScalarProps(sel.Memo()).FuncDeps)
	}
	equivReps := equivFD.EquivReps()

	// Calculate distinct counts for constrained columns
	// -------------------------------------------------
	numUnappliedConjuncts, constrainedCols := sb.applyFilter(sel.Filters, sel, relProps)

	// Try to reduce the number of columns used for selectivity
	// calculation based on functional dependencies.
	inputFD := &sel.Input.Relational().FuncDeps
	nonReducedCols := constrainedCols
	constrainedCols = sb.tryReduceCols(constrainedCols, s, inputFD)

	// Calculate selectivity and row count
	// -----------------------------------
	inputStats := &sel.Input.Relational().Stats
	s.RowCount = inputStats.RowCount
	inputRowCount := s.RowCount
	s.ApplySelectivity(sb.selectivityFromDistinctCounts(constrainedCols, sel, s))
	s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, &relProps.FuncDeps, sel, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(numUnappliedConjuncts))

	// Update distinct counts based on equivalencies; this should happen after
	// selectivityFromDistinctCounts and selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, &relProps.FuncDeps, sel, relProps)

	// Update null counts for non-nullable columns.
	sb.updateNullCountsFromProps(sel, relProps, inputRowCount)
	// Note that for null count selectivity calculations, we use the un-reduced constraint
	// columns. This is so we don't miss any null-rejecting filters. For example, in this
	// query: SELECT min(y) FROM xyz WHERE x = 1 , where y is nullable and x is a key (so
	// x -> y in the FD set), we still need to be able to catch the null rejection property
	// on y inferred by the min aggregation and modify the row count accordingly.
	//
	// TODO(itsbilal): Calculate the one column in each FD group that yields the most
	// selective (i.e. lowest selectivity value) from its null count reduction, and use
	// that instead of just calculating selectivities from all columns. The current
	// solution could easily lead to double-counting of null selectivities across
	// multiple correlated columns.
	s.ApplySelectivity(sb.selectivityFromNullCounts(nonReducedCols, sel, s, inputRowCount))

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatSelect(
	colSet opt.ColSet, sel *SelectExpr,
) *props.ColumnStatistic {
	relProps := sel.Relational()
	s := &relProps.Stats
	inputStats := &sel.Input.Relational().Stats
	colStat := sb.copyColStatFromChild(colSet, sel, s)

	// It's not safe to use s.Selectivity, because it's possible that some of the
	// filter conditions were pushed down into the input after s.Selectivity
	// was calculated. For example, an index scan or index join created during
	// exploration could absorb some of the filter conditions.
	selectivity := s.RowCount / inputStats.RowCount
	colStat.ApplySelectivity(selectivity, inputStats.RowCount)
	if colSet.SubsetOf(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +---------+
// | Project |
// +---------+

func (sb *statisticsBuilder) buildProject(prj *ProjectExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &prj.Input.Relational().Stats

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatProject(
	colSet opt.ColSet, prj *ProjectExpr,
) *props.ColumnStatistic {
	relProps := prj.Relational()
	s := &relProps.Stats

	// Columns may be passed through from the input, or they may reference a
	// higher scope (in the case of a correlated subquery), or they
	// may be synthesized by the projection operation.
	inputCols := prj.Input.Relational().OutputCols
	reqInputCols := colSet.Intersection(inputCols)
	nullOpFound := false
	if reqSynthCols := colSet.Difference(inputCols); !reqSynthCols.Empty() {
		// Some of the columns in colSet were synthesized or from a higher scope
		// (in the case of a correlated subquery). We assume that the statistics of
		// the synthesized columns are the same as the statistics of their input
		// columns. For example, the distinct count of (x + 2) is the same as the
		// distinct count of x.
		// TODO(rytaft): This assumption breaks down for certain types of
		// expressions, such as (x < y).
		for i := range prj.Projections {
			item := &prj.Projections[i]
			if reqSynthCols.Contains(int(item.Col)) {
				reqInputCols.UnionWith(item.scalar.OuterCols)

				// If the element is a null constant, account for that
				// when calculating null counts.
				if item.Element.Op() == opt.NullOp {
					nullOpFound = true
				}
			}
		}

		// Intersect with the input columns one more time to remove any columns
		// from higher scopes. Columns from higher scopes are effectively constant
		// in this scope, and therefore have distinct count = 1.
		reqInputCols.IntersectionWith(inputCols)
	}

	colStat, _ := s.ColStats.Add(colSet)

	if !reqInputCols.Empty() {
		// Inherit column statistics from input, using the reqInputCols identified
		// above.
		inputColStat := sb.colStatFromChild(reqInputCols, prj, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
		if nullOpFound {
			colStat.NullCount = s.RowCount
		} else {
			colStat.NullCount = inputColStat.NullCount
		}
	} else {
		// There are no columns in this expression, so it must be a constant.
		if nullOpFound {
			colStat.DistinctCount = 0
			colStat.NullCount = s.RowCount
		} else {
			colStat.DistinctCount = 1
			colStat.NullCount = 0
		}
	}
	if colSet.SubsetOf(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +------+
// | Join |
// +------+

func (sb *statisticsBuilder) buildJoin(
	join RelExpr, relProps *props.Relational, h *joinPropsHelper,
) {
	// Zigzag joins have their own stats builder case.
	if join.Op() == opt.ZigzagJoinOp {
		sb.buildZigzagJoin(join.(*ZigzagJoinExpr), relProps, h)
		return
	}

	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	leftStats := &h.leftProps.Stats
	rightStats := &h.rightProps.Stats
	leftCols := h.leftProps.OutputCols.Copy()
	rightCols := h.rightProps.OutputCols.Copy()
	equivReps := h.filtersFD.EquivReps()

	// Estimating selectivity for semi-join and anti-join is error-prone.
	// For now, just propagate stats from the left side.
	switch h.joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		s.RowCount = leftStats.RowCount
		s.Selectivity = 1
		return
	}

	// Shortcut if there are no ON conditions. Note that for lookup join, there
	// are implicit equality conditions on KeyCols.
	if h.filterIsTrue {
		s.RowCount = leftStats.RowCount * rightStats.RowCount
		s.Selectivity = 1
		return
	}

	// Shortcut if the ON condition is false or there is a contradiction.
	if h.filters.IsFalse() {
		switch h.joinType {
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

	// Calculate distinct counts for constrained columns in the ON conditions
	// ----------------------------------------------------------------------
	numUnappliedConjuncts, constrainedCols := sb.applyFilter(h.filters, join, relProps)

	// Try to reduce the number of columns used for selectivity
	// calculation based on functional dependencies.
	constrainedCols = sb.tryReduceJoinCols(
		constrainedCols,
		s,
		h.leftProps.OutputCols,
		h.rightProps.OutputCols,
		&h.leftProps.FuncDeps,
		&h.rightProps.FuncDeps,
	)

	// Calculate selectivity and row count
	// -----------------------------------
	s.RowCount = leftStats.RowCount * rightStats.RowCount
	inputRowCount := s.RowCount
	s.ApplySelectivity(sb.selectivityFromDistinctCounts(constrainedCols, join, s))
	s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, &h.filtersFD, join, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(numUnappliedConjuncts))

	// Update distinct counts based on equivalencies; this should happen after
	// selectivityFromDistinctCounts and selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, &h.filtersFD, join, relProps)

	// Update null counts for non-nullable columns.
	sb.updateNullCountsFromProps(join, relProps, inputRowCount)

	s.ApplySelectivity(sb.joinSelectivityFromNullCounts(
		constrainedCols,
		join,
		s,
		inputRowCount,
		leftCols,
		leftStats.RowCount,
		rightCols,
		rightStats.RowCount,
	))

	// The above calculation is for inner joins. Other joins need to remove stats
	// that involve outer columns.
	switch h.joinType {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// Keep only column stats from the right side. The stats from the left side
		// are not valid.
		s.ColStats.RemoveIntersecting(h.leftProps.OutputCols)

	case opt.RightJoinOp, opt.RightJoinApplyOp:
		// Keep only column stats from the left side. The stats from the right side
		// are not valid.
		s.ColStats.RemoveIntersecting(h.rightProps.OutputCols)

	case opt.FullJoinOp, opt.FullJoinApplyOp:
		// Do not keep any column stats.
		s.ColStats.Clear()
	}

	// Tweak the row count.
	innerJoinRowCount := s.RowCount
	switch h.joinType {
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

	// Loop through all colSets added in this step, and adjust null counts.
	for i := 0; i < s.ColStats.Count(); i++ {
		colStat := s.ColStats.Get(i)
		leftSideCols := leftCols.Intersection(colStat.Cols)
		rightSideCols := rightCols.Intersection(colStat.Cols)
		leftNullCount, rightNullCount := sb.leftRightNullCounts(
			join,
			leftSideCols,
			rightSideCols,
		)

		// Update all null counts not zeroed out in updateNullCountsFromProps
		// to equal the inner join count, instead of what it currently is (either
		// leftNullCount or rightNullCount).
		if colStat.NullCount != 0 {
			colStat.NullCount = innerJoinNullCount(
				s.RowCount,
				leftNullCount,
				leftStats.RowCount,
				rightNullCount,
				rightStats.RowCount,
			)
		}

		switch h.joinType {
		case opt.LeftJoinOp, opt.LeftJoinApplyOp, opt.RightJoinOp, opt.RightJoinApplyOp,
			opt.FullJoinOp, opt.FullJoinApplyOp:
			if !colStat.Cols.SubsetOf(relProps.NotNullCols) {
				sb.adjustNullCountsForOuterJoins(
					colStat,
					h.joinType,
					leftSideCols,
					rightSideCols,
					leftNullCount,
					leftStats.RowCount,
					rightNullCount,
					rightStats.RowCount,
					s.RowCount,
					innerJoinRowCount,
				)
			}
		}
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatJoin(colSet opt.ColSet, join RelExpr) *props.ColumnStatistic {
	relProps := join.Relational()
	s := &relProps.Stats

	var leftProps, rightProps *props.Relational
	var lookupJoin *LookupJoinExpr
	var zigzagJoin *ZigzagJoinExpr

	joinType := join.Op()
	if joinType == opt.LookupJoinOp {
		leftProps = join.Child(0).(RelExpr).Relational()
		lookupJoin = join.(*LookupJoinExpr)
		joinType = lookupJoin.JoinType
		ensureLookupJoinInputProps(lookupJoin, sb)
		rightProps = &lookupJoin.lookupProps
	} else if joinType == opt.ZigzagJoinOp {
		zigzagJoin = join.(*ZigzagJoinExpr)
		joinType = opt.InnerJoinOp
		ensureZigzagJoinInputProps(zigzagJoin, sb)
		leftProps = &zigzagJoin.leftProps
		rightProps = &zigzagJoin.rightProps
	} else {
		leftProps = join.Child(0).(RelExpr).Relational()
		rightProps = join.Child(1).(RelExpr).Relational()
	}

	switch joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Column stats come from left side of join.
		colStat := sb.copyColStat(colSet, s, sb.colStatFromJoinLeft(colSet, join))
		colStat.ApplySelectivity(s.Selectivity, leftProps.Stats.RowCount)
		return colStat

	default:
		// Column stats come from both sides of join.
		leftCols := leftProps.OutputCols.Intersection(colSet)
		rightCols := rightProps.OutputCols.Intersection(colSet)

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
		inputRowCount := leftProps.Stats.RowCount * rightProps.Stats.RowCount
		var leftNullCount, rightNullCount float64
		if rightCols.Empty() {
			colStat = sb.copyColStat(colSet, s, sb.colStatFromJoinLeft(colSet, join))
			leftNullCount = colStat.NullCount
			switch joinType {
			case opt.InnerJoinOp, opt.InnerJoinApplyOp, opt.RightJoinOp, opt.RightJoinApplyOp:
				colStat.ApplySelectivity(s.Selectivity, inputRowCount)
			}
		} else if leftCols.Empty() {
			colStat = sb.copyColStat(colSet, s, sb.colStatFromJoinRight(colSet, join))
			rightNullCount = colStat.NullCount
			switch joinType {
			case opt.InnerJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinOp, opt.LeftJoinApplyOp:
				colStat.ApplySelectivity(s.Selectivity, inputRowCount)
			}
		} else {
			// Make a copy of the input column stats so we don't modify the originals.
			leftColStat := *sb.colStatFromJoinLeft(leftCols, join)
			rightColStat := *sb.colStatFromJoinRight(rightCols, join)

			leftNullCount = leftColStat.NullCount
			rightNullCount = rightColStat.NullCount
			switch joinType {
			case opt.InnerJoinOp, opt.InnerJoinApplyOp:
				leftColStat.ApplySelectivity(s.Selectivity, inputRowCount)
				rightColStat.ApplySelectivity(s.Selectivity, inputRowCount)

			case opt.LeftJoinOp, opt.LeftJoinApplyOp:
				rightColStat.ApplySelectivity(s.Selectivity, inputRowCount)

			case opt.RightJoinOp, opt.RightJoinApplyOp:
				leftColStat.ApplySelectivity(s.Selectivity, inputRowCount)
			}
			colStat, _ = s.ColStats.Add(colSet)
			colStat.DistinctCount = leftColStat.DistinctCount * rightColStat.DistinctCount
		}

		// Null count estimation - assume an inner join and then bump the null count later
		// based on the type of join.
		colStat.NullCount = innerJoinNullCount(
			s.RowCount,
			leftNullCount,
			leftProps.Stats.RowCount,
			rightNullCount,
			rightProps.Stats.RowCount,
		)

		switch joinType {
		case opt.LeftJoinOp, opt.LeftJoinApplyOp, opt.RightJoinOp, opt.RightJoinApplyOp,
			opt.FullJoinOp, opt.FullJoinApplyOp:
			sb.adjustNullCountsForOuterJoins(
				colStat,
				joinType,
				leftCols,
				rightCols,
				leftNullCount,
				leftProps.Stats.RowCount,
				rightNullCount,
				rightProps.Stats.RowCount,
				s.RowCount,
				s.Selectivity*inputRowCount,
			)
		}

		// The distinct count should be no larger than the row count.
		if colStat.DistinctCount > s.RowCount {
			colStat.DistinctCount = s.RowCount
		}
		// Similarly, the null count should be no larger than RowCount.
		colStat.NullCount = min(s.RowCount, colStat.NullCount)
		if colSet.SubsetOf(relProps.NotNullCols) {
			colStat.NullCount = 0
		}
		return colStat
	}
}

// leftRightNullCounts returns null counts on the left and right sides of the
// specified join. If either leftCols or rightCols are empty, the corresponding
// side's return value is zero.
func (sb *statisticsBuilder) leftRightNullCounts(
	e RelExpr, leftCols opt.ColSet, rightCols opt.ColSet,
) (leftNullCount, rightNullCount float64) {
	if !leftCols.Empty() {
		leftColStat := sb.colStatFromJoinLeft(leftCols, e)
		leftNullCount = leftColStat.NullCount
	}
	if !rightCols.Empty() {
		rightColStat := sb.colStatFromJoinRight(rightCols, e)
		rightNullCount = rightColStat.NullCount
	}

	return leftNullCount, rightNullCount
}

// innerJoinNullCount returns an estimate of the number of nulls in an inner
// join with the specified column stats.
func innerJoinNullCount(
	rowCount float64,
	leftNullCount float64,
	leftRowCount float64,
	rightNullCount float64,
	rightRowCount float64,
) float64 {
	// Here, f1 and f2 are probabilities of nulls on either side of the join.
	var f1, f2 float64
	if leftRowCount != 0 {
		f1 = leftNullCount / leftRowCount
	}
	if rightRowCount != 0 {
		f2 = rightNullCount / rightRowCount
	}

	// Rough estimate of nulls in the combined result set,
	// assuming independence.
	return rowCount * (f1 + f2 - f1*f2)
}

// adjustNullCountsForOuterJoins modifies the null counts for the specified columns
// to adjust for additional nulls created in this type of outer join. It first does
// a max of the nulls on the appropriate side of the join (i.e. left for left, both
// for full), and then adds an expected number of nulls created by column extension
// on non-matching rows (such as on right cols for left joins and both for full).
func (sb *statisticsBuilder) adjustNullCountsForOuterJoins(
	colStat *props.ColumnStatistic,
	joinType opt.Operator,
	leftCols opt.ColSet,
	rightCols opt.ColSet,
	leftNullCount float64,
	leftRowCount float64,
	rightNullCount float64,
	rightRowCount float64,
	rowCount float64,
	innerJoinRowCount float64,
) {
	// This switch is based on the same premise as the RowCount one in buildJoin.
	// Adjust null counts for non-inner joins. The additive factor denotes
	// nulls created due to column extension - such as right columns for non-matching
	// rows in left joins.
	switch joinType {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// All nulls from left side should be in the result.
		colStat.NullCount = max(colStat.NullCount, leftNullCount)
		if !rightCols.Empty() {
			colStat.NullCount += (rowCount - innerJoinRowCount) * (1 - leftNullCount/leftRowCount)
		}

	case opt.RightJoinOp, opt.RightJoinApplyOp:
		// All nulls from right side should be in the result.
		colStat.NullCount = max(colStat.NullCount, rightNullCount)
		if !leftCols.Empty() {
			colStat.NullCount += (rowCount - innerJoinRowCount) * (1 - rightNullCount/rightRowCount)
		}

	case opt.FullJoinOp, opt.FullJoinApplyOp:
		// All nulls from both sides should be in the result.
		leftJoinNullCount := max(colStat.NullCount, leftNullCount)
		rightJoinNullCount := max(colStat.NullCount, rightNullCount)
		colStat.NullCount = leftJoinNullCount + rightJoinNullCount - colStat.NullCount

		leftJoinRowCount := max(innerJoinRowCount, leftRowCount)
		rightJoinRowCount := max(innerJoinRowCount, rightRowCount)

		if !leftCols.Empty() {
			colStat.NullCount += (rightJoinRowCount - innerJoinRowCount) * (1 - rightNullCount/rightRowCount)
		}
		if !rightCols.Empty() {
			colStat.NullCount += (leftJoinRowCount - innerJoinRowCount) * (1 - leftNullCount/leftRowCount)
		}
	}
}

// colStatfromJoinLeft returns a column statistic from the left input of a join.
func (sb *statisticsBuilder) colStatFromJoinLeft(
	cols opt.ColSet, join RelExpr,
) *props.ColumnStatistic {
	if join.Op() == opt.ZigzagJoinOp {
		return sb.colStatTable(join.Private().(*ZigzagJoinPrivate).LeftTable, cols)
	}
	return sb.colStatFromChild(cols, join, 0 /* childIdx */)
}

// colStatfromJoinRight returns a column statistic from the right input of a
// join (or the table for a lookup join).
func (sb *statisticsBuilder) colStatFromJoinRight(
	cols opt.ColSet, join RelExpr,
) *props.ColumnStatistic {
	if join.Op() == opt.ZigzagJoinOp {
		return sb.colStatTable(join.Private().(*ZigzagJoinPrivate).RightTable, cols)
	} else if join.Op() == opt.LookupJoinOp {
		lookupPrivate := join.Private().(*LookupJoinPrivate)
		return sb.colStatTable(lookupPrivate.Table, cols)
	}
	return sb.colStatFromChild(cols, join, 1 /* childIdx */)
}

// +------------+
// | Index Join |
// +------------+

func (sb *statisticsBuilder) buildIndexJoin(indexJoin *IndexJoinExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &indexJoin.Input.Relational().Stats

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatIndexJoin(
	colSet opt.ColSet, join *IndexJoinExpr,
) *props.ColumnStatistic {
	relProps := join.Relational()
	s := &relProps.Stats

	inputProps := join.Input.Relational()
	inputCols := inputProps.OutputCols

	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = 1
	colStat.NullCount = 0

	// Some of the requested columns may be from the input index.
	reqInputCols := colSet.Intersection(inputCols)
	if !reqInputCols.Empty() {
		inputColStat := sb.colStatFromChild(reqInputCols, join, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
		colStat.NullCount = inputColStat.NullCount
	}

	// Other requested columns may be from the primary index.
	reqLookupCols := colSet.Difference(inputCols).Intersection(join.Cols)
	if !reqLookupCols.Empty() {
		// Make a copy of the lookup column stats so we don't modify the originals.
		lookupColStat := *sb.colStatTable(join.Table, reqLookupCols)

		// Calculate the distinct count of the lookup columns given the selectivity
		// of any filters on the input.
		inputStats := &inputProps.Stats
		tableStats := sb.makeTableStatistics(join.Table)
		selectivity := inputStats.RowCount / tableStats.RowCount
		lookupColStat.ApplySelectivity(selectivity, tableStats.RowCount)

		// Multiply the distinct counts in case colStat.DistinctCount is
		// already populated with a statistic from the subset of columns
		// provided by the input index. Multiplying the counts gives a worst-case
		// estimate of the joint distinct count.
		colStat.DistinctCount *= lookupColStat.DistinctCount

		// Assuming null columns are completely independent, calculate
		// the expected value of having nulls in either column set.
		f1 := lookupColStat.NullCount / inputStats.RowCount
		f2 := colStat.NullCount / inputStats.RowCount
		colStat.NullCount = inputStats.RowCount * (f1 + f2 - f1*f2)
	}

	// The distinct count should be no larger than the row count.
	if colStat.DistinctCount > s.RowCount {
		colStat.DistinctCount = s.RowCount
	}
	// Similarly, the null count should be no larger than RowCount.
	colStat.NullCount = min(s.RowCount, colStat.NullCount)
	if colSet.SubsetOf(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +-------------+
// | Zigzag Join |
// +-------------+

// buildZigzagJoin builds the rowCount for a zigzag join. The colStat case
// for ZigzagJoins is shared with Joins, while the builder code is more similar
// to that for a Select. This is to ensure zigzag joins have select/scan-like
// RowCounts, while at an individual column stats level, distinct and null
// counts are handled like they would be for a join with two sides.
func (sb *statisticsBuilder) buildZigzagJoin(
	zigzag *ZigzagJoinExpr, relProps *props.Relational, h *joinPropsHelper,
) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	leftStats := zigzag.leftProps.Stats
	equivReps := h.filtersFD.EquivReps()

	// We assume that we only plan zigzag joins in cases where the result set
	// will have a row count smaller than or equal to the left/right index
	// row counts, and where the left and right sides are indexes on the same
	// table. Their row count should be the same, so use either row count.
	s.RowCount = leftStats.RowCount

	// Calculate distinct counts for constrained columns.
	// Note that fixed columns (i.e. columns constrained to constant values)
	// specified in zigzag.FixedVals and zigzag.{Left,Right}FixedCols
	// still have corresponding filters in zigzag.On. So we don't need
	// to iterate through FixedCols here if we are already processing the ON
	// clause.
	numUnappliedConjuncts, constrainedCols := sb.applyFilter(zigzag.On, zigzag, relProps)

	// Application of constraints on inverted indexes needs to be handled a
	// little differently since a constraint on an inverted index key column
	// could match multiple distinct values in the actual column. This is
	// because inverted index keys could correspond to partial values, like JSON
	// paths.
	//
	// Since filters on inverted index columns cannot be pushed down into the ON
	// clause (due to them containing partial values), we need to explicitly
	// increment numUnappliedConjuncts here for every constrained inverted index
	// column. The multiplication by 2 is to mimic the logic in
	// numConjunctsInConstraint, which counts an equality as 2 conjuncts. This
	// comes from the intuition that a = 1 is probably more selective than a > 1
	// and if we do not mimic a similar selectivity calculation here, the zigzag
	// join ends up having a higher row count and therefore higher cost than
	// a competing index join + constrained scan.
	tab := sb.md.Table(zigzag.LeftTable)
	if tab.Index(zigzag.LeftIndex).IsInverted() {
		numUnappliedConjuncts += float64(len(zigzag.LeftFixedCols) * 2)
	}
	if tab.Index(zigzag.RightIndex).IsInverted() {
		numUnappliedConjuncts += float64(len(zigzag.RightFixedCols) * 2)
	}

	// Try to reduce the number of columns used for selectivity
	// calculation based on functional dependencies. Note that
	// these functional dependencies already include equalities
	// inferred by zigzag.{Left,Right}EqCols.
	inputFD := &zigzag.Relational().FuncDeps
	nonReducedCols := constrainedCols
	constrainedCols = sb.tryReduceCols(constrainedCols, s, inputFD)

	// Calculate selectivity and row count.
	inputRowCount := s.RowCount
	s.ApplySelectivity(sb.selectivityFromDistinctCounts(constrainedCols, zigzag, s))
	s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, &relProps.FuncDeps, zigzag, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(numUnappliedConjuncts))

	// Update distinct counts based on equivalencies; this should happen after
	// selectivityFromDistinctCounts and selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, &relProps.FuncDeps, zigzag, relProps)

	// Update null counts for non-nullable columns.
	sb.updateNullCountsFromProps(zigzag, relProps, inputRowCount)
	// Note that for null count selectivity calculations, we use the un-reduced constraint
	// columns. This is so we don't miss any null-rejecting filters. For example, in this
	// query: SELECT min(y) FROM xyz WHERE x = 1 , where y is nullable and x is a key (so
	// x -> y in the FD set), we still need to be able to catch the null rejection property
	// on y inferred by the min aggregation and modify the row count accordingly.
	//
	// TODO(itsbilal): Calculate the one column in each FD group that yields the most
	// selective (i.e. lowest selectivity value) from its null count reduction, and use
	// that instead of just calculating selectivities from all columns. The current
	// solution could easily lead to double-counting of null selectivities across
	// multiple correlated columns.
	s.ApplySelectivity(sb.selectivityFromNullCounts(nonReducedCols, zigzag, s, inputRowCount))

	sb.finalizeFromCardinality(relProps)
}

// +----------+
// | Group By |
// +----------+

func (sb *statisticsBuilder) buildGroupBy(groupNode RelExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	groupingColSet := groupNode.Private().(*GroupingPrivate).GroupingCols

	if groupingColSet.Empty() {
		// ScalarGroupBy or GroupBy with empty grouping columns.
		s.RowCount = 1
	} else {
		// Estimate the row count based on the distinct count of the grouping
		// columns.
		//
		// TODO(itsbilal): Update null count here, using a formula similar to the
		// ones in colStatGroupBy.
		colStat := sb.copyColStatFromChild(groupingColSet, groupNode, s)
		s.RowCount = colStat.DistinctCount
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatGroupBy(
	colSet opt.ColSet, groupNode RelExpr,
) *props.ColumnStatistic {
	relProps := groupNode.Relational()
	s := &relProps.Stats

	groupingColSet := groupNode.Private().(*GroupingPrivate).GroupingCols
	if groupingColSet.Empty() {
		// ScalarGroupBy or GroupBy with empty grouping columns.
		colStat, _ := s.ColStats.Add(colSet)
		colStat.DistinctCount = 1
		// TODO(itsbilal): Handle case where the scalar resolves to NULL.
		colStat.NullCount = 0
		return colStat
	}

	var colStat *props.ColumnStatistic
	var inputColStat *props.ColumnStatistic
	if !colSet.SubsetOf(groupingColSet) {
		// Some of the requested columns are aggregates. Estimate the distinct
		// count to be the same as the grouping columns.
		colStat, _ = s.ColStats.Add(colSet)
		inputColStat = sb.colStatFromChild(groupingColSet, groupNode, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
	} else {
		// Make a copy so we don't modify the original
		colStat = sb.copyColStatFromChild(colSet, groupNode, s)
		inputColStat = sb.colStatFromChild(colSet, groupNode, 0 /* childIdx */)
	}

	// For null counts - we either only have 1 possible null value (if we're
	// grouping on a single column), or we have as many nulls as in the grouping
	// col set, multiplied by (DistinctCount+1)/RowCount - an inverse of a rough
	// duplicate factor. The + 1 accounts for the fact that nulls are not
	// counted in distinct counts.
	if groupingColSet.Len() == 1 {
		colStat.NullCount = min(1, inputColStat.NullCount)
	} else {
		inputRowCount := sb.statsFromChild(groupNode, 0 /* childIdx */).RowCount
		colStat.NullCount = ((colStat.DistinctCount + 1) / inputRowCount) * inputColStat.NullCount
		colStat.NullCount = min(s.RowCount, colStat.NullCount)
	}

	if colSet.SubsetOf(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +--------+
// | Set Op |
// +--------+

func (sb *statisticsBuilder) buildSetNode(setNode RelExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	leftStats := &setNode.Child(0).(RelExpr).Relational().Stats
	rightStats := &setNode.Child(1).(RelExpr).Relational().Stats

	// These calculations are an upper bound on the row count. It's likely that
	// there is some overlap between the two sets, but not full overlap.
	switch setNode.Op() {
	case opt.UnionOp, opt.UnionAllOp:
		s.RowCount = leftStats.RowCount + rightStats.RowCount

	case opt.IntersectOp, opt.IntersectAllOp:
		s.RowCount = min(leftStats.RowCount, rightStats.RowCount)

	case opt.ExceptOp, opt.ExceptAllOp:
		s.RowCount = leftStats.RowCount
	}

	switch setNode.Op() {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		// Since UNION, INTERSECT and EXCEPT eliminate duplicate rows, the row
		// count will equal the distinct count of the set of output columns.
		setPrivate := setNode.Private().(*SetPrivate)
		outputCols := setPrivate.OutCols.ToSet()
		colStat := sb.colStatSetNodeImpl(outputCols, setNode, relProps)
		s.RowCount = colStat.DistinctCount
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatSetNode(
	colSet opt.ColSet, setNode RelExpr,
) *props.ColumnStatistic {
	return sb.colStatSetNodeImpl(colSet, setNode, setNode.Relational())
}

func (sb *statisticsBuilder) colStatSetNodeImpl(
	outputCols opt.ColSet, setNode RelExpr, relProps *props.Relational,
) *props.ColumnStatistic {
	s := &relProps.Stats
	setPrivate := setNode.Private().(*SetPrivate)

	leftCols := translateColSet(outputCols, setPrivate.OutCols, setPrivate.LeftCols)
	rightCols := translateColSet(outputCols, setPrivate.OutCols, setPrivate.RightCols)
	leftColStat := sb.colStatFromChild(leftCols, setNode, 0 /* childIdx */)
	rightColStat := sb.colStatFromChild(rightCols, setNode, 1 /* childIdx */)

	colStat, _ := s.ColStats.Add(outputCols)

	// Use the actual null counts for bag operations, and normalize them for set
	// operations. See comment in colStatGroupBy for the reasoning behind the null
	// count formula.
	leftNullCount := leftColStat.NullCount
	rightNullCount := rightColStat.NullCount
	switch setNode.Op() {
	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp:
		if len(setPrivate.OutCols) == 1 {
			leftNullCount = min(1, leftNullCount)
			rightNullCount = min(1, rightNullCount)
		} else {
			leftRowCount := sb.statsFromChild(setNode, 0 /* childIdx */).RowCount
			leftNullCount *= (leftColStat.DistinctCount + 1) / leftRowCount
			rightRowCount := sb.statsFromChild(setNode, 1 /* childIdx */).RowCount
			rightNullCount *= (rightColStat.DistinctCount + 1) / rightRowCount
		}
	}

	// These calculations are an upper bound on the distinct count. It's likely
	// that there is some overlap between the two sets, but not full overlap.
	switch setNode.Op() {
	case opt.UnionOp, opt.UnionAllOp:
		colStat.DistinctCount = leftColStat.DistinctCount + rightColStat.DistinctCount
		colStat.NullCount = leftNullCount + rightNullCount

	case opt.IntersectOp, opt.IntersectAllOp:
		colStat.DistinctCount = min(leftColStat.DistinctCount, rightColStat.DistinctCount)
		colStat.NullCount = min(leftNullCount, rightNullCount)

	case opt.ExceptOp, opt.ExceptAllOp:
		colStat.DistinctCount = leftColStat.DistinctCount
		colStat.NullCount = leftNullCount
	}

	if outputCols.SubsetOf(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +--------+
// | Values |
// +--------+

// buildValues builds the statistics for a VALUES expression.
func (sb *statisticsBuilder) buildValues(values *ValuesExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	s.RowCount = float64(len(values.Rows))
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatValues(
	colSet opt.ColSet, values *ValuesExpr,
) *props.ColumnStatistic {
	s := &values.Relational().Stats
	if len(values.Rows) == 0 {
		colStat, _ := s.ColStats.Add(colSet)
		return colStat
	}

	// Determine distinct count from the number of distinct memo groups. Use a
	// map to find the exact count of distinct values for the columns in colSet.
	// Use a hash to combine column values (this does not have to be exact).
	distinct := make(map[uint64]struct{}, values.Rows[0].ChildCount())
	// Determine null count by looking at tuples that have at least one
	// NullOp in them.
	nullCount := 0
	for _, row := range values.Rows {
		// Use the FNV-1a algorithm. See comments for the interner class.
		hash := uint64(offset64)
		hasNull := false
		for i, elem := range row.(*TupleExpr).Elems {
			if elem.Op() == opt.NullOp {
				hasNull = true
			} else if colSet.Contains(int(values.Cols[i])) {
				// Use the pointer value of the scalar expression, since it's already
				// been interned. Therefore, two expressions with the same pointer
				// have the same value.
				ptr := reflect.ValueOf(elem).Pointer()
				hash ^= uint64(ptr)
				hash *= prime64
			}
		}
		if hash != offset64 {
			distinct[hash] = struct{}{}
		}
		if hasNull {
			nullCount++
		}
	}

	// Update the column statistics.
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = float64(len(distinct))
	colStat.NullCount = float64(nullCount)
	return colStat
}

// +-------+
// | Limit |
// +-------+

func (sb *statisticsBuilder) buildLimit(limit *LimitExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &limit.Input.Relational().Stats

	// Copy row count from input.
	s.RowCount = inputStats.RowCount

	// Update row count if limit is a constant and row count is non-zero.
	if cnst, ok := limit.Limit.(*ConstExpr); ok && inputStats.RowCount > 0 {
		hardLimit := *cnst.Value.(*tree.DInt)
		if hardLimit > 0 {
			s.RowCount = min(float64(hardLimit), inputStats.RowCount)
			s.Selectivity = s.RowCount / inputStats.RowCount
		}
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatLimit(
	colSet opt.ColSet, limit *LimitExpr,
) *props.ColumnStatistic {
	relProps := limit.Relational()
	s := &relProps.Stats
	inputStats := &limit.Input.Relational().Stats
	colStat := sb.copyColStatFromChild(colSet, limit, s)

	// Scale distinct count based on the selectivity of the limit operation.
	colStat.ApplySelectivity(s.Selectivity, inputStats.RowCount)
	if colSet.SubsetOf(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +--------+
// | Offset |
// +--------+

func (sb *statisticsBuilder) buildOffset(offset *OffsetExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &offset.Input.Relational().Stats

	// Copy row count from input.
	s.RowCount = inputStats.RowCount

	// Update row count if offset is a constant and row count is non-zero.
	if cnst, ok := offset.Offset.(*ConstExpr); ok && inputStats.RowCount > 0 {
		hardOffset := *cnst.Value.(*tree.DInt)
		if float64(hardOffset) >= inputStats.RowCount {
			s.RowCount = 0
		} else if hardOffset > 0 {
			s.RowCount = inputStats.RowCount - float64(hardOffset)
		}
		s.Selectivity = s.RowCount / inputStats.RowCount
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatOffset(
	colSet opt.ColSet, offset *OffsetExpr,
) *props.ColumnStatistic {
	relProps := offset.Relational()
	s := &relProps.Stats
	inputStats := &offset.Input.Relational().Stats
	colStat := sb.copyColStatFromChild(colSet, offset, s)

	// Scale distinct count based on the selectivity of the offset operation.
	colStat.ApplySelectivity(s.Selectivity, inputStats.RowCount)
	if colSet.SubsetOf(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +---------+
// | Max1Row |
// +---------+

func (sb *statisticsBuilder) buildMax1Row(max1Row *Max1RowExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	s.RowCount = 1
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatMax1Row(
	colSet opt.ColSet, max1Row *Max1RowExpr,
) *props.ColumnStatistic {
	s := &max1Row.Relational().Stats
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = 1
	colStat.NullCount = s.RowCount * unknownNullCountRatio
	if colSet.SubsetOf(max1Row.Relational().NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +------------+
// | Row Number |
// +------------+

func (sb *statisticsBuilder) buildRowNumber(rowNum *RowNumberExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &rowNum.Input.Relational().Stats

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatRowNumber(
	colSet opt.ColSet, rowNum *RowNumberExpr,
) *props.ColumnStatistic {
	relProps := rowNum.Relational()
	s := &relProps.Stats

	colStat, _ := s.ColStats.Add(colSet)

	if colSet.Contains(int(rowNum.ColID)) {
		// The ordinality column is a key, so every row is distinct.
		colStat.DistinctCount = s.RowCount
		if colSet.Len() == 1 {
			// The generated column is the only column being requested.
			colStat.NullCount = 0
		} else {
			// Copy NullCount from child.
			colSetChild := colSet.Copy()
			colSetChild.Remove(int(rowNum.ColID))
			inputColStat := sb.colStatFromChild(colSetChild, rowNum, 0 /* childIdx */)
			colStat.NullCount = inputColStat.NullCount
		}
	} else {
		inputColStat := sb.colStatFromChild(colSet, rowNum, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
		colStat.NullCount = inputColStat.NullCount
	}

	if colSet.SubsetOf(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +-------------+
// | Project Set |
// +-------------+

func (sb *statisticsBuilder) buildProjectSet(
	projectSet *ProjectSetExpr, relProps *props.Relational,
) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	// The row count of a zip operation is equal to the maximum row count of its
	// children.
	for i := range projectSet.Zip {
		if fn, ok := projectSet.Zip[i].Func.(*FunctionExpr); ok {
			if fn.Overload.Generator != nil {
				// TODO(rytaft): We may want to estimate the number of rows based on
				// the type of generator function and its parameters.
				s.RowCount = unknownGeneratorRowCount
				break
			}
		}

		// A scalar function generates one row.
		s.RowCount = 1
	}

	// Multiply by the input row count to get the total.
	inputStats := &projectSet.Input.Relational().Stats
	s.RowCount *= inputStats.RowCount

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatProjectSet(
	colSet opt.ColSet, projectSet *ProjectSetExpr,
) *props.ColumnStatistic {
	s := &projectSet.Relational().Stats
	if s.RowCount == 0 {
		// Short cut if cardinality is 0.
		colStat, _ := s.ColStats.Add(colSet)
		return colStat
	}

	inputProps := projectSet.Input.Relational()
	inputStats := inputProps.Stats
	inputCols := inputProps.OutputCols

	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = 1
	colStat.NullCount = 0

	// Some of the requested columns may be from the input.
	reqInputCols := colSet.Intersection(inputCols)
	if !reqInputCols.Empty() {
		inputColStat := sb.colStatFromChild(reqInputCols, projectSet, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
		colStat.NullCount = inputColStat.NullCount * (s.RowCount / inputStats.RowCount)
	}

	// Other requested columns may be from the output columns of the zip.
	zipCols := projectSet.Zip.OutputCols()
	reqZipCols := colSet.Difference(inputCols).Intersection(zipCols)
	if !reqZipCols.Empty() {
		// Calculate the the distinct count and null count for the zip columns
		// after the cross join has been applied.
		zipColsDistinctCount := float64(1)
		zipColsNullCount := float64(0)
		for i := range projectSet.Zip {
			item := &projectSet.Zip[i]
			if item.Cols.ToSet().Intersects(reqZipCols) {
				if fn, ok := item.Func.(*FunctionExpr); ok && fn.Overload.Generator != nil {
					// The columns(s) contain a generator function.
					// TODO(rytaft): We may want to determine which generator function the
					// requested columns correspond to, and estimate the distinct count and
					// null count based on the type of generator function and its parameters.
					zipColsDistinctCount *= unknownGeneratorRowCount * unknownGeneratorDistinctCountRatio
					zipColsNullCount += (s.RowCount - zipColsNullCount) * unknownNullCountRatio
				} else {
					// The columns(s) contain a scalar function or expression.
					// These columns can contain many null values if the zip also
					// contains a generator function. For example:
					//
					//    SELECT * FROM ROWS FROM (generate_series(0, 3), upper('abc'));
					//
					// Produces:
					//
					//     generate_series | upper
					//    -----------------+-------
					//                   0 | ABC
					//                   1 | NULL
					//                   2 | NULL
					//                   3 | NULL
					//
					// After the cross product with the input, the total number of nulls
					// for the column(s) equals (output row count - input row count).
					// (Also subtract the expected chance of collisions with nulls
					// already collected.)
					zipColsNullCount += (s.RowCount - inputStats.RowCount) * (1 - zipColsNullCount/s.RowCount)
				}

				if item.ScalarProps(projectSet.Memo()).OuterCols.Intersects(inputProps.OutputCols) {
					// The column(s) are correlated with the input, so they may have a
					// distinct value for each distinct row of the input.
					zipColsDistinctCount *= inputStats.RowCount * unknownDistinctCountRatio
				}
			}
		}

		// Multiply the distinct counts in case colStat.DistinctCount is
		// already populated with a statistic from the subset of columns
		// provided by the input. Multiplying the counts gives a worst-case
		// estimate of the joint distinct count.
		colStat.DistinctCount *= zipColsDistinctCount

		// Assuming null columns are completely independent, calculate
		// the expected value of having nulls in either column set.
		f1 := zipColsNullCount / s.RowCount
		f2 := colStat.NullCount / s.RowCount
		colStat.NullCount = s.RowCount * (f1 + f2 - f1*f2)
	}

	// The distinct count and null count should be no larger than the row count.
	colStat.DistinctCount = min(s.RowCount, colStat.DistinctCount)
	colStat.NullCount = min(s.RowCount, colStat.NullCount)

	if colSet.SubsetOf(projectSet.Relational().NotNullCols) {
		colStat.NullCount = 0
	}
	return colStat
}

// +--------------------------------+
// | Insert, Update, Upsert, Delete |
// +--------------------------------+

func (sb *statisticsBuilder) buildMutation(mutation RelExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	inputStats := &mutation.Child(0).(RelExpr).Relational().Stats

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatMutation(
	colSet opt.ColSet, mutation RelExpr,
) *props.ColumnStatistic {
	s := &mutation.Relational().Stats
	private := mutation.Private().(*MutationPrivate)

	// Get colstat from child by mapping requested columns to corresponding
	// input columns.
	inColSet := private.MapToInputCols(colSet)
	inColStat := sb.colStatFromChild(inColSet, mutation, 0 /* childIdx */)

	// Construct mutation colstat using the corresponding input stats.
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = inColStat.DistinctCount
	colStat.NullCount = inColStat.NullCount
	return colStat
}

/////////////////////////////////////////////////
// General helper functions for building stats //
/////////////////////////////////////////////////

// copyColStatFromChild copies the column statistic for the given colSet from
// the first child of ev into ev. colStatFromChild may trigger recursive
// calls if the requested statistic is not already cached in the child.
func (sb *statisticsBuilder) copyColStatFromChild(
	colSet opt.ColSet, e RelExpr, s *props.Statistics,
) *props.ColumnStatistic {
	childColStat := sb.colStatFromChild(colSet, e, 0 /* childIdx */)
	return sb.copyColStat(colSet, s, childColStat)
}

// ensureColStat creates a column statistic for column "col" if it doesn't
// already exist in s.ColStats, copying the statistic from a child.
// Then, ensureColStat sets the distinct count to the minimum of the existing
// value and the new value.
func (sb *statisticsBuilder) ensureColStat(
	colSet opt.ColSet, maxDistinctCount float64, e RelExpr, relProps *props.Relational,
) *props.ColumnStatistic {
	s := &relProps.Stats

	colStat, ok := s.ColStats.Lookup(colSet)
	if !ok {
		colStat = sb.copyColStat(colSet, s, sb.colStatFromInput(colSet, e))
	}

	colStat.DistinctCount = min(colStat.DistinctCount, maxDistinctCount)
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
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = inputColStat.DistinctCount
	colStat.NullCount = inputColStat.NullCount
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

	// The distinct and null counts should be no larger than the row count.
	for i, n := 0, s.ColStats.Count(); i < n; i++ {
		colStat := s.ColStats.Get(i)
		colStat.DistinctCount = min(colStat.DistinctCount, s.RowCount)
		colStat.NullCount = min(colStat.NullCount, s.RowCount)
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
	unknownDistinctCountRatio = 0.1

	// This is the ratio of null column values to number of rows for nullable
	// columns, which is used in the absence of any real statistics.
	unknownNullCountRatio = 0.01

	// Use a small row count for generator functions; this allows use of lookup
	// join in cases like using json_array_elements with a small constant array.
	unknownGeneratorRowCount = 10

	// Since the generator row count is so small, we need a larger distinct count
	// ratio for generator functions.
	unknownGeneratorDistinctCountRatio = 0.7
)

// countJSONPaths returns the number of JSON paths in the specified
// FiltersItem. Used in the calculation of unapplied conjuncts in a JSON
// Contains operator. Returns 0 if paths could not be counted for any
// reason, such as malformed JSON.
func countJSONPaths(conjunct *FiltersItem) int {
	rhs := conjunct.Condition.Child(1)
	if !CanExtractConstDatum(rhs) {
		return 0
	}
	rightDatum := ExtractConstDatum(rhs)
	if rightDatum == tree.DNull {
		return 0
	}
	rd, ok := rightDatum.(*tree.DJSON)
	if !ok {
		return 0
	}

	// TODO(itsbilal): Replace this with a method that only counts paths
	// instead of generating a slice for all of them.
	paths, err := json.AllPaths(rd.JSON)
	if err != nil {
		return 0
	}
	return len(paths)
}

// applyFilter uses constraints to update the distinct counts for the
// constrained columns in the filter. The changes in the distinct counts will be
// used later to determine the selectivity of the filter.
//
// Some filters can be translated directly to distinct counts using the
// constraint set. For example, the tight constraint `/a: [/1 - /1]` indicates
// that column `a` has exactly one distinct value.  Other filters may not have
// a tight constraint, or the constraint may be an open inequality such as
// `/a: [/0 - ]`. In this case, it is not possible to determine the distinct
// count for column `a`, so instead we increment numUnappliedConjuncts,
// which will be used later for selectivity calculation. See comments in
// applyConstraintSet and updateDistinctCountsFromConstraint for more details
// about how distinct counts are calculated from constraints.
//
// Equalities between two variables (e.g., var1=var2) are handled separately.
// See applyEquivalencies and selectivityFromEquivalencies for details.
//
func (sb *statisticsBuilder) applyFilter(
	filters FiltersExpr, e RelExpr, relProps *props.Relational,
) (numUnappliedConjuncts float64, constrainedCols opt.ColSet) {
	applyConjunct := func(conjunct *FiltersItem) {
		if isEqualityWithTwoVars(conjunct.Condition) {
			// We'll handle equalities later.
			return
		}

		// Special case: The current conjunct is a JSON Contains operator.
		// If so, count every path to a leaf node in the RHS as a separate
		// conjunct. If for whatever reason we can't get to the JSON datum
		// or enumerate its paths, count the whole operator as one conjunct.
		if conjunct.Condition.Op() == opt.ContainsOp {
			numPaths := countJSONPaths(conjunct)
			if numPaths == 0 {
				numUnappliedConjuncts++
			} else {
				// Multiply the number of paths by 2 to mimic the logic in
				// numConjunctsInConstraint, for constraints like
				// /1: [/'{"a":"b"}' - /'{"a":"b"}'] . That function counts
				// this as 2 conjuncts, and to keep row counts as consistent
				// as possible between competing filtered selects and
				// constrained scans, we apply the same logic here.
				numUnappliedConjuncts += 2 * float64(numPaths)
			}
			return
		}

		// Update constrainedCols after the above check for isEqualityWithTwoVars.
		// We will use constrainedCols later to determine which columns to use for
		// selectivity calculation in selectivityFromDistinctCounts, and we want to
		// make sure that we don't include columns that were only present in
		// equality conjuncts such as var1=var2. The selectivity of these conjuncts
		// will be accounted for in selectivityFromEquivalencies.
		scalarProps := conjunct.ScalarProps(e.Memo())
		constrainedCols.UnionWith(scalarProps.OuterCols)
		if scalarProps.Constraints != nil {
			n := sb.applyConstraintSet(scalarProps.Constraints, e, relProps)
			if !scalarProps.TightConstraints && n < 1 {
				numUnappliedConjuncts++
			} else {
				numUnappliedConjuncts += n
			}
		} else {
			numUnappliedConjuncts++
		}
	}

	for i := range filters {
		applyConjunct(&filters[i])
	}

	return numUnappliedConjuncts, constrainedCols
}

func (sb *statisticsBuilder) applyIndexConstraint(
	c *constraint.Constraint, e RelExpr, relProps *props.Relational,
) (numUnappliedConjuncts float64) {
	// If unconstrained, then no constraint could be derived from the expression,
	// so fall back to estimate.
	// If a contradiction, then optimizations must not be enabled (say for
	// testing), or else this would have been reduced.
	if c.IsUnconstrained() || c.IsContradiction() {
		return 0 /* numUnappliedConjuncts */
	}

	applied := sb.updateDistinctCountsFromConstraint(c, e, relProps)
	for i, n := applied, c.ConstrainedColumns(sb.evalCtx); i < n; i++ {
		// Unlike the constraints found in Select and Join filters, an index
		// constraint may represent multiple conjuncts. Therefore, we need to
		// calculate the number of unapplied conjuncts for each constrained column.
		numUnappliedConjuncts += sb.numConjunctsInConstraint(c, i)
	}

	return numUnappliedConjuncts
}

func (sb *statisticsBuilder) applyConstraintSet(
	cs *constraint.Set, e RelExpr, relProps *props.Relational,
) (numUnappliedConjuncts float64) {
	// If unconstrained, then no constraint could be derived from the expression,
	// so fall back to estimate.
	// If a contradiction, then optimizations must not be enabled (say for
	// testing), or else this would have been reduced.
	if cs.IsUnconstrained() || cs == constraint.Contradiction {
		return 0 /* numUnappliedConjuncts */
	}

	numUnappliedConjuncts = 0
	for i := 0; i < cs.Length(); i++ {
		applied := sb.updateDistinctCountsFromConstraint(cs.Constraint(i), e, relProps)
		if applied == 0 {
			// If a constraint cannot be applied, it may represent an
			// inequality like x < 1. As a result, distinctCounts does not fully
			// represent the selectivity of the constraint set.
			// We return an estimate of the number of unapplied conjuncts to the
			// caller function to be used for selectivity calculation.
			numUnappliedConjuncts += sb.numConjunctsInConstraint(cs.Constraint(i), 0 /* nth */)
		}
	}

	return numUnappliedConjuncts
}

// updateNullCountsFromProps zeroes null counts for columns that cannot
// have nulls in them, usually due to a column property or an application.
// of a null-excluding filter. The actual determination of non-nullable
// columns is done in the logical props builder.
//
// For example, consider the following constraint sets:
//   /a/b/c: [/1 - /1/2/3] [/1/2/5 - /1/2/8]
//   /c: [/6 - /6]
//
// The first constraint set filters nulls out of column a, and the
// second constraint set filters nulls out of column c.
//
// This function should be called before selectivityFromNullCounts but *after*
// all other calls to ApplySelectivity (such as the ones that calculate and
// apply selectivities from distinct counts, equivalences, and unapplied
// constraints).
//
func (sb *statisticsBuilder) updateNullCountsFromProps(
	e RelExpr, relProps *props.Relational, inputRowCount float64,
) {
	s := &relProps.Stats
	relProps.NotNullCols.ForEach(func(col int) {
		colSet := util.MakeFastIntSet(col)
		colStat, ok := s.ColStats.Lookup(colSet)
		if !ok {
			colStat = sb.copyColStat(colSet, s, sb.colStatFromInput(colSet, e))
			if s.Selectivity != 1.0 {
				colStat.ApplySelectivity(s.Selectivity, inputRowCount)
			}
		}
		colStat.NullCount = 0
	})
}

// updateDistinctCountsFromConstraint updates the distinct count for each
// column in a constraint that can be determined to have a finite number of
// possible values. It returns the number of columns for which the distinct
// count could be inferred from the constraint. If the same column appears
// in multiple constraints, the distinct count is the minimum for that column
// across all constraints.
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
	c *constraint.Constraint, e RelExpr, relProps *props.Relational,
) (applied int) {
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
		sb.ensureColStat(util.MakeFastIntSet(int(colID)), distinctCount, e, relProps)
		applied = col + 1
	}

	return applied
}

func (sb *statisticsBuilder) applyEquivalencies(
	equivReps opt.ColSet, filterFD *props.FuncDepSet, e RelExpr, relProps *props.Relational,
) {
	equivReps.ForEach(func(i int) {
		equivGroup := filterFD.ComputeEquivGroup(opt.ColumnID(i))
		sb.updateDistinctNullCountsFromEquivalency(equivGroup, e, relProps)
	})
}

func (sb *statisticsBuilder) updateDistinctNullCountsFromEquivalency(
	equivGroup opt.ColSet, e RelExpr, relProps *props.Relational,
) {
	s := &relProps.Stats

	// Find the minimum distinct and null counts for all columns in this equivalency
	// group.
	minDistinctCount := s.RowCount
	minNullCount := s.RowCount
	equivGroup.ForEach(func(i int) {
		colSet := util.MakeFastIntSet(i)
		colStat, ok := s.ColStats.Lookup(colSet)
		if !ok {
			colStat = sb.copyColStat(colSet, s, sb.colStatFromInput(colSet, e))
		}
		if colStat.DistinctCount < minDistinctCount {
			minDistinctCount = colStat.DistinctCount
		}
		if colStat.NullCount < minNullCount {
			minNullCount = colStat.NullCount
		}
	})

	// Set the distinct and null counts to the minimum for all columns in this
	// equivalency group.
	equivGroup.ForEach(func(i int) {
		colStat, _ := s.ColStats.Lookup(util.MakeFastIntSet(i))
		colStat.DistinctCount = minDistinctCount
		colStat.NullCount = minNullCount
	})
}

// selectivityFromDistinctCounts calculates the selectivity of a filter by
// taking the product of selectivities of each constrained column. In the
// general case, this can be represented by the formula:
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
	cols opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity float64) {
	selectivity = 1.0
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colStat, ok := s.ColStats.Lookup(util.MakeFastIntSet(col))
		if !ok {
			continue
		}

		inputStat := sb.colStatFromInput(colStat.Cols, e)
		newDistinct := colStat.DistinctCount
		oldDistinct := inputStat.DistinctCount

		if oldDistinct != 0 && newDistinct < oldDistinct {
			selectivity *= newDistinct / oldDistinct
		}
	}

	return selectivity
}

// selectivityFromNullCounts calculates the selectivity of a filter from the number
// of null values removed. This can be represented by this formula:
//
//               ┬-┬ ⎛     old null(i) - new null(i)⎞
// selectivity = │ │ ⎜ 1 - ------------------------ ⎟
//               ┴ ┴ ⎝     old row count(i)         ⎠
//              i in
//            {constrained
//               columns}
//
//
// This selectivity will be used later to update the row count. Note that
// this formula doesn't return the right selectivity for joins; the
// function joinSelectivityFromNullCounts is for that purpose.
//
func (sb *statisticsBuilder) selectivityFromNullCounts(
	cols opt.ColSet, e RelExpr, s *props.Statistics, rowCount float64,
) (selectivity float64) {
	selectivity = 1.0
	// Short circuit if no rows.
	if rowCount <= 0 {
		return selectivity
	}
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colStat, ok := s.ColStats.Lookup(util.MakeFastIntSet(col))
		if !ok {
			continue
		}

		inputStat := sb.colStatFromInput(colStat.Cols, e)
		if inputStat.NullCount > rowCount {
			panic("rowCount passed in was too small")
		}
		if colStat.NullCount < inputStat.NullCount {
			selectivity *= (1 - (inputStat.NullCount-colStat.NullCount)/rowCount)
		}
	}

	return selectivity
}

// joinSelectivityFromNullCounts calculates the selectivity of a join from the
// number of null values removed. The formula used here is similar to the
// selectivityFromNullCounts one above, except with old null and new null
// being calculated from the sides of the join.
//
// This selectivity will be used later to update the row count.
//
func (sb *statisticsBuilder) joinSelectivityFromNullCounts(
	cols opt.ColSet,
	e RelExpr,
	s *props.Statistics,
	inputRowCount float64,
	leftCols opt.ColSet,
	leftRowCount float64,
	rightCols opt.ColSet,
	rightRowCount float64,
) (selectivity float64) {
	selectivity = 1.0
	// Short circuit if no rows.
	if inputRowCount <= 0 {
		return selectivity
	}
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colSet := util.MakeFastIntSet(col)
		colStat, ok := s.ColStats.Lookup(colSet)
		if !ok {
			continue
		}

		var leftNullCount, rightNullCount float64
		if leftCols.Contains(col) {
			leftNullCount = sb.colStatFromJoinLeft(util.MakeFastIntSet(col), e).NullCount
		}
		if rightCols.Contains(col) {
			rightNullCount = sb.colStatFromJoinLeft(util.MakeFastIntSet(col), e).NullCount
		}
		// One of leftNullCount or rightNullCount will be 0, so no need to account for
		// collisions in this cross join count.
		crossJoinNullCount := leftNullCount*rightRowCount + rightNullCount*leftRowCount

		if crossJoinNullCount > inputRowCount {
			panic("row count passed in was too small")
		}
		// We make the assumption that colStat.NullCount is either 0 or equal
		// to / greater than crossJoinNullCount. In the zero case, account
		// for this null elimination in the returned selectivity.
		if colStat.NullCount == 0 {
			selectivity *= (1 - crossJoinNullCount/inputRowCount)
		}
	}

	return selectivity
}

// selectivityFromEquivalencies determines the selectivity of equality
// constraints. It must be called before applyEquivalencies.
func (sb *statisticsBuilder) selectivityFromEquivalencies(
	equivReps opt.ColSet, filterFD *props.FuncDepSet, e RelExpr, s *props.Statistics,
) (selectivity float64) {
	selectivity = 1.0
	equivReps.ForEach(func(i int) {
		equivGroup := filterFD.ComputeEquivGroup(opt.ColumnID(i))
		selectivity *= sb.selectivityFromEquivalency(equivGroup, e, s)
	})
	return selectivity
}

func (sb *statisticsBuilder) selectivityFromEquivalency(
	equivGroup opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity float64) {
	// Find the maximum input distinct count for all columns in this equivalency
	// group.
	maxDistinctCount := float64(0)
	equivGroup.ForEach(func(i int) {
		// If any of the distinct counts were updated by the filter, we want to use
		// the updated value.
		colSet := util.MakeFastIntSet(i)
		colStat, ok := s.ColStats.Lookup(colSet)
		if !ok {
			colStat = sb.colStatFromInput(colSet, e)
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

func (sb *statisticsBuilder) selectivityFromUnappliedConjuncts(
	numUnappliedConjuncts float64,
) (selectivity float64) {
	return math.Pow(unknownFilterSelectivity, numUnappliedConjuncts)
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
		colStat, ok := s.ColStats.Lookup(util.MakeFastIntSet(i))
		if !ok || colStat.DistinctCount != 1 {
			// The reduced columns are not all constant, so return the original
			// column set.
			return cols
		}
	}

	return reducedCols
}

// tryReduceJoinCols is used to determine which columns to use for join ON
// condition selectivity calculation. See tryReduceCols.
func (sb *statisticsBuilder) tryReduceJoinCols(
	cols opt.ColSet,
	s *props.Statistics,
	leftCols, rightCols opt.ColSet,
	leftFD, rightFD *props.FuncDepSet,
) opt.ColSet {
	leftCols = sb.tryReduceCols(leftCols.Intersection(cols), s, leftFD)
	rightCols = sb.tryReduceCols(rightCols.Intersection(cols), s, rightFD)
	return leftCols.Union(rightCols)
}

func isEqualityWithTwoVars(cond opt.ScalarExpr) bool {
	if eq, ok := cond.(*EqExpr); ok {
		return eq.Left.Op() == opt.VariableOp && eq.Right.Op() == opt.VariableOp
	}
	return false
}

// numConjunctsInConstraint returns a rough estimate of the number of conjuncts
// used to build the given constraint for the column at position nth.
func (sb *statisticsBuilder) numConjunctsInConstraint(
	c *constraint.Constraint, nth int,
) (numConjuncts float64) {
	if c.Spans.Count() == 0 {
		return 0 /* numConjuncts */
	}

	numConjuncts = math.MaxFloat64
	for i := 0; i < c.Spans.Count(); i++ {
		span := c.Spans.Get(i)
		numSpanConjuncts := float64(0)
		if span.StartKey().Length() > nth {
			// Cases of NULL in a constraint should be ignored. For example,
			// without knowledge of the data distribution, /a: (/NULL - /10] should
			// have the same estimated selectivity as /a: [/10 - ]. Selectivity
			// of NULL constraints is handled in selectivityFromNullCounts.
			if c.Columns.Get(nth).Descending() ||
				span.StartKey().Value(nth) != tree.DNull {
				numSpanConjuncts++
			}
		}
		if span.EndKey().Length() > nth {
			// Ignore cases of NULL in constraints. (see above comment).
			if !c.Columns.Get(nth).Descending() ||
				span.EndKey().Value(nth) != tree.DNull {
				numSpanConjuncts++
			}
		}
		if numSpanConjuncts < numConjuncts {
			numConjuncts = numSpanConjuncts
		}
	}

	return numConjuncts
}

// RequestColStat causes a column statistic to be calculated on the relational
// expression. This is used for testing.
func RequestColStat(evalCtx *tree.EvalContext, e RelExpr, cols opt.ColSet) {
	var sb statisticsBuilder
	sb.init(evalCtx, e.Memo().Metadata())
	sb.colStat(cols, e)
}
