// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"math"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*sb = statisticsBuilder{
		evalCtx: evalCtx,
		md:      md,
	}
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

// availabilityFromInput determines the availability of the underlying table
// statistics from the children of the expression.
func (sb *statisticsBuilder) availabilityFromInput(e RelExpr) bool {
	switch t := e.(type) {
	case *ScanExpr:
		return sb.makeTableStatistics(t.Table).Available

	case *LookupJoinExpr:
		ensureLookupJoinInputProps(t, sb)
		return t.lookupProps.Stats.Available && t.Input.Relational().Stats.Available

	case *InvertedJoinExpr:
		ensureInvertedJoinInputProps(t, sb)
		return t.lookupProps.Stats.Available && t.Input.Relational().Stats.Available

	case *ZigzagJoinExpr:
		ensureZigzagJoinInputProps(t, sb)
		return t.leftProps.Stats.Available
	}

	available := true
	for i, n := 0, e.ChildCount(); i < n; i++ {
		if child, ok := e.Child(i).(RelExpr); ok {
			available = available && child.Relational().Stats.Available
		}
	}
	return available
}

// colStatFromInput retrieves a column statistic from the input(s) of a Scan,
// Select, or Join. The input to the Scan is the "raw" table.
//
// colStatFromInput also retrieves a pointer to the full statistics from the
// relevant input.
func (sb *statisticsBuilder) colStatFromInput(
	colSet opt.ColSet, e RelExpr,
) (*props.ColumnStatistic, *props.Statistics) {
	var lookupJoin *LookupJoinExpr
	var invertedJoin *InvertedJoinExpr
	var zigzagJoin *ZigzagJoinExpr

	switch t := e.(type) {
	case *ScanExpr:
		return sb.colStatTable(t.Table, colSet), sb.makeTableStatistics(t.Table)

	case *SelectExpr, *InvertedFilterExpr:
		return sb.colStatFromChild(colSet, t, 0 /* childIdx */), sb.statsFromChild(e, 0 /* childIdx */)

	case *LookupJoinExpr:
		lookupJoin = t
		ensureLookupJoinInputProps(lookupJoin, sb)

	case *InvertedJoinExpr:
		invertedJoin = t
		ensureInvertedJoinInputProps(invertedJoin, sb)

	case *ZigzagJoinExpr:
		zigzagJoin = t
		ensureZigzagJoinInputProps(zigzagJoin, sb)
	}

	if lookupJoin != nil || invertedJoin != nil || zigzagJoin != nil ||
		opt.IsJoinOp(e) || e.Op() == opt.MergeJoinOp {
		var leftProps *props.Relational
		if zigzagJoin != nil {
			leftProps = &zigzagJoin.leftProps
		} else {
			leftProps = e.Child(0).(RelExpr).Relational()
		}

		intersectsLeft := leftProps.OutputCols.Intersects(colSet)
		var intersectsRight bool
		if lookupJoin != nil {
			intersectsRight = lookupJoin.lookupProps.OutputCols.Intersects(colSet)
		} else if invertedJoin != nil {
			intersectsRight = invertedJoin.lookupProps.OutputCols.Intersects(colSet)
		} else if zigzagJoin != nil {
			intersectsRight = zigzagJoin.rightProps.OutputCols.Intersects(colSet)
		} else {
			intersectsRight = e.Child(1).(RelExpr).Relational().OutputCols.Intersects(colSet)
		}

		// It's possible that colSet intersects both left and right if we have a
		// lookup join that was converted from an index join, so check the left
		// side first.
		if intersectsLeft {
			if zigzagJoin != nil {
				return sb.colStatTable(zigzagJoin.LeftTable, colSet),
					sb.makeTableStatistics(zigzagJoin.LeftTable)
			}
			return sb.colStatFromChild(colSet, e, 0 /* childIdx */),
				sb.statsFromChild(e, 0 /* childIdx */)
		}
		if intersectsRight {
			if lookupJoin != nil {
				return sb.colStatTable(lookupJoin.Table, colSet),
					sb.makeTableStatistics(lookupJoin.Table)
			}
			if invertedJoin != nil {
				// TODO(rytaft): use inverted index stats when available.
				return sb.colStatTable(invertedJoin.Table, colSet),
					sb.makeTableStatistics(invertedJoin.Table)
			}
			if zigzagJoin != nil {
				return sb.colStatTable(zigzagJoin.RightTable, colSet),
					sb.makeTableStatistics(zigzagJoin.RightTable)
			}
			return sb.colStatFromChild(colSet, e, 1 /* childIdx */),
				sb.statsFromChild(e, 1 /* childIdx */)
		}
		// All columns in colSet are outer columns; therefore, we can treat them
		// as a constant. Return table stats from the left side.
		if zigzagJoin != nil {
			return &props.ColumnStatistic{Cols: colSet, DistinctCount: 1},
				sb.makeTableStatistics(zigzagJoin.LeftTable)
		}
		return &props.ColumnStatistic{Cols: colSet, DistinctCount: 1}, sb.statsFromChild(e, 0 /* childIdx */)
	}

	panic(errors.AssertionFailedf("unsupported operator type %s", log.Safe(e.Op())))
}

// colStat gets a column statistic for the given set of columns if it exists.
// If the column statistic is not available in the current expression, colStat
// recursively tries to find it in the children of the expression, lazily
// populating s.ColStats with the statistic as it gets passed up the expression
// tree.
func (sb *statisticsBuilder) colStat(colSet opt.ColSet, e RelExpr) *props.ColumnStatistic {
	if colSet.Empty() {
		panic(errors.AssertionFailedf("column statistics cannot be determined for empty column set"))
	}

	// Check if the requested column statistic is already cached.
	if stat, ok := e.Relational().Stats.ColStats.Lookup(colSet); ok {
		return stat
	}

	// We only calculate statistics on the normalized expression in a memo group.
	e = e.FirstExpr()

	// The statistic was not found in the cache, so calculate it based on the
	// type of expression.
	switch e.Op() {
	case opt.ScanOp:
		return sb.colStatScan(colSet, e.(*ScanExpr))

	case opt.SelectOp:
		return sb.colStatSelect(colSet, e.(*SelectExpr))

	case opt.ProjectOp:
		return sb.colStatProject(colSet, e.(*ProjectExpr))

	case opt.InvertedFilterOp:
		return sb.colStatInvertedFilter(colSet, e.(*InvertedFilterExpr))

	case opt.ValuesOp:
		return sb.colStatValues(colSet, e.(*ValuesExpr))

	case opt.InnerJoinOp, opt.LeftJoinOp, opt.RightJoinOp, opt.FullJoinOp,
		opt.SemiJoinOp, opt.AntiJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinApplyOp,
		opt.SemiJoinApplyOp, opt.AntiJoinApplyOp, opt.MergeJoinOp, opt.LookupJoinOp,
		opt.InvertedJoinOp, opt.ZigzagJoinOp:
		return sb.colStatJoin(colSet, e)

	case opt.IndexJoinOp:
		return sb.colStatIndexJoin(colSet, e.(*IndexJoinExpr))

	case opt.UnionOp, opt.IntersectOp, opt.ExceptOp,
		opt.UnionAllOp, opt.IntersectAllOp, opt.ExceptAllOp:
		return sb.colStatSetNode(colSet, e)

	case opt.GroupByOp, opt.ScalarGroupByOp, opt.DistinctOnOp, opt.EnsureDistinctOnOp,
		opt.UpsertDistinctOnOp, opt.EnsureUpsertDistinctOnOp:
		return sb.colStatGroupBy(colSet, e)

	case opt.LimitOp:
		return sb.colStatLimit(colSet, e.(*LimitExpr))

	case opt.OffsetOp:
		return sb.colStatOffset(colSet, e.(*OffsetExpr))

	case opt.Max1RowOp:
		return sb.colStatMax1Row(colSet, e.(*Max1RowExpr))

	case opt.OrdinalityOp:
		return sb.colStatOrdinality(colSet, e.(*OrdinalityExpr))

	case opt.WindowOp:
		return sb.colStatWindow(colSet, e.(*WindowExpr))

	case opt.ProjectSetOp:
		return sb.colStatProjectSet(colSet, e.(*ProjectSetExpr))

	case opt.WithScanOp:
		return sb.colStatWithScan(colSet, e.(*WithScanExpr))

	case opt.InsertOp, opt.UpdateOp, opt.UpsertOp, opt.DeleteOp:
		return sb.colStatMutation(colSet, e)

	case opt.SequenceSelectOp:
		return sb.colStatSequenceSelect(colSet, e.(*SequenceSelectExpr))

	case opt.ExplainOp, opt.ShowTraceForSessionOp,
		opt.OpaqueRelOp, opt.OpaqueMutationOp, opt.OpaqueDDLOp, opt.RecursiveCTEOp:
		return sb.colStatUnknown(colSet, e.Relational())

	case opt.WithOp:
		return sb.colStat(colSet, e.Child(1).(RelExpr))

	case opt.FakeRelOp:
		rel := e.Relational()
		return sb.colStatLeaf(colSet, &rel.Stats, &rel.FuncDeps, rel.NotNullCols)
	}

	panic(errors.AssertionFailedf("unrecognized relational expression type: %v", log.Safe(e.Op())))
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
	// count. The null count is 0 if any of these columns are not nullable,
	// otherwise copy the null count from the nullable columns in colSet.
	if fd.ColsAreLaxKey(colSet) {
		if colSet.Intersects(notNullCols) {
			colStat.NullCount = 0
		} else {
			nullableCols := colSet.Difference(notNullCols)
			if nullableCols.Equals(colSet) {
				// No column statistics on this colSet - use the unknown
				// null count ratio.
				colStat.NullCount = s.RowCount * unknownNullCountRatio
			} else {
				colStatLeaf := sb.colStatLeaf(nullableCols, s, fd, notNullCols)
				// Fetch the colStat again since it may now have a different address.
				colStat, _ = s.ColStats.Lookup(colSet)
				colStat.NullCount = colStatLeaf.NullCount
			}
		}
		// Only one of the null values counts towards the distinct count.
		colStat.DistinctCount = s.RowCount - max(colStat.NullCount-1, 0)
		return colStat
	}

	if colSet.Len() == 1 {
		col, _ := colSet.Next(0)
		colStat.DistinctCount = unknownDistinctCountRatio * s.RowCount
		colStat.NullCount = unknownNullCountRatio * s.RowCount
		if notNullCols.Contains(col) {
			colStat.NullCount = 0
		}
		// Some types (e.g., bool and enum) have a known maximum number of distinct
		// values.
		maxDistinct, ok := distinctCountFromType(sb.md, sb.md.ColumnMeta(col).Type)
		if ok {
			if colStat.NullCount > 0 {
				// Add one for the null value.
				maxDistinct++
			}
			colStat.DistinctCount = min(colStat.DistinctCount, float64(maxDistinct))
		}
	} else {
		distinctCount := 1.0
		nullCount := s.RowCount
		colSet.ForEach(func(i opt.ColumnID) {
			colStatLeaf := sb.colStatLeaf(opt.MakeColSet(i), s, fd, notNullCols)
			distinctCount *= colStatLeaf.DistinctCount
			// Multiply by the expected chance of collisions with nulls already
			// collected.
			nullCount *= colStatLeaf.NullCount / s.RowCount
		})
		// Fetch the colStat again since it may now have a different address.
		colStat, _ = s.ColStats.Lookup(colSet)
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

	tab := sb.md.Table(tabID)

	// Create a mapping from table column ordinals to inverted index virtual column
	// ordinals. This allows us to do a fast lookup while iterating over all stats
	// from a statistic's column to any associated virtual columns. We don't merely
	// loop over all table columns looking for virtual columns here because other
	// things may one day use virtual columns and we want these to be explicitly
	// tied to inverted indexes.
	invIndexVirtualCols := make(map[int][]int)
	for indexI, indexN := 0, tab.IndexCount(); indexI < indexN; indexI++ {
		index := tab.Index(indexI)
		if !index.IsInverted() {
			continue
		}
		// The inverted column of an inverted index is virtual.
		col := index.VirtualInvertedColumn()
		srcOrd := col.InvertedSourceColumnOrdinal()
		invIndexVirtualCols[srcOrd] = append(invIndexVirtualCols[srcOrd], col.Ordinal())
	}

	// Make now and annotate the metadata table with it for next time.
	stats = &props.Statistics{}
	if tab.StatisticCount() == 0 {
		// No statistics.
		stats.Available = false
		stats.RowCount = unknownRowCount
	} else {
		// Get the RowCount from the most recent statistic. Stats are ordered
		// with most recent first.
		stats.Available = true
		stats.RowCount = float64(tab.Statistic(0).RowCount())

		// Make sure the row count is at least 1. The stats may be stale, and we
		// can end up with weird and inefficient plans if we estimate 0 rows.
		stats.RowCount = max(stats.RowCount, 1)

		// Add all the column statistics, using the most recent statistic for each
		// column set. Stats are ordered with most recent first.
		for i := 0; i < tab.StatisticCount(); i++ {
			stat := tab.Statistic(i)
			if stat.ColumnCount() > 1 && !sb.evalCtx.SessionData.OptimizerUseMultiColStats {
				continue
			}

			var cols opt.ColSet
			for i := 0; i < stat.ColumnCount(); i++ {
				cols.Add(tabID.ColumnID(stat.ColumnOrdinal(i)))
			}

			if colStat, ok := stats.ColStats.Add(cols); ok {
				colStat.DistinctCount = float64(stat.DistinctCount())
				colStat.NullCount = float64(stat.NullCount())
				if cols.Len() == 1 && stat.Histogram() != nil &&
					sb.evalCtx.SessionData.OptimizerUseHistograms {
					col, _ := cols.Next(0)

					// If this column is invertable, the histogram describes the inverted index
					// entries, and we need to create a new stat for it, and not apply a histogram
					// to the source column.
					virtualColOrds := invIndexVirtualCols[stat.ColumnOrdinal(0)]
					if len(virtualColOrds) == 0 {
						colStat.Histogram = &props.Histogram{}
						colStat.Histogram.Init(sb.evalCtx, col, stat.Histogram())
					} else {
						for _, virtualColOrd := range virtualColOrds {
							invCol := tabID.ColumnID(virtualColOrd)
							invCols := opt.MakeColSet(invCol)
							if invColStat, ok := stats.ColStats.Add(invCols); ok {
								invColStat.Histogram = &props.Histogram{}
								invColStat.Histogram.Init(sb.evalCtx, invCol, stat.Histogram())
								// Set inverted entry counts from the histogram.
								invColStat.DistinctCount = invColStat.Histogram.DistinctValuesCount()
								// Inverted indexes don't have nulls.
								invColStat.NullCount = 0
							}
						}
					}
				}

				// Fetch the colStat again since it may now have a different address due
				// to calling stats.ColStats.Add() on any inverted column statistics
				// created above.
				colStat, _ = stats.ColStats.Lookup(cols)

				// Make sure the distinct count is at least 1, for the same reason as
				// the row count above.
				colStat.DistinctCount = max(colStat.DistinctCount, 1)

				// Make sure the values are consistent in case some of the column stats
				// were added at different times (and therefore have a different row
				// count).
				sb.finalizeFromRowCountAndDistinctCounts(colStat, stats)
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
	tableFD := MakeTableFuncDep(sb.md, tabID)
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
	s.Available = sb.availabilityFromInput(scan)

	inputStats := sb.makeTableStatistics(scan.Table)
	s.RowCount = inputStats.RowCount
	pred := scan.PartialIndexPredicate(sb.md)

	// If the constraints and pred are nil, then this scan is an unconstrained
	// scan on a non-partial index. The stats of the scan are the same as the
	// underlying table stats.
	if scan.Constraint == nil && scan.InvertedConstraint == nil && pred == nil {
		sb.finalizeFromCardinality(relProps)
		return
	}

	// If the constraints are nil but pred is not, then this scan is an
	// unconstrained scan over a partial index. The selectivity of the partial
	// index predicate expression must be applied to the underlying table stats.
	if scan.Constraint == nil && scan.InvertedConstraint == nil {
		notNullCols := relProps.NotNullCols.Copy()
		// Add any not-null columns from the predicate constraints.
		for i := range pred {
			if c := pred[i].ScalarProps().Constraints; c != nil {
				notNullCols.UnionWith(c.ExtractNotNullCols(sb.evalCtx))
			}
		}
		sb.filterRelExpr(pred, scan, notNullCols, relProps, s, MakeTableFuncDep(sb.md, scan.Table))
		sb.finalizeFromCardinality(relProps)
		return
	}

	// If the constraint is nil or it has a single span, apply the constraint
	// selectivity, the inverted constraint selectivity, and the partial index
	// predicate (if they exist) to the underlying table stats.
	if scan.Constraint == nil || scan.Constraint.Spans.Count() < 2 {
		sb.constrainScan(scan, scan.Constraint, pred, relProps, s)
		sb.finalizeFromCardinality(relProps)
		return
	}

	// Otherwise, there are multiple spans in this constraint. To calculate the
	// row count and selectivity, split the spans up and apply each one
	// separately, then union the result. This is important for correctly
	// handling a constraint such as:
	//
	//   /a/b: [/5 - /5] [/NULL/5 - /NULL/5]
	//
	// If we didn't split the spans, the selectivity of column b would be
	// completely ignored, and the calculated row count would be too high.

	var spanStats, spanStatsUnion props.Statistics
	var c constraint.Constraint
	keyCtx := constraint.KeyContext{EvalCtx: sb.evalCtx, Columns: scan.Constraint.Columns}

	// Make a copy of the stats so we don't modify the original.
	spanStatsUnion.CopyFrom(s)

	// Get the stats for each span and union them together.
	c.InitSingleSpan(&keyCtx, scan.Constraint.Spans.Get(0))
	sb.constrainScan(scan, &c, pred, relProps, &spanStatsUnion)
	for i, n := 1, scan.Constraint.Spans.Count(); i < n; i++ {
		spanStats.CopyFrom(s)
		c.InitSingleSpan(&keyCtx, scan.Constraint.Spans.Get(i))
		sb.constrainScan(scan, &c, pred, relProps, &spanStats)
		spanStatsUnion.UnionWith(&spanStats)
	}

	// Now that we have the correct row count, use the combined spans and the
	// partial index predicate (if it exists) to get the correct column stats.
	sb.constrainScan(scan, scan.Constraint, pred, relProps, s)

	// Copy in the row count and selectivity that were calculated above, if
	// less than the values calculated from the combined spans.
	//
	// We must take the minimum in case we used unknownFilterSelectivity for
	// some of the spans. For example, if no histogram is available for the
	// constraint /1: [/'a' - /'b'] [/'c' - /'d'] [/'e' - /'f'], we would
	// calculate selectivity = 1/9 + 1/9 + 1/9 = 1/3 in spanStatsUnion, which
	// is too high. Instead, we should use the value calculated from the
	// combined spans, which in this case is simply 1/9.
	s.Selectivity = props.MinSelectivity(s.Selectivity, spanStatsUnion.Selectivity)
	s.RowCount = min(s.RowCount, spanStatsUnion.RowCount)

	sb.finalizeFromCardinality(relProps)
}

// constrainScan is called from buildScan to calculate the stats for the scan
// based on the given constraints and partial index predicate.
//
// The constraint and invertedConstraint arguments are both non-nil only when a
// multi-column inverted index is scanned. In this case, the constraint must
// have only single-key spans.
//
// The pred argument is the predicate expression of the partial index that the
// scan operates on. If it is not a partial index, pred should be nil.
func (sb *statisticsBuilder) constrainScan(
	scan *ScanExpr,
	constraint *constraint.Constraint,
	pred FiltersExpr,
	relProps *props.Relational,
	s *props.Statistics,
) {
	var numUnappliedConjuncts float64
	var constrainedCols, histCols opt.ColSet
	idx := sb.md.Table(scan.Table).Index(scan.Index)

	// Calculate distinct counts and histograms for inverted constrained columns
	// -------------------------------------------------------------------------
	if scan.InvertedConstraint != nil {
		// The constrained column is the virtual inverted column in the inverted
		// index. Using scan.Cols here would also include the PK, which we don't
		// want.
		invertedConstrainedCol := scan.Table.ColumnID(idx.VirtualInvertedColumn().Ordinal())
		constrainedCols.Add(invertedConstrainedCol)
		if sb.shouldUseHistogram(relProps) {
			// TODO(mjibson): set distinctCount to something correct. Max is
			// fine for now because ensureColStat takes the minimum of the
			// passed value and colSet's distinct count.
			const distinctCount = math.MaxFloat64
			colSet := opt.MakeColSet(invertedConstrainedCol)
			sb.ensureColStat(colSet, distinctCount, scan, s)

			inputStat, _ := sb.colStatFromInput(colSet, scan)
			if inputHist := inputStat.Histogram; inputHist != nil {
				// If we have a histogram, set the row count to its total,
				// unfiltered count. This is needed because s.RowCount is
				// currently the row count of the table, but should instead
				// reflect the number of inverted index entries.
				s.RowCount = inputHist.ValuesCount()
				if colStat, ok := s.ColStats.Lookup(colSet); ok {
					colStat.Histogram = inputHist.InvertedFilter(scan.InvertedConstraint)
					histCols.Add(invertedConstrainedCol)
					sb.updateDistinctCountFromHistogram(colStat, inputStat.DistinctCount)
				}
			} else {
				// Just assume a single closed span such as ["\xfd", "\xfe").
				// This corresponds to two "conjuncts" as defined in
				// numConjunctsInConstraint.
				numUnappliedConjuncts += 2
			}
		} else {
			// Assume a single closed span.
			numUnappliedConjuncts += 2
		}
	}

	// Calculate distinct counts and histograms for constrained columns
	// ----------------------------------------------------------------
	if constraint != nil {
		constrainedColsLocal, histColsLocal := sb.applyIndexConstraint(constraint, scan, relProps, s)
		constrainedCols.UnionWith(constrainedColsLocal)
		histCols.UnionWith(histColsLocal)
	}

	// Calculate distinct counts and histograms for the partial index predicate
	// ------------------------------------------------------------------------
	if pred != nil {
		predUnappliedConjucts, predConstrainedCols, predHistCols := sb.applyFilters(pred, scan, relProps)
		numUnappliedConjuncts += predUnappliedConjucts
		constrainedCols.UnionWith(predConstrainedCols)
		constrainedCols = sb.tryReduceCols(constrainedCols, s, MakeTableFuncDep(sb.md, scan.Table))
		histCols.UnionWith(predHistCols)
	}

	// Set null counts to 0 for non-nullable columns
	// ---------------------------------------------
	notNullCols := relProps.NotNullCols.Copy()
	if constraint != nil {
		// Add any not-null columns from this constraint.
		notNullCols.UnionWith(constraint.ExtractNotNullCols(sb.evalCtx))
	}
	// Add any not-null columns from the predicate constraints.
	for i := range pred {
		if c := pred[i].ScalarProps().Constraints; c != nil {
			notNullCols.UnionWith(c.ExtractNotNullCols(sb.evalCtx))
		}
	}
	sb.updateNullCountsFromNotNullCols(notNullCols, s)

	// Calculate row count and selectivity
	// -----------------------------------
	s.ApplySelectivity(sb.selectivityFromHistograms(histCols, scan, s))
	s.ApplySelectivity(sb.selectivityFromMultiColDistinctCounts(constrainedCols, scan, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(numUnappliedConjuncts))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(scan, notNullCols, constrainedCols))

	// Adjust the selectivity so we don't double-count the histogram columns.
	s.UnapplySelectivity(sb.selectivityFromSingleColDistinctCounts(histCols, scan, s))
}

func (sb *statisticsBuilder) colStatScan(colSet opt.ColSet, scan *ScanExpr) *props.ColumnStatistic {
	relProps := scan.Relational()
	s := &relProps.Stats

	inputColStat := sb.colStatTable(scan.Table, colSet)
	colStat := sb.copyColStat(colSet, s, inputColStat)

	if sb.shouldUseHistogram(relProps) {
		colStat.Histogram = inputColStat.Histogram
	}

	if s.Selectivity != props.OneSelectivity {
		tableStats := sb.makeTableStatistics(scan.Table)
		colStat.ApplySelectivity(s.Selectivity, tableStats.RowCount)
	}

	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}

	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
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
	s.Available = sb.availabilityFromInput(sel)
	inputStats := &sel.Input.Relational().Stats
	s.RowCount = inputStats.RowCount

	sb.filterRelExpr(sel.Filters, sel, relProps.NotNullCols, relProps, s, &sel.Input.Relational().FuncDeps)

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
	selectivity := props.MakeSelectivity(s.RowCount / inputStats.RowCount)
	colStat.ApplySelectivity(selectivity, inputStats.RowCount)
	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = sb.availabilityFromInput(prj)

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
	nonNullFound := false
	reqSynthCols := colSet.Difference(inputCols)
	if !reqSynthCols.Empty() {
		// Some of the columns in colSet were synthesized or from a higher scope
		// (in the case of a correlated subquery). We assume that the statistics of
		// the synthesized columns are the same as the statistics of their input
		// columns. For example, the distinct count of (x + 2) is the same as the
		// distinct count of x.
		// TODO(rytaft): This assumption breaks down for certain types of
		// expressions, such as (x < y).
		for i := range prj.Projections {
			item := &prj.Projections[i]
			if reqSynthCols.Contains(item.Col) {
				reqInputCols.UnionWith(item.scalar.OuterCols)

				// If the element is not a null constant, account for that
				// when calculating null counts.
				if item.Element.Op() != opt.NullOp {
					nonNullFound = true
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
		if nonNullFound {
			colStat.NullCount = 0
		} else {
			colStat.NullCount = inputColStat.NullCount
		}
	} else {
		// There are no columns in this expression, so it must be a constant.
		colStat.DistinctCount = 1
		if nonNullFound {
			colStat.NullCount = 0
		} else {
			colStat.NullCount = s.RowCount
		}
	}
	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +-----------------+
// | Inverted Filter |
// +-----------------+

func (sb *statisticsBuilder) buildInvertedFilter(
	invFilter *InvertedFilterExpr, relProps *props.Relational,
) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(invFilter)

	// Calculate distinct counts and histograms for constrained columns
	// ----------------------------------------------------------------
	var constrainedCols, histCols opt.ColSet
	// TODO(rytaft, mjibson): estimate distinct count and histogram for inverted
	//  column.

	// Set null counts to 0 for non-nullable columns
	// -------------------------------------------
	sb.updateNullCountsFromNotNullCols(relProps.NotNullCols, s)

	// Calculate selectivity and row count
	// -----------------------------------
	inputStats := &invFilter.Input.Relational().Stats
	s.RowCount = inputStats.RowCount
	s.ApplySelectivity(sb.selectivityFromHistograms(histCols, invFilter, s))
	s.ApplySelectivity(sb.selectivityFromMultiColDistinctCounts(constrainedCols, invFilter, s))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(invFilter, relProps.NotNullCols, constrainedCols))

	// Adjust the selectivity so we don't double-count the histogram columns.
	s.UnapplySelectivity(sb.selectivityFromSingleColDistinctCounts(histCols, invFilter, s))

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatInvertedFilter(
	colSet opt.ColSet, invFilter *InvertedFilterExpr,
) *props.ColumnStatistic {
	relProps := invFilter.Relational()
	s := &relProps.Stats
	inputStats := &invFilter.Input.Relational().Stats
	colStat := sb.copyColStatFromChild(colSet, invFilter, s)

	if s.Selectivity != props.OneSelectivity {
		colStat.ApplySelectivity(s.Selectivity, inputStats.RowCount)
	}

	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = sb.availabilityFromInput(join)

	leftStats := &h.leftProps.Stats
	rightStats := &h.rightProps.Stats
	leftCols := h.leftProps.OutputCols.Copy()
	rightCols := h.rightProps.OutputCols.Copy()
	equivReps := h.filtersFD.EquivReps()

	// Shortcut if there are no ON conditions. Note that for lookup join, there
	// are implicit equality conditions on KeyCols.
	if h.filterIsTrue {
		s.RowCount = leftStats.RowCount * rightStats.RowCount
		s.Selectivity = props.OneSelectivity
		switch h.joinType {
		case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		case opt.LeftJoinOp, opt.LeftJoinApplyOp:
			// All rows from left side should be in the result.
			s.RowCount = max(s.RowCount, leftStats.RowCount)

		case opt.RightJoinOp:
			// All rows from right side should be in the result.
			s.RowCount = max(s.RowCount, rightStats.RowCount)

		case opt.FullJoinOp:
			// All rows from both sides should be in the result.
			s.RowCount = max(s.RowCount, leftStats.RowCount)
			s.RowCount = max(s.RowCount, rightStats.RowCount)

		case opt.SemiJoinOp, opt.SemiJoinApplyOp:
			s.RowCount = leftStats.RowCount

		case opt.AntiJoinOp, opt.AntiJoinApplyOp:
			// Don't set the row count to 0 since we can't guarantee that the
			// cardinality is 0.
			s.RowCount = epsilon
			s.Selectivity = props.MakeSelectivity(epsilon)
		}
		return
	}

	// Shortcut if the ON condition is false or there is a contradiction.
	if h.filters.IsFalse() {
		s.Selectivity = props.ZeroSelectivity
		switch h.joinType {
		case opt.InnerJoinOp, opt.InnerJoinApplyOp:
			s.RowCount = 0

		case opt.LeftJoinOp, opt.LeftJoinApplyOp:
			// All rows from left side should be in the result.
			s.RowCount = leftStats.RowCount

		case opt.RightJoinOp:
			// All rows from right side should be in the result.
			s.RowCount = rightStats.RowCount

		case opt.FullJoinOp:
			// All rows from both sides should be in the result.
			s.RowCount = leftStats.RowCount + rightStats.RowCount

		case opt.SemiJoinOp, opt.SemiJoinApplyOp:
			s.RowCount = 0

		case opt.AntiJoinOp, opt.AntiJoinApplyOp:
			s.RowCount = leftStats.RowCount
			s.Selectivity = props.OneSelectivity
		}
		return
	}

	// Calculate distinct counts for constrained columns in the ON conditions
	// ----------------------------------------------------------------------
	numUnappliedConjuncts, constrainedCols, histCols := sb.applyFilters(h.filters, join, relProps)

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

	// Set null counts to 0 for non-nullable columns
	// ---------------------------------------------
	sb.updateNullCountsFromNotNullCols(relProps.NotNullCols, s)

	// Calculate selectivity and row count
	// -----------------------------------
	switch h.joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Treat anti join as if it were a semi join for the selectivity
		// calculations. It will be fixed below.
		s.RowCount = leftStats.RowCount
		s.ApplySelectivity(sb.selectivityFromEquivalenciesSemiJoin(
			equivReps, h.leftProps.OutputCols, h.rightProps.OutputCols, &h.filtersFD, join, s,
		))

	default:
		s.RowCount = leftStats.RowCount * rightStats.RowCount
		if h.rightProps.FuncDeps.ColsAreStrictKey(h.selfJoinCols) {
			// This is like an index join, so apply a selectivity that will result
			// in leftStats.RowCount rows.
			if rightStats.RowCount != 0 {
				s.ApplySelectivity(props.MakeSelectivity(1 / rightStats.RowCount))
			}
		} else {
			// Add the self join columns to equivReps so they are included in the
			// calculation for selectivityFromEquivalencies below.
			equivReps.UnionWith(h.selfJoinCols)
		}

		s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, &h.filtersFD, join, s))
	}

	if join.Op() == opt.InvertedJoinOp || hasInvertedJoinCond(h.filters) {
		s.ApplySelectivity(sb.selectivityFromInvertedJoinCondition(join, s))
	}
	s.ApplySelectivity(sb.selectivityFromHistograms(histCols, join, s))
	s.ApplySelectivity(sb.selectivityFromMultiColDistinctCounts(
		constrainedCols.Intersection(leftCols), join, s,
	))
	s.ApplySelectivity(sb.selectivityFromMultiColDistinctCounts(
		constrainedCols.Intersection(rightCols), join, s,
	))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(numUnappliedConjuncts))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(join, relProps.NotNullCols, constrainedCols))

	// Adjust the selectivity so we don't double-count the histogram columns.
	s.UnapplySelectivity(sb.selectivityFromSingleColDistinctCounts(histCols, join, s))

	// Update distinct counts based on equivalencies; this should happen after
	// selectivityFromMultiColDistinctCounts and selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, &h.filtersFD, join, relProps.NotNullCols, s)

	switch h.joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Keep only column stats from the left side.
		s.ColStats.RemoveIntersecting(h.rightProps.OutputCols)
	}

	// The above calculation is for inner joins. Other joins need to remove stats
	// that involve outer columns.
	switch h.joinType {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// Keep only column stats from the right side. The stats from the left side
		// are not valid.
		s.ColStats.RemoveIntersecting(h.leftProps.OutputCols)

	case opt.RightJoinOp:
		// Keep only column stats from the left side. The stats from the right side
		// are not valid.
		s.ColStats.RemoveIntersecting(h.rightProps.OutputCols)

	case opt.FullJoinOp:
		// Do not keep any column stats.
		s.ColStats.Clear()
	}

	// Tweak the row count.
	innerJoinRowCount := s.RowCount
	switch h.joinType {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// All rows from left side should be in the result.
		s.RowCount = max(innerJoinRowCount, leftStats.RowCount)

	case opt.RightJoinOp:
		// All rows from right side should be in the result.
		s.RowCount = max(innerJoinRowCount, rightStats.RowCount)

	case opt.FullJoinOp:
		// All rows from both sides should be in the result.
		// T(A FOJ B) = T(A LOJ B) + T(A ROJ B) - T(A IJ B)
		leftJoinRowCount := max(innerJoinRowCount, leftStats.RowCount)
		rightJoinRowCount := max(innerJoinRowCount, rightStats.RowCount)
		s.RowCount = leftJoinRowCount + rightJoinRowCount - innerJoinRowCount
	}

	// Fix the stats for anti join.
	switch h.joinType {
	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		s.RowCount = max(leftStats.RowCount-s.RowCount, epsilon)
		s.Selectivity = props.MakeSelectivity(1 - s.Selectivity.AsFloat())

		// Converting column stats is error-prone. If any column stats are needed,
		// colStatJoin will use the selectivity calculated above to estimate the
		// column stats from the input.
		s.ColStats.Clear()
	}

	// Loop through all colSets added in this step, and adjust null counts,
	// distinct counts, and histograms.
	for i := 0; i < s.ColStats.Count(); i++ {
		colStat := s.ColStats.Get(i)
		leftSideCols := leftCols.Intersection(colStat.Cols)
		rightSideCols := rightCols.Intersection(colStat.Cols)
		leftNullCount, rightNullCount := sb.leftRightNullCounts(
			join,
			leftSideCols,
			rightSideCols,
			leftStats.RowCount,
			rightStats.RowCount,
		)

		// Update all null counts not zeroed out in updateNullCountsFromNotNullCols
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
		case opt.LeftJoinOp, opt.LeftJoinApplyOp, opt.RightJoinOp, opt.FullJoinOp:
			if !colStat.Cols.Intersects(relProps.NotNullCols) {
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

			// Ensure distinct count is non-zero.
			colStat.DistinctCount = max(colStat.DistinctCount, 1)
		}

		// We don't yet calculate histograms correctly for joins, so remove any
		// histograms that have been created above.
		colStat.Histogram = nil
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatJoin(colSet opt.ColSet, join RelExpr) *props.ColumnStatistic {
	relProps := join.Relational()
	s := &relProps.Stats

	var joinType opt.Operator
	var leftProps, rightProps *props.Relational

	switch j := join.(type) {
	case *LookupJoinExpr:
		joinType = j.JoinType
		leftProps = j.Input.Relational()
		ensureLookupJoinInputProps(j, sb)
		rightProps = &j.lookupProps

	case *InvertedJoinExpr:
		joinType = j.JoinType
		leftProps = j.Input.Relational()
		ensureInvertedJoinInputProps(j, sb)
		rightProps = &j.lookupProps

	case *ZigzagJoinExpr:
		joinType = opt.InnerJoinOp
		ensureZigzagJoinInputProps(j, sb)
		leftProps = &j.leftProps
		rightProps = &j.rightProps

	default:
		joinType = join.Op()
		leftProps = join.Child(0).(RelExpr).Relational()
		rightProps = join.Child(1).(RelExpr).Relational()
	}

	switch joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Column stats come from left side of join.
		colStat := sb.copyColStat(colSet, s, sb.colStatFromJoinLeft(colSet, join))
		colStat.ApplySelectivity(s.Selectivity, leftProps.Stats.RowCount)
		sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
		leftNullCount, rightNullCount := leftProps.Stats.RowCount, rightProps.Stats.RowCount
		if rightCols.Empty() {
			colStat = sb.copyColStat(colSet, s, sb.colStatFromJoinLeft(colSet, join))
			leftNullCount = colStat.NullCount
			switch joinType {
			case opt.InnerJoinOp, opt.InnerJoinApplyOp, opt.RightJoinOp:
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

			case opt.RightJoinOp:
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
		case opt.LeftJoinOp, opt.LeftJoinApplyOp, opt.RightJoinOp, opt.FullJoinOp:
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
				s.Selectivity.AsFloat()*inputRowCount,
			)

			// Ensure distinct count is non-zero.
			colStat.DistinctCount = max(colStat.DistinctCount, 1)
		}

		if colSet.Intersects(relProps.NotNullCols) {
			colStat.NullCount = 0
		}
		sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
		return colStat
	}
}

// leftRightNullCounts returns null counts on the left and right sides of the
// specified join. If either leftCols or rightCols are empty, the corresponding
// side's return value is equal to the number of rows on that side.
func (sb *statisticsBuilder) leftRightNullCounts(
	e RelExpr, leftCols, rightCols opt.ColSet, leftRowCount, rightRowCount float64,
) (leftNullCount, rightNullCount float64) {
	leftNullCount, rightNullCount = leftRowCount, rightRowCount
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
//
// The caller should ensure that if leftCols are empty then leftNullCount
// equals leftRowCount, and if rightCols are empty then rightNullCount equals
// rightRowCount.
func innerJoinNullCount(
	rowCount, leftNullCount, leftRowCount, rightNullCount, rightRowCount float64,
) float64 {
	// Here, f1 and f2 are probabilities of nulls on either side of the join.
	var f1, f2 float64
	if leftRowCount != 0 {
		f1 = leftNullCount / leftRowCount
	}
	if rightRowCount != 0 {
		f2 = rightNullCount / rightRowCount
	}

	// Rough estimate of nulls in the combined result set, assuming independence.
	return rowCount * f1 * f2
}

// adjustNullCountsForOuterJoins modifies the null counts for the specified
// columns to adjust for additional nulls created in this type of outer join.
// It adds an expected number of nulls created by column extension on
// non-matching rows (such as on right cols for left joins and both for full).
//
// The caller should ensure that if leftCols are empty then leftNullCount
// equals leftRowCount, and if rightCols are empty then rightNullCount equals
// rightRowCount.
func (sb *statisticsBuilder) adjustNullCountsForOuterJoins(
	colStat *props.ColumnStatistic,
	joinType opt.Operator,
	leftCols, rightCols opt.ColSet,
	leftNullCount, leftRowCount, rightNullCount, rightRowCount, rowCount, innerJoinRowCount float64,
) {
	// Adjust null counts for non-inner joins, adding nulls created due to column
	// extension - such as right columns for non-matching rows in left joins.
	switch joinType {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		if !rightCols.Empty() {
			colStat.NullCount += (rowCount - innerJoinRowCount) * leftNullCount / leftRowCount
		}

	case opt.RightJoinOp:
		if !leftCols.Empty() {
			colStat.NullCount += (rowCount - innerJoinRowCount) * rightNullCount / rightRowCount
		}

	case opt.FullJoinOp:
		leftJoinRowCount := max(innerJoinRowCount, leftRowCount)
		rightJoinRowCount := max(innerJoinRowCount, rightRowCount)

		if !leftCols.Empty() {
			colStat.NullCount += (rightJoinRowCount - innerJoinRowCount) * rightNullCount / rightRowCount
		}
		if !rightCols.Empty() {
			colStat.NullCount += (leftJoinRowCount - innerJoinRowCount) * leftNullCount / leftRowCount
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
	switch join.Op() {
	case opt.ZigzagJoinOp:
		return sb.colStatTable(join.Private().(*ZigzagJoinPrivate).RightTable, cols)
	case opt.LookupJoinOp:
		lookupPrivate := join.Private().(*LookupJoinPrivate)
		return sb.colStatTable(lookupPrivate.Table, cols)
	case opt.InvertedJoinOp:
		invertedJoinPrivate := join.Private().(*InvertedJoinPrivate)
		return sb.colStatTable(invertedJoinPrivate.Table, cols)
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
	s.Available = sb.availabilityFromInput(indexJoin)

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
	colStat.NullCount = s.RowCount

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
		selectivity := props.MakeSelectivity(inputStats.RowCount / tableStats.RowCount)
		lookupColStat.ApplySelectivity(selectivity, tableStats.RowCount)

		// Multiply the distinct counts in case colStat.DistinctCount is
		// already populated with a statistic from the subset of columns
		// provided by the input index. Multiplying the counts gives a worst-case
		// estimate of the joint distinct count.
		colStat.DistinctCount *= lookupColStat.DistinctCount

		// Assuming null columns are completely independent, calculate
		// the expected value of having nulls in both column sets.
		f1 := lookupColStat.NullCount / inputStats.RowCount
		f2 := colStat.NullCount / inputStats.RowCount
		colStat.NullCount = inputStats.RowCount * f1 * f2
	}

	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = sb.availabilityFromInput(zigzag)

	leftStats := zigzag.leftProps.Stats
	equivReps := h.filtersFD.EquivReps()

	// We assume that we only plan zigzag joins in cases where the result set
	// will have a row count smaller than or equal to the left/right index
	// row counts, and where the left and right sides are indexes on the same
	// table. Their row count should be the same, so use either row count.
	s.RowCount = leftStats.RowCount

	// Calculate distinct counts for constrained columns
	// -------------------------------------------------
	// Note that fixed columns (i.e. columns constrained to constant values)
	// specified in zigzag.FixedVals and zigzag.{Left,Right}FixedCols
	// still have corresponding filters in zigzag.On. So we don't need
	// to iterate through FixedCols here if we are already processing the ON
	// clause.
	// TODO(rytaft): use histogram for zig zag join.
	numUnappliedConjuncts, constrainedCols, _ := sb.applyFilters(zigzag.On, zigzag, relProps)

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
	constrainedCols = sb.tryReduceCols(constrainedCols, s, inputFD)

	// Set null counts to 0 for non-nullable columns
	// ---------------------------------------------
	sb.updateNullCountsFromNotNullCols(relProps.NotNullCols, s)

	// Calculate selectivity and row count
	// -----------------------------------
	s.ApplySelectivity(sb.selectivityFromMultiColDistinctCounts(constrainedCols, zigzag, s))
	s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, &relProps.FuncDeps, zigzag, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(numUnappliedConjuncts))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(zigzag, relProps.NotNullCols, constrainedCols))

	// Update distinct counts based on equivalencies; this should happen after
	// selectivityFromMultiColDistinctCounts and selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, &relProps.FuncDeps, zigzag, relProps.NotNullCols, s)

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
	s.Available = sb.availabilityFromInput(groupNode)

	groupingPrivate := groupNode.Private().(*GroupingPrivate)
	groupingColSet := groupingPrivate.GroupingCols

	if groupingColSet.Empty() {
		if groupNode.Op() == opt.ScalarGroupByOp {
			// ScalarGroupBy always returns exactly one row.
			s.RowCount = 1
		} else {
			// GroupBy with empty grouping columns returns 0 or 1 rows, depending
			// on whether input has rows. If input has < 1 row, use that, as that
			// represents the probability of having 0 vs. 1 rows.
			inputStats := sb.statsFromChild(groupNode, 0 /* childIdx */)
			s.RowCount = min(1, inputStats.RowCount)
		}
	} else {
		inputStats := sb.statsFromChild(groupNode, 0 /* childIdx */)

		if groupingPrivate.ErrorOnDup != "" {
			// If any input group has more than one row, then the distinct operator
			// will raise an error, so in non-error cases it has the same number of
			// rows as its input.
			s.RowCount = inputStats.RowCount
		} else {
			// Estimate the row count based on the distinct count of the grouping
			// columns. Non-scalar GroupBy should never increase the number of rows.
			colStat := sb.copyColStatFromChild(groupingColSet, groupNode, s)
			s.RowCount = min(colStat.DistinctCount, inputStats.RowCount)

			// Update the null counts for the column statistic.
			if groupNode.Op() != opt.UpsertDistinctOnOp {
				// UpsertDistinctOp inherits NullCount from child, since it does not
				// group NULL values. Other group by operators only have 1 possible
				// null value.
				colStat.NullCount = min(1, colStat.NullCount)
			}
			if groupingColSet.Intersects(relProps.NotNullCols) {
				colStat.NullCount = 0
			}
		}
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatGroupBy(
	colSet opt.ColSet, groupNode RelExpr,
) *props.ColumnStatistic {
	relProps := groupNode.Relational()
	s := &relProps.Stats

	groupingPrivate := groupNode.Private().(*GroupingPrivate)
	groupingColSet := groupingPrivate.GroupingCols
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

		if groupingPrivate.ErrorOnDup != "" && colSet.Equals(groupingColSet) {
			// If any input group has more than one row, then the distinct operator
			// will raise an error, so in non-error cases its distinct count is the
			// same as its row count.
			colStat.DistinctCount = s.RowCount
		}
	}

	if groupNode.Op() == opt.UpsertDistinctOnOp || groupNode.Op() == opt.EnsureUpsertDistinctOnOp {
		// UpsertDistinctOnOp and EnsureUpsertDistinctOnOp inherit NullCount from
		// child, since they do not group NULL values.
		colStat.NullCount = inputColStat.NullCount
	} else {
		// For other group by operators, we only have 1 possible null value.
		colStat.NullCount = min(1, inputColStat.NullCount)
	}

	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = sb.availabilityFromInput(setNode)

	leftStats := sb.statsFromChild(setNode, 0 /* childIdx */)
	rightStats := sb.statsFromChild(setNode, 1 /* childIdx */)

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

	leftCols := opt.TranslateColSetStrict(outputCols, setPrivate.OutCols, setPrivate.LeftCols)
	rightCols := opt.TranslateColSetStrict(outputCols, setPrivate.OutCols, setPrivate.RightCols)
	leftColStat := sb.colStatFromChild(leftCols, setNode, 0 /* childIdx */)
	rightColStat := sb.colStatFromChild(rightCols, setNode, 1 /* childIdx */)

	colStat, _ := s.ColStats.Add(outputCols)

	leftNullCount := leftColStat.NullCount
	rightNullCount := rightColStat.NullCount

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
		colStat.NullCount = max(leftNullCount-rightNullCount, 0)
	}

	// Use the actual null counts for bag operations, and normalize them for set
	// operations.
	switch setNode.Op() {
	case opt.UnionOp, opt.IntersectOp:
		colStat.NullCount = min(1, colStat.NullCount)
	case opt.ExceptOp:
		colStat.NullCount = min(1, colStat.NullCount)
		if rightNullCount > 0 {
			colStat.NullCount = 0
		}
	}

	if outputCols.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = sb.availabilityFromInput(values)

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
	// Determine null count by looking at tuples that have only NullOps in them.
	nullCount := 0
	for _, row := range values.Rows {
		// Use the FNV-1a algorithm. See comments for the interner class.
		hash := uint64(offset64)
		hasNonNull := false
		for i, elem := range row.(*TupleExpr).Elems {
			if colSet.Contains(values.Cols[i]) {
				if elem.Op() != opt.NullOp {
					hasNonNull = true
				}
				// Use the pointer value of the scalar expression, since it's already
				// been interned. Therefore, two expressions with the same pointer
				// have the same value.
				ptr := reflect.ValueOf(elem).Pointer()
				hash ^= uint64(ptr)
				hash *= prime64
			}
		}
		if !hasNonNull {
			nullCount++
		}
		distinct[hash] = struct{}{}
	}

	// Update the column statistics.
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = float64(len(distinct))
	colStat.NullCount = float64(nullCount)
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = sb.availabilityFromInput(limit)

	inputStats := &limit.Input.Relational().Stats

	// Copy row count from input.
	s.RowCount = inputStats.RowCount

	// Update row count if limit is a constant and row count is non-zero.
	if cnst, ok := limit.Limit.(*ConstExpr); ok && inputStats.RowCount > 0 {
		hardLimit := *cnst.Value.(*tree.DInt)
		if hardLimit > 0 {
			s.RowCount = min(float64(hardLimit), inputStats.RowCount)
			s.Selectivity = props.MakeSelectivity(s.RowCount / inputStats.RowCount)
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
	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = sb.availabilityFromInput(offset)

	inputStats := &offset.Input.Relational().Stats

	// Copy row count from input.
	s.RowCount = inputStats.RowCount

	// Update row count if offset is a constant and row count is non-zero.
	if cnst, ok := offset.Offset.(*ConstExpr); ok && inputStats.RowCount > 0 {
		hardOffset := *cnst.Value.(*tree.DInt)
		if float64(hardOffset) >= inputStats.RowCount {
			// The correct estimate for row count here is 0, but we don't ever want
			// row count to be zero unless the cardinality is zero. This is because
			// the stats may be stale, and we can end up with weird and inefficient
			// plans if we estimate 0 rows. Use a small number instead.
			s.RowCount = min(1, inputStats.RowCount)
		} else if hardOffset > 0 {
			s.RowCount = inputStats.RowCount - float64(hardOffset)
		}
		s.Selectivity = props.MakeSelectivity(s.RowCount / inputStats.RowCount)
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
	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = true

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
	if colSet.Intersects(max1Row.Relational().NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +------------+
// | Row Number |
// +------------+

func (sb *statisticsBuilder) buildOrdinality(ord *OrdinalityExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(ord)

	inputStats := &ord.Input.Relational().Stats

	s.RowCount = inputStats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatOrdinality(
	colSet opt.ColSet, ord *OrdinalityExpr,
) *props.ColumnStatistic {
	relProps := ord.Relational()
	s := &relProps.Stats

	colStat, _ := s.ColStats.Add(colSet)

	if colSet.Contains(ord.ColID) {
		// The ordinality column is a key, so every row is distinct.
		colStat.DistinctCount = s.RowCount
		colStat.NullCount = 0
	} else {
		inputColStat := sb.colStatFromChild(colSet, ord, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
		colStat.NullCount = inputColStat.NullCount
	}

	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +------------+
// |   Window   |
// +------------+

func (sb *statisticsBuilder) buildWindow(window *WindowExpr, relProps *props.Relational) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(window)

	inputStats := &window.Input.Relational().Stats

	// The row count of a window is equal to the row count of its input.
	s.RowCount = inputStats.RowCount

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatWindow(
	colSet opt.ColSet, window *WindowExpr,
) *props.ColumnStatistic {
	relProps := window.Relational()
	s := &relProps.Stats

	colStat, _ := s.ColStats.Add(colSet)

	var windowCols opt.ColSet
	for _, w := range window.Windows {
		windowCols.Add(w.Col)
	}

	if colSet.Intersects(windowCols) {
		// These can be quite complicated and differ dramatically based on which
		// window function is being computed. For now, just assume row_number and
		// that every row is distinct.
		// TODO(justin): make these accurate and take into consideration the window
		// function being computed.
		colStat.DistinctCount = s.RowCount

		// Just assume that no NULLs are output.
		// TODO(justin): there are window fns for which this is not true, make
		// sure those are handled.
		if colSet.SubsetOf(windowCols) {
			// The generated columns are the only columns being requested.
			colStat.NullCount = 0
		} else {
			// Copy NullCount from child.
			colSetChild := colSet.Difference(windowCols)
			inputColStat := sb.colStatFromChild(colSetChild, window, 0 /* childIdx */)
			colStat.NullCount = inputColStat.NullCount
		}
	} else {
		inputColStat := sb.colStatFromChild(colSet, window, 0 /* childIdx */)
		colStat.DistinctCount = inputColStat.DistinctCount
		colStat.NullCount = inputColStat.NullCount
	}

	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = sb.availabilityFromInput(projectSet)

	// The row count of a zip operation is equal to the maximum row count of its
	// children.
	var zipRowCount float64
	for i := range projectSet.Zip {
		if fn, ok := projectSet.Zip[i].Fn.(*FunctionExpr); ok {
			if fn.Overload.Generator != nil {
				// TODO(rytaft): We may want to estimate the number of rows based on
				// the type of generator function and its parameters.
				zipRowCount = unknownGeneratorRowCount
				break
			}
		}

		// A scalar function generates one row.
		zipRowCount = 1
	}

	// Multiply by the input row count to get the total.
	inputStats := &projectSet.Input.Relational().Stats
	s.RowCount = zipRowCount * inputStats.RowCount

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
	colStat.NullCount = s.RowCount

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
		// Calculate the distinct count and null count for the zip columns
		// after the cross join has been applied.
		zipColsDistinctCount := float64(1)
		zipColsNullCount := s.RowCount
		for i := range projectSet.Zip {
			item := &projectSet.Zip[i]
			if item.Cols.ToSet().Intersects(reqZipCols) {
				if fn, ok := item.Fn.(*FunctionExpr); ok && fn.Overload.Generator != nil {
					// The columns(s) contain a generator function.
					// TODO(rytaft): We may want to determine which generator function the
					// requested columns correspond to, and estimate the distinct count and
					// null count based on the type of generator function and its parameters.
					zipColsDistinctCount *= unknownGeneratorRowCount * unknownGeneratorDistinctCountRatio
					zipColsNullCount *= unknownNullCountRatio
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
					// (Also multiply by the expected chance of collisions with nulls
					// already collected.)
					zipColsNullCount *= (s.RowCount - inputStats.RowCount) / s.RowCount
				}

				if item.ScalarProps().OuterCols.Intersects(inputProps.OutputCols) {
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
		// the expected value of having nulls in both column sets.
		f1 := zipColsNullCount / s.RowCount
		f2 := colStat.NullCount / s.RowCount
		colStat.NullCount = s.RowCount * f1 * f2
	}

	if colSet.Intersects(projectSet.Relational().NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +----------+
// | WithScan |
// +----------+

func (sb *statisticsBuilder) buildWithScan(
	withScan *WithScanExpr, relProps, bindingProps *props.Relational,
) {
	s := &relProps.Stats
	if zeroCardinality := s.Init(relProps); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	s.Available = bindingProps.Stats.Available
	s.RowCount = bindingProps.Stats.RowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatWithScan(
	colSet opt.ColSet, withScan *WithScanExpr,
) *props.ColumnStatistic {
	s := &withScan.Relational().Stats

	boundExpr := sb.md.WithBinding(withScan.With).(RelExpr)

	// Calculate the corresponding col stat in the bound expression and convert
	// the result.
	inColSet := opt.TranslateColSetStrict(colSet, withScan.OutCols, withScan.InCols)
	inColStat := sb.colStat(inColSet, boundExpr)

	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = inColStat.DistinctCount
	colStat.NullCount = inColStat.NullCount
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	s.Available = sb.availabilityFromInput(mutation)

	inputStats := sb.statsFromChild(mutation, 0 /* childIdx */)

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
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +-----------------+
// | Sequence Select |
// +-----------------+

func (sb *statisticsBuilder) buildSequenceSelect(relProps *props.Relational) {
	s := &relProps.Stats
	s.Available = true
	s.RowCount = 1
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatSequenceSelect(
	colSet opt.ColSet, seq *SequenceSelectExpr,
) *props.ColumnStatistic {
	s := &seq.Relational().Stats

	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = 1
	colStat.NullCount = 0
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +---------+
// | Unknown |
// +---------+

func (sb *statisticsBuilder) buildUnknown(relProps *props.Relational) {
	s := &relProps.Stats
	s.Available = false
	s.RowCount = unknownGeneratorRowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatUnknown(
	colSet opt.ColSet, relProps *props.Relational,
) *props.ColumnStatistic {
	s := &relProps.Stats

	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = s.RowCount
	colStat.NullCount = 0
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
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
	colSet opt.ColSet, maxDistinctCount float64, e RelExpr, s *props.Statistics,
) *props.ColumnStatistic {
	colStat, ok := s.ColStats.Lookup(colSet)
	if !ok {
		colStat, _ = sb.colStatFromInput(colSet, e)
		colStat = sb.copyColStat(colSet, s, colStat)
	}

	colStat.DistinctCount = min(colStat.DistinctCount, maxDistinctCount)
	return colStat
}

// copyColStat creates a column statistic and copies the data from an existing
// column statistic. Does not copy the histogram.
func (sb *statisticsBuilder) copyColStat(
	colSet opt.ColSet, s *props.Statistics, inputColStat *props.ColumnStatistic,
) *props.ColumnStatistic {
	if !inputColStat.Cols.SubsetOf(colSet) {
		panic(errors.AssertionFailedf(
			"copyColStat colSet: %v inputColSet: %v\n", log.Safe(colSet), log.Safe(inputColStat.Cols),
		))
	}
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = inputColStat.DistinctCount
	colStat.NullCount = inputColStat.NullCount
	return colStat
}

func (sb *statisticsBuilder) finalizeFromCardinality(relProps *props.Relational) {
	s := &relProps.Stats

	// We don't ever want row count to be zero unless the cardinality is zero.
	// This is because the stats may be stale, and we can end up with weird and
	// inefficient plans if we estimate 0 rows.
	if s.RowCount <= 0 && relProps.Cardinality.Max > 0 {
		panic(errors.AssertionFailedf("estimated row count must be non-zero"))
	}

	// The row count should be between the min and max cardinality.
	if s.RowCount > float64(relProps.Cardinality.Max) && relProps.Cardinality.Max != math.MaxUint32 {
		s.RowCount = float64(relProps.Cardinality.Max)
	} else if s.RowCount < float64(relProps.Cardinality.Min) {
		s.RowCount = float64(relProps.Cardinality.Min)
	}

	for i, n := 0, s.ColStats.Count(); i < n; i++ {
		colStat := s.ColStats.Get(i)
		sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	}
}

func (sb *statisticsBuilder) finalizeFromRowCountAndDistinctCounts(
	colStat *props.ColumnStatistic, s *props.Statistics,
) {
	rowCount := s.RowCount

	// We should always have at least one distinct value if row count > 0.
	if rowCount > 0 && colStat.DistinctCount == 0 {
		panic(errors.AssertionFailedf("estimated distinct count must be non-zero"))
	}

	// If this is a multi-column statistic, the distinct count should be no
	// larger than the product of all the distinct counts of its individual
	// columns, and no smaller than the distinct count of any single column.
	if colStat.Cols.Len() > 1 && rowCount > 1 {
		product := 1.0
		maxDistinct := 0.0
		colStat.Cols.ForEach(func(col opt.ColumnID) {
			if singleColStat, ok := s.ColStats.Lookup(opt.MakeColSet(col)); ok {
				if singleColStat.DistinctCount > 1 {
					product *= singleColStat.DistinctCount
				}
				if singleColStat.DistinctCount > maxDistinct {
					maxDistinct = singleColStat.DistinctCount
				}
			} else {
				// This is just a best-effort check, so if we don't have one of the
				// column stats, assume its distinct count equals the row count.
				product *= rowCount
			}
		})
		colStat.DistinctCount = min(colStat.DistinctCount, product)
		colStat.DistinctCount = max(colStat.DistinctCount, maxDistinct)
	}

	// The distinct and null counts should be no larger than the row count.
	colStat.DistinctCount = min(colStat.DistinctCount, rowCount)
	colStat.NullCount = min(colStat.NullCount, rowCount)

	// Uniformly reduce the size of each histogram bucket so the number of values
	// is no larger than the row count.
	if colStat.Histogram != nil {
		valuesCount := colStat.Histogram.ValuesCount()
		if valuesCount > rowCount {
			colStat.Histogram = colStat.Histogram.ApplySelectivity(props.MakeSelectivity(rowCount / valuesCount))
		}
	}
}

func (sb *statisticsBuilder) shouldUseHistogram(relProps *props.Relational) bool {
	// If we know that the cardinality is below a certain threshold (e.g., due to
	// a constraint on a key column), don't bother adding the overhead of
	// creating a histogram.
	return relProps.Cardinality.Max >= minCardinalityForHistogram
}

// rowsProcessed calculates and returns the number of rows processed by the
// relational expression. It is currently only supported for joins.
func (sb *statisticsBuilder) rowsProcessed(e RelExpr) float64 {
	semiAntiJoinToInnerJoin := func(joinType opt.Operator) opt.Operator {
		switch joinType {
		case opt.SemiJoinOp, opt.AntiJoinOp:
			return opt.InnerJoinOp
		case opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
			return opt.InnerJoinApplyOp
		default:
			return joinType
		}
	}

	switch t := e.(type) {
	case *IndexJoinExpr:
		// An index join is like a lookup join with no additional ON filters. The
		// number of rows processed equals the number of output rows.
		return e.Relational().Stats.RowCount

	case *LookupJoinExpr:
		var lookupJoinPrivate *LookupJoinPrivate
		switch t.JoinType {
		case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
			// The number of rows processed for semi and anti joins is closer to the
			// number of output rows for an equivalent inner join.
			copy := t.LookupJoinPrivate
			copy.JoinType = semiAntiJoinToInnerJoin(t.JoinType)
			lookupJoinPrivate = &copy

		default:
			if t.On.IsTrue() {
				// If there are no additional ON filters, the number of rows processed
				// equals the number of output rows.
				return e.Relational().Stats.RowCount
			}
			lookupJoinPrivate = &t.LookupJoinPrivate
		}

		// We need to determine the row count of the join before the
		// ON conditions are applied.
		withoutOn := e.Memo().MemoizeLookupJoin(t.Input, nil /* on */, lookupJoinPrivate)
		return withoutOn.Relational().Stats.RowCount

	case *InvertedJoinExpr:
		var invertedJoinPrivate *InvertedJoinPrivate
		switch t.JoinType {
		case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
			// The number of rows processed for semi and anti joins is closer to the
			// number of output rows for an equivalent inner join.
			copy := t.InvertedJoinPrivate
			copy.JoinType = semiAntiJoinToInnerJoin(t.JoinType)
			invertedJoinPrivate = &copy

		default:
			if t.On.IsTrue() {
				// If there are no additional ON filters, the number of rows processed
				// equals the number of output rows.
				return e.Relational().Stats.RowCount
			}
			invertedJoinPrivate = &t.InvertedJoinPrivate
		}

		// We need to determine the row count of the join before the
		// ON conditions are applied.
		withoutOn := e.Memo().MemoizeInvertedJoin(t.Input, nil /* on */, invertedJoinPrivate)
		return withoutOn.Relational().Stats.RowCount

	case *MergeJoinExpr:
		var mergeJoinPrivate *MergeJoinPrivate
		switch t.JoinType {
		case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
			// The number of rows processed for semi and anti joins is closer to the
			// number of output rows for an equivalent inner join.
			copy := t.MergeJoinPrivate
			copy.JoinType = semiAntiJoinToInnerJoin(t.JoinType)
			mergeJoinPrivate = &copy

		default:
			if t.On.IsTrue() {
				// If there are no additional ON filters, the number of rows processed
				// equals the number of output rows.
				return e.Relational().Stats.RowCount
			}
			mergeJoinPrivate = &t.MergeJoinPrivate
		}

		// We need to determine the row count of the join before the
		// ON conditions are applied.
		withoutOn := e.Memo().MemoizeMergeJoin(t.Left, t.Right, nil /* on */, mergeJoinPrivate)
		return withoutOn.Relational().Stats.RowCount

	default:
		if !opt.IsJoinOp(e) {
			panic(errors.AssertionFailedf("rowsProcessed not supported for operator type %v", log.Safe(e.Op())))
		}

		leftCols := e.Child(0).(RelExpr).Relational().OutputCols
		rightCols := e.Child(1).(RelExpr).Relational().OutputCols
		filters := e.Child(2).(*FiltersExpr)

		// Remove ON conditions that are not equality conditions,
		on := ExtractJoinEqualityFilters(leftCols, rightCols, *filters)

		switch t := e.(type) {
		// The number of rows processed for semi and anti joins is closer to the
		// number of output rows for an equivalent inner join.
		case *SemiJoinExpr:
			e = e.Memo().MemoizeInnerJoin(t.Left, t.Right, on, &t.JoinPrivate)
		case *SemiJoinApplyExpr:
			e = e.Memo().MemoizeInnerJoinApply(t.Left, t.Right, on, &t.JoinPrivate)
		case *AntiJoinExpr:
			e = e.Memo().MemoizeInnerJoin(t.Left, t.Right, on, &t.JoinPrivate)
		case *AntiJoinApplyExpr:
			e = e.Memo().MemoizeInnerJoinApply(t.Left, t.Right, on, &t.JoinPrivate)

		default:
			if len(on) == len(*filters) {
				// No filters were removed.
				return e.Relational().Stats.RowCount
			}

			switch t := e.(type) {
			case *InnerJoinExpr:
				e = e.Memo().MemoizeInnerJoin(t.Left, t.Right, on, &t.JoinPrivate)
			case *InnerJoinApplyExpr:
				e = e.Memo().MemoizeInnerJoinApply(t.Left, t.Right, on, &t.JoinPrivate)
			case *LeftJoinExpr:
				e = e.Memo().MemoizeLeftJoin(t.Left, t.Right, on, &t.JoinPrivate)
			case *LeftJoinApplyExpr:
				e = e.Memo().MemoizeLeftJoinApply(t.Left, t.Right, on, &t.JoinPrivate)
			case *RightJoinExpr:
				e = e.Memo().MemoizeRightJoin(t.Left, t.Right, on, &t.JoinPrivate)
			case *FullJoinExpr:
				e = e.Memo().MemoizeFullJoin(t.Left, t.Right, on, &t.JoinPrivate)
			default:
				panic(errors.AssertionFailedf("join type %v not handled", log.Safe(e.Op())))
			}
		}
		return e.Relational().Stats.RowCount
	}
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
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

	// When subtracting floating point numbers, avoid precision errors by making
	// sure the result is greater than or equal to epsilon.
	epsilon = 1e-10

	// This is the minimum cardinality an expression should have in order to make
	// it worth adding the overhead of using a histogram.
	minCardinalityForHistogram = 100

	// This is the default selectivity estimated for inverted joins until we can
	// get better statistics on inverted indexes.
	unknownInvertedJoinSelectivity = 1.0 / 100.0

	// multiColWeight is the weight to assign the selectivity calculation using
	// multi-column statistics versus the calculation using single-column
	// statistics. See the comment above selectivityFromMultiColDistinctCounts for
	// details.
	multiColWeight = 9.0 / 10.0
)

// countPaths returns the number of JSON or Array paths in the specified
// FiltersItem. Used in the calculation of unapplied conjuncts in a Contains
// operator, or an equality operator with a JSON fetch val operator on the left.
// Returns 0 if paths could not be counted for any reason, such as malformed
// JSON.
func countPaths(conjunct *FiltersItem) int {
	// TODO(mgartner): If the right side of the equality is a JSON object or
	// array, there may be more than 1 path.
	if conjunct.Condition.Op() == opt.EqOp && conjunct.Condition.Child(0).Op() == opt.FetchValOp {
		return 1
	}

	rhs := conjunct.Condition.Child(1)
	if !CanExtractConstDatum(rhs) {
		return 0
	}
	rightDatum := ExtractConstDatum(rhs)
	if rightDatum == tree.DNull {
		return 0
	}

	if rd, ok := rightDatum.(*tree.DJSON); ok {
		// TODO(itsbilal): Replace this with a method that only counts paths
		// instead of generating a slice for all of them.
		paths, err := json.AllPaths(rd.JSON)
		if err != nil {
			return 0
		}
		return len(paths)
	}

	if rd, ok := rightDatum.(*tree.DArray); ok {
		return rd.Len()
	}

	return 0
}

// filterRelExpr is called from buildScan and buildSelect to calculate the stats
// for a relational expression based on the given filters expression. In the
// case of Select, the filters are the select filters. In the case of a Scan,
// the filters are the partial index predicate expression of the index that the
// Scan operates on.
func (sb *statisticsBuilder) filterRelExpr(
	filters FiltersExpr,
	e RelExpr,
	notNullCols opt.ColSet,
	relProps *props.Relational,
	s *props.Statistics,
	inputFD *props.FuncDepSet,
) {
	// Update stats based on equivalencies in the filter conditions. Note that
	// EquivReps from e's FD should not be used, as they include equivalencies
	// derived from input expressions.
	var equivFD props.FuncDepSet
	for i := range filters {
		equivFD.AddEquivFrom(&filters[i].ScalarProps().FuncDeps)
	}
	equivReps := equivFD.EquivReps()

	// Calculate distinct counts and histograms for constrained columns
	// ----------------------------------------------------------------
	numUnappliedConjuncts, constrainedCols, histCols := sb.applyFilters(filters, e, relProps)

	// Try to reduce the number of columns used for selectivity
	// calculation based on functional dependencies.
	constrainedCols = sb.tryReduceCols(constrainedCols, s, inputFD)

	// Set null counts to 0 for non-nullable columns
	// ---------------------------------------------
	sb.updateNullCountsFromNotNullCols(notNullCols, s)

	// Calculate row count and selectivity
	// -----------------------------------
	s.ApplySelectivity(sb.selectivityFromHistograms(histCols, e, s))
	s.ApplySelectivity(sb.selectivityFromMultiColDistinctCounts(constrainedCols, e, s))
	s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, &relProps.FuncDeps, e, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(numUnappliedConjuncts))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(e, notNullCols, constrainedCols))

	// Adjust the selectivity so we don't double-count the histogram columns.
	s.UnapplySelectivity(sb.selectivityFromSingleColDistinctCounts(histCols, e, s))

	// Update distinct and null counts based on equivalencies; this should
	// happen after selectivityFromMultiColDistinctCounts and
	// selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, &equivFD, e, notNullCols, s)
}

// applyFilters uses constraints to update the distinct counts and histograms
// for the constrained columns in the filter. The changes in the distinct
// counts and histograms will be used later to determine the selectivity of
// the filter.
//
// See applyFiltersItem for more details.
func (sb *statisticsBuilder) applyFilters(
	filters FiltersExpr, e RelExpr, relProps *props.Relational,
) (numUnappliedConjuncts float64, constrainedCols, histCols opt.ColSet) {
	// Special hack for lookup and inverted joins. Add constant filters from the
	// equality conditions.
	// TODO(rytaft): the correct way to do this is probably to fully implement
	// histograms in Project and Join expressions, and use them in
	// selectivityFromEquivalencies. See Issue #38082.
	switch t := e.(type) {
	case *LookupJoinExpr:
		filters = append(filters, t.ConstFilters...)
	case *InvertedJoinExpr:
		filters = append(filters, t.ConstFilters...)
	}

	for i := range filters {
		numUnappliedConjunctsLocal, constrainedColsLocal, histColsLocal :=
			sb.applyFiltersItem(&filters[i], e, relProps)
		numUnappliedConjuncts += numUnappliedConjunctsLocal
		constrainedCols.UnionWith(constrainedColsLocal)
		histCols.UnionWith(histColsLocal)
	}

	return numUnappliedConjuncts, constrainedCols, histCols
}

// applyFiltersItem uses constraints to update the distinct counts and
// histograms for the constrained columns in the filters item. The changes in
// the distinct counts and histograms will be used later to determine the
// selectivity of the filter.
//
// Some filters can be translated directly to distinct counts using the
// constraint set. For example, the tight constraint `/a: [/1 - /1]` indicates
// that column `a` has exactly one distinct value.  Other filters, such as
// `a % 2 = 0` may not have a tight constraint. In this case, it is not
// possible to determine the distinct count for column `a`, so instead we
// increment numUnappliedConjuncts, which will be used later for selectivity
// calculation. See comments in applyConstraintSet and
// updateDistinctCountsFromConstraint for more details about how distinct
// counts are calculated from constraints.
//
// Equalities between two variables (e.g., var1=var2) are handled separately.
// See applyEquivalencies and selectivityFromEquivalencies for details.
//
// Inverted join conditions are handled separately. See
// selectivityFromInvertedJoinCondition.
func (sb *statisticsBuilder) applyFiltersItem(
	filter *FiltersItem, e RelExpr, relProps *props.Relational,
) (numUnappliedConjuncts float64, constrainedCols, histCols opt.ColSet) {
	if isEqualityWithTwoVars(filter.Condition) {
		// Equalities are handled by applyEquivalencies.
		return 0, opt.ColSet{}, opt.ColSet{}
	}

	// Special case: The current conjunct is an inverted join condition which is
	// handled by selectivityFromInvertedJoinCondition.
	if isInvertedJoinCond(filter.Condition) {
		return 0, opt.ColSet{}, opt.ColSet{}
	}

	// Special case: The current conjunct is a JSON or Array Contains
	// operator, or an equality operator with a JSON fetch value operator on
	// the left (for example j->'a' = '1'). If so, count every path to a
	// leaf node in the RHS as a separate conjunct. If for whatever reason
	// we can't get to the JSON or Array datum or enumerate its paths, count
	// the whole operator as one conjunct.
	if filter.Condition.Op() == opt.ContainsOp ||
		(filter.Condition.Op() == opt.EqOp && filter.Condition.Child(0).Op() == opt.FetchValOp) {
		numPaths := countPaths(filter)
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
		return numUnappliedConjuncts, opt.ColSet{}, opt.ColSet{}
	}

	// Update constrainedCols after the above check for isEqualityWithTwoVars.
	// We will use constrainedCols later to determine which columns to use for
	// selectivity calculation in selectivityFromMultiColDistinctCounts, and we
	// want to make sure that we don't include columns that were only present in
	// equality conjuncts such as var1=var2. The selectivity of these conjuncts
	// will be accounted for in selectivityFromEquivalencies.
	s := &relProps.Stats
	scalarProps := filter.ScalarProps()
	constrainedCols.UnionWith(scalarProps.OuterCols)
	if scalarProps.Constraints != nil {
		histColsLocal := sb.applyConstraintSet(
			scalarProps.Constraints, scalarProps.TightConstraints, e, relProps, s,
		)
		histCols.UnionWith(histColsLocal)
		if !scalarProps.TightConstraints {
			numUnappliedConjuncts++
			// Mimic constrainScan in the case of no histogram information
			// that assumes a geo function is a single closed span that
			// corresponds to two "conjuncts".
			if isGeoIndexScanCond(filter.Condition) {
				numUnappliedConjuncts++
			}
		}
	} else if constraintUnion := sb.buildDisjunctionConstraints(filter); len(constraintUnion) > 0 {
		// The filters are one or more disjunctions and tight constraint sets
		// could be built for each.
		var tmpStats, unionStats props.Statistics
		unionStats.CopyFrom(s)

		// Get the stats for each constraint set, apply the selectivity to a
		// temporary stats struct, and union the selectivity and row counts.
		sb.constrainExpr(e, constraintUnion[0], relProps, &unionStats)
		for i := 1; i < len(constraintUnion); i++ {
			tmpStats.CopyFrom(s)
			sb.constrainExpr(e, constraintUnion[i], relProps, &tmpStats)
			unionStats.UnionWith(&tmpStats)
		}

		// The stats are unioned naively; the selectivity may be greater than 1
		// and the row count may be greater than the row count of the input
		// stats. We use the minimum selectivity and row count of the unioned
		// stats and the input stats.
		// TODO(mgartner): Calculate and set the column statistics based on
		// constraintUnion.
		s.Selectivity = props.MinSelectivity(s.Selectivity, unionStats.Selectivity)
		s.RowCount = min(s.RowCount, unionStats.RowCount)
	} else {
		numUnappliedConjuncts++
	}

	return numUnappliedConjuncts, constrainedCols, histCols
}

// buildDisjunctionConstraints returns a slice of tight constraint sets that are
// built from one or more adjacent Or expressions in filter. This allows more
// accurate stats to be calculated for disjunctions. If any adjacent Or cannot
// be tightly constrained, then nil is returned.
func (sb *statisticsBuilder) buildDisjunctionConstraints(filter *FiltersItem) []*constraint.Set {
	expr := filter.Condition

	// If the expression is not an Or, we cannot build disjunction constraint
	// sets.
	or, ok := expr.(*OrExpr)
	if !ok {
		return nil
	}

	cb := constraintsBuilder{md: sb.md, evalCtx: sb.evalCtx}

	unconstrained := false
	var constraints []*constraint.Set
	var collectConstraints func(opt.ScalarExpr)
	collectConstraints = func(e opt.ScalarExpr) {
		// If a constraint can be built from e, collect the constraint and its
		// tightness.
		c, tight := cb.buildConstraints(e)
		if !c.IsUnconstrained() && tight {
			constraints = append(constraints, c)
			return
		}

		innerOr, ok := e.(*OrExpr)
		if !ok {
			// If a tight constraint could not be built and the expression is
			// not an Or, set unconstrained so we can return nil.
			unconstrained = true
			return
		}

		// If a constraint could not be built and the expression is an Or,
		// attempt to build a constraint for the left and right children.
		collectConstraints(innerOr.Left)
		collectConstraints(innerOr.Right)
	}

	// We intentionally call collectConstraints on the left and right of the
	// top-level Or expression here. collectConstraints attempts to build a
	// constraint for the given expression before recursing. This would be
	// wasted computation because if a constraint could have been built for the
	// top-level or expression, we would not have reached this point -
	// applyFiltersItem would have handled this case before calling
	// buildDisjunctionConstraints.
	collectConstraints(or.Left)
	collectConstraints(or.Right)

	if unconstrained {
		return nil
	}

	return constraints
}

// constrainExpr calculates the stats for a relational expression based on the
// constraint set. The constraint set must be tight.
func (sb *statisticsBuilder) constrainExpr(
	e RelExpr, cs *constraint.Set, relProps *props.Relational, s *props.Statistics,
) {
	constrainedCols := cs.ExtractCols()

	// Calculate distinct counts and histograms for constrained columns
	// ----------------------------------------------------------------
	histCols := sb.applyConstraintSet(cs, true /* tight */, e, relProps, s)

	// Set null counts to 0 for non-nullable columns
	// ---------------------------------------------
	notNullCols := relProps.NotNullCols.Copy()
	// Add any not-null columns from this constraint set.
	notNullCols.UnionWith(cs.ExtractNotNullCols(sb.evalCtx))
	sb.updateNullCountsFromNotNullCols(notNullCols, s)

	// Calculate row count and selectivity
	// -----------------------------------
	s.ApplySelectivity(sb.selectivityFromHistograms(histCols, e, s))
	s.ApplySelectivity(sb.selectivityFromMultiColDistinctCounts(constrainedCols, e, s))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(e, notNullCols, constrainedCols))

	// Adjust the selectivity so we don't double-count the histogram columns.
	s.UnapplySelectivity(sb.selectivityFromSingleColDistinctCounts(histCols, e, s))
}

// applyIndexConstraint is used to update the distinct counts and histograms
// for the constrained columns in an index constraint. Returns the set of
// constrained columns and the set of columns with a filtered histogram.
func (sb *statisticsBuilder) applyIndexConstraint(
	c *constraint.Constraint, e RelExpr, relProps *props.Relational, s *props.Statistics,
) (constrainedCols, histCols opt.ColSet) {
	// If unconstrained, then no constraint could be derived from the expression,
	// so fall back to estimate.
	// If a contradiction, then optimizations must not be enabled (say for
	// testing), or else this would have been reduced.
	if c.IsUnconstrained() || c.IsContradiction() {
		return
	}

	// Calculate distinct counts.
	applied, lastColMinDistinct := sb.updateDistinctCountsFromConstraint(c, e, s)

	// Collect the set of constrained columns for which we were able to estimate
	// a distinct count, including the first column after the constraint prefix
	// (if applicable, add a distinct count estimate for that column using the
	// function updateDistinctCountFromUnappliedConjuncts).
	//
	// Note that the resulting set might not include *all* constrained columns.
	// For example, we cannot make any assumptions about the distinct count of
	// column b based on the constraints /a/b: [/1 - /5/6] or /a/b: [ - /5/6].
	// TODO(rytaft): Consider treating remaining constrained columns as
	//  "unapplied conjuncts" and account for their selectivity in
	//  selectivityFromUnappliedConjuncts.
	prefix := c.Prefix(sb.evalCtx)
	for i, n := 0, c.ConstrainedColumns(sb.evalCtx); i < n && i <= prefix; i++ {
		col := c.Columns.Get(i).ID()
		constrainedCols.Add(col)
		if i < applied {
			continue
		}

		// Unlike the constraints found in Select and Join filters, an index
		// constraint may represent multiple conjuncts. Therefore, we need to
		// calculate the number of unapplied conjuncts for each constrained column.
		numConjuncts := sb.numConjunctsInConstraint(c, i)

		// Set the distinct count for the current column of the constraint
		// according to unknownDistinctCountRatio.
		var lowerBound float64
		if i == applied {
			lowerBound = lastColMinDistinct
		}
		sb.updateDistinctCountFromUnappliedConjuncts(col, e, s, numConjuncts, lowerBound)
	}

	if !sb.shouldUseHistogram(relProps) {
		return constrainedCols, histCols
	}

	// Calculate histograms.
	constrainedCols.ForEach(func(col opt.ColumnID) {
		colSet := opt.MakeColSet(col)
		if sb.updateHistogram(c, colSet, e, s) {
			histCols.Add(col)
		}
	})

	return constrainedCols, histCols
}

// applyConstraintSet is used to update the distinct counts and histograms
// for the constrained columns in a constraint set. Returns the set of
// columns with a filtered histogram.
func (sb *statisticsBuilder) applyConstraintSet(
	cs *constraint.Set, tight bool, e RelExpr, relProps *props.Relational, s *props.Statistics,
) (histCols opt.ColSet) {
	// If unconstrained, then no constraint could be derived from the expression,
	// so fall back to estimate.
	// If a contradiction, then optimizations must not be enabled (say for
	// testing), or else this would have been reduced.
	if cs.IsUnconstrained() || cs == constraint.Contradiction {
		return opt.ColSet{}
	}

	for i := 0; i < cs.Length(); i++ {
		c := cs.Constraint(i)
		col := c.Columns.Get(0).ID()

		// Calculate distinct counts.
		applied, lastColMinDistinct := sb.updateDistinctCountsFromConstraint(c, e, s)
		if applied == 0 {
			// If a constraint cannot be applied, it may represent an
			// inequality like x < 1. As a result, distinctCounts does not fully
			// represent the selectivity of the constraint set.
			// We return an estimate of the number of unapplied conjuncts to the
			// caller function to be used for selectivity calculation.
			numConjuncts := sb.numConjunctsInConstraint(c, 0 /* nth */)

			// Set the distinct count for the first column of the constraint
			// according to unknownDistinctCountRatio.
			sb.updateDistinctCountFromUnappliedConjuncts(col, e, s, numConjuncts, lastColMinDistinct)
		}

		if !tight {
			// TODO(rytaft): it may still be beneficial to calculate the histogram
			// even if the constraint is not tight, but don't bother for now.
			continue
		}

		if !sb.shouldUseHistogram(relProps) {
			continue
		}

		// Calculate histogram.
		cols := opt.MakeColSet(col)
		if sb.updateHistogram(c, cols, e, s) {
			histCols.UnionWith(cols)
		}
	}

	return histCols
}

// updateNullCountsFromNotNullCols zeroes null counts for columns that cannot
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
func (sb *statisticsBuilder) updateNullCountsFromNotNullCols(
	notNullCols opt.ColSet, s *props.Statistics,
) {
	notNullCols.ForEach(func(col opt.ColumnID) {
		colSet := opt.MakeColSet(col)
		colStat, ok := s.ColStats.Lookup(colSet)
		if ok {
			colStat.NullCount = 0
		}
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
//
// Even if the distinct count can not be inferred for a particular column,
// It's possible that we can determine a lower bound. In particular, if the
// query specifically mentions some exact values we should use that as a hint.
// For example, consider the following constraint:
//
//   /a: [ - 5][10 - 10][15 - 15]
//
// In this case, updateDistinctCountsFromConstraint will infer that there
// are at least two distinct values (10 and 15). This lower bound will be
// returned in the second return value, lastColMinDistinct.
func (sb *statisticsBuilder) updateDistinctCountsFromConstraint(
	c *constraint.Constraint, e RelExpr, s *props.Statistics,
) (applied int, lastColMinDistinct float64) {
	// All of the columns that are part of the prefix have a finite number of
	// distinct values.
	prefix := c.Prefix(sb.evalCtx)
	keyCtx := constraint.MakeKeyContext(&c.Columns, sb.evalCtx)

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
		countable := true
		for i := 0; i < c.Spans.Count(); i++ {
			sp := c.Spans.Get(i)
			spanDistinctVals, ok := sp.KeyCount(&keyCtx, col+1)
			if !ok {
				countable = false
				continue
			}
			// Subtract 1 from the span distinct count since we started with
			// distinctCount = 1 above and we increment for each new value below.
			distinctCount += float64(spanDistinctVals) - 1

			startVal := sp.StartKey().Value(col)
			endVal := sp.EndKey().Value(col)
			if i != 0 && val != nil {
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
					countable = false
					continue
				}
			}
			val = endVal
		}

		if !countable {
			// The last column was not fully applied since there was at least one
			// uncountable span. The calculated distinct count will be used as a
			// lower bound for updateDistinctCountFromUnappliedConjuncts.
			return applied, distinctCount
		}

		colID := c.Columns.Get(col).ID()
		sb.ensureColStat(opt.MakeColSet(colID), distinctCount, e, s)
		applied = col + 1
	}

	return applied, 0
}

// updateDistinctCountFromUnappliedConjuncts is used to update the distinct
// count for a constrained column when the exact count cannot be determined.
// The provided lowerBound serves as a lower bound on the calculated distinct
// count.
func (sb *statisticsBuilder) updateDistinctCountFromUnappliedConjuncts(
	colID opt.ColumnID, e RelExpr, s *props.Statistics, numConjuncts, lowerBound float64,
) {
	colSet := opt.MakeColSet(colID)
	inputStat, _ := sb.colStatFromInput(colSet, e)
	distinctCount := inputStat.DistinctCount * math.Pow(unknownFilterSelectivity, numConjuncts)
	distinctCount = max(distinctCount, lowerBound)
	sb.ensureColStat(colSet, distinctCount, e, s)
}

// updateHistogram updates the histogram for the given cols based on the
// constraint. It returns true if a histogram was available from the input and a
// new histogram was calculated by filtering by the constraint.
func (sb *statisticsBuilder) updateHistogram(
	c *constraint.Constraint, cols opt.ColSet, e RelExpr, s *props.Statistics,
) (ok bool) {
	// Fetch the histogram from the input.
	inputStat, _ := sb.colStatFromInput(cols, e)
	inputHist := inputStat.Histogram

	// If a histogram has already been calculated for the cols in the current
	// RelExpr, use it instead of the input histogram. This is necessary when
	// calculating histograms for constrained partial index scans because both
	// the constraint and the partial index predicate can filter the histogram.
	// We want each filter to apply, one after the other, rather than resetting
	// the histogram to the input histogram each time updateHistogram is called.
	colStat, ok := s.ColStats.Lookup(cols)
	if ok && colStat.Histogram != nil {
		inputHist = colStat.Histogram
	}

	if inputHist != nil && ok {
		if _, _, ok := inputHist.CanFilter(c); ok {
			colStat.Histogram = inputHist.Filter(c)
			sb.updateDistinctCountFromHistogram(colStat, inputStat.DistinctCount)
			return true
		}
	}

	return false
}

// updateDistinctCountFromHistogram updates the distinct count for the given
// column statistic based on the estimated number of distinct values in the
// histogram. The updated count will be no larger than the provided value for
// maxDistinctCount.
//
// This function should be called after updateDistinctCountsFromConstraint
// and/or updateDistinctCountFromUnappliedConjuncts.
func (sb *statisticsBuilder) updateDistinctCountFromHistogram(
	colStat *props.ColumnStatistic, maxDistinctCount float64,
) {
	distinct := colStat.Histogram.DistinctValuesCount()
	if distinct == 0 {
		// Make sure the count is non-zero. The stats may be stale, and we
		// can end up with weird and inefficient plans if we estimate 0 rows.
		distinct = min(1, colStat.DistinctCount)
	}

	// The distinct count estimate from the histogram should always be more
	// accurate than the distinct count analysis performed in
	// updateDistinctCountsFromConstraint, so use the histogram estimate to
	// replace the distinct count value.
	colStat.DistinctCount = min(distinct, maxDistinctCount)
}

func (sb *statisticsBuilder) applyEquivalencies(
	equivReps opt.ColSet,
	filterFD *props.FuncDepSet,
	e RelExpr,
	notNullCols opt.ColSet,
	s *props.Statistics,
) {
	equivReps.ForEach(func(i opt.ColumnID) {
		equivGroup := filterFD.ComputeEquivGroup(i)
		sb.updateDistinctNullCountsFromEquivalency(equivGroup, e, notNullCols, s)
	})
}

func (sb *statisticsBuilder) updateDistinctNullCountsFromEquivalency(
	equivGroup opt.ColSet, e RelExpr, notNullCols opt.ColSet, s *props.Statistics,
) {
	// Find the minimum distinct and null counts for all columns in this equivalency
	// group.
	minDistinctCount := s.RowCount
	minNullCount := s.RowCount
	equivGroup.ForEach(func(i opt.ColumnID) {
		colSet := opt.MakeColSet(i)
		colStat, ok := s.ColStats.Lookup(colSet)
		if !ok {
			colStat, _ = sb.colStatFromInput(colSet, e)
			colStat = sb.copyColStat(colSet, s, colStat)
			if colStat.NullCount > 0 && colSet.Intersects(notNullCols) {
				colStat.NullCount = 0
				colStat.DistinctCount = max(colStat.DistinctCount-1, epsilon)
			}
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
	equivGroup.ForEach(func(i opt.ColumnID) {
		colStat, _ := s.ColStats.Lookup(opt.MakeColSet(i))
		colStat.DistinctCount = minDistinctCount
		colStat.NullCount = minNullCount
	})
}

// selectivityFromMultiColDistinctCounts calculates the selectivity of a filter
// by using estimated distinct counts of each constrained column before
// and after the filter was applied. We can perform this calculation in
// two different ways: (1) by treating the columns as completely independent,
// or (2) by assuming they are correlated.
//
// (1) Assuming independence between columns, we can calculate the selectivity
//     by taking the product of selectivities of each constrained column. In
//     the general case, this can be represented by the formula:
//
//                      ┬-┬ ⎛ new_distinct(i) ⎞
//       selectivity =  │ │ ⎜ --------------- ⎟
//                      ┴ ┴ ⎝ old_distinct(i) ⎠
//                     i in
//                  {constrained
//                    columns}
//
// (2) If we instead assume there is some correlation between columns, we
//     calculate the selectivity using multi-column statistics.
//
//                     ⎛ new_distinct({constrained columns}) ⎞
//       selectivity = ⎜ ----------------------------------- ⎟
//                     ⎝ old_distinct({constrained columns}) ⎠
//
//     This formula looks simple, but the challenge is that it is difficult
//     to determine the correct value for new_distinct({constrained columns})
//     if each column is not constrained to a single value. For example, if
//     new_distinct(x)=2 and new_distinct(y)=2, new_distinct({x,y}) could be 2,
//     3 or 4. We estimate the new distinct count as follows, using the concept
//     of "soft functional dependency (FD) strength" as defined in [1]:
//
//       new_distinct({x,y}) = min_value + range * (1 - FD_strength_scaled)
//
//     where
//
//       min_value = max(new_distinct(x), new_distinct(y))
//       max_value = new_distinct(x) * new_distinct(y)
//       range     = max_value - min_value
//
//                     ⎛ max(old_distinct(x),old_distinct(y)) ⎞
//       FD_strength = ⎜ ------------------------------------ ⎟
//                     ⎝         old_distinct({x,y})          ⎠
//
//                         ⎛ max(old_distinct(x), old_distinct(y)) ⎞
//       min_FD_strength = ⎜ ------------------------------------- ⎟
//                         ⎝   old_distinct(x) * old_distinct(y)   ⎠
//
//                            ⎛ FD_strength - min_FD_strength ⎞  // scales FD_strength
//       FD_strength_scaled = ⎜ ----------------------------- ⎟  // to be between
//                            ⎝      1 - min_FD_strength      ⎠  // 0 and 1
//
//     Suppose that old_distinct(x)=100 and old_distinct(y)=10. If x and y are
//     perfectly correlated, old_distinct({x,y})=100. Using the example from
//     above, new_distinct(x)=2 and new_distinct(y)=2. Plugging in the values
//     into the equation, we get:
//
//       FD_strength_scaled  = 1
//       new_distinct({x,y}) = 2 + (4 - 2) * (1 - 1) = 2
//
//     If x and y are completely independent, however, old_distinct({x,y})=1000.
//     In this case, we get:
//
//       FD_strength_scaled  = 0
//       new_distinct({x,y}) = 2 + (4 - 2) * (1 - 0) = 4
//
// Note that even if we calculate the selectivity based on equation (2) above,
// we still want to take equation (1) into account. This is because it is
// possible that there are two predicates that each have selectivity s, but the
// multi-column selectivity is also s. In order to ensure that the cost model
// considers the two predicates combined to be more selective than either one
// individually, we must give some weight to equation (1). Therefore, instead
// of equation (2) we actually return the following selectivity:
//
//   selectivity = (1 - w) * (equation 1) + w * (equation 2)
//
// where w is the constant multiColWeight.
//
// This selectivity will be used later to update the row count and the
// distinct count for the unconstrained columns.
//
// [1] Ilyas, Ihab F., et al. "CORDS: automatic discovery of correlations and
//     soft functional dependencies." SIGMOD 2004.
//
func (sb *statisticsBuilder) selectivityFromMultiColDistinctCounts(
	cols opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity props.Selectivity) {
	// Respect the session setting OptimizerUseMultiColStats.
	if !sb.evalCtx.SessionData.OptimizerUseMultiColStats {
		return sb.selectivityFromSingleColDistinctCounts(cols, e, s)
	}

	// Make a copy of cols so we can remove columns that are not constrained.
	multiColSet := cols.Copy()

	// First calculate the selectivity from equation (1) (see function comment),
	// and collect the inputs to equation (2).
	singleColSelectivity := props.OneSelectivity
	newDistinctProduct, oldDistinctProduct := 1.0, 1.0
	maxNewDistinct, maxOldDistinct := float64(0), float64(0)
	multiColNullCount := -1.0
	minLocalSel := props.OneSelectivity
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colStat, ok := s.ColStats.Lookup(opt.MakeColSet(col))
		if !ok {
			multiColSet.Remove(col)
			continue
		}

		inputColStat, inputStats := sb.colStatFromInput(colStat.Cols, e)
		localSel := sb.selectivityFromDistinctCount(colStat, inputColStat, inputStats.RowCount)
		singleColSelectivity.Multiply(localSel)

		// Don't bother including columns in the multi-column calculation that
		// don't contribute to the selectivity.
		if localSel == props.OneSelectivity {
			multiColSet.Remove(col)
			continue
		}

		// Calculate values needed for the multi-column stats calculation below.
		newDistinctProduct *= colStat.DistinctCount
		oldDistinctProduct *= inputColStat.DistinctCount
		if colStat.DistinctCount > maxNewDistinct {
			maxNewDistinct = colStat.DistinctCount
		}
		if inputColStat.DistinctCount > maxOldDistinct {
			maxOldDistinct = inputColStat.DistinctCount
		}
		minLocalSel = props.MinSelectivity(localSel, minLocalSel)
		if multiColNullCount < 0 {
			multiColNullCount = inputStats.RowCount
		}
		// Multiply by the expected chance of collisions with nulls already
		// collected.
		multiColNullCount *= colStat.NullCount / inputStats.RowCount
	}

	// If we don't need to use a multi-column statistic, we're done.
	if multiColSet.Len() <= 1 {
		return singleColSelectivity
	}

	// Otherwise, calculate the selectivity using multi-column stats from
	// equation (2). See the comment above the function definition for details
	// about the formula.
	inputColStat, inputStats := sb.colStatFromInput(multiColSet, e)
	fdStrength := min(maxOldDistinct/inputColStat.DistinctCount, 1.0)
	maxMutiColOldDistinct := min(oldDistinctProduct, inputStats.RowCount)
	minFdStrength := min(maxOldDistinct/maxMutiColOldDistinct, fdStrength)
	if minFdStrength < 1 {
		// Scale the fdStrength so it ranges between 0 and 1.
		fdStrength = (fdStrength - minFdStrength) / (1 - minFdStrength)
	}
	distinctCountRange := max(newDistinctProduct-maxNewDistinct, 0)

	colStat, _ := s.ColStats.Add(multiColSet)
	colStat.DistinctCount = maxNewDistinct + distinctCountRange*(1-fdStrength)
	colStat.NullCount = multiColNullCount
	multiColSelectivity := sb.selectivityFromDistinctCount(colStat, inputColStat, inputStats.RowCount)

	// Now, we must adjust multiColSelectivity so that it is not greater than
	// the selectivity of any subset of the columns in multiColSet. This would
	// be internally inconsistent and could lead to bad plans. For example,
	// x=1 AND y=1 should always be considered more selective (i.e., with lower
	// selectivity) than x=1 alone.
	//
	// We have already found the minimum selectivity of all the individual
	// columns (subsets of size 1) above and stored it in minLocalSel. It's not
	// practical, however, to calculate the minimum selectivity for all subsets
	// larger than size 1.
	//
	// Instead, we focus on a specific case known to occasionally have this
	// problem: when multiColSet contains 3 or more columns and at least one has
	// distinct count greater than 1, the subset of columns that have distinct
	// count less than or equal to 1 may have a smaller selectivity according to
	// equation (2).
	//
	// In this case, update minLocalSel and adjust multiColSelectivity as needed.
	//
	if maxNewDistinct > 1 && multiColSet.Len() > 2 {
		var lowDistinctCountCols opt.ColSet
		multiColSet.ForEach(func(col opt.ColumnID) {
			// We already know the column stat exists if it's in multiColSet.
			colStat, _ := s.ColStats.Lookup(opt.MakeColSet(col))
			if colStat.DistinctCount <= 1 {
				lowDistinctCountCols.Add(col)
			}
		})

		if lowDistinctCountCols.Len() > 1 {
			selLowDistinctCountCols := sb.selectivityFromMultiColDistinctCounts(
				lowDistinctCountCols, e, s,
			)
			minLocalSel = props.MinSelectivity(minLocalSel, selLowDistinctCountCols)
		}
	}
	multiColSelectivity = props.MinSelectivity(multiColSelectivity, minLocalSel)

	// As described in the function comment, we actually return a weighted sum
	// of multi-column and single-column selectivity estimates.
	return props.MakeSelectivity((1-multiColWeight)*singleColSelectivity.AsFloat() + multiColWeight*multiColSelectivity.AsFloat())
}

// selectivityFromSingleColDistinctCounts calculates the selectivity of a
// filter by using estimated distinct counts of each constrained column before
// and after the filter was applied. It assumes independence between columns,
// so it uses equation (1) from selectivityFromMultiColDistinctCounts. See the
// comment above that function for details.
func (sb *statisticsBuilder) selectivityFromSingleColDistinctCounts(
	cols opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity props.Selectivity) {
	selectivity = props.OneSelectivity
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colStat, ok := s.ColStats.Lookup(opt.MakeColSet(col))
		if !ok {
			continue
		}

		inputColStat, inputStats := sb.colStatFromInput(colStat.Cols, e)
		selectivity.Multiply(sb.selectivityFromDistinctCount(colStat, inputColStat, inputStats.RowCount))
	}

	return selectivity
}

// selectivityFromDistinctCount calculates the selectivity of a filter by using
// the estimated distinct count of a single constrained column or set of
// columns before and after the filter was applied.
func (sb *statisticsBuilder) selectivityFromDistinctCount(
	colStat, inputColStat *props.ColumnStatistic, inputRowCount float64,
) props.Selectivity {
	newDistinct := colStat.DistinctCount
	oldDistinct := inputColStat.DistinctCount

	// Nulls are included in the distinct count, so remove 1 from the
	// distinct counts if needed.
	if inputColStat.NullCount > 0 {
		oldDistinct = max(oldDistinct-1, 0)
	}
	if colStat.NullCount > 0 {
		newDistinct = max(newDistinct-1, 0)
	}

	// Calculate the selectivity of the predicate.
	nonNullSelectivity := props.MakeSelectivityFromFraction(newDistinct, oldDistinct)
	nullSelectivity := props.MakeSelectivityFromFraction(colStat.NullCount, inputColStat.NullCount)
	return sb.predicateSelectivity(
		nonNullSelectivity, nullSelectivity, inputColStat.NullCount, inputRowCount,
	)
}

// selectivityFromHistograms is similar to selectivityFromSingleColDistinctCounts,
// in that it calculates the selectivity of a filter by taking the product of
// selectivities of each constrained column.
//
// For histograms, the selectivity of a constrained column is calculated as
// (# values in histogram after filter) / (# values in histogram before filter).
func (sb *statisticsBuilder) selectivityFromHistograms(
	cols opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity props.Selectivity) {
	selectivity = props.OneSelectivity
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colStat, ok := s.ColStats.Lookup(opt.MakeColSet(col))
		if !ok {
			continue
		}

		inputColStat, inputStats := sb.colStatFromInput(colStat.Cols, e)
		newHist := colStat.Histogram
		oldHist := inputColStat.Histogram
		if newHist == nil || oldHist == nil {
			continue
		}

		newCount := newHist.ValuesCount()
		oldCount := oldHist.ValuesCount()

		// Calculate the selectivity of the predicate.
		nonNullSelectivity := props.MakeSelectivityFromFraction(newCount, oldCount)
		nullSelectivity := props.MakeSelectivityFromFraction(colStat.NullCount, inputColStat.NullCount)
		selectivity.Multiply(sb.predicateSelectivity(
			nonNullSelectivity, nullSelectivity, inputColStat.NullCount, inputStats.RowCount,
		))
	}

	return selectivity
}

// selectivityFromNullsRemoved calculates the selectivity from null-rejecting
// filters that were not already accounted for in selectivityFromMultiColDistinctCounts
// or selectivityFromHistograms. The columns for filters already accounted for
// should be designated by ignoreCols.
func (sb *statisticsBuilder) selectivityFromNullsRemoved(
	e RelExpr, notNullCols opt.ColSet, ignoreCols opt.ColSet,
) (selectivity props.Selectivity) {
	selectivity = props.OneSelectivity
	notNullCols.ForEach(func(col opt.ColumnID) {
		if !ignoreCols.Contains(col) {
			inputColStat, inputStats := sb.colStatFromInput(opt.MakeColSet(col), e)
			selectivity.Multiply(sb.predicateSelectivity(
				props.OneSelectivity,  /* nonNullSelectivity */
				props.ZeroSelectivity, /* nullSelectivity */
				inputColStat.NullCount,
				inputStats.RowCount,
			))
		}
	})

	return selectivity
}

// predicateSelectivity calculates the selectivity of a predicate, using the
// following formula:
//
//   sel = (output row count) / (input row count)
//
// where
//
//   output row count =
//     (fraction of non-null values preserved) * (number of non-null input rows) +
//     (fraction of null values preserved) * (number of null input rows)
//
func (sb *statisticsBuilder) predicateSelectivity(
	nonNullSelectivity, nullSelectivity props.Selectivity, inputNullCount, inputRowCount float64,
) props.Selectivity {
	outRowCount := nonNullSelectivity.AsFloat()*(inputRowCount-inputNullCount) + nullSelectivity.AsFloat()*inputNullCount
	sel := props.MakeSelectivity(outRowCount / inputRowCount)

	return sel
}

// selectivityFromEquivalencies determines the selectivity of equality
// constraints. It must be called before applyEquivalencies.
func (sb *statisticsBuilder) selectivityFromEquivalencies(
	equivReps opt.ColSet, filterFD *props.FuncDepSet, e RelExpr, s *props.Statistics,
) (selectivity props.Selectivity) {
	selectivity = props.OneSelectivity
	equivReps.ForEach(func(i opt.ColumnID) {
		equivGroup := filterFD.ComputeEquivGroup(i)
		selectivity.Multiply(sb.selectivityFromEquivalency(equivGroup, e, s))
	})

	return selectivity
}

func (sb *statisticsBuilder) selectivityFromEquivalency(
	equivGroup opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity props.Selectivity) {
	// Find the maximum input distinct count for all columns in this equivalency
	// group.
	maxDistinctCount := float64(0)
	equivGroup.ForEach(func(i opt.ColumnID) {
		// If any of the distinct counts were updated by the filter, we want to use
		// the updated value.
		colSet := opt.MakeColSet(i)
		colStat, ok := s.ColStats.Lookup(colSet)
		if !ok {
			colStat, _ = sb.colStatFromInput(colSet, e)
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
	return props.MakeSelectivityFromFraction(1, maxDistinctCount)
}

// selectivityFromEquivalenciesSemiJoin determines the selectivity of equality
// constraints on a semi join. It must be called before applyEquivalencies.
func (sb *statisticsBuilder) selectivityFromEquivalenciesSemiJoin(
	equivReps, leftOutputCols, rightOutputCols opt.ColSet,
	filterFD *props.FuncDepSet,
	e RelExpr,
	s *props.Statistics,
) (selectivity props.Selectivity) {
	selectivity = props.OneSelectivity
	equivReps.ForEach(func(i opt.ColumnID) {
		equivGroup := filterFD.ComputeEquivGroup(i)
		selectivity.Multiply(sb.selectivityFromEquivalencySemiJoin(
			equivGroup, leftOutputCols, rightOutputCols, e, s,
		))
	})

	return selectivity
}

func (sb *statisticsBuilder) selectivityFromEquivalencySemiJoin(
	equivGroup, leftOutputCols, rightOutputCols opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity props.Selectivity) {
	// Find the minimum (maximum) input distinct count for all columns in this
	// equivalency group from the right (left).
	minDistinctCountRight := math.MaxFloat64
	maxDistinctCountLeft := float64(0)
	equivGroup.ForEach(func(i opt.ColumnID) {
		// If any of the distinct counts were updated by the filter, we want to use
		// the updated value.
		colSet := opt.MakeColSet(i)
		colStat, ok := s.ColStats.Lookup(colSet)
		if !ok {
			colStat, _ = sb.colStatFromInput(colSet, e)
		}
		if leftOutputCols.Contains(i) {
			if maxDistinctCountLeft < colStat.DistinctCount {
				maxDistinctCountLeft = colStat.DistinctCount
			}
		} else if rightOutputCols.Contains(i) {
			if minDistinctCountRight > colStat.DistinctCount {
				minDistinctCountRight = colStat.DistinctCount
			}
		}
	})
	if maxDistinctCountLeft > s.RowCount {
		maxDistinctCountLeft = s.RowCount
	}

	return props.MakeSelectivityFromFraction(minDistinctCountRight, maxDistinctCountLeft)
}

func (sb *statisticsBuilder) selectivityFromInvertedJoinCondition(
	e RelExpr, s *props.Statistics,
) (selectivity props.Selectivity) {
	return props.MakeSelectivity(unknownInvertedJoinSelectivity)
}

func (sb *statisticsBuilder) selectivityFromUnappliedConjuncts(
	numUnappliedConjuncts float64,
) (selectivity props.Selectivity) {
	selectivity = props.MakeSelectivity(math.Pow(unknownFilterSelectivity, numUnappliedConjuncts))

	return selectivity
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
		colStat, ok := s.ColStats.Lookup(opt.MakeColSet(i))
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

// isInvertedJoinCond returns true if the given condition is either an index-
// accelerated geospatial function, a bounding box operation, or a contains
// operation with two variable arguments.
func isInvertedJoinCond(cond opt.ScalarExpr) bool {
	switch t := cond.(type) {
	case *FunctionExpr:
		if _, ok := geoindex.RelationshipMap[t.Name]; ok && len(t.Args) >= 2 {
			return t.Args[0].Op() == opt.VariableOp && t.Args[1].Op() == opt.VariableOp
		}

	case *BBoxIntersectsExpr, *BBoxCoversExpr, *ContainsExpr:
		return t.Child(0).Op() == opt.VariableOp && t.Child(1).Op() == opt.VariableOp
	}

	return false
}

// isGeoIndexScanCond returns true if the given condition is an
// index-accelerated geospatial function with one variable argument.
func isGeoIndexScanCond(cond opt.ScalarExpr) bool {
	if fn, ok := cond.(*FunctionExpr); ok {
		if _, ok := geoindex.RelationshipMap[fn.Name]; ok && len(fn.Args) >= 2 {
			firstIsVar := fn.Args[0].Op() == opt.VariableOp
			secondIsVar := fn.Args[1].Op() == opt.VariableOp
			return (firstIsVar && !secondIsVar) || (!firstIsVar && secondIsVar)
		}
	}
	return false
}

func hasInvertedJoinCond(filters FiltersExpr) bool {
	for i := range filters {
		if isInvertedJoinCond(filters[i].Condition) {
			return true
		}
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
			// of NULL constraints is handled in selectivityFromMultiColDistinctCounts,
			// selectivityFromHistograms, and selectivityFromNullsRemoved.
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
