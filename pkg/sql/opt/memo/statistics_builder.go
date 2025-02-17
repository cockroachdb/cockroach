// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"context"
	"math"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

var statsAnnID = opt.NewTableAnnID()

const (
	// This is the value used for inequality filters such as x < 1 in
	// "Access Path Selection in a Relational Database Management System"
	// by Pat Selinger et al.
	unknownFilterSelectivity = 1.0 / 3.0

	// This is the selectivity used for trigram similarity filters, like s %
	// 'foo'.
	similarityFilterSelectivity = 1.0 / 100.0

	// TODO(rytaft): Add other selectivities for other types of predicates.

	// This is an arbitrary row count used in the absence of any real statistics.
	unknownRowCount = 1000

	// UnknownDistinctCountRatio is the ratio of distinct column values to number
	// of rows, which is used in the absence of any real statistics for non-key
	// columns.  TODO(rytaft): See if there is an industry standard value for
	// this.
	UnknownDistinctCountRatio = 0.1

	// UnknownNullCountRatio is the ratio of null column values to number of rows
	// for nullable columns, which is used in the absence of any real statistics.
	UnknownNullCountRatio = 0.01

	// UnknownAvgRowSize is the average size of a row in bytes, which is used in
	// the absence of any real statistics.
	UnknownAvgRowSize = 8

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

	// defaultColSize is the default size of a column in bytes. This is used
	// when the table statistics have an avgSize of 0 for a given column.
	defaultColSize = 4.0

	// maxValuesForFullHistogramFromCheckConstraint is the maximum number of
	// values from the spans a check constraint is allowed to have in order to build
	// a histogram from it.
	maxValuesForFullHistogramFromCheckConstraint = tabledesc.MaxBucketAllowed
)

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
//	Query:    SELECT y FROM a WHERE x=1
//
//	Plan:            Project y        Row Count: 10, Distinct(x): 1
//	                     |
//	                 Select x=1       Row Count: 10, Distinct(x): 1
//	                     |
//	                  Scan a          Row Count: 100, Distinct(x): 10
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
//	SELECT count(*), x, y FROM t GROUP BY x, y
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
//	      +-------------+               +--------------+
//	1.    | buildScan t |           2.  | buildGroupBy |
//	      +-------------+               +--------------+
//	             |                             |
//	   +-----------------------+   +-------------------------+
//	   | makeTableStatistics t |   | colStatFromChild (x, y) |
//	   +-----------------------+   +-------------------------+
//	                                           |
//	                                 +--------------------+
//	                                 | colStatScan (x, y) |
//	                                 +--------------------+
//	                                           |
//	                                 +---------------------+
//	                                 | colStatTable (x, y) |
//	                                 +---------------------+
//	                                           |
//	                                 +--------------------+
//	                                 | colStatLeaf (x, y) |
//	                                 +--------------------+
//
// See props/statistics.go for more details.
type statisticsBuilder struct {
	ctx                   context.Context
	evalCtx               *eval.Context
	mem                   *Memo
	md                    *opt.Metadata
	checkInputMinRowCount float64
	minRowCount           float64
}

func (sb *statisticsBuilder) init(ctx context.Context, evalCtx *eval.Context, mem *Memo) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*sb = statisticsBuilder{
		ctx:                   ctx,
		evalCtx:               evalCtx,
		mem:                   mem,
		md:                    mem.Metadata(),
		checkInputMinRowCount: evalCtx.SessionData().OptimizerCheckInputMinRowCount,
		minRowCount:           evalCtx.SessionData().OptimizerMinRowCount,
	}
}

func (sb *statisticsBuilder) clear() {
	*sb = statisticsBuilder{}
}

// colStatCols returns the set of columns which may be looked up in
// props.Statistics().ColStats, which includes props.OutputCols and any virtual
// computed columns we have statistics on that are in scope.
func (sb *statisticsBuilder) colStatCols(props *props.Relational) opt.ColSet {
	if sb.evalCtx.SessionData().OptimizerUseVirtualComputedColumnStats &&
		!props.Statistics().VirtualCols.Empty() {
		return props.OutputCols.Union(props.Statistics().VirtualCols)
	}
	return props.OutputCols
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
		colSet = colSet.Intersection(sb.colStatCols(childProps))
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
	return e.Child(childIdx).(RelExpr).Relational().Statistics()
}

// availabilityFromInput determines the availability of the underlying table
// statistics from the children of the expression.
func (sb *statisticsBuilder) availabilityFromInput(e RelExpr) bool {
	switch t := e.(type) {
	case *ScanExpr:
		return sb.makeTableStatistics(t.Table).Available

	case *LookupJoinExpr:
		ensureLookupJoinInputProps(t, sb)
		return t.lookupProps.Statistics().Available && t.Input.Relational().Statistics().Available

	case *InvertedJoinExpr:
		ensureInvertedJoinInputProps(t, sb)
		return t.lookupProps.Statistics().Available && t.Input.Relational().Statistics().Available

	case *ZigzagJoinExpr:
		ensureZigzagJoinInputProps(t, sb)
		return t.leftProps.Statistics().Available
	}

	available := true
	for i, n := 0, e.ChildCount(); i < n; i++ {
		if child, ok := e.Child(i).(RelExpr); ok {
			available = available && child.Relational().Statistics().Available
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
		intersectsLeft := sb.colStatCols(leftProps).Intersects(colSet)

		var rightProps *props.Relational
		if lookupJoin != nil {
			rightProps = &lookupJoin.lookupProps
		} else if invertedJoin != nil {
			rightProps = &invertedJoin.lookupProps
		} else if zigzagJoin != nil {
			rightProps = &zigzagJoin.rightProps
		} else {
			rightProps = e.Child(1).(RelExpr).Relational()
		}
		intersectsRight := sb.colStatCols(rightProps).Intersects(colSet)

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

	panic(errors.AssertionFailedf("unsupported operator type %s", redact.Safe(e.Op())))
}

// colStat gets a column statistic for the given set of columns if it exists.
// If the column statistic is not available in the current expression,
// colStat recursively tries to find it in the children of the expression,
// lazily populating s.ColStats with the statistic as it gets passed up the
// expression tree.
func (sb *statisticsBuilder) colStat(colSet opt.ColSet, e RelExpr) *props.ColumnStatistic {
	if colSet.Empty() {
		panic(errors.AssertionFailedf("column statistics cannot be determined for empty column set"))
	}

	// Check if the requested column statistic is already cached.
	if stat, ok := e.Relational().Statistics().ColStats.Lookup(colSet); ok {
		return stat
	}

	// Only calculate statistics on the normalized expression in a memo group.
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

	case opt.LiteralValuesOp:
		return sb.colStatLiteralValues(colSet, e.(*LiteralValuesExpr))

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

	case opt.TopKOp:
		return sb.colStatTopK(colSet, e.(*TopKExpr))

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

	case opt.LockOp:
		return sb.colStatLock(colSet, e.(*LockExpr))

	case opt.VectorSearchOp:
		return sb.colStatVectorSearch(colSet, e.(*VectorSearchExpr))

	case opt.VectorMutationSearchOp:
		return sb.colStatVectorMutationSearch(colSet, e.(*VectorMutationSearchExpr))

	case opt.BarrierOp:
		return sb.colStatBarrier(colSet, e.(*BarrierExpr))

	case opt.CallOp:
		return sb.colStatCall(colSet, e.(*CallExpr))

	case opt.SequenceSelectOp:
		return sb.colStatSequenceSelect(colSet, e.(*SequenceSelectExpr))

	case opt.ExplainOp, opt.ShowTraceForSessionOp,
		opt.OpaqueRelOp, opt.OpaqueMutationOp, opt.OpaqueDDLOp, opt.RecursiveCTEOp,
		opt.AlterTableSplitOp, opt.AlterTableUnsplitOp,
		opt.AlterTableUnsplitAllOp, opt.AlterTableRelocateOp,
		opt.ControlJobsOp, opt.ControlSchedulesOp, opt.ShowCompletionsOp,
		opt.CancelQueriesOp, opt.CancelSessionsOp, opt.ExportOp:
		return sb.colStatUnknown(colSet, e.Relational())

	case opt.WithOp:
		return sb.colStat(colSet, e.Child(1).(RelExpr))

	case opt.FakeRelOp:
		rel := e.Relational()
		return sb.colStatLeaf(colSet, rel.Statistics(), &rel.FuncDeps, rel.NotNullCols)
	}

	panic(errors.AssertionFailedf("unrecognized relational expression type: %v", redact.Safe(e.Op())))
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
	// Build single-column stats from non-null check constraints, if they exist.
	if colSet.Len() == 1 {
		if ok := sb.buildStatsFromCheckConstraints(colStat, s); ok {
			return colStat
		}
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
				colStat.NullCount = s.RowCount * UnknownNullCountRatio
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
		// There is only one column, and it was not found in the cache above, so we
		// do not have statistics available for it.
		col, _ := colSet.Next(0)
		colStat.DistinctCount = UnknownDistinctCountRatio * s.RowCount
		colStat.NullCount = UnknownNullCountRatio * s.RowCount
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
	// Create a mapping from table column ordinals to inverted index column
	// ordinals. This allows us to do a fast lookup while iterating over all
	// stats from a statistic's column to any associated inverted columns.
	// TODO(mgartner): It might be simpler to iterate over all the table columns
	// looking for inverted columns.
	invertedIndexCols := make(map[int]invertedIndexColInfo)
	for indexI, indexN := 0, tab.IndexCount(); indexI < indexN; indexI++ {
		index := tab.Index(indexI)
		if index.Type() != idxtype.INVERTED {
			continue
		}
		col := index.InvertedColumn()
		srcOrd := col.InvertedSourceColumnOrdinal()
		info := invertedIndexCols[srcOrd]
		info.invIdxColOrds = append(info.invIdxColOrds, col.Ordinal())
		invertedIndexCols[srcOrd] = info
	}

	// Make now and annotate the metadata table with it for next time.
	stats = &props.Statistics{}

	// Find the most recent full statistic. (Stats are ordered with most recent first.)
	var first int
	for first < tab.StatisticCount() &&
		(tab.Statistic(first).IsPartial() ||
			tab.Statistic(first).IsMerged() && !sb.evalCtx.SessionData().OptimizerUseMergedPartialStatistics ||
			tab.Statistic(first).IsForecast() && !sb.evalCtx.SessionData().OptimizerUseForecasts) {
		first++
	}

	if first >= tab.StatisticCount() {
		// No statistics.
		stats.Available = false
		stats.RowCount = unknownRowCount
	} else {
		// Use the RowCount from the most recent statistic.
		stats.Available = true
		stats.RowCount = float64(tab.Statistic(first).RowCount())

		// Make sure the row count is at least 1. The stats may be stale, and we
		// can end up with weird and inefficient plans if we estimate 0 rows.
		stats.RowCount = max(stats.RowCount, 1)

		// Add all the column statistics, using the most recent statistic for each
		// column set. Stats are ordered with most recent first.
	EachStat:
		for i := first; i < tab.StatisticCount(); i++ {
			stat := tab.Statistic(i)
			if stat.IsPartial() {
				continue
			}
			if stat.IsMerged() && !sb.evalCtx.SessionData().OptimizerUseMergedPartialStatistics {
				continue
			}
			if stat.IsForecast() && !sb.evalCtx.SessionData().OptimizerUseForecasts {
				continue
			}
			if stat.ColumnCount() > 1 && !sb.evalCtx.SessionData().OptimizerUseMultiColStats {
				continue
			}

			var cols opt.ColSet
			var colOrd int
			for i := 0; i < stat.ColumnCount(); i++ {
				colOrd = stat.ColumnOrdinal(i)
				col := tabID.ColumnID(colOrd)
				cols.Add(col)
				if tab.Column(colOrd).IsVirtualComputed() {
					if !sb.evalCtx.SessionData().OptimizerUseVirtualComputedColumnStats {
						continue EachStat
					}
					// We only add virtual columns if we have statistics on them, so that
					// in higher groups we can decide whether to look up statistics on
					// virtual columns or on the columns used in their defining
					// expressions.
					stats.VirtualCols.Add(col)
				}
			}

			// We currently only use average column sizes of single column
			// statistics, so we can ignore multi-column average sizes.
			if stat.ColumnCount() == 1 && stat.AvgSize() != 0 {
				if stats.AvgColSizes == nil {
					stats.AvgColSizes = make([]uint64, tab.ColumnCount())
				}
				stats.AvgColSizes[colOrd] = stat.AvgSize()
			}

			needHistogram := cols.Len() == 1 && len(stat.Histogram()) > 0 &&
				sb.evalCtx.SessionData().OptimizerUseHistograms
			seenInvertedStat := false
			invertedStatistic := false
			var invertedColOrds []int
			if needHistogram {
				info := invertedIndexCols[stat.ColumnOrdinal(0)]
				invertedColOrds = info.invIdxColOrds
				seenInvertedStat = info.foundInvertedHistogram
				// If some of the columns are inverted and the statistics is of type
				// BYTES, it means we have an inverted statistic on this column set.
				invertedStatistic = len(invertedColOrds) > 0 && stat.HistogramType().Family() == types.BytesFamily
			}

			colStat, ok := stats.ColStats.Add(cols)
			if ok || (colStat.Histogram == nil && !invertedStatistic && seenInvertedStat) {
				// Set the statistic if either:
				// 1. We have no statistic for the current colset at all
				// 2. All of the following conditions hold:
				//    a. The previously found statistic for the colset has no histogram
				//    b. the current statistic is not inverted
				//    c. the previously found statistic for this colset was inverted
				//    If these conditions hold, it means that the previous histogram
				//    we found for the current colset was derived from an inverted
				//    histogram, and therefore the existing forward statistic doesn't have
				//    a histogram at all, and the new statistic we just found has a
				//    non-inverted histogram that we should be using instead.
				colStat.DistinctCount = float64(stat.DistinctCount())
				colStat.NullCount = float64(stat.NullCount())
				if needHistogram && !invertedStatistic {
					// A statistic is inverted if the column is invertible and its
					// histogram contains buckets of types BYTES.
					// NOTE: this leaves an ambiguity which would surface if we ever
					// permitted an inverted index on BYTES-type columns. A deeper fix
					// is tracked here: https://github.com/cockroachdb/cockroach/issues/50655
					col := cols.SingleColumn()
					colStat.Histogram = &props.Histogram{}
					colStat.Histogram.Init(sb.evalCtx, col, stat.Histogram())
				}

				// Make sure the distinct count is at least 1, for the same reason as
				// the row count above.
				colStat.DistinctCount = max(colStat.DistinctCount, 1)

				// Make sure the values are consistent in case some of the column stats
				// were added at different times (and therefore have a different row
				// count).
				sb.finalizeFromRowCountAndDistinctCounts(colStat, stats)
			}

			// Add inverted histograms if necessary.
			if needHistogram && invertedStatistic {
				// Record the fact that we are adding an inverted statistic to this
				// column set.
				info := invertedIndexCols[stat.ColumnOrdinal(0)]
				info.foundInvertedHistogram = true
				invertedIndexCols[stat.ColumnOrdinal(0)] = info
				for _, invertedColOrd := range invertedColOrds {
					invCol := tabID.ColumnID(invertedColOrd)
					invCols := opt.MakeColSet(invCol)
					if invColStat, ok := stats.ColStats.Add(invCols); ok {
						invColStat.Histogram = &props.Histogram{}
						invColStat.Histogram.Init(sb.evalCtx, invCol, stat.Histogram())
						// Set inverted entry counts from the histogram. Make sure the
						// distinct count is at least 1, for the same reason as the row
						// count above.
						invColStat.DistinctCount = max(invColStat.Histogram.DistinctValuesCount(), 1)
						// Inverted indexes don't have nulls.
						invColStat.NullCount = 0
						if stat.AvgSize() != 0 {
							if stats.AvgColSizes == nil {
								stats.AvgColSizes = make([]uint64, tab.ColumnCount())
							}
							stats.AvgColSizes[invertedColOrd] = stat.AvgSize()
						}
					}
				}
			}
		}
	}
	sb.md.SetTableAnnotation(tabID, statsAnnID, stats)
	return stats
}

// invertedIndexColInfo is used to store information about an inverted column.
type invertedIndexColInfo struct {
	// invIdxColOrds is the list of inverted index column ordinals for a given
	// inverted column.
	invIdxColOrds []int
	// foundInvertedHistogram is set to true if we've found an inverted histogram
	// for a given inverted column.
	foundInvertedHistogram bool
}

func (sb *statisticsBuilder) colStatTable(
	tabID opt.TableID, colSet opt.ColSet,
) *props.ColumnStatistic {
	tableStats := sb.makeTableStatistics(tabID)
	tableFD := MakeTableFuncDep(sb.md, tabID)
	tableNotNullCols := makeTableNotNullCols(sb.md, tabID)
	return sb.colStatLeaf(colSet, tableStats, tableFD, tableNotNullCols)
}

func (sb *statisticsBuilder) colAvgSize(tabID opt.TableID, col opt.ColumnID) uint64 {
	tableStats := sb.makeTableStatistics(tabID)
	ord := tabID.ColumnOrdinal(col)
	if ord >= len(tableStats.AvgColSizes) {
		return defaultColSize
	}
	if avgSize := tableStats.AvgColSizes[ord]; avgSize > 0 {
		return avgSize
	}
	return defaultColSize
}

// +------+
// | Scan |
// +------+

func (sb *statisticsBuilder) buildScan(scan *ScanExpr, relProps *props.Relational) {
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(scan)

	inputStats := sb.makeTableStatistics(scan.Table)
	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
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
				notNullCols.UnionWith(c.ExtractNotNullCols(sb.ctx, sb.evalCtx))
			}
		}
		sb.filterRelExpr(pred, scan, notNullCols, relProps, s, MakeTableFuncDep(sb.md, scan.Table))
		sb.finalizeFromCardinality(relProps)
		return
	}

	// Inverted indexes can have more tuples than logical rows in the table, so
	// set the row count accordingly. Currently, we only make this adjustment if
	// there is no inverted constraint. If there is an inverted constraint, the
	// row count is adjusted in constrainScan.
	if scan.IsInvertedScan(sb.md) && scan.InvertedConstraint == nil {
		idx := sb.md.Table(scan.Table).Index(scan.Index)
		invertedConstrainedCol := scan.Table.ColumnID(idx.InvertedColumn().Ordinal())
		// TODO(mgartner): Set distinctCount to something correct. See the
		// related TODO in constrainScan.
		const distinctCount = math.MaxFloat64
		colSet := opt.MakeColSet(invertedConstrainedCol)
		sb.ensureColStat(colSet, distinctCount, scan, s)
		inputStat, _ := sb.colStatFromInput(colSet, scan)
		if inputHist := inputStat.Histogram; inputHist != nil {
			// If we have a histogram, set the row count to its total,
			// unfiltered count. This is needed because s.RowCount is currently
			// the row count of the table, but should instead reflect the number
			// of inverted index entries.
			s.RowCount = max(inputHist.ValuesCount(), 1)
		}
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
	keyCtx := constraint.KeyContext{Ctx: sb.ctx, EvalCtx: sb.evalCtx, Columns: scan.Constraint.Columns}

	// Make a copy of the stats so we don't modify the original.
	spanStatsUnion.CopyFrom(s)
	origRowCount := s.RowCount
	origSelectivity := s.Selectivity

	// Get the stats for each span and union them together.
	c.InitSingleSpan(&keyCtx, scan.Constraint.Spans.Get(0))
	sb.constrainScan(scan, &c, pred, relProps, &spanStatsUnion)
	s.RowCount = origRowCount
	s.Selectivity = origSelectivity
	for i, n := 1, scan.Constraint.Spans.Count(); i < n; i++ {
		spanStats.CopyFrom(s)
		c.InitSingleSpan(&keyCtx, scan.Constraint.Spans.Get(i))
		sb.constrainScan(scan, &c, pred, relProps, &spanStats)
		spanStatsUnion.UnionWith(&spanStats)
		s.RowCount = origRowCount
		s.Selectivity = origSelectivity
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
	var unapplied filterCount
	var constrainedCols, histCols opt.ColSet
	idx := sb.md.Table(scan.Table).Index(scan.Index)

	// Calculate distinct counts and histograms for inverted constrained columns
	// -------------------------------------------------------------------------
	if scan.InvertedConstraint != nil {
		// The constrained column is the inverted column in the inverted index.
		// Using scan.Cols here would also include the PK, which we don't want.
		invertedConstrainedCol := scan.Table.ColumnID(idx.InvertedColumn().Ordinal())
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
				// If we have a histogram, set the row count to its total, unfiltered
				// count. This is needed because s.RowCount is currently the row count
				// of the table, but should instead reflect the number of inverted index
				// entries. (Make sure the row count is at least 1. The stats may be
				// stale, and we can end up with weird and inefficient plans if we
				// estimate 0 rows.)
				s.RowCount = max(inputHist.ValuesCount(), 1)
				if colStat, ok := s.ColStats.Lookup(colSet); ok {
					colStat.Histogram = inputHist.InvertedFilter(sb.ctx, scan.InvertedConstraint)
					histCols.Add(invertedConstrainedCol)
					sb.updateDistinctCountFromHistogram(colStat, inputStat.DistinctCount)
				}
			} else {
				// Just assume a single closed span such as ["\xfd", "\xfe").
				// This corresponds to two "conjuncts" as defined in
				// numConjunctsInConstraint.
				unapplied.unknown += 2
			}
		} else {
			// Assume a single closed span.
			unapplied.unknown += 2
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
		predConstrainedCols, predHistCols :=
			sb.applyFilters(pred, scan, relProps, false /* skipOrTermAccounting */, &unapplied)
		constrainedCols.UnionWith(predConstrainedCols)
		constrainedCols = sb.tryReduceCols(constrainedCols, s, MakeTableFuncDep(sb.md, scan.Table))
		histCols.UnionWith(predHistCols)
	}

	// Set null counts to 0 for non-nullable columns
	// ---------------------------------------------
	notNullCols := relProps.NotNullCols.Copy()
	if constraint != nil {
		// Add any not-null columns from this constraint.
		notNullCols.UnionWith(constraint.ExtractNotNullCols(sb.ctx, sb.evalCtx))
	}
	// Add any not-null columns from the predicate constraints.
	for i := range pred {
		if c := pred[i].ScalarProps().Constraints; c != nil {
			notNullCols.UnionWith(c.ExtractNotNullCols(sb.ctx, sb.evalCtx))
		}
	}
	sb.updateNullCountsFromNotNullCols(notNullCols, s)

	// Calculate row count and selectivity
	// -----------------------------------
	corr := sb.correlationFromMultiColDistinctCounts(constrainedCols, scan, s)
	s.ApplySelectivity(sb.selectivityFromConstrainedCols(constrainedCols, histCols, scan, s, corr))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(unapplied))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(scan, notNullCols, constrainedCols))
}

func (sb *statisticsBuilder) colStatScan(colSet opt.ColSet, scan *ScanExpr) *props.ColumnStatistic {
	relProps := scan.Relational()
	s := relProps.Statistics()

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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(sel)
	inputStats := sel.Input.Relational().Statistics()
	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)

	sb.filterRelExpr(sel.Filters, sel, relProps.NotNullCols, relProps, s, &sel.Input.Relational().FuncDeps)

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatSelect(
	colSet opt.ColSet, sel *SelectExpr,
) *props.ColumnStatistic {
	relProps := sel.Relational()
	s := relProps.Statistics()
	inputStats := sel.Input.Relational().Statistics()
	colStat := sb.copyColStatFromChild(colSet, sel, s)

	// It's not safe to use s.Selectivity, because it's possible that some of the
	// filter conditions were pushed down into the input after s.Selectivity
	// was calculated. For example, an index scan or index join created during
	// exploration could absorb some of the filter conditions.
	selectivity := props.MakeSelectivityFromFraction(s.RowCount, inputStats.RowCount)
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(prj)

	inputStats := prj.Input.Relational().Statistics()

	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatProject(
	colSet opt.ColSet, prj *ProjectExpr,
) *props.ColumnStatistic {
	relProps := prj.Relational()
	s := relProps.Statistics()

	// Columns may be passed through from the input, or they may reference a
	// higher scope (in the case of a correlated subquery), or they
	// may be synthesized by the projection operation.
	inputCols := sb.colStatCols(prj.Input.Relational())
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
				// Before checking OuterCols, try to replace any virtual computed column
				// expressions with the corresponding virtual column.
				if sb.evalCtx.SessionData().OptimizerUseVirtualComputedColumnStats &&
					!s.VirtualCols.Empty() {
					element := sb.factorOutVirtualCols(
						item.Element, &item.scalar.Shared, s.VirtualCols, sb.mem,
					).(opt.ScalarExpr)
					if element != item.Element {
						newItem := constructProjectionsItem(element, item.Col, sb.mem)
						item = &newItem
					}
				}

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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
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
	inputStats := invFilter.Input.Relational().Statistics()
	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	corr := sb.correlationFromMultiColDistinctCounts(constrainedCols, invFilter, s)
	s.ApplySelectivity(sb.selectivityFromConstrainedCols(constrainedCols, histCols, invFilter, s, corr))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(invFilter, relProps.NotNullCols, constrainedCols))

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatInvertedFilter(
	colSet opt.ColSet, invFilter *InvertedFilterExpr,
) *props.ColumnStatistic {
	relProps := invFilter.Relational()
	s := relProps.Statistics()
	inputStats := invFilter.Input.Relational().Statistics()
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

	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(join)

	leftStats := h.leftProps.Statistics()
	rightStats := h.rightProps.Statistics()
	leftCols := sb.colStatCols(h.leftProps)
	rightCols := sb.colStatCols(h.rightProps)
	equivReps := h.filtersFD.EquivReps()

	switch h.joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// For semi- and anti-joins, colStatJoin will only be called with columns
		// from the left side.
		s.VirtualCols.UnionWith(leftStats.VirtualCols)
	default:
		s.VirtualCols.UnionWith(leftStats.VirtualCols)
		s.VirtualCols.UnionWith(rightStats.VirtualCols)
	}

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
	var unapplied filterCount
	constrainedCols, histCols :=
		sb.applyFilters(h.filters, join, relProps, true /* skipOrTermAccounting */, &unapplied)

	// Try to reduce the number of columns used for selectivity
	// calculation based on functional dependencies.
	constrainedCols = sb.tryReduceJoinCols(
		constrainedCols,
		s,
		leftCols,
		rightCols,
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
			equivReps, leftCols, rightCols, &h.filtersFD, join, s,
		))
		var oredTermSelectivity props.Selectivity
		oredTermSelectivity =
			sb.selectivityFromOredEquivalencies(h, join, s, &unapplied, true /* semiJoin */)
		s.ApplySelectivity(oredTermSelectivity)

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
		var oredTermSelectivity props.Selectivity
		oredTermSelectivity = sb.selectivityFromOredEquivalencies(h, join, s, &unapplied, false /* semiJoin */)
		s.ApplySelectivity(oredTermSelectivity)
	}

	if join.Op() == opt.InvertedJoinOp || hasInvertedJoinCond(h.filters) {
		s.ApplySelectivity(sb.selectivityFromInvertedJoinCondition(join, s))
	}
	corr := sb.correlationFromMultiColDistinctCountsForJoin(constrainedCols, leftCols, rightCols, join, s)
	s.ApplySelectivity(sb.selectivityFromConstrainedCols(constrainedCols, histCols, join, s, corr))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(unapplied))

	// Ignore columns that are already null in the input when calculating
	// selectivity from null-removing filters - the selectivity would always be
	// 1.
	//
	// NOTE: Aliasing constrainedCols without copying it is safe here because
	// constrainedCols is not used after this point of the function. We set
	// constrainedCols to the empty set to prevent future changes to this
	// function from unknowingly breaking this invariant.
	var ignoreCols opt.ColSet
	ignoreCols, constrainedCols = constrainedCols, opt.ColSet{}
	_ = constrainedCols
	if relProps.NotNullCols.Intersects(h.leftProps.NotNullCols) ||
		relProps.NotNullCols.Intersects(h.rightProps.NotNullCols) {
		ignoreCols = ignoreCols.Union(h.leftProps.NotNullCols)
		ignoreCols.UnionWith(h.rightProps.NotNullCols)
	}
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(join, relProps.NotNullCols, ignoreCols))

	// Update distinct counts based on equivalencies; this should happen after
	// selectivityFromMultiColDistinctCounts and selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, &h.filtersFD, join, relProps.NotNullCols, s)

	switch h.joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Keep only column stats from the left side.
		s.ColStats.RemoveIntersecting(rightCols)
	}

	// The above calculation is for inner joins. Other joins need to remove stats
	// that involve outer columns.
	switch h.joinType {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		// Keep only column stats from the right side. The stats from the left side
		// are not valid.
		s.ColStats.RemoveIntersecting(leftCols)

	case opt.RightJoinOp:
		// Keep only column stats from the left side. The stats from the right side
		// are not valid.
		s.ColStats.RemoveIntersecting(rightCols)

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
					leftSideCols.Empty(),
					rightSideCols.Empty(),
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
	s := relProps.Statistics()

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

	leftCols := sb.colStatCols(leftProps)
	rightCols := sb.colStatCols(rightProps)

	switch joinType {
	case opt.SemiJoinOp, opt.SemiJoinApplyOp, opt.AntiJoinOp, opt.AntiJoinApplyOp:
		// Column stats come from left side of join.
		colStat := sb.copyColStat(colSet, s, sb.colStatFromJoinLeft(colSet, join))
		colStat.ApplySelectivity(s.Selectivity, leftProps.Statistics().RowCount)
		sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
		return colStat

	default:
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
		inputRowCount := leftProps.Statistics().RowCount * rightProps.Statistics().RowCount
		leftNullCount := leftProps.Statistics().RowCount
		rightNullCount := rightProps.Statistics().RowCount
		leftColsAreEmpty := !leftCols.Intersects(colSet)
		rightColsAreEmpty := !rightCols.Intersects(colSet)
		if rightColsAreEmpty {
			colStat = sb.copyColStat(colSet, s, sb.colStatFromJoinLeft(colSet, join))
			leftNullCount = colStat.NullCount
			switch joinType {
			case opt.InnerJoinOp, opt.InnerJoinApplyOp, opt.RightJoinOp:
				colStat.ApplySelectivity(s.Selectivity, inputRowCount)
			}
		} else if leftColsAreEmpty {
			colStat = sb.copyColStat(colSet, s, sb.colStatFromJoinRight(colSet, join))
			rightNullCount = colStat.NullCount
			switch joinType {
			case opt.InnerJoinOp, opt.InnerJoinApplyOp, opt.LeftJoinOp, opt.LeftJoinApplyOp:
				colStat.ApplySelectivity(s.Selectivity, inputRowCount)
			}
		} else {
			// Make a copy of the input column stats so we don't modify the originals.
			leftColStat := *sb.colStatFromJoinLeft(leftCols.Intersection(colSet), join)
			rightColStat := *sb.colStatFromJoinRight(rightCols.Intersection(colSet), join)

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
			leftProps.Statistics().RowCount,
			rightNullCount,
			rightProps.Statistics().RowCount,
		)

		switch joinType {
		case opt.LeftJoinOp, opt.LeftJoinApplyOp, opt.RightJoinOp, opt.FullJoinOp:
			sb.adjustNullCountsForOuterJoins(
				colStat,
				joinType,
				leftColsAreEmpty,
				rightColsAreEmpty,
				leftNullCount,
				leftProps.Statistics().RowCount,
				rightNullCount,
				rightProps.Statistics().RowCount,
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
// The caller should ensure that if leftColsAreEmpty is true then leftNullCount
// equals leftRowCount, and if rightColsAreEmpty is true then rightNullCount
// equals rightRowCount.
func (sb *statisticsBuilder) adjustNullCountsForOuterJoins(
	colStat *props.ColumnStatistic,
	joinType opt.Operator,
	leftColsAreEmpty, rightColsAreEmpty bool,
	leftNullCount, leftRowCount, rightNullCount, rightRowCount, rowCount, innerJoinRowCount float64,
) {
	// Adjust null counts for non-inner joins, adding nulls created due to column
	// extension - such as right columns for non-matching rows in left joins.
	switch joinType {
	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		if !rightColsAreEmpty && leftNullCount > 0 && rowCount > innerJoinRowCount {
			addedRows := max(rowCount-innerJoinRowCount, epsilon)
			colStat.NullCount += addedRows * leftNullCount / leftRowCount
		}

	case opt.RightJoinOp:
		if !leftColsAreEmpty && rightNullCount > 0 && rowCount > innerJoinRowCount {
			addedRows := max(rowCount-innerJoinRowCount, epsilon)
			colStat.NullCount += addedRows * rightNullCount / rightRowCount
		}

	case opt.FullJoinOp:
		if !leftColsAreEmpty && rightNullCount > 0 && rightRowCount > innerJoinRowCount {
			addedRows := max(rightRowCount-innerJoinRowCount, epsilon)
			colStat.NullCount += addedRows * rightNullCount / rightRowCount
		}
		if !rightColsAreEmpty && leftNullCount > 0 && leftRowCount > innerJoinRowCount {
			addedRows := max(leftRowCount-innerJoinRowCount, epsilon)
			colStat.NullCount += addedRows * leftNullCount / leftRowCount
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(indexJoin)

	inputStats := indexJoin.Input.Relational().Statistics()

	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	s.VirtualCols.UnionWith(sb.makeTableStatistics(indexJoin.Table).VirtualCols)
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatIndexJoin(
	colSet opt.ColSet, join *IndexJoinExpr,
) *props.ColumnStatistic {
	relProps := join.Relational()
	s := relProps.Statistics()

	inputProps := join.Input.Relational()
	inputCols := sb.colStatCols(inputProps)

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
		inputStats := inputProps.Statistics()
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(zigzag)

	leftStats := zigzag.leftProps.Statistics()
	rightStats := zigzag.rightProps.Statistics()
	equivReps := h.filtersFD.EquivReps()

	// We assume that we only plan zigzag joins in cases where the result set
	// will have a row count smaller than or equal to the left/right index
	// row counts, and where the left and right sides are indexes on the same
	// table. Their row count should be the same, so use either row count.
	s.RowCount = leftStats.RowCount

	s.VirtualCols.UnionWith(leftStats.VirtualCols)
	s.VirtualCols.UnionWith(rightStats.VirtualCols)

	// Calculate distinct counts for constrained columns
	// -------------------------------------------------
	// Note that fixed columns (i.e. columns constrained to constant values)
	// specified in zigzag.FixedVals and zigzag.{Left,Right}FixedCols
	// still have corresponding filters in zigzag.On. So we don't need
	// to iterate through FixedCols here if we are already processing the ON
	// clause.
	var unapplied filterCount
	constrainedCols, histCols :=
		sb.applyFilters(zigzag.On, zigzag, relProps, false /* skipOrTermAccounting */, &unapplied)

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
	leftIndexInverted := tab.Index(zigzag.LeftIndex).Type() == idxtype.INVERTED
	rightIndexInverted := tab.Index(zigzag.RightIndex).Type() == idxtype.INVERTED
	if leftIndexInverted {
		unapplied.unknown += len(zigzag.LeftFixedCols) * 2
	}
	if rightIndexInverted {
		unapplied.unknown += len(zigzag.RightFixedCols) * 2
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
	if !leftIndexInverted && !rightIndexInverted {
		// A zigzag join is equivalent to a Select with Filters above a base table
		// scan, in terms of which rows are included in the output. The selectivity
		// estimate should never deviate between these two operations, so we apply
		// the exact same calculations here as in buildSelect -> filterRelExpr to
		// ensure the estimates always match. This is currently only done for
		// non-inverted indexes where the filters can be pushed to the zigzag join
		// ON clause.
		// TODO(msirek): Validate stats for inverted index zigzag join match
		//               non-zigzag join stats.
		corr := sb.correlationFromMultiColDistinctCounts(constrainedCols, zigzag, s)
		s.ApplySelectivity(sb.selectivityFromConstrainedCols(constrainedCols, histCols, zigzag, s, corr))
	} else {
		multiColSelectivity, _, _ := sb.selectivityFromMultiColDistinctCounts(constrainedCols, zigzag, s)
		s.ApplySelectivity(multiColSelectivity)
	}
	s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, &relProps.FuncDeps, zigzag, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(unapplied))
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(groupNode)

	groupingPrivate := groupNode.Private().(*GroupingPrivate)
	groupingColSet := groupingPrivate.GroupingCols

	inputStats := sb.statsFromChild(groupNode, 0 /* childIdx */)
	s.VirtualCols.UnionWith(inputStats.VirtualCols)

	if groupingColSet.Empty() {
		if groupNode.Op() == opt.ScalarGroupByOp {
			// ScalarGroupBy always returns exactly one row.
			s.RowCount = 1
		} else {
			// GroupBy with empty grouping columns returns 0 or 1 rows, depending
			// on whether input has rows. If input has < 1 row, use that, as that
			// represents the probability of having 0 vs. 1 rows.
			s.RowCount = min(1, inputStats.RowCount)
		}
	} else {
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
	s := relProps.Statistics()

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
		// TODO(harding): We should be able to get const-agg or first-agg stats from
		// the input.
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(setNode)

	leftStats := sb.statsFromChild(setNode, 0 /* childIdx */)
	rightStats := sb.statsFromChild(setNode, 1 /* childIdx */)

	// TODO(michae2): Set operations and with-scans currently act as barriers for
	// VirtualCols, due to the column ID translation. To fix this we would need to
	// rewrite virtual computed column expressions in terms of the translated
	// column IDs, and would need to store the rewritten expressions somewhere
	// other than TableMeta.

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
	s := relProps.Statistics()
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
func (sb *statisticsBuilder) buildValues(values ValuesContainer, relProps *props.Relational) {
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(values)

	s.RowCount = float64(values.Len())
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatValues(
	colSet opt.ColSet, values *ValuesExpr,
) *props.ColumnStatistic {
	s := values.Relational().Statistics()
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

func (sb *statisticsBuilder) colStatLiteralValues(
	colSet opt.ColSet, values *LiteralValuesExpr,
) *props.ColumnStatistic {
	s := values.Relational().Statistics()
	if values.Len() == 0 {
		colStat, _ := s.ColStats.Add(colSet)
		return colStat
	}

	// Determine distinct count from the number of distinct memo groups. Use a
	// map to find the exact count of distinct values for the columns in colSet.
	// Use a hash to combine column values (this does not have to be exact).
	distinct := make(map[uint64]struct{}, len(values.Cols))
	// Determine null count by looking at tuples that have only NullOps in them.
	nullCount := 0

	for rowIndex := 0; rowIndex < values.Len(); rowIndex++ {
		var h hasher
		h.Init()
		hasNonNull := false
		for colIndex, col := range values.Cols {
			if colSet.Contains(col) {
				elem := values.Rows.Rows.Get(rowIndex, colIndex).(tree.Datum)
				if elem.ResolvedType().Family() == types.UnknownFamily {
					hasNonNull = true
				}
				h.HashDatum(elem)
			}
		}
		if !hasNonNull {
			nullCount++
		}
		distinct[uint64(h.hash)] = struct{}{}
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(limit)

	inputStats := limit.Input.Relational().Statistics()

	// Copy row count from input.
	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)

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
	s := relProps.Statistics()
	inputStats := limit.Input.Relational().Statistics()
	colStat := sb.copyColStatFromChild(colSet, limit, s)

	// Scale distinct count based on the selectivity of the limit operation.
	colStat.ApplySelectivity(s.Selectivity, inputStats.RowCount)
	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

func (sb *statisticsBuilder) buildTopK(topK *TopKExpr, relProps *props.Relational) {
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(topK)

	inputStats := topK.Input.Relational().Statistics()

	// Copy row count from input.
	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)

	// Update row count if row count and k are non-zero.
	if inputStats.RowCount > 0 && topK.K > 0 {
		s.RowCount = min(float64(topK.K), inputStats.RowCount)
		s.Selectivity = props.MakeSelectivity(s.RowCount / inputStats.RowCount)
	}

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatTopK(colSet opt.ColSet, topK *TopKExpr) *props.ColumnStatistic {
	relProps := topK.Relational()
	s := relProps.Statistics()
	inputStats := topK.Input.Relational().Statistics()
	colStat := sb.copyColStatFromChild(colSet, topK, s)

	// Scale distinct count based on the selectivity of the topK operation.
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(offset)

	inputStats := offset.Input.Relational().Statistics()

	// Copy row count from input.
	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)

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
	s := relProps.Statistics()
	inputStats := offset.Input.Relational().Statistics()
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
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
	s := max1Row.Relational().Statistics()
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = 1
	colStat.NullCount = s.RowCount * UnknownNullCountRatio
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(ord)

	inputStats := ord.Input.Relational().Statistics()

	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatOrdinality(
	colSet opt.ColSet, ord *OrdinalityExpr,
) *props.ColumnStatistic {
	relProps := ord.Relational()
	s := relProps.Statistics()

	colStat, _ := s.ColStats.Add(colSet)

	inputColStat := sb.colStatFromChild(colSet, ord, 0 /* childIdx */)

	if colSet.Contains(ord.ColID) {
		// The ordinality column is a key, so every row is distinct.
		colStat.DistinctCount = s.RowCount
		colStat.NullCount = 0
	} else {
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(window)

	inputStats := window.Input.Relational().Statistics()

	// The row count of a window is equal to the row count of its input.
	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatWindow(
	colSet opt.ColSet, window *WindowExpr,
) *props.ColumnStatistic {
	relProps := window.Relational()
	s := relProps.Statistics()

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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(projectSet)

	// The row count of a zip operation is equal to the maximum row count of its
	// children.
	var zipRowCount float64
	for i := range projectSet.Zip {
		if fn, ok := projectSet.Zip[i].Fn.(*FunctionExpr); ok {
			if fn.Overload.IsGenerator() {
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
	inputStats := projectSet.Input.Relational().Statistics()
	s.RowCount = zipRowCount * inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatProjectSet(
	colSet opt.ColSet, projectSet *ProjectSetExpr,
) *props.ColumnStatistic {
	s := projectSet.Relational().Statistics()
	if s.RowCount == 0 {
		// Short cut if cardinality is 0.
		colStat, _ := s.ColStats.Add(colSet)
		return colStat
	}

	inputProps := projectSet.Input.Relational()
	inputStats := inputProps.Statistics()
	inputCols := sb.colStatCols(inputProps)

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
				if fn, ok := item.Fn.(*FunctionExpr); ok && fn.Overload.IsGenerator() {
					// The columns(s) contain a generator function.
					// TODO(rytaft): We may want to determine which generator function the
					// requested columns correspond to, and estimate the distinct count and
					// null count based on the type of generator function and its parameters.
					zipColsDistinctCount *= unknownGeneratorRowCount * unknownGeneratorDistinctCountRatio
					zipColsNullCount *= UnknownNullCountRatio
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

				if item.ScalarProps().OuterCols.Intersects(inputCols) {
					// The column(s) are correlated with the input, so they may have a
					// distinct value for each distinct row of the input.
					zipColsDistinctCount *= inputStats.RowCount * UnknownDistinctCountRatio
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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}

	s.Available = bindingProps.Statistics().Available
	s.RowCount = bindingProps.Statistics().RowCount
	if withScan.CheckInput {
		s.RowCount = max(s.RowCount, sb.checkInputMinRowCount)
	}

	// TODO(michae2): Set operations and with-scans currently act as barriers for
	// VirtualCols, due to the column ID translation. To fix this we would need to
	// rewrite virtual computed column expressions in terms of the translated
	// column IDs, and would need to store the rewritten expressions somewhere
	// other than TableMeta.

	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatWithScan(
	colSet opt.ColSet, withScan *WithScanExpr,
) *props.ColumnStatistic {
	s := withScan.Relational().Statistics()

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
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(mutation)

	inputStats := sb.statsFromChild(mutation, 0 /* childIdx */)

	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatMutation(
	colSet opt.ColSet, mutation RelExpr,
) *props.ColumnStatistic {
	s := mutation.Relational().Statistics()
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

// +------+
// | Lock |
// +------+

func (sb *statisticsBuilder) buildLock(lock *LockExpr, relProps *props.Relational) {
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(lock)

	inputStats := lock.Input.Relational().Statistics()

	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatLock(colSet opt.ColSet, lock *LockExpr) *props.ColumnStatistic {
	s := lock.Relational().Statistics()

	inColStat := sb.colStatFromChild(colSet, lock, 0 /* childIdx */)

	// Construct colstat using the corresponding input stats.
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = inColStat.DistinctCount
	colStat.NullCount = inColStat.NullCount
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +--------------+
// | VectorSearch |
// +--------------+

func (sb *statisticsBuilder) buildVectorSearch(
	search *VectorSearchExpr, relProps *props.Relational,
) {
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	inputStats := sb.makeTableStatistics(search.Table)
	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)

	// Expect the number of candidates to be at most 2 times the number of
	// neighbors requested.
	// TODO(drewk, mw5h): determine if we need to adjust this multiplier or do
	// something more sophisticated. Maybe we should also expect that at most some
	// fraction of the table is returned as candidates?
	const candidateMultiplier = 2
	expectedCandidates := float64(candidateMultiplier * search.TargetNeighborCount)
	if s.RowCount > expectedCandidates {
		s.RowCount = expectedCandidates
	}

	// TODO(drewk, mw5h): consider adding stats for the PrefixConstraint.
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatVectorSearch(
	colSet opt.ColSet, search *VectorSearchExpr,
) *props.ColumnStatistic {
	relProps := search.Relational()
	s := relProps.Statistics()

	inputColStat := sb.colStatTable(search.Table, colSet)
	colStat := sb.copyColStat(colSet, s, inputColStat)

	if sb.shouldUseHistogram(relProps) {
		colStat.Histogram = inputColStat.Histogram
	}

	if s.Selectivity != props.OneSelectivity {
		tableStats := sb.makeTableStatistics(search.Table)
		colStat.ApplySelectivity(s.Selectivity, tableStats.RowCount)
	}

	if colSet.Intersects(relProps.NotNullCols) {
		colStat.NullCount = 0
	}

	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +-----------------------+
// | VectorMutationSearch |
// +-----------------------+

func (sb *statisticsBuilder) buildVectorMutationSearch(
	search *VectorMutationSearchExpr, relProps *props.Relational,
) {
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(search)

	inputStats := search.Input.Relational().Statistics()

	// VectorMutationSearch operators do not change the cardinality of the input.
	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatVectorMutationSearch(
	colSet opt.ColSet, search *VectorMutationSearchExpr,
) *props.ColumnStatistic {
	s := search.Relational().Statistics()

	inColStat := sb.colStatFromChild(colSet, search, 0 /* childIdx */)

	// Construct colstat using the corresponding input stats.
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = inColStat.DistinctCount
	colStat.NullCount = inColStat.NullCount
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +---------+
// | Barrier |
// +---------+

func (sb *statisticsBuilder) buildBarrier(barrier *BarrierExpr, relProps *props.Relational) {
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short cut if cardinality is 0.
		return
	}
	s.Available = sb.availabilityFromInput(barrier)

	inputStats := barrier.Input.Relational().Statistics()

	s.RowCount = inputStats.RowCount
	s.VirtualCols.UnionWith(inputStats.VirtualCols)
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatBarrier(
	colSet opt.ColSet, barrier *BarrierExpr,
) *props.ColumnStatistic {
	s := barrier.Relational().Statistics()

	inColStat := sb.colStatFromChild(colSet, barrier, 0 /* childIdx */)

	// Construct colstat using the corresponding input stats.
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = inColStat.DistinctCount
	colStat.NullCount = inColStat.NullCount
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +------+
// | Call |
// +------+

func (sb *statisticsBuilder) buildCall(call *CallExpr, relProps *props.Relational) {
	s := relProps.Statistics()
	if zeroCardinality := s.Init(relProps, sb.minRowCount); zeroCardinality {
		// Short-cut if cardinality is 0.
		return
	}
	s.Available = true
	s.RowCount = 1
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatCall(colSet opt.ColSet, call *CallExpr) *props.ColumnStatistic {
	s := call.Relational().Statistics()
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = 1
	colStat.NullCount = s.RowCount * UnknownNullCountRatio
	sb.finalizeFromRowCountAndDistinctCounts(colStat, s)
	return colStat
}

// +-----------------+
// | Sequence Select |
// +-----------------+

func (sb *statisticsBuilder) buildSequenceSelect(relProps *props.Relational) {
	s := relProps.Statistics()
	s.Available = true
	s.RowCount = 1
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatSequenceSelect(
	colSet opt.ColSet, seq *SequenceSelectExpr,
) *props.ColumnStatistic {
	s := seq.Relational().Statistics()

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
	s := relProps.Statistics()
	s.Available = false
	s.RowCount = unknownGeneratorRowCount
	sb.finalizeFromCardinality(relProps)
}

func (sb *statisticsBuilder) colStatUnknown(
	colSet opt.ColSet, relProps *props.Relational,
) *props.ColumnStatistic {
	s := relProps.Statistics()

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
			"copyColStat colSet: %v inputColSet: %v\n", redact.Safe(colSet), redact.Safe(inputColStat.Cols),
		))
	}
	colStat, _ := s.ColStats.Add(colSet)
	colStat.DistinctCount = inputColStat.DistinctCount
	colStat.NullCount = inputColStat.NullCount
	return colStat
}

func (sb *statisticsBuilder) finalizeFromCardinality(relProps *props.Relational) {
	s := relProps.Statistics()

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
			if singleColStat, ok := s.ColStats.LookupSingleton(col); ok {
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
	if sb.evalCtx.SessionData().OptimizerAlwaysUseHistograms {
		return true
	}
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
		return e.Relational().Statistics().RowCount

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
				return e.Relational().Statistics().RowCount
			}
			lookupJoinPrivate = &t.LookupJoinPrivate
		}

		// We need to determine the row count of the join before the
		// ON conditions are applied.
		withoutOn := sb.mem.MemoizeLookupJoin(t.Input, nil /* on */, lookupJoinPrivate)
		return withoutOn.Relational().Statistics().RowCount

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
				return e.Relational().Statistics().RowCount
			}
			invertedJoinPrivate = &t.InvertedJoinPrivate
		}

		// We need to determine the row count of the join before the
		// ON conditions are applied.
		withoutOn := sb.mem.MemoizeInvertedJoin(t.Input, nil /* on */, invertedJoinPrivate)
		return withoutOn.Relational().Statistics().RowCount

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
				return e.Relational().Statistics().RowCount
			}
			mergeJoinPrivate = &t.MergeJoinPrivate
		}

		// We need to determine the row count of the join before the
		// ON conditions are applied.
		withoutOn := sb.mem.MemoizeMergeJoin(t.Left, t.Right, nil /* on */, mergeJoinPrivate)
		return withoutOn.Relational().Statistics().RowCount

	default:
		if !opt.IsJoinOp(e) {
			panic(errors.AssertionFailedf("rowsProcessed not supported for operator type %v", redact.Safe(e.Op())))
		}

		leftCols := sb.colStatCols(e.Child(0).(RelExpr).Relational())
		rightCols := sb.colStatCols(e.Child(1).(RelExpr).Relational())
		filters := e.Child(2).(*FiltersExpr)

		// Remove ON conditions that are not equality conditions,
		on := ExtractJoinEqualityFilters(leftCols, rightCols, *filters)

		switch t := e.(type) {
		// The number of rows processed for semi and anti joins is closer to the
		// number of output rows for an equivalent inner join.
		case *SemiJoinExpr:
			e = sb.mem.MemoizeInnerJoin(t.Left, t.Right, on, &t.JoinPrivate)
		case *SemiJoinApplyExpr:
			e = sb.mem.MemoizeInnerJoinApply(t.Left, t.Right, on, &t.JoinPrivate)
		case *AntiJoinExpr:
			e = sb.mem.MemoizeInnerJoin(t.Left, t.Right, on, &t.JoinPrivate)
		case *AntiJoinApplyExpr:
			e = sb.mem.MemoizeInnerJoinApply(t.Left, t.Right, on, &t.JoinPrivate)

		default:
			if len(on) == len(*filters) {
				// No filters were removed.
				return e.Relational().Statistics().RowCount
			}

			switch t := e.(type) {
			case *InnerJoinExpr:
				e = sb.mem.MemoizeInnerJoin(t.Left, t.Right, on, &t.JoinPrivate)
			case *InnerJoinApplyExpr:
				e = sb.mem.MemoizeInnerJoinApply(t.Left, t.Right, on, &t.JoinPrivate)
			case *LeftJoinExpr:
				e = sb.mem.MemoizeLeftJoin(t.Left, t.Right, on, &t.JoinPrivate)
			case *LeftJoinApplyExpr:
				e = sb.mem.MemoizeLeftJoinApply(t.Left, t.Right, on, &t.JoinPrivate)
			case *RightJoinExpr:
				e = sb.mem.MemoizeRightJoin(t.Left, t.Right, on, &t.JoinPrivate)
			case *FullJoinExpr:
				e = sb.mem.MemoizeFullJoin(t.Left, t.Right, on, &t.JoinPrivate)
			default:
				panic(errors.AssertionFailedf("join type %v not handled", redact.Safe(e.Op())))
			}
		}
		return e.Relational().Statistics().RowCount
	}
}

//////////////////////////////////////////////////
// Helper functions for selectivity calculation //
//////////////////////////////////////////////////

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

	if conjunct.Condition.Op() == opt.JsonExistsOp {
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

	// This case applies for both array-array exprs and json-array exprs.
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
	var unapplied filterCount
	constrainedCols, histCols :=
		sb.applyFilters(filters, e, relProps, false /* skipOrTermAccounting */, &unapplied)

	// Try to reduce the number of columns used for selectivity
	// calculation based on functional dependencies.
	constrainedCols = sb.tryReduceCols(constrainedCols, s, inputFD)

	// Set null counts to 0 for non-nullable columns
	// ---------------------------------------------
	sb.updateNullCountsFromNotNullCols(notNullCols, s)

	// Calculate row count and selectivity
	// -----------------------------------
	corr := sb.correlationFromMultiColDistinctCounts(constrainedCols, e, s)
	s.ApplySelectivity(sb.selectivityFromConstrainedCols(constrainedCols, histCols, e, s, corr))
	s.ApplySelectivity(sb.selectivityFromEquivalencies(equivReps, &relProps.FuncDeps, e, s))
	s.ApplySelectivity(sb.selectivityFromUnappliedConjuncts(unapplied))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(e, notNullCols, constrainedCols))

	// Update distinct and null counts based on equivalencies; this should
	// happen after selectivityFromMultiColDistinctCounts and
	// selectivityFromEquivalencies.
	sb.applyEquivalencies(equivReps, &equivFD, e, notNullCols, s)
}

// applyFilters uses constraints to update the distinct counts and histograms
// for the constrained columns in the filter. The changes in the distinct
// counts and histograms will be used later to determine the selectivity of
// the filter. If skipOrTermAccounting is true, OrExpr filters are not counted
// towards numUnappliedConjuncts.
//
// See applyFiltersItem for more details.
func (sb *statisticsBuilder) applyFilters(
	filters FiltersExpr,
	e RelExpr,
	relProps *props.Relational,
	skipOrTermAccounting bool,
	unapplied *filterCount,
) (constrainedCols, histCols opt.ColSet) {
	// Special hack for inverted joins. Add constant filters from the equality
	// conditions.
	// TODO(rytaft): the correct way to do this is probably to fully implement
	// histograms in Project and Join expressions, and use them in
	// selectivityFromEquivalencies. See Issue #38082.
	switch t := e.(type) {
	case *InvertedJoinExpr:
		filters = append(filters, t.ConstFilters...)
	}

	for i := range filters {
		var unappliedLocal filterCount
		constrainedColsLocal, histColsLocal :=
			sb.applyFiltersItem(&filters[i], e, relProps, &unappliedLocal)
		// Selectivity from OrExprs is computed elsewhere when skipOrTermAccounting
		// is true.
		if _, ok := filters[i].Condition.(*OrExpr); !skipOrTermAccounting || !ok {
			unapplied.add(unappliedLocal)
		}
		constrainedCols.UnionWith(constrainedColsLocal)
		histCols.UnionWith(histColsLocal)
	}

	return constrainedCols, histCols
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
	filter *FiltersItem, e RelExpr, relProps *props.Relational, unapplied *filterCount,
) (constrainedCols, histCols opt.ColSet) {
	s := relProps.Statistics()

	// Before checking anything, try to replace any virtual computed column
	// expressions with the corresponding virtual column.
	if sb.evalCtx.SessionData().OptimizerUseVirtualComputedColumnStats &&
		!relProps.Statistics().VirtualCols.Empty() {
		cond := sb.factorOutVirtualCols(
			filter.Condition, &filter.scalar.Shared, relProps.Statistics().VirtualCols, sb.mem,
		).(opt.ScalarExpr)
		if cond != filter.Condition {
			newFilter := constructFiltersItem(cond, sb.mem)
			filter = &newFilter
		}
	}

	if isEqualityWithTwoVars(filter.Condition) {
		// Equalities are handled by applyEquivalencies.
		return opt.ColSet{}, opt.ColSet{}
	}

	// Special case: a trigram similarity filter.
	if isSimilarityFilter(filter.Condition) &&
		sb.evalCtx.SessionData().OptimizerUseImprovedTrigramSimilaritySelectivity {
		unapplied.similarity++
		return opt.ColSet{}, opt.ColSet{}
	}

	// Special case: a placeholder equality filter.
	if col, ok := isPlaceholderEqualityFilter(filter.Condition); ok {
		cols := opt.MakeColSet(col)
		sb.ensureColStat(cols, 1 /* maxDistinctCount */, e, s)
		return cols, opt.ColSet{}
	}

	// Special case: The current conjunct is an inverted join condition which is
	// handled by selectivityFromInvertedJoinCondition.
	if isInvertedJoinCond(filter.Condition) {
		return opt.ColSet{}, opt.ColSet{}
	}

	// Special case: The current conjunct is a JSON or Array Contains
	// operator, or an equality operator with a JSON fetch value operator on
	// the left (for example j->'a' = '1'), or a JSON exists operator. If so,
	// count every path to a leaf node in the RHS as a separate conjunct. If for
	// whatever reason we can't get to the JSON or Array datum or enumerate its
	// paths, count the whole operator as one conjunct.
	filterOp := filter.Condition.Op()
	if filterOp == opt.ContainsOp ||
		filterOp == opt.JsonExistsOp ||
		filterOp == opt.JsonAllExistsOp ||
		filterOp == opt.JsonSomeExistsOp ||
		(filterOp == opt.EqOp && filter.Condition.Child(0).Op() == opt.FetchValOp) {
		numPaths := countPaths(filter)
		if numPaths == 0 {
			unapplied.unknown++
		} else {
			// Multiply the number of paths by 2 to mimic the logic in
			// numConjunctsInConstraint, for constraints like
			// /1: [/'{"a":"b"}' - /'{"a":"b"}'] . That function counts
			// this as 2 conjuncts, and to keep row counts as consistent
			// as possible between competing filtered selects and
			// constrained scans, we apply the same logic here.
			unapplied.unknown += 2 * numPaths
		}
		return opt.ColSet{}, opt.ColSet{}
	}

	// Update constrainedCols after the above check for isEqualityWithTwoVars.
	// We will use constrainedCols later to determine which columns to use for
	// selectivity calculation in selectivityFromMultiColDistinctCounts, and we
	// want to make sure that we don't include columns that were only present in
	// equality conjuncts such as var1=var2. The selectivity of these conjuncts
	// will be accounted for in selectivityFromEquivalencies.
	scalarProps := filter.ScalarProps()
	constrainedCols.UnionWith(scalarProps.OuterCols)
	if scalarProps.Constraints != nil {
		histColsLocal := sb.applyConstraintSet(
			scalarProps.Constraints, scalarProps.TightConstraints, e, relProps, s,
		)
		histCols.UnionWith(histColsLocal)
		if !scalarProps.TightConstraints {
			unapplied.unknown++
			// Mimic constrainScan in the case of no histogram information
			// that assumes a geo function is a single closed span that
			// corresponds to two "conjuncts".
			if isGeoIndexScanCond(filter.Condition) {
				unapplied.unknown++
			}
		}
		return constrainedCols, histCols
	}

	if constraintUnion, numUnappliedDisjuncts := sb.buildDisjunctionConstraints(filter); len(constraintUnion) > 0 {
		if sb.evalCtx.SessionData().OptimizerUseImprovedDisjunctionStats {
			// The filters are one or more disjuncts and tight constraint sets
			// could be built for at least one disjunct. numUnappliedDisjuncts
			// contains the count of disjuncts which are not tight constraints.
			var tmpStats props.Statistics

			selectivities := make([]props.Selectivity, 0, len(constraintUnion)+numUnappliedDisjuncts)
			// Get the stats for each constraint set, apply the selectivity to a
			// temporary stats struct, and combine the disjunct selectivities.
			// union the selectivity and row counts.
			for i := 0; i < len(constraintUnion); i++ {
				tmpStats.CopyFrom(s)
				sb.constrainExpr(e, constraintUnion[i], relProps, &tmpStats)
				selectivities = append(selectivities, tmpStats.Selectivity)
			}
			defaultSelectivity := props.MakeSelectivity(unknownFilterSelectivity)
			for i := 0; i < numUnappliedDisjuncts; i++ {
				selectivities = append(selectivities, defaultSelectivity)
			}

			// TODO(mgartner): Calculate and set the column statistics based on
			// constraintUnion.
			disjunctionSelectivity := combineOredSelectivities(selectivities)
			s.RowCount *= disjunctionSelectivity.AsFloat()
			s.Selectivity.Multiply(disjunctionSelectivity)
		} else if numUnappliedDisjuncts == 0 {
			// The filters are one or more disjunctions and tight constraint
			// sets could be built for each.
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

			// The stats are unioned naively; the selectivity may be greater
			// than 1 and the row count may be greater than the row count of the
			// input stats. We use the minimum selectivity and row count of the
			// unioned stats and the input stats.
			s.Selectivity = props.MinSelectivity(s.Selectivity, unionStats.Selectivity)
			s.RowCount = min(s.RowCount, unionStats.RowCount)
		} else {
			unapplied.unknown++
		}
	} else {
		unapplied.unknown++
	}

	return constrainedCols, histCols
}

// buildDisjunctionConstraints returns a slice of tight constraint sets that are
// built from one or more adjacent Or expressions in filter. This allows more
// accurate stats to be calculated for disjunctions. If any adjacent Or cannot
// be tightly constrained, then numUnappliedDisjuncts is incremented, indicating
// unknownFilterSelectivity should be used for that disjunct.
func (sb *statisticsBuilder) buildDisjunctionConstraints(
	filter *FiltersItem,
) (constraintSet []*constraint.Set, numUnappliedDisjuncts int) {
	expr := filter.Condition

	// If the expression is not an Or, we cannot build disjunction constraint
	// sets.
	or, ok := expr.(*OrExpr)
	if !ok {
		return nil, 0
	}

	cb := constraintsBuilder{md: sb.md, ctx: sb.ctx, evalCtx: sb.evalCtx}

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
			// not an Or, bump the number of unapplied disjuncts.
			numUnappliedDisjuncts++
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

	return constraints, numUnappliedDisjuncts
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
	notNullCols.UnionWith(cs.ExtractNotNullCols(sb.ctx, sb.evalCtx))
	sb.updateNullCountsFromNotNullCols(notNullCols, s)

	// Calculate row count and selectivity
	// -----------------------------------
	corr := sb.correlationFromMultiColDistinctCounts(constrainedCols, e, s)
	s.ApplySelectivity(sb.selectivityFromConstrainedCols(constrainedCols, histCols, e, s, corr))
	s.ApplySelectivity(sb.selectivityFromNullsRemoved(e, notNullCols, constrainedCols))
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
	prefix := c.Prefix(sb.ctx, sb.evalCtx)
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
		// according to UnknownDistinctCountRatio.
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
			// according to UnknownDistinctCountRatio.
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
//
//	/a/b/c: [/1 - /1/2/3] [/1/2/5 - /1/2/8]
//	/c: [/6 - /6]
//
// The first constraint set filters nulls out of column a, and the
// second constraint set filters nulls out of column c.
func (sb *statisticsBuilder) updateNullCountsFromNotNullCols(
	notNullCols opt.ColSet, s *props.Statistics,
) {
	notNullCols.ForEach(func(col opt.ColumnID) {
		colStat, ok := s.ColStats.LookupSingleton(col)
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
//	/a/b/c: [/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]
//	/c: [/6 - /6]
//
// After the first constraint is processed, s.ColStats contains the
// following:
//
//	[a] -> { ... DistinctCount: 1 ... }
//	[b] -> { ... DistinctCount: 1 ... }
//	[c] -> { ... DistinctCount: 5 ... }
//
// After the second constraint is processed, column c is further constrained,
// so s.ColStats contains the following:
//
//	[a] -> { ... DistinctCount: 1 ... }
//	[b] -> { ... DistinctCount: 1 ... }
//	[c] -> { ... DistinctCount: 1 ... }
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
//	/a: [ - 5][10 - 10][15 - 15]
//
// In this case, updateDistinctCountsFromConstraint will infer that there
// are at least two distinct values (10 and 15). This lower bound will be
// returned in the second return value, lastColMinDistinct.
func (sb *statisticsBuilder) updateDistinctCountsFromConstraint(
	c *constraint.Constraint, e RelExpr, s *props.Statistics,
) (applied int, lastColMinDistinct float64) {
	// All of the columns that are part of the prefix have a finite number of
	// distinct values.
	prefix := c.Prefix(sb.ctx, sb.evalCtx)
	keyCtx := constraint.MakeKeyContext(sb.ctx, &c.Columns, sb.evalCtx)

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
				compare, err := startVal.Compare(sb.ctx, sb.evalCtx, val)
				if err != nil {
					panic(err)
				}
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
		if _, _, ok := inputHist.CanFilter(sb.ctx, c); ok {
			colStat.Histogram = inputHist.Filter(sb.ctx, c)
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
	equivGroup.ForEach(func(col opt.ColumnID) {
		colStat, ok := s.ColStats.LookupSingleton(col)
		if !ok {
			colSet := opt.MakeColSet(col)
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
	equivGroup.ForEach(func(col opt.ColumnID) {
		colStat, _ := s.ColStats.LookupSingleton(col)
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
//
//	by taking the product of selectivities of each constrained column. In
//	the general case, this can be represented by the formula:
//
//	                 ┬-┬ ⎛ new_distinct(i) ⎞
//	  selectivity =  │ │ ⎜ --------------- ⎟
//	                 ┴ ┴ ⎝ old_distinct(i) ⎠
//	                i in
//	             {constrained
//	               columns}
//
// (2) If we instead assume there is some correlation between columns, we
//
//	calculate the selectivity using multi-column statistics.
//
//	                ⎛ new_distinct({constrained columns}) ⎞
//	  selectivity = ⎜ ----------------------------------- ⎟
//	                ⎝ old_distinct({constrained columns}) ⎠
//
//	This formula looks simple, but the challenge is that it is difficult
//	to determine the correct value for new_distinct({constrained columns})
//	if each column is not constrained to a single value. For example, if
//	new_distinct(x)=2 and new_distinct(y)=2, new_distinct({x,y}) could be 2,
//	3 or 4. We estimate the new distinct count as follows, using the concept
//	of "soft functional dependency (FD) strength" as defined in [1]:
//
//	  new_distinct({x,y}) = min_distinct + distinct_range * (1 - FD_strength_scaled)
//
//	where
//
//	  min_distinct   = max(new_distinct(x), new_distinct(y))
//	  max_distinct   = new_distinct(x) * new_distinct(y)
//	  distinct_range = max_distinct - min_distinct
//
//	                ⎛ max(old_distinct(x),old_distinct(y)) ⎞
//	  FD_strength = ⎜ ------------------------------------ ⎟
//	                ⎝         old_distinct({x,y})          ⎠
//
//	                    ⎛ max(old_distinct(x), old_distinct(y)) ⎞
//	  min_FD_strength = ⎜ ------------------------------------- ⎟
//	                    ⎝   old_distinct(x) * old_distinct(y)   ⎠
//
//	                       ⎛ FD_strength - min_FD_strength ⎞  // scales FD_strength
//	  FD_strength_scaled = ⎜ ----------------------------- ⎟  // to be between
//	                       ⎝      1 - min_FD_strength      ⎠  // 0 and 1
//
//	Suppose that old_distinct(x)=100 and old_distinct(y)=10. If x and y are
//	perfectly correlated, old_distinct({x,y})=100. Using the example from
//	above, new_distinct(x)=2 and new_distinct(y)=2. Plugging in the values
//	into the equation, we get:
//
//	  FD_strength_scaled  = 1
//	  new_distinct({x,y}) = 2 + (4 - 2) * (1 - 1) = 2
//
//	If x and y are completely independent, however, old_distinct({x,y})=1000.
//	In this case, we get:
//
//	  FD_strength_scaled  = 0
//	  new_distinct({x,y}) = 2 + (4 - 2) * (1 - 0) = 4
//
// Note that even if we calculate the selectivity based on equation (2) above,
// we still want to take equation (1) into account. This is because it is
// possible that there are two predicates that each have selectivity s, but the
// multi-column selectivity is also s. In order to ensure that the cost model
// considers the two predicates combined to be more selective than either one
// individually, we must give some weight to equation (1). Therefore, instead
// of equation (2) we actually return the following selectivity:
//
//	selectivity = (1 - w) * (equation 1) + w * (equation 2)
//
// where w is the constant multiColWeight.
//
// This selectivity will be used later to update the row count and the
// distinct count for the unconstrained columns.
//
// We also return selectivityUpperBound and selectivityLowerBound.
// selectivityUpperBound would be the value of equation (2) if the columns were
// completely correlated. It is equal to the minimum selectivity of any single
// column. selectivityLowerBound would be the value of equation (2) if the
// columns were completely independent. It is the minimum new distinct count
// (min_distinct) divided by the product of the old single column distinct counts
// (or the old row count, whichever is smaller). These values will be used later
// to measure the level of correlation.
//
// [1] Ilyas, Ihab F., et al. "CORDS: automatic discovery of correlations and
//
//	soft functional dependencies." SIGMOD 2004.
func (sb *statisticsBuilder) selectivityFromMultiColDistinctCounts(
	cols opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity, selectivityUpperBound, selectivityLowerBound props.Selectivity) {
	// Respect the session setting OptimizerUseMultiColStats.
	if !sb.evalCtx.SessionData().OptimizerUseMultiColStats {
		selectivity, selectivityUpperBound = sb.selectivityFromSingleColDistinctCounts(cols, e, s)
		return selectivity, selectivityUpperBound, selectivity
	}

	// Make a copy of cols so we can remove columns that are not constrained.
	multiColSet := cols.Copy()

	// First calculate the selectivity from equation (1) (see function comment),
	// and collect the inputs to equation (2).
	multiColSelWithIndepAssumption := props.OneSelectivity
	newSingleColDistinctProduct, oldSingleColDistinctProduct := 1.0, 1.0
	maxNewSingleColDistinct, maxOldSingleColDistinct := float64(0), float64(0)
	multiColNullCount := -1.0
	minSingleColSel := props.OneSelectivity
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		singleColStat, ok := s.ColStats.LookupSingleton(col)
		if !ok {
			multiColSet.Remove(col)
			continue
		}

		inputSingleColStat, inputStats := sb.colStatFromInput(singleColStat.Cols, e)
		singleColSel := sb.selectivityFromDistinctCount(singleColStat, inputSingleColStat, inputStats.RowCount)
		multiColSelWithIndepAssumption.Multiply(singleColSel)

		// Don't bother including columns in the multi-column calculation that
		// don't contribute to the selectivity.
		if singleColSel == props.OneSelectivity {
			multiColSet.Remove(col)
			continue
		}

		// Calculate values needed for the multi-column stats calculation below.
		newSingleColDistinctProduct *= singleColStat.DistinctCount
		oldSingleColDistinctProduct *= inputSingleColStat.DistinctCount
		if singleColStat.DistinctCount > maxNewSingleColDistinct {
			maxNewSingleColDistinct = singleColStat.DistinctCount
		}
		if inputSingleColStat.DistinctCount > maxOldSingleColDistinct {
			maxOldSingleColDistinct = inputSingleColStat.DistinctCount
		}
		minSingleColSel = props.MinSelectivity(singleColSel, minSingleColSel)
		if multiColNullCount < 0 {
			multiColNullCount = inputStats.RowCount
		}
		// Multiply by the expected chance of collisions with nulls already
		// collected.
		multiColNullCount *= singleColStat.NullCount / inputStats.RowCount
	}

	// If we don't need to use a multi-column statistic, we're done.
	if multiColSet.Len() <= 1 {
		return multiColSelWithIndepAssumption, minSingleColSel, multiColSelWithIndepAssumption
	}

	// Otherwise, calculate the selectivity using multi-column stats from
	// equation (2). See the comment above the function definition for details
	// about the formula.
	inputMultiColStat, inputStats := sb.colStatFromInput(multiColSet, e)
	fdStrength := min(maxOldSingleColDistinct/inputMultiColStat.DistinctCount, 1.0)
	minFdStrength := min(maxOldSingleColDistinct/oldSingleColDistinctProduct, fdStrength)
	if minFdStrength < 1 {
		// Scale the fdStrength so it ranges between 0 and 1.
		fdStrength = (fdStrength - minFdStrength) / (1 - minFdStrength)
	}

	// These variables correspond to min_distinct, max_distinct, and distinct_range
	// in the comment above the function definition.
	minNewMultiColDistinct := maxNewSingleColDistinct
	maxNewMultiColDistinct := newSingleColDistinctProduct
	multiColDistinctRange := max(maxNewMultiColDistinct-minNewMultiColDistinct, 0)

	multiColStat, _ := s.ColStats.Add(multiColSet)
	multiColStat.DistinctCount = minNewMultiColDistinct + multiColDistinctRange*(1-fdStrength)
	multiColStat.NullCount = multiColNullCount
	multiColSelectivity := sb.selectivityFromDistinctCount(multiColStat, inputMultiColStat, inputStats.RowCount)

	// multiColSelectivity must be at least as large as
	// multiColSelWithIndepAssumption, since multiColSelWithIndepAssumption
	// corresponds to equation (1).
	multiColSelectivity = props.MaxSelectivity(multiColSelectivity, multiColSelWithIndepAssumption)

	// Now, we must adjust multiColSelectivity so that it is not greater than
	// the selectivity of any subset of the columns in multiColSet. This would
	// be internally inconsistent and could lead to bad plans. For example,
	// x=1 AND y=1 should always be considered more selective (i.e., with lower
	// selectivity) than x=1 alone.
	//
	// We have already found the minimum selectivity of all the individual columns
	// (subsets of size 1) above and stored it in minSingleColSel. It's not
	// practical, however, to calculate the minimum selectivity for all subsets
	// larger than size 1.
	//
	// Instead, we focus on a specific case known to occasionally have this
	// problem: when multiColSet contains 3 or more columns and at least one has
	// distinct count greater than 1, the subset of columns that have distinct
	// count less than or equal to 1 may have a smaller selectivity according to
	// equation (2).
	//
	// In this case, adjust multiColSelectivity as needed.
	//
	if maxNewSingleColDistinct > 1 && multiColSet.Len() > 2 {
		var lowDistinctCountCols opt.ColSet
		multiColSet.ForEach(func(col opt.ColumnID) {
			// We already know the column stat exists if it's in multiColSet.
			colStat, _ := s.ColStats.LookupSingleton(col)
			if colStat.DistinctCount <= 1 {
				lowDistinctCountCols.Add(col)
			}
		})

		if lowDistinctCountCols.Len() > 1 {
			// Find the selectivity of the low distinct count columns to ensure that
			// multiColSelectivity is lower (see above comment). Additionally,
			// multiply by the selectivity of the other columns to differentiate
			// between different plans that constrain different subsets of these
			// columns.
			selLowDistinctCountCols, _, _ := sb.selectivityFromMultiColDistinctCounts(
				lowDistinctCountCols, e, s,
			)
			selOtherCols, _, _ := sb.selectivityFromMultiColDistinctCounts(
				multiColSet.Difference(lowDistinctCountCols), e, s,
			)
			selLowDistinctCountCols.Multiply(selOtherCols)
			multiColSelectivity = props.MinSelectivity(multiColSelectivity, selLowDistinctCountCols)
		}
	}
	multiColSelectivity = props.MinSelectivity(multiColSelectivity, minSingleColSel)

	// multiColSelectivityLowerBound is the minimum multi-column selectivity, which
	// is the minimum possible new multi-col distinct count divided by the maximum
	// old multi-column distinct count (either the product of the old single column
	// distinct counts or the old row count, whichever is smaller).
	maxOldMultiColDistinct := min(inputStats.RowCount, oldSingleColDistinctProduct)
	multiColSelectivityLowerBound := props.MakeSelectivityFromFraction(
		minNewMultiColDistinct, maxOldMultiColDistinct,
	)
	// Ensure that multiColSelectivityLowerBound is not larger than
	// multiColSelectivity.
	multiColSelectivityLowerBound = props.MinSelectivity(
		multiColSelectivityLowerBound, multiColSelectivity,
	)

	// As described in the function comment, we actually return a weighted sum of
	// multi-column selectivity estimates with and without an independence
	// assumption. To ensure selectivityLowerBound is not larger than this
	// selectivity, use the same weighting scheme.
	//
	// Use MaxSelectivity to handle floating point rounding errors.
	w := multiColWeight
	return props.MaxSelectivity(multiColSelWithIndepAssumption, props.MakeSelectivity(
			(1-w)*multiColSelWithIndepAssumption.AsFloat()+w*multiColSelectivity.AsFloat(),
		)),
		minSingleColSel,
		props.MaxSelectivity(multiColSelWithIndepAssumption, props.MakeSelectivity(
			(1-w)*multiColSelWithIndepAssumption.AsFloat()+w*multiColSelectivityLowerBound.AsFloat(),
		))
}

// correlationFromMultiColDistinctCounts returns the correlation between the
// given set of columns, as indicated by multi-column stats. It is a number
// between 0 and 1, where 0 means the columns are completely independent, and 1
// means the columns are completely correlated.
//
// Note that this isn't the real correlation; that would require calculating the
// correlation coefficient between pairs of columns during table stats
// collection. Instead, this is just a proxy obtained by estimating three values
// for the selectivity of the filter constraining the given columns:
//  1. lb (lower bound): if optimizer_use_improved_multi_column_selectivity_estimate
//     is true, this is the minimum possible multi-column selectivity, calculated
//     based on the minimum new multi-column distinct count and the maximum old
//     multi-column distinct count.
//     Else, it is the value returned by multiplying the individual conjunct
//     selectivities together, estimated from single-column distinct counts. This
//     would be the selectivity of the entire predicate if the columns were
//     completely independent.
//  2. ub (upper bound): the lowest single-conjunct selectivity estimated from
//     single-column distinct counts. This would be the selectivity of the entire
//     predicate if the columns were completely correlated. In other words, this
//     would be the selectivity if the value of the column with the most
//     selective predicate functionally determined the value of all other
//     constrained columns.
//  3. mc (multi-column selectivity): the value returned by estimating the
//     predicate selectivity with a combination of single-column and multi-column
//     distinct counts. It falls somewhere between upper and lower bound, and
//     thus approximates the level of correlation of the columns.
//
// The "correlation" returned by this function is thus:
//
//	corr = (mc - lb) / (ub - lb)
//
// where
//
//	lb <= mc <= ub
//
// This value will be used to refine the selectivity estimate from single-column
// histograms, which do not contain information about column correlations.
//
// TODO(rytaft): There's probably a way to do this directly from distinct counts
// without first estimating selectivities, but I couldn't find a formula that
// didn't result in a lot of bad plans.
func (sb *statisticsBuilder) correlationFromMultiColDistinctCounts(
	cols opt.ColSet, e RelExpr, s *props.Statistics,
) float64 {
	// Respect the session setting OptimizerUseMultiColStats.
	if !sb.evalCtx.SessionData().OptimizerUseMultiColStats {
		return 0
	}

	selectivity, upperBound, lowerBound := sb.selectivityFromMultiColDistinctCounts(cols, e, s)
	// Check s.Available since OptimizerUseImprovedMultiColumnSelectivityEstimate
	// tends to cause some hash join plans to turn into lookup join plans, and
	// when we're operating without stats, that bias is more risky.
	if !sb.evalCtx.SessionData().OptimizerUseImprovedMultiColumnSelectivityEstimate || !s.Available {
		lowerBound, _ = sb.selectivityFromSingleColDistinctCounts(cols, e, s)
	}
	if upperBound == lowerBound {
		return 0
	}
	return (selectivity.AsFloat() - lowerBound.AsFloat()) / (upperBound.AsFloat() - lowerBound.AsFloat())
}

// correlationFromMultiColDistinctCountsForJoin is similar to
// correlationFromMultiColDistinctCounts, but used for join expressions.
func (sb *statisticsBuilder) correlationFromMultiColDistinctCountsForJoin(
	cols, leftCols, rightCols opt.ColSet, e RelExpr, s *props.Statistics,
) float64 {
	// Respect the session setting OptimizerUseMultiColStats.
	if !sb.evalCtx.SessionData().OptimizerUseMultiColStats {
		return 0
	}

	selectivityLeft, upperBoundLeft, lowerBoundLeft := sb.selectivityFromMultiColDistinctCounts(
		cols.Intersection(leftCols), e, s,
	)
	selectivityRight, upperBoundRight, lowerBoundRight := sb.selectivityFromMultiColDistinctCounts(
		cols.Intersection(rightCols), e, s,
	)
	selectivity := selectivityLeft
	selectivity.Multiply(selectivityRight)
	upperBound := upperBoundLeft
	upperBound.Multiply(upperBoundRight)
	lowerBound := lowerBoundLeft
	// Check s.Available since OptimizerUseImprovedMultiColumnSelectivityEstimate
	// tends to cause some hash join plans to turn into lookup join plans, and
	// when we're operating without stats, that bias is more risky.
	if sb.evalCtx.SessionData().OptimizerUseImprovedMultiColumnSelectivityEstimate && s.Available {
		lowerBound.Multiply(lowerBoundRight)
	} else {
		lowerBound, _ = sb.selectivityFromSingleColDistinctCounts(cols, e, s)
	}
	if upperBound == lowerBound {
		return 0
	}
	if selectivity.AsFloat() < lowerBound.AsFloat() {
		// This can happen due to floating point rounding errors.
		return 0
	}
	return (selectivity.AsFloat() - lowerBound.AsFloat()) / (upperBound.AsFloat() - lowerBound.AsFloat())
}

// selectivityFromSingleColDistinctCounts calculates the selectivity of a
// filter by using estimated distinct counts of each constrained column before
// and after the filter was applied. It assumes independence between columns,
// so it uses equation (1) from selectivityFromMultiColDistinctCounts. See the
// comment above that function for details.
//
// We also return selectivityUpperBound, which is the minimum selectivity of any
// single column. This is the maximum possible selectivity of the entire
// expression.
func (sb *statisticsBuilder) selectivityFromSingleColDistinctCounts(
	cols opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity, selectivityUpperBound props.Selectivity) {
	selectivity = props.OneSelectivity
	selectivityUpperBound = props.OneSelectivity
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colStat, ok := s.ColStats.LookupSingleton(col)
		if !ok {
			continue
		}

		inputColStat, inputStats := sb.colStatFromInput(colStat.Cols, e)
		predicateSelectivity := sb.selectivityFromDistinctCount(colStat, inputColStat, inputStats.RowCount)

		// The maximum possible selectivity of the entire expression is the minimum
		// selectivity of all individual predicates.
		selectivityUpperBound = props.MinSelectivity(selectivityUpperBound, predicateSelectivity)

		selectivity.Multiply(predicateSelectivity)
	}

	return selectivity, selectivityUpperBound
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
//
// In addition to returning the product of selectivities,
// selectivityFromHistograms returns the selectivity of the most selective
// predicate (i.e., the predicate with minimum selectivity) as
// selectivityUpperBound, since it is an upper bound on the selectivity of the
// entire expression.
func (sb *statisticsBuilder) selectivityFromHistograms(
	cols opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity props.Selectivity, selectivityUpperBound props.Selectivity) {
	selectivity = props.OneSelectivity
	selectivityUpperBound = props.OneSelectivity
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		colStat, ok := s.ColStats.LookupSingleton(col)
		if !ok {
			continue
		}

		inputColStat, _ := sb.colStatFromInput(colStat.Cols, e)
		newHist := colStat.Histogram
		oldHist := inputColStat.Histogram
		if newHist == nil || oldHist == nil {
			continue
		}

		newCount := newHist.ValuesCount()
		oldCount := oldHist.ValuesCount()

		// Calculate the selectivity of the predicate. Nulls are already included
		// in the histogram, so we do not need to account for them separately.
		predicateSelectivity := props.MakeSelectivityFromFraction(newCount, oldCount)

		// The maximum possible selectivity of the entire expression is the minimum
		// selectivity of all individual predicates.
		selectivityUpperBound = props.MinSelectivity(selectivityUpperBound, predicateSelectivity)

		selectivity.Multiply(predicateSelectivity)
	}

	return selectivity, selectivityUpperBound
}

// selectivityFromConstrainedCols calculates the selectivity from the
// constrained columns. histCols is a subset of constrainedCols, and represents
// the columns that have histograms available. correlation represents the
// correlation between the columns, and is a number between 0 and 1, where 0
// means the columns are completely independent, and 1 means the columns are
// completely correlated.
func (sb *statisticsBuilder) selectivityFromConstrainedCols(
	constrainedCols, histCols opt.ColSet, e RelExpr, s *props.Statistics, correlation float64,
) (selectivity props.Selectivity) {
	if buildutil.CrdbTestBuild && (correlation < 0 || correlation > 1) {
		panic(errors.AssertionFailedf("correlation must be between 0 and 1. Found %f", correlation))
	}
	selectivity, selectivityUpperBound := sb.selectivityFromHistograms(histCols, e, s)
	selectivity2, selectivityUpperBound2 := sb.selectivityFromSingleColDistinctCounts(
		constrainedCols.Difference(histCols), e, s,
	)
	selectivity.Multiply(selectivity2)
	selectivityUpperBound = props.MinSelectivity(selectivityUpperBound, selectivityUpperBound2)
	selectivity.Add(props.MakeSelectivity(
		correlation * (selectivityUpperBound.AsFloat() - selectivity.AsFloat()),
	))
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
//	sel = (output row count) / (input row count)
//
// where
//
//	output row count =
//	  (fraction of non-null values preserved) * (number of non-null input rows) +
//	  (fraction of null values preserved) * (number of null input rows)
func (sb *statisticsBuilder) predicateSelectivity(
	nonNullSelectivity, nullSelectivity props.Selectivity, inputNullCount, inputRowCount float64,
) props.Selectivity {
	outRowCount := nonNullSelectivity.AsFloat()*(inputRowCount-inputNullCount) + nullSelectivity.AsFloat()*inputNullCount
	sel := props.MakeSelectivityFromFraction(outRowCount, inputRowCount)

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

// selectivityFromOredEquivalencies determines the selectivity of all
// disjunctions of equality constraints present in the array of conjuncts,
// h.filters. For every conjunct that is an OrExpr chain in which at least one
// disjunct exists with no column equivalency set, selectivity estimation is
// skipped for that conjunct and numUnappliedConjuncts is incremented by 1.
// If in the future the accuracy of inequality join predicate selectivity
// estimation is improved, this method can be updated to handle those predicates
// as well.
func (sb *statisticsBuilder) selectivityFromOredEquivalencies(
	h *joinPropsHelper, e RelExpr, s *props.Statistics, unapplied *filterCount, semiJoin bool,
) (selectivity props.Selectivity) {
	selectivity = props.OneSelectivity
	var conjunctSelectivity props.Selectivity

	for f := 0; f < len(h.filters); f++ {
		disjunction := &h.filters[f]
		if disjunction.ScalarProps().TightConstraints {
			// applyFilters will have already handled this filter.
			continue
		}
		var disjuncts []opt.ScalarExpr
		if orExpr, ok := disjunction.Condition.(*OrExpr); !ok {
			continue
		} else {
			disjuncts = CollectContiguousOrExprs(orExpr)
		}
		var filter FiltersItem
		var filters FiltersExpr
		var filtersFDs []props.FuncDepSet
		var ok = true

		for i := 0; ok && i < len(disjuncts); i++ {
			var andFilters FiltersExpr
			var filtersFD props.FuncDepSet

			// Only handle ANDed equality expressions for which constructFiltersItem
			// is able to build column equivalencies.
			switch disjuncts[i].(type) {
			case *EqExpr, *AndExpr:
				if andFilters, ok = addEqExprConjuncts(disjuncts[i], andFilters, sb.mem); !ok {
					unapplied.unknown++
					continue
				}
				sb.mem.logPropsBuilder.addFiltersToFuncDep(andFilters, &filtersFD)
			default:
				unapplied.unknown++
				ok = false
				continue
			}
			// If no column equivalencies are found, we know nothing about this term,
			// so should skip selectivity estimation on the entire conjunct.
			if filtersFD.Empty() || filtersFD.EquivReps().Empty() {
				unapplied.unknown++
				ok = false
				break
			}
			filters = append(filters, filter)
			filtersFDs = append(filtersFDs, filtersFD)
		}
		if !ok {
			continue
		}
		var singleSelectivity props.Selectivity
		var selectivities []props.Selectivity
		for i := 0; i < len(filters); i++ {
			FD := &filtersFDs[i]
			equivReps := FD.EquivReps()
			if semiJoin {
				singleSelectivity = sb.selectivityFromEquivalenciesSemiJoin(
					equivReps, sb.colStatCols(h.leftProps), sb.colStatCols(h.rightProps),
					FD, e, s,
				)
			} else {
				singleSelectivity = sb.selectivityFromEquivalencies(equivReps, FD, e, s)
			}
			selectivities = append(selectivities, singleSelectivity)
		}
		if !ok {
			continue
		}
		// Combine the selectivities of each ORed predicate to get the total
		// combined selectivity of the entire OR expression.
		conjunctSelectivity = combineOredSelectivities(selectivities)

		// Combine this disjunction's selectivity with that of other disjunctions.
		selectivity.Multiply(conjunctSelectivity)
	}
	return selectivity
}

// combineOredSelectivities iteratively applies the General Disjunction Rule
// on a slice of 'selectivities' where each selectivity in the slice is the
// selectivity of an individual ORed predicate.
//
// Given predicates A and B and probability function P:
//
//	P(A or B) = P(A) + P(B) - P(A and B)
//
// Continuation:
//
//	P(A or B or C) = P(A or B) + P(C) - P((A or B) and C)
//	P(A or B or C or D) = P(A or B or C) + P(D) - P((A or B or C) and D)
//	...
//
// This seems to be a standard approach in the database world.
// The textbook solution is more complex:
// https://www.thoughtco.com/probability-union-of-three-sets-more-3126263
// https://stats.stackexchange.com/questions/87533/whats-the-general-disjunction-rule-for-n-events
//
// The iterative approach seems to be equivalent to textbook solutions which
// expand into a large number of terms.
// In the formula we assume A and B are independent, so:
//
//	P(A and B) = P(A) * P(B)
//
// The independence assumption may not be correct in all cases.
//
//	TODO(msirek): Use multicolumn stats and column correlations to get better
//								estimates of P(A and B).
func combineOredSelectivities(selectivities []props.Selectivity) props.Selectivity {
	totalSelectivity := selectivities[0]
	var combinedSel props.Selectivity
	for i := 1; i < len(selectivities); i++ {
		// combinedSel represents P(A and B)
		combinedSel = props.MakeSelectivity((&totalSelectivity).AsFloat())
		(&combinedSel).Multiply(selectivities[i])
		totalSelectivity.UnsafeAdd(selectivities[i])
		totalSelectivity.Subtract(combinedSel)
	}
	return totalSelectivity
}

// constructFiltersItem constructs an expression for the FiltersItem operator,
// with identical behavior to the Factory method of the same name, but for use
// in cases where the Factory is not accessible.
func constructFiltersItem(condition opt.ScalarExpr, m *Memo) FiltersItem {
	item := FiltersItem{Condition: condition}
	item.PopulateProps(m)
	return item
}

// constructProjectionsItem constructs an expression for the ProjectionsItem
// operator, with identical behavior to the Factor method of the same name, but
// for use in cases where the Factory is not accessible.
func constructProjectionsItem(element opt.ScalarExpr, col opt.ColumnID, m *Memo) ProjectionsItem {
	item := ProjectionsItem{Element: element, Col: col}
	item.PopulateProps(m)
	return item
}

// addEqExprConjuncts recursively walks a scalar expression as long as it
// continues to find nested And operators. It adds any equality expression
// conjuncts to the given FiltersExpr and returns true.
// This function is inspired by norm.CustomFuncs.addConjuncts, but serves
// a different purpose.
// The whole purpose of this new function is to enable the building of
// functional dependencies based on equality predicates for selectivity
// estimation purposes, so it is not important to traverse OrExpr expression
// subtrees or make filters for any other expression type.
func addEqExprConjuncts(
	scalar opt.ScalarExpr, filters FiltersExpr, m *Memo,
) (_ FiltersExpr, ok bool) {
	switch t := scalar.(type) {
	case *AndExpr:
		var ok1, ok2 bool
		filters, ok1 = addEqExprConjuncts(t.Left, filters, m)
		filters, ok2 = addEqExprConjuncts(t.Right, filters, m)
		return filters, ok1 || ok2
	case *EqExpr:
		filters = append(filters, constructFiltersItem(t, m))
	default:
		return nil, false
	}
	return filters, true
}

func (sb *statisticsBuilder) selectivityFromEquivalency(
	equivGroup opt.ColSet, e RelExpr, s *props.Statistics,
) (selectivity props.Selectivity) {
	var derivedEquivCols opt.ColSet
	if lookupJoinExpr, ok := e.(*LookupJoinExpr); ok {
		derivedEquivCols = lookupJoinExpr.DerivedEquivCols
	}
	// Find the maximum input distinct count for all columns in this equivalency
	// group.
	maxDistinctCount := float64(0)
	equivGroup.ForEach(func(col opt.ColumnID) {
		if derivedEquivCols.Contains(col) {
			// Don't apply selectivity from derived equivalencies internally
			// manufactured by lookup join solely to facilitate index lookups.
			return
		}
		// If any of the distinct counts were updated by the filter, we want to use
		// the updated value.
		colStat, ok := s.ColStats.LookupSingleton(col)
		if !ok {
			colStat, _ = sb.colStatFromInput(opt.MakeColSet(col), e)
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
	equivGroup.ForEach(func(col opt.ColumnID) {
		// If any of the distinct counts were updated by the filter, we want to use
		// the updated value.
		colStat, ok := s.ColStats.LookupSingleton(col)
		if !ok {
			colStat, _ = sb.colStatFromInput(opt.MakeColSet(col), e)
		}
		if leftOutputCols.Contains(col) {
			if maxDistinctCountLeft < colStat.DistinctCount {
				maxDistinctCountLeft = colStat.DistinctCount
			}
		} else if rightOutputCols.Contains(col) {
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
	c filterCount,
) (selectivity props.Selectivity) {
	s := math.Pow(unknownFilterSelectivity, float64(c.unknown)) *
		math.Pow(similarityFilterSelectivity, float64(c.similarity))
	return props.MakeSelectivity(s)
}

// tryReduceCols is used to determine which columns to use for selectivity
// calculation.
//
// When columns in the colStats are functionally determined by other columns,
// and the determinant columns each have distinctCount = 1, we should consider
// the implied correlations for selectivity calculation. Consider the query:
//
//	SELECT * FROM customer WHERE id = 123 and name = 'John Smith'
//
// If id is the primary key of customer, then name is functionally determined
// by id. We only need to consider the selectivity of id, not name, since id
// and name are fully correlated. To determine if we have a case such as this
// one, we functionally reduce the set of columns which have column statistics,
// eliminating columns that can be functionally determined by other columns.
// If the distinct count on all of these reduced columns is one, then we return
// this reduced column set to be used for selectivity calculation.
func (sb *statisticsBuilder) tryReduceCols(
	cols opt.ColSet, s *props.Statistics, fd *props.FuncDepSet,
) opt.ColSet {
	reducedCols := fd.ReduceCols(cols)
	if reducedCols.Empty() {
		// There are no reduced columns so we return the original column set.
		return cols
	}

	for i, ok := reducedCols.Next(0); ok; i, ok = reducedCols.Next(i + 1) {
		colStat, ok := s.ColStats.LookupSingleton(i)
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

// isSimilarityFilter returns true if the given condition is a trigram
// similarity filter.
func isSimilarityFilter(e opt.ScalarExpr) bool {
	if sim, ok := e.(*ModExpr); ok {
		return sim.Left.DataType().Family() == types.StringFamily &&
			sim.Right.DataType().Family() == types.StringFamily
	}
	return false
}

// isPlaceholderEqualityFilter returns a column ID and true if the given
// condition is an equality between a column and a placeholder.
func isPlaceholderEqualityFilter(e opt.ScalarExpr) (opt.ColumnID, bool) {
	if e.Op() == opt.EqOp && e.Child(1).Op() == opt.PlaceholderOp {
		if v, ok := e.Child(0).(*VariableExpr); ok {
			return v.Col, true
		}
	}
	return 0, false
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
func RequestColStat(
	ctx context.Context, evalCtx *eval.Context, mem *Memo, e RelExpr, cols opt.ColSet,
) {
	var sb statisticsBuilder
	sb.init(ctx, evalCtx, mem)
	sb.colStat(cols, e)
}

// buildStatsFromCheckConstraints checks if there are any non-null CHECK
// constraints which constrain indexes and has a first index column id the same
// as the single column in colStat.Cols, and modifies the colStat entry with
// samples from the CHECK constraint. It is assumed colStat has just been added
// to the stats cache and no actual table statistics exist on it. If the
// optimizer_use_histograms session setting is on, and the first index column is
// not of a data type which is invertable-indexable (JSON, ARRAY or spatial
// types), and the number of rows in the table is not less than the number of
// values found in the constraint, then an EquiDepthHistogram is built from the
// CHECK constraint values using an even data distribution. If the number of
// values is greater than maxValuesForFullHistogramFromCheckConstraint, no
// histogram is built. If no statistics exist on any columns in this table (as
// indicated via statistics.Available), buildStatsFromCheckConstraints does not
// modify colStat. True is returned if colStat was modified, otherwise false.
func (sb *statisticsBuilder) buildStatsFromCheckConstraints(
	colStat *props.ColumnStatistic, statistics *props.Statistics,
) bool {
	if !statistics.Available {
		return false
	}
	// This function only builds single-column stats.
	if colStat.Cols.Len() != 1 {
		return false
	}
	cols := colStat.Cols
	colID := cols.SingleColumn()
	tabID := sb.md.ColumnMeta(colID).Table
	colType := sb.md.ColumnMeta(colID).Type

	// Only handle columns that come from tables.
	if tabID == opt.TableID(0) {
		return false
	}
	tabMeta := sb.md.TableMeta(tabID)

	// Reuse a previous building of this ColumnStatistic if possible, but
	// referencing the new ColumnID.
	if origColStatPtr, ok := tabMeta.OrigCheckConstraintsStats(colID); ok {
		if origColStat, ok := origColStatPtr.(*props.ColumnStatistic); ok {
			if origColStat.Cols.Len() == 1 && origColStat.Cols.SingleColumn() != colID {
				colStat.CopyFromOther(origColStat, sb.evalCtx)
				return true
			}
		}
	}

	var firstColID opt.ColumnID
	constraints := tabMeta.Constraints
	if constraints == nil {
		return false
	}
	if constraints.Op() != opt.FiltersOp {
		return false
	}
	filters := *constraints.(*FiltersExpr)
	// For each ANDed check constraint...
	for i := 0; i < len(filters); i++ {
		filter := &filters[i]
		// This must be some type of comparison operation, or an OR or AND
		// expression. These operations have at least 2 children.
		if filter.Condition.ChildCount() < 2 {
			continue
		}
		if filter.ScalarProps().Constraints == nil {
			continue
		}
		for j := 0; j < filter.ScalarProps().Constraints.Length(); j++ {
			filterConstraint := filter.ScalarProps().Constraints.Constraint(j)
			firstColID = filterConstraint.Columns.Get(0).ID()
			if firstColID != colID {
				continue
			}
			dataType := tabMeta.Table.Column(tabID.ColumnOrdinal(firstColID)).DatumType()
			numRows := int64(statistics.RowCount)
			var hasNullValue, ok bool
			var values tree.Datums
			var distinctVals uint64
			onlyInvertedIndexableColumnType := colinfo.ColumnTypeIsOnlyInvertedIndexable(colType)
			if distinctVals, ok = filterConstraint.CalculateMaxResults(sb.ctx, sb.evalCtx, cols, cols); ok {
				// If the number of values is excessive, don't spend too much time building the histogram,
				// as it may slow down the query.
				// TODO(msirek): Consider bumping up this limit for tables with high RowCount, as they
				//   may take longer to scan, and compared to scan costs, a slight increase in query
				//   planning time may be worth it to get more detailed stats.
				if distinctVals <= maxValuesForFullHistogramFromCheckConstraint {
					values, hasNullValue, _ = filterConstraint.CollectFirstColumnValues(sb.ctx, sb.evalCtx)
					if hasNullValue {
						log.Infof(
							sb.ctx, "null value seen in histogram built from check constraint: %s", filterConstraint.String(),
						)
					}
				}
			} else {
				continue
			}
			numValues := len(values)
			// Histograms are not typically built on inverted columns, and the data
			// types of such columns (JSON, ARRAY, and spatial types) aren't likely to
			// occur in CHECK constraints.  So, let's play it safe and don't create a
			// histogram directly on columns that have a data type which is
			// OnlyInvertedIndexable since the possible user benefit of adding this
			// support seems low.
			//
			// Also, histogram building errors out when the number of samples is
			// greater than the number of rows, so just skip that case.
			// Non-null check constraints are not expected to have nulls, so we do
			// not expect to see null values. If we do see a null, something may have
			// gone wrong, so do not build a histogram in this case either.
			useHistogram := sb.evalCtx.SessionData().OptimizerUseHistograms && numValues > 0 &&
				!hasNullValue && !onlyInvertedIndexableColumnType && int64(numValues) <= numRows
			if !useHistogram {
				if distinctVals > math.MaxInt32 {
					continue
				}
				numValues = int(distinctVals)
			}
			var histogram []cat.HistogramBucket

			distinctCount := max(float64(numValues), 1)
			nullCount := float64(0)
			if useHistogram {
				// Each single-column prefix value from the Spans is a sample value.
				// Give each sample value its own bucket, up to a maximum of 200
				// buckets, with even distribution.
				_, unencodedBuckets, err := stats.EquiDepthHistogram(
					sb.ctx,
					sb.evalCtx,
					dataType,
					values, /* samples */
					numRows,
					int64(numValues), /* distinctCount */
					int(stats.DefaultHistogramBuckets.Get(&sb.evalCtx.Settings.SV)), /* maxBuckets */
					sb.evalCtx.Settings,
				)
				// This shouldn't error out, but if it does, let's not punish the user.
				// Just build stats without the histogram in that case.
				if err == nil {
					histogram = unencodedBuckets
				} else {
					useHistogram = false
					log.Infof(
						sb.ctx, "histogram could not be generated from check constraint due to error: %v", err,
					)
				}
			}

			// Modify the actual stats entry in statistics.ColStats that can be looked
			// up via a ColSet.
			colStat.DistinctCount = distinctCount
			colStat.NullCount = nullCount
			if useHistogram {
				colStat.Histogram = &props.Histogram{}
				colStat.Histogram.Init(sb.evalCtx, firstColID, histogram)
			}
			sb.finalizeFromRowCountAndDistinctCounts(colStat, statistics)
			tabMeta.AddCheckConstraintsStats(firstColID, colStat)
			return true
		}
	}
	return false
}

// factorOutVirtualCols replaces any subexpressions matching any virtual
// computed column expressions with a reference to the virtual computed
// column. This allows us to directly use statistics collected on the virtual
// computed column instead of estimating from the expression.
func (sb *statisticsBuilder) factorOutVirtualCols(
	e opt.Expr, shared *props.Shared, virtualCols opt.ColSet, m *Memo,
) opt.Expr {
	if m.replacer == nil {
		panic(errors.AssertionFailedf("Memo.replacer unset"))
	}

	// Optimization: build a slice of virtual computed column expressions to find,
	// and also prune any virtual cols whose outer columns are not contained in
	// the expression's outer columns.
	type virtExpr struct {
		colID opt.ColumnID
		expr  opt.Expr
	}
	virtExprs := make([]virtExpr, virtualCols.Len())
	virtualCols.ForEach(func(colID opt.ColumnID) {
		col := sb.md.ColumnMeta(colID)
		tab := sb.md.TableMeta(col.Table)
		if !tab.ColsInComputedColsExpressions.Intersects(shared.OuterCols) {
			return
		}
		expr, ok := tab.ComputedCols[colID]
		if !ok {
			// If we can't find the computed column expression, this is probably a
			// mutation column. Whatever the reason, it's safe to skip: we simply
			// won't factor out matching expressions.
			if buildutil.CrdbTestBuild &&
				!tab.Table.Column(tab.MetaID.ColumnOrdinal(colID)).IsMutation() {
				panic(errors.AssertionFailedf(
					"could not find computed column expression for column %v in table %v", colID, tab.Alias,
				))
			}
			return
		}
		virtExprs = append(virtExprs, virtExpr{colID: colID, expr: expr})
	})

	if len(virtExprs) == 0 {
		return e
	}

	// Replace all virtual col expressions with the corresponding virtual col.
	var replace ReplaceFunc
	replace = func(e opt.Expr) opt.Expr {
		for _, ve := range virtExprs {
			if e == ve.expr {
				return m.MemoizeVariable(ve.colID)
			}
		}
		return m.replacer(e, replace)
	}
	return replace(e)
}

// filterCount tracks counts of different types of filters. It is used to track
// the number of filters which are not applied to selectivities via more exact
// means like constraints and histogram filtering.
type filterCount struct {
	unknown    int
	similarity int
}

// add adds the counts of other to c.
func (c *filterCount) add(other filterCount) {
	c.unknown += other.unknown
	c.similarity += other.similarity
}
