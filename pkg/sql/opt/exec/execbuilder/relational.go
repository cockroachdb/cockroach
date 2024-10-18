// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execbuilder

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/distribution"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type execPlan struct {
	root exec.Node
}

// makeBuildScalarCtx returns a buildScalarCtx that can be used with expressions
// that refer the output columns of this plan.
func makeBuildScalarCtx(cols colOrdMap) buildScalarCtx {
	return buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, cols.MaxOrd()+1),
		ivarMap: cols,
	}
}

// getNodeColumnOrdinal takes a column that is known to be produced by the execPlan
// and returns the ordinal index of that column in the result columns of the
// node.
func getNodeColumnOrdinal(colMap colOrdMap, col opt.ColumnID) (exec.NodeColumnOrdinal, error) {
	ord, ok := colMap.Get(col)
	if !ok {
		return 0, errors.AssertionFailedf("column %d not in input", redact.Safe(col))
	}
	return exec.NodeColumnOrdinal(ord), nil
}

// reqOrdering converts the provided ordering of a relational expression to an
// OutputOrdering (according to the outputCols map).
func reqOrdering(expr memo.RelExpr, cols colOrdMap) (exec.OutputOrdering, error) {
	ordering, err := sqlOrdering(expr.ProvidedPhysical().Ordering, cols)
	return exec.OutputOrdering(ordering), err
}

// sqlOrdering converts an Ordering to a ColumnOrdering (according to the
// outputCols map).
func sqlOrdering(ordering opt.Ordering, cols colOrdMap) (colinfo.ColumnOrdering, error) {
	if ordering.Empty() {
		return nil, nil
	}
	colOrder := make(colinfo.ColumnOrdering, len(ordering))
	for i := range ordering {
		ord, err := getNodeColumnOrdinal(cols, ordering[i].ID())
		if err != nil {
			return nil, err
		}
		colOrder[i].ColIdx = int(ord)
		if ordering[i].Descending() {
			colOrder[i].Direction = encoding.Descending
		} else {
			colOrder[i].Direction = encoding.Ascending
		}
	}

	return colOrder, nil
}

// builRelational builds a relational expression into an execPlan.
//
// outputCols is a map from opt.ColumnID to exec.NodeColumnOrdinal. It maps
// columns in the output set of a relational expression to indices in the
// result columns of the exec.Node.
//
// The reason we need to keep track of this (instead of using just the
// relational properties) is that the relational properties don't force a
// single "schema": any ordering of the output columns is possible. We choose
// the schema that is most convenient: for scans, we use the table's column
// ordering. Consider:
//
//	SELECT a, b FROM t WHERE a = b
//
// and the following two cases:
//  1. The table is defined as (k INT PRIMARY KEY, a INT, b INT). The scan will
//     return (k, a, b).
//  2. The table is defined as (k INT PRIMARY KEY, b INT, a INT). The scan will
//     return (k, b, a).
//
// In these two cases, the relational properties are effectively the same.
//
// An alternative to this would be to always use a "canonical" schema, for
// example the output columns in increasing index order. However, this would
// require a lot of otherwise unnecessary projections.
//
// Note: conceptually, this could be a ColList; however, the map is more
// convenient when converting VariableOps to IndexedVars.
// outputCols colOrdMap
func (b *Builder) buildRelational(e memo.RelExpr) (_ execPlan, outputCols colOrdMap, err error) {
	var ep execPlan

	if opt.IsDDLOp(e) {
		// Mark the statement as containing DDL for use
		// in the SQL executor.
		b.flags.Set(exec.PlanFlagIsDDL)
	}

	if opt.IsMutationOp(e) {
		b.flags.Set(exec.PlanFlagContainsMutation)
		// Raise error if mutation op is part of a read-only transaction.
		if b.evalCtx.TxnReadOnly {
			switch tag := b.statementTag(e); tag {
			// DISCARD can drop temp tables but is still allowed in read-only
			// transactions for PG compatibility.
			case "DISCARD ALL", "DISCARD":
			default:
				return execPlan{}, colOrdMap{}, pgerror.Newf(pgcode.ReadOnlySQLTransaction,
					"cannot execute %s in a read-only transaction", tag)
			}
		}
	}

	// Raise error if bounded staleness is used incorrectly.
	if b.boundedStaleness() {
		if _, ok := boundedStalenessAllowList[e.Op()]; !ok {
			return execPlan{}, colOrdMap{}, unimplemented.NewWithIssuef(67562,
				"cannot use bounded staleness for %s", b.statementTag(e),
			)
		}
	}

	// Collect usage telemetry for relational node, if appropriate.
	if !b.disableTelemetry {
		if c := opt.OpTelemetryCounters[e.Op()]; c != nil {
			telemetry.Inc(c)
		}
	}

	var saveTableName string
	if b.nameGen != nil {
		// Don't save tables for operators that don't produce any columns (most
		// importantly, for SET which is used to disable saving of tables).
		if !e.Relational().OutputCols.Empty() {
			// This function must be called in a pre-order traversal of the tree.
			saveTableName = b.nameGen.GenerateName(e.Op())
		}
	}

	switch t := e.(type) {
	case *memo.ValuesExpr:
		ep, outputCols, err = b.buildValues(t)

	case *memo.LiteralValuesExpr:
		ep, outputCols, err = b.buildLiteralValues(t)

	case *memo.ScanExpr:
		ep, outputCols, err = b.buildScan(t)

	case *memo.PlaceholderScanExpr:
		ep, outputCols, err = b.buildPlaceholderScan(t)

	case *memo.SelectExpr:
		ep, outputCols, err = b.buildSelect(t)

	case *memo.ProjectExpr:
		ep, outputCols, err = b.buildProject(t)

	case *memo.GroupByExpr, *memo.ScalarGroupByExpr:
		ep, outputCols, err = b.buildGroupBy(e)

	case *memo.DistinctOnExpr, *memo.EnsureDistinctOnExpr, *memo.UpsertDistinctOnExpr,
		*memo.EnsureUpsertDistinctOnExpr:
		ep, outputCols, err = b.buildDistinct(t)

	case *memo.TopKExpr:
		ep, outputCols, err = b.buildTopK(t)

	case *memo.LimitExpr, *memo.OffsetExpr:
		ep, outputCols, err = b.buildLimitOffset(e)

	case *memo.SortExpr:
		ep, outputCols, err = b.buildSort(t)

	case *memo.DistributeExpr:
		ep, outputCols, err = b.buildDistribute(t)

	case *memo.IndexJoinExpr:
		ep, outputCols, err = b.buildIndexJoin(t)

	case *memo.LookupJoinExpr:
		ep, outputCols, err = b.buildLookupJoin(t)

	case *memo.InvertedJoinExpr:
		ep, outputCols, err = b.buildInvertedJoin(t)

	case *memo.ZigzagJoinExpr:
		ep, outputCols, err = b.buildZigzagJoin(t)

	case *memo.OrdinalityExpr:
		ep, outputCols, err = b.buildOrdinality(t)

	case *memo.MergeJoinExpr:
		ep, outputCols, err = b.buildMergeJoin(t)

	case *memo.Max1RowExpr:
		ep, outputCols, err = b.buildMax1Row(t)

	case *memo.ProjectSetExpr:
		ep, outputCols, err = b.buildProjectSet(t)

	case *memo.WindowExpr:
		ep, outputCols, err = b.buildWindow(t)

	case *memo.SequenceSelectExpr:
		ep, outputCols, err = b.buildSequenceSelect(t)

	case *memo.InsertExpr:
		ep, outputCols, err = b.buildInsert(t)

	case *memo.InvertedFilterExpr:
		ep, outputCols, err = b.buildInvertedFilter(t)

	case *memo.UpdateExpr:
		ep, outputCols, err = b.buildUpdate(t)

	case *memo.UpsertExpr:
		ep, outputCols, err = b.buildUpsert(t)

	case *memo.DeleteExpr:
		ep, outputCols, err = b.buildDelete(t)

	case *memo.LockExpr:
		ep, outputCols, err = b.buildLock(t)

	case *memo.BarrierExpr:
		ep, outputCols, err = b.buildBarrier(t)

	case *memo.CreateTableExpr:
		ep, outputCols, err = b.buildCreateTable(t)

	case *memo.CreateViewExpr:
		ep, outputCols, err = b.buildCreateView(t)

	case *memo.CreateFunctionExpr:
		ep, outputCols, err = b.buildCreateFunction(t)

	case *memo.CreateTriggerExpr:
		ep, outputCols, err = b.buildCreateTrigger(t)

	case *memo.WithExpr:
		ep, outputCols, err = b.buildWith(t)

	case *memo.WithScanExpr:
		ep, outputCols, err = b.buildWithScan(t)

	case *memo.RecursiveCTEExpr:
		ep, outputCols, err = b.buildRecursiveCTE(t)

	case *memo.CallExpr:
		ep, outputCols, err = b.buildCall(t)

	case *memo.ExplainExpr:
		ep, outputCols, err = b.buildExplain(t)

	case *memo.ShowTraceForSessionExpr:
		ep, outputCols, err = b.buildShowTrace(t)

	case *memo.OpaqueRelExpr, *memo.OpaqueMutationExpr, *memo.OpaqueDDLExpr:
		ep, outputCols, err = b.buildOpaque(t.Private().(*memo.OpaqueRelPrivate))

	case *memo.AlterTableSplitExpr:
		ep, outputCols, err = b.buildAlterTableSplit(t)

	case *memo.AlterTableUnsplitExpr:
		ep, outputCols, err = b.buildAlterTableUnsplit(t)

	case *memo.AlterTableUnsplitAllExpr:
		ep, outputCols, err = b.buildAlterTableUnsplitAll(t)

	case *memo.AlterTableRelocateExpr:
		ep, outputCols, err = b.buildAlterTableRelocate(t)

	case *memo.AlterRangeRelocateExpr:
		ep, outputCols, err = b.buildAlterRangeRelocate(t)

	case *memo.ControlJobsExpr:
		ep, outputCols, err = b.buildControlJobs(t)

	case *memo.ControlSchedulesExpr:
		ep, outputCols, err = b.buildControlSchedules(t)

	case *memo.ShowCompletionsExpr:
		ep, outputCols, err = b.buildShowCompletions(t)

	case *memo.CancelQueriesExpr:
		ep, outputCols, err = b.buildCancelQueries(t)

	case *memo.CancelSessionsExpr:
		ep, outputCols, err = b.buildCancelSessions(t)

	case *memo.CreateStatisticsExpr:
		ep, outputCols, err = b.buildCreateStatistics(t)

	case *memo.ExportExpr:
		ep, outputCols, err = b.buildExport(t)

	default:
		switch {
		case opt.IsSetOp(e):
			ep, outputCols, err = b.buildSetOp(e)

		case opt.IsJoinNonApplyOp(e):
			ep, outputCols, err = b.buildHashJoin(e)

		case opt.IsJoinApplyOp(e):
			ep, outputCols, err = b.buildApplyJoin(e)

		default:
			err = errors.AssertionFailedf("no execbuild for %T", t)
		}
	}
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// In test builds, assert that the exec plan output columns match the opt
	// plan output columns.
	if buildutil.CrdbTestBuild {
		optCols := e.Relational().OutputCols
		var execCols opt.ColSet
		outputCols.ForEach(func(col opt.ColumnID, ord int) {
			execCols.Add(col)
		})
		if !execCols.Equals(optCols) {
			return execPlan{}, colOrdMap{}, errors.AssertionFailedf(
				"exec columns do not match opt columns: expected %v, got %v. op: %T", optCols, execCols, e)
		}
	}

	b.maybeAnnotateWithEstimates(ep.root, e)

	if saveTableName != "" {
		// The output columns do not change in applySaveTable.
		ep, err = b.applySaveTable(ep, outputCols, e, saveTableName)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}

	// Wrap the expression in a render expression if presentation requires it.
	if p := e.RequiredPhysical(); !p.Presentation.Any() {
		ep, outputCols, err = b.applyPresentation(ep, outputCols, p.Presentation)
	}
	return ep, outputCols, err
}

// maybeAnnotateWithEstimates checks if we are building against an
// ExplainFactory and annotates the node with more information if so.
func (b *Builder) maybeAnnotateWithEstimates(node exec.Node, e memo.RelExpr) {
	if ef, ok := b.factory.(exec.ExplainFactory); ok {
		stats := e.Relational().Statistics()
		val := exec.EstimatedStats{
			TableStatsAvailable: stats.Available,
			RowCount:            stats.RowCount,
			Cost:                float64(e.Cost()),
		}
		if scan, ok := e.(*memo.ScanExpr); ok {
			tab := b.mem.Metadata().Table(scan.Table)
			if tab.StatisticCount() > 0 {
				// The first stat is the most recent full one.
				var first int
				for first < tab.StatisticCount() &&
					tab.Statistic(first).IsPartial() ||
					(tab.Statistic(first).IsForecast() && !b.evalCtx.SessionData().OptimizerUseForecasts) {
					first++
				}

				if first < tab.StatisticCount() {
					stat := tab.Statistic(first)
					val.TableStatsRowCount = stat.RowCount()
					if val.TableStatsRowCount == 0 {
						val.TableStatsRowCount = 1
					}
					val.TableStatsCreatedAt = stat.CreatedAt()
					val.LimitHint = scan.RequiredPhysical().LimitHint
					val.Forecast = stat.IsForecast()
					if val.Forecast {
						val.ForecastAt = stat.CreatedAt()
						// Find the first non-forecast full stat.
						for i := first + 1; i < tab.StatisticCount(); i++ {
							nextStat := tab.Statistic(i)
							if !nextStat.IsPartial() && !nextStat.IsForecast() {
								val.TableStatsCreatedAt = nextStat.CreatedAt()
								break
							}
						}
					}
				}
			}
		}
		ef.AnnotateNode(node, exec.EstimatedStatsID, &val)
	}
}

func (b *Builder) buildValues(
	values *memo.ValuesExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	rows, err := b.buildValuesRows(values)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return b.constructValues(rows, values.Cols)
}

func (b *Builder) buildValuesRows(values *memo.ValuesExpr) ([][]tree.TypedExpr, error) {
	numCols := len(values.Cols)

	rows := makeTypedExprMatrix(len(values.Rows), numCols)
	scalarCtx := buildScalarCtx{}
	for i := range rows {
		tup := values.Rows[i].(*memo.TupleExpr)
		if len(tup.Elems) != numCols {
			return nil, fmt.Errorf("inconsistent row length %d vs %d", len(tup.Elems), numCols)
		}
		var err error
		for j := 0; j < numCols; j++ {
			rows[i][j], err = b.buildScalar(&scalarCtx, tup.Elems[j])
			if err != nil {
				return nil, err
			}
		}
	}
	return rows, nil
}

// makeTypedExprMatrix allocates a TypedExpr matrix of the given size.
func makeTypedExprMatrix(numRows, numCols int) [][]tree.TypedExpr {
	rows := make([][]tree.TypedExpr, numRows)
	rowBuf := make([]tree.TypedExpr, numRows*numCols)
	for i := range rows {
		// Chop off prefix of rowBuf and limit its capacity.
		rows[i] = rowBuf[:numCols:numCols]
		rowBuf = rowBuf[numCols:]
	}
	return rows
}

func (b *Builder) constructValues(
	rows [][]tree.TypedExpr, cols opt.ColList,
) (_ execPlan, outputCols colOrdMap, err error) {
	md := b.mem.Metadata()
	resultCols := make(colinfo.ResultColumns, len(cols))
	for i, col := range cols {
		colMeta := md.ColumnMeta(col)
		resultCols[i].Name = colMeta.Alias
		resultCols[i].Typ = colMeta.Type
	}
	node, err := b.factory.ConstructValues(rows, resultCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	ep := execPlan{root: node}
	outputCols = b.colOrdsAlloc.Alloc()
	for i, col := range cols {
		outputCols.Set(col, i)
	}

	return ep, outputCols, nil
}

func (b *Builder) buildLiteralValues(
	values *memo.LiteralValuesExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	md := b.mem.Metadata()
	resultCols := make(colinfo.ResultColumns, len(values.ColList()))
	for i, col := range values.ColList() {
		colMeta := md.ColumnMeta(col)
		resultCols[i].Name = colMeta.Alias
		resultCols[i].Typ = colMeta.Type
	}
	node, err := b.factory.ConstructLiteralValues(values.Rows.Rows, resultCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	ep := execPlan{root: node}
	outputCols = b.colOrdsAlloc.Alloc()
	for i, col := range values.ColList() {
		outputCols.Set(col, i)
	}

	return ep, outputCols, nil
}

// getColumns returns the set of column ordinals in the table for the set of
// column IDs, along with a mapping from the column IDs to output ordinals
// (starting with 0).
func (b *Builder) getColumns(
	cols opt.ColSet, tableID opt.TableID,
) (exec.TableColumnOrdinalSet, colOrdMap) {
	var needed exec.TableColumnOrdinalSet
	output := b.colOrdsAlloc.Alloc()

	columnCount := b.mem.Metadata().Table(tableID).ColumnCount()
	n := 0
	for i := 0; i < columnCount; i++ {
		colID := tableID.ColumnID(i)
		if cols.Contains(colID) {
			needed.Add(i)
			output.Set(colID, n)
			n++
		}
	}

	return needed, output
}

// indexConstraintMaxResults returns the maximum number of results for a scan;
// if successful (ok=true), the scan is guaranteed never to return more results
// than maxRows.
func (b *Builder) indexConstraintMaxResults(
	scan *memo.ScanPrivate, relProps *props.Relational,
) (maxRows uint64, ok bool) {
	c := scan.Constraint
	if c == nil || c.IsContradiction() || c.IsUnconstrained() {
		return 0, false
	}

	numCols := c.Columns.Count()
	var indexCols opt.ColSet
	for i := 0; i < numCols; i++ {
		indexCols.Add(c.Columns.Get(i).ID())
	}
	if !relProps.FuncDeps.ColsAreLaxKey(indexCols) {
		return 0, false
	}

	return c.CalculateMaxResults(b.ctx, b.evalCtx, indexCols, relProps.NotNullCols)
}

// scanParams populates ScanParams and the output column mapping.
func (b *Builder) scanParams(
	tab cat.Table, scan *memo.ScanPrivate, relProps *props.Relational, reqProps *physical.Required,
) (exec.ScanParams, colOrdMap, error) {
	// Check if we tried to force a specific index but there was no Scan with that
	// index in the memo.
	if scan.Flags.ForceIndex && scan.Flags.Index != scan.Index {
		idx := tab.Index(scan.Flags.Index)
		isInverted := idx.IsInverted()
		_, isPartial := idx.Predicate()

		var err error
		switch {
		case isInverted && isPartial:
			err = pgerror.Newf(pgcode.WrongObjectType,
				"index \"%s\" is a partial inverted index and cannot be used for this query",
				idx.Name(),
			)
		case isInverted:
			err = pgerror.Newf(pgcode.WrongObjectType,
				"index \"%s\" is inverted and cannot be used for this query",
				idx.Name(),
			)
		case isPartial:
			err = pgerror.Newf(pgcode.WrongObjectType,
				"index \"%s\" is a partial index that does not contain all the rows needed to execute this query",
				idx.Name(),
			)
		default:
			err = pgerror.Newf(pgcode.WrongObjectType,
				"index \"%s\" cannot be used for this query", idx.Name())
			if b.evalCtx.SessionData().DisallowFullTableScans &&
				(b.flags.IsSet(exec.PlanFlagContainsLargeFullTableScan) ||
					b.flags.IsSet(exec.PlanFlagContainsLargeFullIndexScan)) {
				// TODO(#123783): this code might need an adjustment for virtual
				// tables.
				err = errors.WithHint(err,
					"try overriding the `disallow_full_table_scans` or increasing the `large_full_scan_rows` cluster/session settings",
				)
			}
		}

		return exec.ScanParams{}, colOrdMap{}, err
	}

	locking, err := b.buildLocking(scan.Table, scan.Locking)

	if err != nil {
		return exec.ScanParams{}, colOrdMap{}, err
	}

	needed, outputMap := b.getColumns(scan.Cols, scan.Table)

	// Get the estimated row count from the statistics. When there are no
	// statistics available, we construct a scan node with
	// the estimated row count of zero rows.
	//
	// Note: if this memo was originally created as part of a PREPARE
	// statement or was stored in the query cache, the column stats would have
	// been removed by DetachMemo. Update that function if the column stats are
	// needed here in the future.
	var rowCount uint64
	if relProps.Statistics().Available {
		rowCount = uint64(math.Ceil(relProps.Statistics().RowCount))
	}

	if scan.PartitionConstrainedScan {
		sqltelemetry.IncrementPartitioningCounter(sqltelemetry.PartitionConstrainedScan)
	}

	softLimit := reqProps.LimitHintInt64()
	hardLimit := scan.HardLimit.RowCount()
	maxResults, maxResultsOk := b.indexConstraintMaxResults(scan, relProps)

	// If the txn_rows_read_err guardrail is set, make sure that we never read
	// more than txn_rows_read_err+1 rows on any single scan. Adding a hard limit
	// of txn_rows_read_err+1 ensures that the results will still be correct since
	// the conn_executor will return an error if the limit is actually reached.
	if txnRowsReadErr := b.evalCtx.SessionData().TxnRowsReadErr; txnRowsReadErr > 0 &&
		(hardLimit == 0 || hardLimit > txnRowsReadErr+1) &&
		(!maxResultsOk || maxResults > uint64(txnRowsReadErr+1)) {
		hardLimit = txnRowsReadErr + 1
	}

	// If this is a bounded staleness query, check that it touches at most one
	// range.
	if b.boundedStaleness() {
		valid := true
		if b.containsBoundedStalenessScan {
			// We already planned a scan, perhaps as part of a subquery.
			valid = false
		} else if hardLimit != 0 {
			// If hardLimit is not 0, from KV's perspective, this is a multi-row scan
			// with a limit. That means that even if the limit is 1, the scan can span
			// multiple ranges if the first range is empty.
			valid = false
		} else {
			valid = maxResultsOk && maxResults == 1
		}
		if !valid {
			return exec.ScanParams{}, colOrdMap{}, unimplemented.NewWithIssuef(67562,
				"cannot use bounded staleness for queries that may touch more than one range or require an index join",
			)
		}
		b.containsBoundedStalenessScan = true
	}

	parallelize := false
	if hardLimit == 0 && softLimit == 0 {
		if maxResultsOk &&
			maxResults < getParallelScanResultThreshold(b.evalCtx.TestingKnobs.ForceProductionValues) {
			// Don't set the flag when we have a single span which returns a single
			// row: it does nothing in this case except litter EXPLAINs.
			// There are still cases where the flag doesn't do anything when the spans
			// cover a single range, but there is nothing we can do about that.
			if !(maxResults == 1 && scan.Constraint.Spans.Count() == 1) {
				parallelize = true
			}
		} else if b.evalCtx != nil {
			// If 'unbounded_parallel_scans' is true, even though we cannot
			// guarantee a sufficiently small upper bound on the number of rows
			// scanned, we still will parallelize this scan.
			parallelize = b.evalCtx.SessionData().UnboundedParallelScans
		}
	}

	// Figure out if we need to scan in reverse (ScanPrivateCanProvide takes
	// HardLimit.Reverse() into account).
	ok, reverse := ordering.ScanPrivateCanProvide(
		b.mem.Metadata(),
		scan,
		&reqProps.Ordering,
	)
	if !ok {
		return exec.ScanParams{}, colOrdMap{}, errors.AssertionFailedf("scan can't provide required ordering")
	}

	return exec.ScanParams{
		NeededCols:         needed,
		IndexConstraint:    scan.Constraint,
		InvertedConstraint: scan.InvertedConstraint,
		HardLimit:          hardLimit,
		SoftLimit:          softLimit,
		Reverse:            reverse,
		Parallelize:        parallelize,
		Locking:            locking,
		EstimatedRowCount:  rowCount,
		LocalityOptimized:  scan.LocalityOptimized,
	}, outputMap, nil
}

func (b *Builder) buildScan(scan *memo.ScanExpr) (_ execPlan, outputCols colOrdMap, err error) {
	md := b.mem.Metadata()
	tab := md.Table(scan.Table)

	if !b.disableTelemetry && scan.PartialIndexPredicate(md) != nil {
		telemetry.Inc(sqltelemetry.PartialIndexScanUseCounter)
	}

	if scan.Flags.ForceZigzag {
		return execPlan{}, colOrdMap{}, fmt.Errorf("could not produce a query plan conforming to the FORCE_ZIGZAG hint")
	}

	isUnfiltered := scan.IsUnfiltered(md)
	if scan.Flags.NoFullScan {
		// Normally a full scan of a partial index would be allowed with the
		// NO_FULL_SCAN hint (isUnfiltered is false for partial indexes), but if the
		// user has explicitly forced the partial index *and* used NO_FULL_SCAN, we
		// disallow the full index scan.
		if isUnfiltered || (scan.Flags.ForceIndex && scan.IsFullIndexScan()) {
			return execPlan{}, colOrdMap{}, fmt.Errorf("could not produce a query plan conforming to the NO_FULL_SCAN hint")
		}
	}

	if scan.Flags.ForceInvertedIndex && !scan.IsInvertedScan(md) {
		return execPlan{}, colOrdMap{}, fmt.Errorf("could not produce a query plan conforming to the FORCE_INVERTED_INDEX hint")
	}

	idx := tab.Index(scan.Index)
	if idx.IsInverted() && len(scan.InvertedConstraint) == 0 && scan.Constraint == nil {
		return execPlan{}, colOrdMap{},
			errors.AssertionFailedf("expected inverted index scan to have a constraint")
	}
	b.IndexesUsed.add(tab.ID(), idx.ID())

	// Save if we planned a full (large) table/index scan on the builder so that
	// the planner can be made aware later. We only do this for non-virtual
	// tables.
	// TODO(#27611): consider doing this for virtual tables too once we have
	// stats for them and adjust the comments if so.
	// TODO(#123783): not setting the plan flags allows plans with full scans of
	// virtual tables to not be rejected when disallow_full_table_scans is set.
	relProps := scan.Relational()
	stats := relProps.Statistics()
	if !tab.IsVirtualTable() && isUnfiltered {
		large := !stats.Available || stats.RowCount >= b.evalCtx.SessionData().LargeFullScanRows
		if scan.Index == cat.PrimaryIndex {
			b.flags.Set(exec.PlanFlagContainsFullTableScan)
			if large {
				b.flags.Set(exec.PlanFlagContainsLargeFullTableScan)
			}
		} else {
			b.flags.Set(exec.PlanFlagContainsFullIndexScan)
			if large {
				b.flags.Set(exec.PlanFlagContainsLargeFullIndexScan)
			}
		}
		if stats.Available && stats.RowCount > b.MaxFullScanRows {
			b.MaxFullScanRows = stats.RowCount
		}
	}

	// Save some instrumentation info.
	b.ScanCounts[exec.ScanCount]++
	if stats.Available {
		b.TotalScanRows += stats.RowCount
		b.ScanCounts[exec.ScanWithStatsCount]++

		// The first stat is the most recent full one. Check if it was a forecast.
		var first int
		for first < tab.StatisticCount() && tab.Statistic(first).IsPartial() {
			first++
		}
		if first < tab.StatisticCount() && tab.Statistic(first).IsForecast() {
			if b.evalCtx.SessionData().OptimizerUseForecasts {
				b.ScanCounts[exec.ScanWithStatsForecastCount]++

				// Calculate time since the forecast (or negative time until the forecast).
				nanosSinceStatsForecasted := timeutil.Since(tab.Statistic(first).CreatedAt())
				if nanosSinceStatsForecasted.Abs() > b.NanosSinceStatsForecasted.Abs() {
					b.NanosSinceStatsForecasted = nanosSinceStatsForecasted
				}
			}
			// Find the first non-forecast full stat.
			for first < tab.StatisticCount() &&
				(tab.Statistic(first).IsPartial() || tab.Statistic(first).IsForecast()) {
				first++
			}
		}

		if first < tab.StatisticCount() {
			tabStat := tab.Statistic(first)

			nanosSinceStatsCollected := timeutil.Since(tabStat.CreatedAt())
			if nanosSinceStatsCollected > b.NanosSinceStatsCollected {
				b.NanosSinceStatsCollected = nanosSinceStatsCollected
			}

			// Calculate another row count estimate using these (non-forecast)
			// stats. If forecasts were not used, this should be the same as
			// stats.RowCount.
			rowCountWithoutForecast := float64(tabStat.RowCount())
			rowCountWithoutForecast *= stats.Selectivity.AsFloat()
			minCardinality, maxCardinality := relProps.Cardinality.Min, relProps.Cardinality.Max
			if rowCountWithoutForecast > float64(maxCardinality) && maxCardinality != math.MaxUint32 {
				rowCountWithoutForecast = float64(maxCardinality)
			} else if rowCountWithoutForecast < float64(minCardinality) {
				rowCountWithoutForecast = float64(minCardinality)
			}
			b.TotalScanRowsWithoutForecasts += rowCountWithoutForecast
		}
	}

	var params exec.ScanParams
	params, outputCols, err = b.scanParams(tab, &scan.ScanPrivate, scan.Relational(), scan.RequiredPhysical())
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	reqOrdering, err := reqOrdering(scan, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	root, err := b.factory.ConstructScan(
		tab,
		tab.Index(scan.Index),
		params,
		reqOrdering,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	var res execPlan
	res.root = root
	if b.evalCtx.SessionData().EnforceHomeRegion && b.IsANSIDML && b.doScanExprCollection {
		if b.builtScans == nil {
			// Make this large enough to handle simple 2-table join queries without
			// wasting memory.
			b.builtScans = make([]*memo.ScanExpr, 0, 2)
		}
		b.builtScans = append(b.builtScans, scan)
	}
	return res, outputCols, nil
}

func (b *Builder) buildPlaceholderScan(
	scan *memo.PlaceholderScanExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	if scan.Constraint != nil || scan.InvertedConstraint != nil {
		return execPlan{}, colOrdMap{}, errors.AssertionFailedf("PlaceholderScan cannot have constraints")
	}

	md := b.mem.Metadata()
	tab := md.Table(scan.Table)
	idx := tab.Index(scan.Index)

	// Build the index constraint.
	spanColumns := make([]opt.OrderingColumn, len(scan.Span))
	for i := range spanColumns {
		col := idx.Column(i)
		ordinal := col.Ordinal()
		colID := scan.Table.ColumnID(ordinal)
		spanColumns[i] = opt.MakeOrderingColumn(colID, col.Descending)
	}
	var columns constraint.Columns
	columns.Init(spanColumns)
	keyCtx := constraint.MakeKeyContext(b.ctx, &columns, b.evalCtx)

	values := make([]tree.Datum, len(scan.Span))
	for i, expr := range scan.Span {
		// The expression is either a placeholder or a constant.
		if p, ok := expr.(*memo.PlaceholderExpr); ok {
			val, err := eval.Expr(b.ctx, b.evalCtx, p.Value)
			if err != nil {
				return execPlan{}, colOrdMap{}, err
			}
			values[i] = val
		} else {
			values[i] = memo.ExtractConstDatum(expr)
		}
	}

	key := constraint.MakeCompositeKey(values...)
	var span constraint.Span
	span.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
	var spans constraint.Spans
	spans.InitSingleSpan(&span)

	var c constraint.Constraint
	c.Init(&keyCtx, &spans)

	private := scan.ScanPrivate
	private.SetConstraint(b.ctx, b.evalCtx, &c)

	var params exec.ScanParams
	params, outputCols, err = b.scanParams(tab, &private, scan.Relational(), scan.RequiredPhysical())
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	reqOrdering, err := reqOrdering(scan, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	root, err := b.factory.ConstructScan(
		tab,
		tab.Index(scan.Index),
		params,
		reqOrdering,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	var res execPlan
	res.root = root
	return res, outputCols, nil
}

func (b *Builder) buildSelect(sel *memo.SelectExpr) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildRelational(sel.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	filter, err := b.buildScalarWithMap(inputCols, &sel.Filters)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	reqOrder, err := reqOrdering(sel, inputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var res execPlan
	res.root, err = b.factory.ConstructFilter(input.root, filter, reqOrder)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	// A filtering node does not modify the schema, so we can pass along the
	// input's output columns.
	return res, inputCols, nil
}

func (b *Builder) buildInvertedFilter(
	invFilter *memo.InvertedFilterExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildRelational(invFilter.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var res execPlan
	invertedCol, err := getNodeColumnOrdinal(inputCols, invFilter.InvertedColumn)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var typedPreFilterExpr tree.TypedExpr
	var typ *types.T
	if invFilter.PreFiltererState != nil && invFilter.PreFiltererState.Expr != nil {
		// The expression has a single variable, corresponding to the indexed
		// column. We assign it an ordinal of 0.
		colMap := b.colOrdsAlloc.Alloc()
		colMap.Set(invFilter.PreFiltererState.Col, 0)
		ctx := buildScalarCtx{
			ivh:     tree.MakeIndexedVarHelper(nil /* container */, 1),
			ivarMap: colMap,
		}
		typedPreFilterExpr, err = b.buildScalar(&ctx, invFilter.PreFiltererState.Expr)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		typ = invFilter.PreFiltererState.Typ
	}
	res.root, err = b.factory.ConstructInvertedFilter(
		input.root, invFilter.InvertedExpression, typedPreFilterExpr, typ, invertedCol)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	// Apply a post-projection to remove the inverted column.
	//
	// TODO(rytaft): the invertedFilter used to do this post-projection, but we
	// had difficulty integrating that behavior. Investigate and restore that
	// original behavior.
	return b.applySimpleProject(res, inputCols, invFilter, invFilter.Relational().OutputCols, invFilter.ProvidedPhysical().Ordering)
}

// applySimpleProject adds a simple projection on top of an existing plan. The
// returned outputCols are always a new map distinct from inputCols.
func (b *Builder) applySimpleProject(
	input execPlan,
	inputCols colOrdMap,
	inputExpr memo.RelExpr,
	cols opt.ColSet,
	providedOrd opt.Ordering,
) (_ execPlan, outputCols colOrdMap, err error) {
	// Since we are constructing a simple project on top of the main operator,
	// we need to explicitly annotate the latter with estimates since the code
	// in buildRelational() will attach them to the project.
	b.maybeAnnotateWithEstimates(input.root, inputExpr)
	// We have only pass-through columns.
	colList := make([]exec.NodeColumnOrdinal, 0, cols.Len())
	var res execPlan
	outputCols = b.colOrdsAlloc.Alloc()
	for i, ok := cols.Next(0); ok; i, ok = cols.Next(i + 1) {
		outputCols.Set(i, len(colList))
		ord, err := getNodeColumnOrdinal(inputCols, i)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		colList = append(colList, ord)
	}
	sqlOrdering, err := sqlOrdering(providedOrd, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	res.root, err = b.factory.ConstructSimpleProject(
		input.root, colList, exec.OutputOrdering(sqlOrdering),
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return res, outputCols, nil
}

func (b *Builder) buildProject(
	prj *memo.ProjectExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	md := b.mem.Metadata()
	input, inputCols, err := b.buildRelational(prj.Input)
	// The input column map is only used for the lifetime of this function, so
	// free the map afterward. In the case of a simple project,
	// applySimpleProject will create a new map.
	defer b.colOrdsAlloc.Free(inputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	projections := prj.Projections
	if len(projections) == 0 {
		// We have only pass-through columns.
		return b.applySimpleProject(input, inputCols, prj.Input, prj.Passthrough, prj.ProvidedPhysical().Ordering)
	}

	numExprs := len(projections) + prj.Passthrough.Len()
	exprs := make(tree.TypedExprs, 0, numExprs)
	cols := make(colinfo.ResultColumns, 0, numExprs)
	ctx := makeBuildScalarCtx(inputCols)
	outputCols = b.colOrdsAlloc.Alloc()
	for i := range projections {
		item := &projections[i]
		expr, err := b.buildScalar(&ctx, item.Element)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		outputCols.Set(item.Col, i)
		exprs = append(exprs, expr)
		cols = append(cols, colinfo.ResultColumn{
			Name: md.ColumnMeta(item.Col).Alias,
			Typ:  item.Typ,
		})
	}
	for colID, ok := prj.Passthrough.Next(0); ok; colID, ok = prj.Passthrough.Next(colID + 1) {
		outputCols.Set(colID, len(exprs))
		indexedVar, err := b.indexedVar(&ctx, md, colID)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		exprs = append(exprs, indexedVar)
		meta := md.ColumnMeta(colID)
		cols = append(cols, colinfo.ResultColumn{
			Name: meta.Alias,
			Typ:  meta.Type,
		})
	}
	reqOrdering, err := reqOrdering(prj, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var res execPlan
	res.root, err = b.factory.ConstructRender(input.root, cols, exprs, reqOrdering)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return res, outputCols, nil
}

func (b *Builder) buildApplyJoin(join memo.RelExpr) (_ execPlan, outputCols colOrdMap, err error) {
	switch join.Op() {
	case opt.InnerJoinApplyOp, opt.LeftJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
	default:
		return execPlan{}, colOrdMap{}, fmt.Errorf("couldn't execute correlated subquery with op %s", join.Op())
	}
	joinType, err := joinOpToJoinType(join.Op())
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	leftExpr := join.Child(0).(memo.RelExpr)
	leftProps := leftExpr.Relational()
	rightExpr := join.Child(1).(memo.RelExpr)
	rightProps := rightExpr.Relational()
	filters := join.Child(2).(*memo.FiltersExpr)

	// We will pre-populate the withExprs of the right-hand side execbuilder.
	withExprs := make([]builtWithExpr, len(b.withExprs))
	copy(withExprs, b.withExprs)

	leftPlan, leftCols, err := b.buildRelational(leftExpr)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Make a copy of the required props for the right side.
	rightRequiredProps := *rightExpr.RequiredPhysical()
	// The right-hand side will produce the output columns in order.
	rightRequiredProps.Presentation = b.makePresentation(rightProps.OutputCols)

	// leftBoundCols is the set of columns that this apply join binds.
	leftBoundCols := leftProps.OutputCols.Intersection(rightProps.OuterCols)
	// leftBoundColMap is a map from opt.ColumnID to opt.ColumnOrdinal that maps
	// a column bound by the left side of this apply join to the column ordinal
	// in the left side that contains the binding.
	leftBoundColMap := b.colOrdsAlloc.Alloc()
	for col, ok := leftBoundCols.Next(0); ok; col, ok = leftBoundCols.Next(col + 1) {
		v, ok := leftCols.Get(col)
		if !ok {
			return execPlan{}, colOrdMap{}, fmt.Errorf("couldn't find binding column %d in left output columns", col)
		}
		leftBoundColMap.Set(col, v)
	}

	// Now, the cool part! We set up an ApplyJoinPlanRightSideFn which plans the
	// right side given a particular left side row. We do this planning in a
	// separate memo. We use an exec.Factory passed to the closure rather than
	// b.factory to support executing plans that are generated with
	// explain.Factory.
	//
	// Note: we put o outside of the function so we allocate it only once.
	var o xform.Optimizer
	planRightSideFn := func(ctx context.Context, ef exec.Factory, leftRow tree.Datums) (_ exec.Plan, err error) {
		defer func() {
			if r := recover(); r != nil {
				// This code allows us to propagate internal errors without having to add
				// error checks everywhere throughout the code. This is only possible
				// because the code does not update shared state and does not manipulate
				// locks.
				//
				// This is the same panic-catching logic that exists in
				// o.Optimize() below. It's required here because it's possible
				// for factory functions to panic below, like
				// CopyAndReplaceDefault.
				if ok, e := errorutil.ShouldCatch(r); ok {
					err = e
					log.VEventf(ctx, 1, "%v", err)
				} else {
					// Other panic objects can't be considered "safe" and thus are
					// propagated as crashes that terminate the session.
					panic(r)
				}
			}
		}()

		o.Init(ctx, b.evalCtx, b.catalog)
		f := o.Factory()

		// Copy the right expression into a new memo, replacing each bound column
		// with the corresponding value from the left row.
		addedWithBindings := false
		var replaceFn norm.ReplaceFunc
		replaceFn = func(e opt.Expr) opt.Expr {
			switch t := e.(type) {
			case *memo.VariableExpr:
				if leftOrd, ok := leftBoundColMap.Get(t.Col); ok {
					return f.ConstructConstVal(leftRow[leftOrd], t.Typ)
				}

			case *memo.WithScanExpr:
				// Allow referring to "outer" With expressions. The bound
				// expressions are not part of this Memo, but they are used only
				// for their relational properties, which should be valid.
				//
				// We must add all With expressions to the metadata even if they
				// aren't referred to directly because they might be referred to
				// transitively through other With expressions. For example, if
				// the RHS refers to With expression &1, and &1 refers to With
				// expression &2, we must include &2 in the metadata so that
				// its relational properties are available. See #87733.
				//
				// We lazily add these With expressions to the metadata here
				// because the call to Factory.CopyAndReplace below clears With
				// expressions in the metadata.
				if !addedWithBindings {
					b.mem.Metadata().ForEachWithBinding(func(id opt.WithID, expr opt.Expr) {
						f.Metadata().AddWithBinding(id, expr)
					})
					addedWithBindings = true
				}
				// Fall through.
			}
			return f.CopyAndReplaceDefault(e, replaceFn)
		}
		f.CopyAndReplace(rightExpr, &rightRequiredProps, replaceFn)

		newRightSide, err := o.Optimize()
		if err != nil {
			return nil, err
		}

		eb := New(ctx, ef, &o, f.Memo(), b.catalog, newRightSide, b.semaCtx, b.evalCtx, false /* allowAutoCommit */, b.IsANSIDML)
		eb.disableTelemetry = true
		eb.withExprs = withExprs
		plan, err := eb.Build()
		if err != nil {
			if errors.IsAssertionFailure(err) {
				// Enhance the error with the EXPLAIN (OPT, VERBOSE) of the inner
				// expression.
				fmtFlags := memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars | memo.ExprFmtHideTypes |
					memo.ExprFmtHideNotVisibleIndexInfo | memo.ExprFmtHideFastPathChecks
				explainOpt := o.FormatExpr(newRightSide, fmtFlags, false /* redactableValues */)
				err = errors.WithDetailf(err, "newRightSide:\n%s", explainOpt)
			}
			return nil, err
		}
		return plan, nil
	}

	// The right plan will always produce the columns in the presentation, in
	// the same order. This map is only used for the lifetime of this function,
	// so free the map afterward.
	rightOutputCols := b.colOrdsAlloc.Alloc()
	defer b.colOrdsAlloc.Free(rightOutputCols)
	for i := range rightRequiredProps.Presentation {
		rightOutputCols.Set(rightRequiredProps.Presentation[i].ID, i)
	}
	allCols := b.joinOutputMap(leftCols, rightOutputCols)

	var onExpr tree.TypedExpr
	if len(*filters) != 0 {
		scalarCtx := buildScalarCtx{
			ivh:     tree.MakeIndexedVarHelper(nil /* container */, allCols.MaxOrd()+1),
			ivarMap: allCols,
		}
		onExpr, err = b.buildScalar(&scalarCtx, filters)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}

	if !joinType.ShouldIncludeRightColsInOutput() {
		outputCols = leftCols
		// allCols is no longer used, so it can be freed.
		b.colOrdsAlloc.Free(allCols)
	} else {
		outputCols = allCols
		// leftCols is no longer used, so it can be freed.
		b.colOrdsAlloc.Free(leftCols)
	}

	var ep execPlan

	b.recordJoinType(joinType)
	b.recordJoinAlgorithm(exec.ApplyJoin)
	ep.root, err = b.factory.ConstructApplyJoin(
		joinType,
		leftPlan.root,
		b.presentationToResultColumns(rightRequiredProps.Presentation),
		onExpr,
		planRightSideFn,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return ep, outputCols, nil
}

// makePresentation creates a Presentation that contains the given columns, in
// order of their IDs.
func (b *Builder) makePresentation(cols opt.ColSet) physical.Presentation {
	md := b.mem.Metadata()
	result := make(physical.Presentation, 0, cols.Len())
	cols.ForEach(func(col opt.ColumnID) {
		result = append(result, opt.AliasedColumn{
			Alias: md.ColumnMeta(col).Alias,
			ID:    col,
		})
	})
	return result
}

// presentationToResultColumns returns ResultColumns corresponding to the
// columns in a presentation.
func (b *Builder) presentationToResultColumns(pres physical.Presentation) colinfo.ResultColumns {
	md := b.mem.Metadata()
	result := make(colinfo.ResultColumns, len(pres))
	for i := range pres {
		result[i] = colinfo.ResultColumn{
			Name: pres[i].Alias,
			Typ:  md.ColumnMeta(pres[i].ID).Type,
		}
	}
	return result
}

func (b *Builder) buildHashJoin(join memo.RelExpr) (_ execPlan, outputCols colOrdMap, err error) {
	if f := join.Private().(*memo.JoinPrivate).Flags; f.Has(memo.DisallowHashJoinStoreRight) {
		// We need to do a bit of reverse engineering here to determine what the
		// hint was.
		hint := tree.AstLookup
		if !f.Has(memo.DisallowMergeJoin) {
			hint = tree.AstMerge
		} else if !f.Has(memo.DisallowInvertedJoinIntoLeft) || !f.Has(memo.DisallowInvertedJoinIntoRight) {
			hint = tree.AstInverted
		}

		return execPlan{}, colOrdMap{}, errors.Errorf(
			"could not produce a query plan conforming to the %s JOIN hint", hint,
		)
	}

	joinType, err := joinOpToJoinType(join.Op())
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	leftExpr := join.Child(0).(memo.RelExpr)
	rightExpr := join.Child(1).(memo.RelExpr)
	filters := join.Child(2).(*memo.FiltersExpr)
	if joinType == descpb.LeftSemiJoin || joinType == descpb.LeftAntiJoin {
		// The execution engine always builds the hash table on the right side
		// of the join, so it is beneficial for the smaller relation to be on
		// the right. Note that the coster assumes that execbuilder will make
		// this decision.
		//
		// There is no need to consider join hints here because there is no way
		// to apply join hints to semi or anti joins. Join hints are only
		// possible on explicit joins using the JOIN keyword, and semi and anti
		// joins are only created from implicit joins without the JOIN keyword.
		leftRowCount := leftExpr.Relational().Statistics().RowCount
		rightRowCount := rightExpr.Relational().Statistics().RowCount
		if leftRowCount < rightRowCount {
			if joinType == descpb.LeftSemiJoin {
				joinType = descpb.RightSemiJoin
			} else {
				joinType = descpb.RightAntiJoin
			}
			leftExpr, rightExpr = rightExpr, leftExpr
		}
	}

	leftEq, rightEq := memo.ExtractJoinEqualityColumns(
		leftExpr.Relational().OutputCols,
		rightExpr.Relational().OutputCols,
		*filters,
	)
	isCrossJoin := len(leftEq) == 0
	if !b.disableTelemetry {
		if isCrossJoin {
			telemetry.Inc(sqltelemetry.JoinAlgoCrossUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.JoinAlgoHashUseCounter)
		}
		telemetry.Inc(opt.JoinTypeToUseCounter(join.Op()))
	}

	left, right, onExpr, leftCols, rightCols, allCols, err := b.initJoinBuild(
		leftExpr,
		rightExpr,
		memo.ExtractRemainingJoinFilters(*filters, leftEq, rightEq),
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	switch {
	case !joinType.ShouldIncludeLeftColsInOutput():
		outputCols = rightCols
		// leftCols and allCols are only used for the lifetime of the function,
		// so they can be freed afterward.
		defer b.colOrdsAlloc.Free(leftCols)
		defer b.colOrdsAlloc.Free(allCols)
	case !joinType.ShouldIncludeRightColsInOutput():
		outputCols = leftCols
		// rightCols and allCols are only used for the lifetime of the function,
		// so they can be freed afterward.
		defer b.colOrdsAlloc.Free(rightCols)
		defer b.colOrdsAlloc.Free(allCols)
	default:
		outputCols = allCols
		// leftCols and rightCols are only used for the lifetime of the
		// function, so they can be freed afterward.
		defer b.colOrdsAlloc.Free(leftCols)
		defer b.colOrdsAlloc.Free(rightCols)
	}

	// Convert leftEq/rightEq to ordinals.
	eqColsBuf := make([]exec.NodeColumnOrdinal, 2*len(leftEq))
	leftEqOrdinals := eqColsBuf[:len(leftEq):len(leftEq)]
	rightEqOrdinals := eqColsBuf[len(leftEq):]
	for i := range leftEq {
		leftEqOrdinals[i], err = getNodeColumnOrdinal(leftCols, leftEq[i])
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		rightEqOrdinals[i], err = getNodeColumnOrdinal(rightCols, rightEq[i])
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}

	leftEqColsAreKey := leftExpr.Relational().FuncDeps.ColsAreStrictKey(leftEq.ToSet())
	rightEqColsAreKey := rightExpr.Relational().FuncDeps.ColsAreStrictKey(rightEq.ToSet())

	b.recordJoinType(joinType)
	if isCrossJoin {
		b.recordJoinAlgorithm(exec.CrossJoin)
	} else {
		b.recordJoinAlgorithm(exec.HashJoin)
	}
	var ep execPlan
	ep.root, err = b.factory.ConstructHashJoin(
		joinType,
		left.root, right.root,
		leftEqOrdinals, rightEqOrdinals,
		leftEqColsAreKey, rightEqColsAreKey,
		onExpr,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return ep, outputCols, nil
}

func (b *Builder) buildMergeJoin(
	join *memo.MergeJoinExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.JoinAlgoMergeUseCounter)
		telemetry.Inc(opt.JoinTypeToUseCounter(join.JoinType))
	}

	joinType, err := joinOpToJoinType(join.JoinType)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	leftExpr, rightExpr := join.Left, join.Right
	leftEq, rightEq := join.LeftEq, join.RightEq

	if joinType == descpb.LeftSemiJoin || joinType == descpb.LeftAntiJoin {
		// We have a partial join, and we want to make sure that the relation
		// with smaller cardinality is on the right side. Note that we assumed
		// it during the costing.
		// TODO(raduberinde): we might also need to look at memo.JoinFlags when
		// choosing a side.
		leftRowCount := leftExpr.Relational().Statistics().RowCount
		rightRowCount := rightExpr.Relational().Statistics().RowCount
		if leftRowCount < rightRowCount {
			if joinType == descpb.LeftSemiJoin {
				joinType = descpb.RightSemiJoin
			} else {
				joinType = descpb.RightAntiJoin
			}
			leftExpr, rightExpr = rightExpr, leftExpr
			leftEq, rightEq = rightEq, leftEq
		}
	}

	left, right, onExpr, leftCols, rightCols, allCols, err := b.initJoinBuild(
		leftExpr, rightExpr, join.On,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	switch {
	case !joinType.ShouldIncludeLeftColsInOutput():
		outputCols = rightCols
		// leftCols and allCols are only used for the lifetime of the function,
		// so they can be freed afterward.
		defer b.colOrdsAlloc.Free(leftCols)
		defer b.colOrdsAlloc.Free(allCols)
	case !joinType.ShouldIncludeRightColsInOutput():
		outputCols = leftCols
		// rightCols and allCols are only used for the lifetime of the function,
		// so they can be freed afterward.
		defer b.colOrdsAlloc.Free(rightCols)
		defer b.colOrdsAlloc.Free(allCols)
	default:
		outputCols = allCols
		// leftCols and rightCols are only used for the lifetime of the
		// function, so they can be freed afterward.
		defer b.colOrdsAlloc.Free(leftCols)
		defer b.colOrdsAlloc.Free(rightCols)
	}

	leftOrd, err := sqlOrdering(leftEq, leftCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	rightOrd, err := sqlOrdering(rightEq, rightCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	reqOrd, err := reqOrdering(join, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	leftEqColsAreKey := leftExpr.Relational().FuncDeps.ColsAreStrictKey(leftEq.ColSet())
	rightEqColsAreKey := rightExpr.Relational().FuncDeps.ColsAreStrictKey(rightEq.ColSet())
	b.recordJoinType(joinType)
	b.recordJoinAlgorithm(exec.MergeJoin)
	var ep execPlan
	ep.root, err = b.factory.ConstructMergeJoin(
		joinType,
		left.root, right.root,
		onExpr,
		leftOrd, rightOrd, reqOrd,
		leftEqColsAreKey, rightEqColsAreKey,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return ep, outputCols, nil
}

// initJoinBuild builds the inputs to the join as well as the ON expression.
func (b *Builder) initJoinBuild(
	leftChild memo.RelExpr, rightChild memo.RelExpr, filters memo.FiltersExpr,
) (
	leftPlan, rightPlan execPlan,
	onExpr tree.TypedExpr,
	leftCols, rightCols, allCols colOrdMap,
	_ error,
) {
	leftPlan, leftCols, err := b.buildRelational(leftChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, colOrdMap{}, colOrdMap{}, colOrdMap{}, err
	}
	rightPlan, rightCols, err = b.buildRelational(rightChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, colOrdMap{}, colOrdMap{}, colOrdMap{}, err
	}

	allCols = b.joinOutputMap(leftCols, rightCols)

	if len(filters) != 0 {
		onExpr, err = b.buildScalarWithMap(allCols, &filters)
		if err != nil {
			return execPlan{}, execPlan{}, nil, colOrdMap{}, colOrdMap{}, colOrdMap{}, err
		}
	}

	return leftPlan, rightPlan, onExpr, leftCols, rightCols, allCols, nil
}

// joinOutputMap determines the outputCols map for a (non-semi/anti) join, given
// the outputCols maps for its inputs.
func (b *Builder) joinOutputMap(left, right colOrdMap) colOrdMap {
	numLeftCols := left.MaxOrd() + 1

	res := b.colOrdsAlloc.Copy(left)
	right.ForEach(func(col opt.ColumnID, rightIdx int) {
		res.Set(col, rightIdx+numLeftCols)
	})
	return res
}

func joinOpToJoinType(op opt.Operator) (descpb.JoinType, error) {
	switch op {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return descpb.InnerJoin, nil

	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return descpb.LeftOuterJoin, nil

	case opt.RightJoinOp:
		return descpb.RightOuterJoin, nil

	case opt.FullJoinOp:
		return descpb.FullOuterJoin, nil

	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return descpb.LeftSemiJoin, nil

	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return descpb.LeftAntiJoin, nil

	default:
		return 0, errors.AssertionFailedf("not a join op %s", redact.Safe(op))
	}
}

func (b *Builder) buildGroupBy(groupBy memo.RelExpr) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildGroupByInput(groupBy)
	// The input column map is only used for the lifetime of this function, so
	// free the map afterward.
	defer b.colOrdsAlloc.Free(inputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	groupingCols := groupBy.Private().(*memo.GroupingPrivate).GroupingCols
	groupingColIdx := make([]exec.NodeColumnOrdinal, 0, groupingCols.Len())
	outputCols = b.colOrdsAlloc.Alloc()
	for i, ok := groupingCols.Next(0); ok; i, ok = groupingCols.Next(i + 1) {
		outputCols.Set(i, len(groupingColIdx))
		ord, err := getNodeColumnOrdinal(inputCols, i)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		groupingColIdx = append(groupingColIdx, ord)
	}

	aggregations := *groupBy.Child(1).(*memo.AggregationsExpr)
	aggInfos := make([]exec.AggInfo, len(aggregations))
	// There will be roughly one column per aggregation.
	argCols := make([]exec.NodeColumnOrdinal, 0, len(aggregations))
	var constArgs tree.Datums
	for i := range aggregations {
		item := &aggregations[i]
		agg := item.Agg

		var filterOrd exec.NodeColumnOrdinal = tree.NoColumnIdx
		if aggFilter, ok := agg.(*memo.AggFilterExpr); ok {
			filter, ok := aggFilter.Filter.(*memo.VariableExpr)
			if !ok {
				return execPlan{}, colOrdMap{}, errors.AssertionFailedf("only VariableOp args supported")
			}
			filterOrd, err = getNodeColumnOrdinal(inputCols, filter.Col)
			if err != nil {
				return execPlan{}, colOrdMap{}, err
			}
			agg = aggFilter.Input
		}

		distinct := false
		if aggDistinct, ok := agg.(*memo.AggDistinctExpr); ok {
			distinct = true
			agg = aggDistinct.Input
		}

		name, overload := memo.FindAggregateOverload(agg)

		// Accumulate variable arguments in argCols and constant arguments in
		// constArgs. Constant arguments must follow variable arguments.
		for j, n := 0, agg.ChildCount(); j < n; j++ {
			child := agg.Child(j)
			if variable, ok := child.(*memo.VariableExpr); ok {
				if len(constArgs) != 0 {
					return execPlan{}, colOrdMap{}, errors.Errorf("constant args must come after variable args")
				}
				ord, err := getNodeColumnOrdinal(inputCols, variable.Col)
				if err != nil {
					return execPlan{}, colOrdMap{}, err
				}
				argCols = append(argCols, ord)
			} else {
				if len(argCols) == 0 {
					return execPlan{}, colOrdMap{}, errors.Errorf("a constant arg requires at least one variable arg")
				}
				if constArgs == nil {
					// Lazily allocate constArgs.
					constArgs = make(tree.Datums, 0, len(aggregations)-i)
				}
				constArgs = append(constArgs, memo.ExtractConstDatum(child))
			}
		}

		aggInfos[i] = exec.AggInfo{
			FuncName:         name,
			Distinct:         distinct,
			ResultType:       item.Agg.DataType(),
			ArgCols:          argCols[:len(argCols):len(argCols)],
			ConstArgs:        constArgs[:len(constArgs):len(constArgs)],
			Filter:           filterOrd,
			DistsqlBlocklist: overload.DistsqlBlocklist,
		}
		outputCols.Set(item.Col, len(groupingColIdx)+i)
		// Slice argCols and constArgs so the rest of their capacity can be
		// reused.
		argCols = argCols[len(argCols):]
		constArgs = constArgs[len(constArgs):]
	}

	var ep execPlan
	if groupBy.Op() == opt.ScalarGroupByOp {
		ep.root, err = b.factory.ConstructScalarGroupBy(input.root, aggInfos)
	} else {
		groupBy := groupBy.(*memo.GroupByExpr)
		var groupingColOrder colinfo.ColumnOrdering
		groupingColOrder, err = sqlOrdering(ordering.StreamingGroupingColOrdering(
			&groupBy.GroupingPrivate, &groupBy.RequiredPhysical().Ordering,
		), inputCols)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		var reqOrd exec.OutputOrdering
		reqOrd, err = reqOrdering(groupBy, outputCols)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		orderType := exec.GroupingOrderType(groupBy.GroupingOrderType(&groupBy.RequiredPhysical().Ordering))
		var rowCount uint64
		if relProps := groupBy.Relational(); relProps.Statistics().Available {
			rowCount = uint64(math.Ceil(relProps.Statistics().RowCount))
		}
		ep.root, err = b.factory.ConstructGroupBy(
			input.root, groupingColIdx, groupingColOrder, aggInfos, reqOrd, orderType, rowCount,
		)
	}
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return ep, outputCols, nil
}

func (b *Builder) buildDistinct(
	distinct memo.RelExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	private := distinct.Private().(*memo.GroupingPrivate)

	if private.GroupingCols.Empty() {
		// A DistinctOn with no grouping columns should have been converted to a
		// LIMIT 1 or Max1Row by normalization rules.
		return execPlan{}, colOrdMap{}, fmt.Errorf("cannot execute distinct on no columns")
	}
	input, inputCols, err := b.buildGroupByInput(distinct)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	var distinctCols exec.NodeColumnOrdinalSet
	for col, ok := private.GroupingCols.Next(0); ok; col, ok = private.GroupingCols.Next(col + 1) {
		ord, err := getNodeColumnOrdinal(inputCols, col)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}

		distinctCols.Add(int(ord))
	}

	var orderedCols exec.NodeColumnOrdinalSet
	ordering := ordering.StreamingGroupingColOrdering(
		private, &distinct.RequiredPhysical().Ordering,
	)
	for i := range ordering {
		ord, err := getNodeColumnOrdinal(inputCols, ordering[i].ID())
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		orderedCols.Add(int(ord))
	}

	reqOrdering, err := reqOrdering(distinct, inputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var ep execPlan
	ep.root, err = b.factory.ConstructDistinct(
		input.root, distinctCols, orderedCols, reqOrdering,
		private.NullsAreDistinct, private.ErrorOnDup)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// buildGroupByInput can add extra sort column(s), so discard those if they
	// are present by using an additional projection.
	outCols := distinct.Relational().OutputCols
	if inputCols.MaxOrd()+1 == outCols.Len() {
		return ep, inputCols, nil
	}
	return b.ensureColumns(
		ep, inputCols, distinct, outCols.ToList(),
		distinct.ProvidedPhysical().Ordering, true, /* reuseInputCols */
	)
}

func (b *Builder) buildGroupByInput(
	groupBy memo.RelExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	groupByInput := groupBy.Child(0).(memo.RelExpr)
	input, inputCols, err := b.buildRelational(groupByInput)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// TODO(radu): this is a one-off fix for an otherwise bigger gap: we should
	// have a more general mechanism (through physical properties or otherwise) to
	// figure out unneeded columns and project them away as necessary. The
	// optimizer doesn't guarantee that it adds ProjectOps everywhere.
	//
	// We address just the GroupBy case for now because there is a particularly
	// important case with COUNT(*) where we can remove all input columns, which
	// leads to significant speedup.
	private := groupBy.Private().(*memo.GroupingPrivate)
	neededCols := private.GroupingCols.Copy()
	aggs := *groupBy.Child(1).(*memo.AggregationsExpr)
	for i := range aggs {
		neededCols = memo.AddAggInputColumns(neededCols, aggs[i].Agg)
	}

	// In rare cases, we might need a column only for its ordering, for example:
	//   SELECT concat_agg(s) FROM (SELECT s FROM kv ORDER BY k)
	// In this case we can't project the column away as it is still needed by
	// distsql to maintain the desired ordering.
	for _, c := range groupByInput.ProvidedPhysical().Ordering {
		neededCols.Add(c.ID())
	}

	if neededCols.Equals(groupByInput.Relational().OutputCols) {
		// All columns produced by the input are used.
		return input, inputCols, nil
	}

	// The input is producing columns that are not useful; set up a projection.
	cols := make([]exec.NodeColumnOrdinal, 0, neededCols.Len())
	outputCols = b.colOrdsAlloc.Alloc()
	for colID, ok := neededCols.Next(0); ok; colID, ok = neededCols.Next(colID + 1) {
		ordinal, ordOk := inputCols.Get(colID)
		if !ordOk {
			return execPlan{}, colOrdMap{},
				errors.AssertionFailedf("needed column not produced by group-by input")
		}
		outputCols.Set(colID, len(cols))
		cols = append(cols, exec.NodeColumnOrdinal(ordinal))
	}

	// The input column map is no longer used, so free it.
	b.colOrdsAlloc.Free(inputCols)

	reqOrdering, err := reqOrdering(groupByInput, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	input.root, err = b.factory.ConstructSimpleProject(input.root, cols, reqOrdering)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return input, outputCols, nil
}

func (b *Builder) buildSetOp(set memo.RelExpr) (_ execPlan, outputCols colOrdMap, err error) {
	leftExpr := set.Child(0).(memo.RelExpr)
	left, leftCols, err := b.buildRelational(leftExpr)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	rightExpr := set.Child(1).(memo.RelExpr)
	right, rightCols, err := b.buildRelational(rightExpr)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	private := set.Private().(*memo.SetPrivate)

	// We need to make sure that the two sides render the columns in the same
	// order; otherwise we add projections.
	//
	// In most cases the projection is needed only to reorder the columns, but not
	// always. For example:
	//  (SELECT a, a, b FROM ab) UNION (SELECT x, y, z FROM xyz)
	// The left input could be just a scan that produces two columns.
	//
	// TODO(radu): we don't have to respect the exact order in the two ColLists;
	// if one side has the right columns but in a different permutation, we could
	// set up a matching projection on the other side. For example:
	//   (SELECT b, c, a FROM abc) UNION (SELECT z, y, x FROM xyz)
	// The expression for this could be a UnionOp on top of two ScanOps (any
	// internal projections could be removed by normalization rules).
	// The scans produce columns `a, b, c` and `x, y, z` respectively. We could
	// leave `b, c, a` as is and project the other side to `x, z, y`.
	// Note that (unless this is part of a larger query) the presentation property
	// will ensure that the columns are presented correctly in the output (i.e. in
	// the order `b, c, a`).
	left, leftCols, err = b.ensureColumns(
		left, leftCols, leftExpr, private.LeftCols,
		leftExpr.ProvidedPhysical().Ordering, true, /* reuseInputCols */
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	right, rightCols, err = b.ensureColumns(
		right, rightCols, rightExpr, private.RightCols,
		rightExpr.ProvidedPhysical().Ordering, true, /* reuseInputCols */
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// The left and right  column maps are only used for the lifetime of this
	// function, so free them afterward.
	defer b.colOrdsAlloc.Free(leftCols)
	defer b.colOrdsAlloc.Free(rightCols)

	var typ tree.UnionType
	var all bool
	switch set.Op() {
	case opt.UnionOp:
		typ, all = tree.UnionOp, false
	case opt.UnionAllOp, opt.LocalityOptimizedSearchOp:
		typ, all = tree.UnionOp, true
	case opt.IntersectOp:
		typ, all = tree.IntersectOp, false
	case opt.IntersectAllOp:
		typ, all = tree.IntersectOp, true
	case opt.ExceptOp:
		typ, all = tree.ExceptOp, false
	case opt.ExceptAllOp:
		typ, all = tree.ExceptOp, true
	default:
		return execPlan{}, colOrdMap{}, errors.AssertionFailedf("invalid operator %s", redact.Safe(set.Op()))
	}

	switch typ {
	case tree.IntersectOp:
		b.recordJoinType(descpb.IntersectAllJoin)
	case tree.ExceptOp:
		b.recordJoinType(descpb.ExceptAllJoin)
	}

	hardLimit := uint64(0)
	enforceHomeRegion := false
	if set.Op() == opt.LocalityOptimizedSearchOp {
		if !b.disableTelemetry {
			telemetry.Inc(sqltelemetry.LocalityOptimizedSearchUseCounter)
		}

		// If we are performing locality optimized search, set a limit equal to
		// the maximum possible number of rows. This will tell the execution engine
		// not to execute the right child if the limit is reached by the left
		// child. A value of `math.MaxUint32` means the cardinality is unbounded,
		// so don't use that as a hard limit.
		// TODO(rytaft): Store the limit in the expression.
		if set.Relational().Cardinality.Max != math.MaxUint32 {
			hardLimit = uint64(set.Relational().Cardinality.Max)
		}
		enforceHomeRegion = b.IsANSIDML && b.evalCtx.SessionData().EnforceHomeRegion
	}

	outputCols = b.colOrdsAlloc.Alloc()
	for i, col := range private.OutCols {
		outputCols.Set(col, i)
	}
	streamingOrdering, err := sqlOrdering(
		ordering.StreamingSetOpOrdering(set, &set.RequiredPhysical().Ordering), outputCols,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	reqOrdering, err := reqOrdering(set, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	var ep execPlan
	if typ == tree.UnionOp && all {
		ep.root, err = b.factory.ConstructUnionAll(left.root, right.root, reqOrdering, hardLimit, enforceHomeRegion)
	} else if len(streamingOrdering) > 0 {
		if typ != tree.UnionOp {
			b.recordJoinAlgorithm(exec.MergeJoin)
		}
		ep.root, err = b.factory.ConstructStreamingSetOp(typ, all, left.root, right.root, streamingOrdering, reqOrdering)
	} else {
		if len(reqOrdering) > 0 {
			return execPlan{}, colOrdMap{}, errors.AssertionFailedf("hash set op is not supported with a required ordering")
		}
		if typ != tree.UnionOp {
			b.recordJoinAlgorithm(exec.HashJoin)
		}
		ep.root, err = b.factory.ConstructHashSetOp(typ, all, left.root, right.root)
	}
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return ep, outputCols, nil
}

// buildTopK builds a plan for a TopKOp, which is like a combined SortOp and LimitOp.
func (b *Builder) buildTopK(e *memo.TopKExpr) (_ execPlan, outputCols colOrdMap, err error) {
	inputExpr := e.Input
	input, inputCols, err := b.buildRelational(inputExpr)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	ordering := e.Ordering.ToOrdering()
	inputOrdering := e.Input.ProvidedPhysical().Ordering
	alreadyOrderedPrefix := 0
	for i := range inputOrdering {
		if i == len(ordering) {
			return execPlan{}, colOrdMap{}, errors.AssertionFailedf("sort ordering already provided by input")
		}
		if inputOrdering[i] != ordering[i] {
			break
		}
		alreadyOrderedPrefix = i + 1
	}
	sqlOrdering, err := sqlOrdering(ordering, inputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var ep execPlan
	ep.root, err = b.factory.ConstructTopK(
		input.root,
		e.K,
		exec.OutputOrdering(sqlOrdering),
		alreadyOrderedPrefix)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return ep, inputCols, nil
}

// buildLimitOffset builds a plan for a LimitOp or OffsetOp
func (b *Builder) buildLimitOffset(e memo.RelExpr) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildRelational(e.Child(0).(memo.RelExpr))
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	// LIMIT/OFFSET expression should never need buildScalarContext, because it
	// can't refer to the input expression.
	expr, err := b.buildScalar(nil, e.Child(1).(opt.ScalarExpr))
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var ep execPlan
	if e.Op() == opt.LimitOp {
		ep.root, err = b.factory.ConstructLimit(input.root, expr, nil)
	} else {
		ep.root, err = b.factory.ConstructLimit(input.root, nil, expr)
	}
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return ep, inputCols, nil
}

func (b *Builder) buildSort(sort *memo.SortExpr) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildRelational(sort.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	ordering := sort.ProvidedPhysical().Ordering
	inputOrdering := sort.Input.ProvidedPhysical().Ordering
	alreadyOrderedPrefix := 0
	for i := range inputOrdering {
		if i == len(ordering) {
			return execPlan{}, colOrdMap{}, errors.AssertionFailedf("sort ordering already provided by input")
		}
		if inputOrdering[i] != ordering[i] {
			break
		}
		alreadyOrderedPrefix = i + 1
	}

	sqlOrdering, err := sqlOrdering(ordering, inputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var ep execPlan
	ep.root, err = b.factory.ConstructSort(
		input.root,
		exec.OutputOrdering(sqlOrdering),
		alreadyOrderedPrefix,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return ep, inputCols, nil
}

func (b *Builder) enforceScanWithHomeRegion(skipID cat.StableID) error {
	homeRegion := ""
	firstTable := ""
	gatewayRegion, foundLocalRegion := b.evalCtx.Locality.Find("region")
	if !foundLocalRegion {
		return errors.AssertionFailedf("The gateway region could not be determined while enforcing query home region.")
	}
	regionSet := make(map[string]struct{})
	moreThanOneRegionScans := make([]*memo.ScanExpr, 0, len(b.builtScans))
	for _, scan := range b.builtScans {
		if scan.Distribution.Any() {
			return pgerror.Newf(pgcode.QueryHasNoHomeRegion,
				"Query has no home region. Try accessing only tables defined in multi-region databases. %s",
				sqlerrors.EnforceHomeRegionFurtherInfo)
		}
		if len(scan.Distribution.Regions) > 1 {
			if scan.Table == opt.TableID(0) {
				return pgerror.Newf(pgcode.QueryHasNoHomeRegion,
					"Query has no home region. Try adding a LIMIT clause. %s",
					sqlerrors.EnforceHomeRegionFurtherInfo)
			}
			moreThanOneRegionScans = append(moreThanOneRegionScans, scan)
		} else {
			regionSet[scan.Distribution.Regions[0]] = struct{}{}
		}
	}
	if len(moreThanOneRegionScans) > 0 {
		md := moreThanOneRegionScans[0].Memo().Metadata()
		tabMeta := md.TableMeta(moreThanOneRegionScans[0].Table)
		if len(moreThanOneRegionScans) == 1 {
			return b.filterSuggestionError(tabMeta, moreThanOneRegionScans[0].Index, nil /* table2Meta */, 0 /* indexOrdinal2 */)
		}
		tabMeta2 := md.TableMeta(moreThanOneRegionScans[1].Table)
		return b.filterSuggestionError(
			tabMeta,
			moreThanOneRegionScans[0].Index,
			tabMeta2,
			moreThanOneRegionScans[1].Index,
		)
	}
	if len(regionSet) > 2 {
		return pgerror.Newf(pgcode.QueryHasNoHomeRegion,
			"Query has no home region. Try adding a LIMIT clause. %s",
			sqlerrors.EnforceHomeRegionFurtherInfo)
	}
	for i, scan := range b.builtScans {
		inputTableMeta := scan.Memo().Metadata().TableMeta(scan.Table)
		inputTable := inputTableMeta.Table
		// Mutation DML errors out with additional information via a call to
		// `filterSuggestionError`, handled by the caller, so skip the target of
		// the mutations if encountered here.
		if inputTable.ID() == skipID {
			continue
		}
		inputTableName := string(inputTable.Name())
		inputIndexOrdinal := scan.Index

		queryHomeRegion, queryHasHomeRegion := scan.ProvidedPhysical().Distribution.GetSingleRegion()
		if homeRegion == "" {
			homeRegion = queryHomeRegion
			firstTable = inputTableName
		}

		if queryHasHomeRegion {
			if homeRegion != queryHomeRegion {
				return pgerror.Newf(pgcode.QueryHasNoHomeRegion,
					`Query has no home region. The home region ('%s') of operation on table '%s' does not match the home region ('%s') of operation on table '%s'. %s`,
					queryHomeRegion,
					inputTableName,
					homeRegion,
					firstTable,
					sqlerrors.EnforceHomeRegionFurtherInfo)
			} else if gatewayRegion != homeRegion {
				return pgerror.Newf(pgcode.QueryNotRunningInHomeRegion,
					`%s. Try running the query from region '%s'. %s`,
					execinfra.QueryNotRunningInHomeRegionMessagePrefix,
					homeRegion,
					sqlerrors.EnforceHomeRegionFurtherInfo,
				)
			}
		} else {
			var inputTableMeta2 *opt.TableMeta
			var inputIndexOrdinal2 cat.IndexOrdinal
			if len(b.builtScans) > 1 && i == 0 {
				scan2 := b.builtScans[1]
				inputTableMeta2 = scan2.Memo().Metadata().TableMeta(scan2.Table)
				inputIndexOrdinal2 = scan2.Index
			}
			return b.filterSuggestionError(inputTableMeta, inputIndexOrdinal, inputTableMeta2, inputIndexOrdinal2)
		}
	}
	b.builtScans = nil
	return nil
}

func (b *Builder) buildDistribute(
	distribute *memo.DistributeExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildRelational(distribute.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if distribute.NoOpDistribution() {
		// Don't bother creating a no-op distribution. This likely exists because
		// the input is a Sort expression, and this is an artifact of how physical
		// properties are enforced.
		return input, inputCols, err
	}

	if b.evalCtx.SessionData().EnforceHomeRegion && b.IsANSIDML {
		var mutationTableID opt.TableID
		if updateExpr, ok := distribute.Input.(*memo.UpdateExpr); ok {
			mutationTableID = updateExpr.Table
		} else if deleteExpr, ok := distribute.Input.(*memo.DeleteExpr); ok {
			mutationTableID = deleteExpr.Table
		}
		var mutationTabMeta *opt.TableMeta
		var mutationStableID cat.StableID
		if mutationTableID != opt.TableID(0) {
			md := b.mem.Metadata()
			mutationTabMeta = md.TableMeta(mutationTableID)
			mutationStableID = mutationTabMeta.Table.ID()
		}
		saveDoScanExprCollection := b.doScanExprCollection
		b.doScanExprCollection = true
		// Traverse the tree again, this time collecting ScanExprs that should
		// be processed for error handling.
		// TODO(mgartner): Can we avoid re-traversing the tree?
		_, _, err = b.buildRelational(distribute.Input)
		b.doScanExprCollection = saveDoScanExprCollection
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		err = b.enforceScanWithHomeRegion(mutationStableID)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}

		homeRegion, ok := distribute.GetInputHomeRegion()
		var errorStringBuilder strings.Builder
		var errCode pgcode.Code
		if ok {
			errCode = pgcode.QueryNotRunningInHomeRegion
			errorStringBuilder.WriteString(execinfra.QueryNotRunningInHomeRegionMessagePrefix)
			errorStringBuilder.WriteString(fmt.Sprintf(`. Try running the query from region '%s'. %s`, homeRegion, sqlerrors.EnforceHomeRegionFurtherInfo))
		} else if distribute.Input.Op() != opt.LookupJoinOp {
			// More detailed error message handling for lookup join occurs in the
			// execbuilder.
			if mutationTableID != opt.TableID(0) {
				err = b.filterSuggestionError(mutationTabMeta, 0 /* indexOrdinal1 */, nil /* table2Meta */, 0 /* indexOrdinal2 */)
			} else {
				errCode = pgcode.QueryHasNoHomeRegion
				errorStringBuilder.WriteString("Query has no home region.")
				errorStringBuilder.WriteString(` Try adding a LIMIT clause. `)
				errorStringBuilder.WriteString(sqlerrors.EnforceHomeRegionFurtherInfo)
			}
		}
		if err == nil {
			msgString := errorStringBuilder.String()
			err = pgerror.Newf(errCode, "%s", msgString)
		}
		return execPlan{}, colOrdMap{}, err
	}

	// TODO(rytaft): This is currently a no-op. We should pass this distribution
	// info to the DistSQL planner.
	return input, inputCols, err
}

func (b *Builder) buildOrdinality(
	ord *memo.OrdinalityExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildRelational(ord.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	colName := b.mem.Metadata().ColumnMeta(ord.ColID).Alias

	var ep execPlan
	ep.root, err = b.factory.ConstructOrdinality(input.root, colName)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// We have one additional ordinality column, which is ordered at the end of
	// the list.
	inputCols.Set(ord.ColID, inputCols.MaxOrd()+1)

	return ep, inputCols, nil
}

func (b *Builder) buildIndexJoin(
	join *memo.IndexJoinExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	md := b.mem.Metadata()
	tab := md.Table(join.Table)

	// TODO(radu): the distsql implementation of index join assumes that the input
	// starts with the PK columns in order (#40749).
	pri := tab.Index(cat.PrimaryIndex)
	b.IndexesUsed.add(tab.ID(), pri.ID())
	keyCols := make([]exec.NodeColumnOrdinal, pri.KeyColumnCount())
	for i := range keyCols {
		keyCols[i], err = getNodeColumnOrdinal(inputCols, join.Table.ColumnID(pri.Column(i).Ordinal()))
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}

	// The input column map is no longer used, so it can be freed.
	b.colOrdsAlloc.Free(inputCols)

	var needed exec.TableColumnOrdinalSet
	needed, outputCols = b.getColumns(join.Cols, join.Table)

	locking, err := b.buildLocking(join.Table, join.Locking)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	b.recordJoinAlgorithm(exec.IndexJoin)
	reqOrdering, err := reqOrdering(join, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var res execPlan
	res.root, err = b.factory.ConstructIndexJoin(
		input.root, tab, keyCols, needed, reqOrdering, locking, join.RequiredPhysical().LimitHintInt64(),
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	return res, outputCols, nil
}

func (b *Builder) indexColumnNames(
	tableMeta *opt.TableMeta, index cat.Index, startColumn int,
) string {
	if startColumn < 0 {
		return ""
	}
	sb := strings.Builder{}
	for i, n := startColumn, index.ExplicitColumnCount(); i < n; i++ {
		if i > startColumn {
			sb.WriteString(", ")
		}
		col := index.Column(i)
		ord := col.Ordinal()
		colID := tableMeta.MetaID.ColumnID(ord)
		colName := b.mem.Metadata().QualifiedAlias(b.ctx, colID, false /* fullyQualify */, true /* alwaysQualify */, b.catalog)
		sb.WriteString(colName)
	}
	return sb.String()
}

func (b *Builder) filterSuggestionError(
	tableMeta *opt.TableMeta,
	indexOrdinal cat.IndexOrdinal,
	table2Meta *opt.TableMeta,
	indexOrdinal2 cat.IndexOrdinal,
) (err error) {
	var index cat.Index
	var table2 cat.Table
	if table2Meta != nil {
		table2 = table2Meta.Table
	}
	if tableMeta != nil {
		table := tableMeta.Table
		index = table.Index(indexOrdinal)

		if crdbRegionColName, ok := table.HomeRegionColName(); ok {
			plural := ""
			if index.ExplicitColumnCount() > 2 {
				plural = "s"
			}
			tableName := tableMeta.Alias.Table()
			args := make([]interface{}, 0, 9)
			args = append(args, tableName)
			args = append(args, crdbRegionColName)
			args = append(args, plural)
			args = append(args, b.indexColumnNames(tableMeta, index, 1))
			if table2 == nil || table2Meta.Alias.Table() == tableName {
				args = append(args, sqlerrors.EnforceHomeRegionFurtherInfo)
				err = pgerror.Newf(pgcode.QueryHasNoHomeRegion,
					"Query has no home region. Try adding a filter on %s.%s and/or on key column%s (%s). %s", args...)
			} else if crdbRegionColName2, ok := table2.HomeRegionColName(); ok {
				index = table2.Index(indexOrdinal2)
				plural = ""
				if index.ExplicitColumnCount() > 2 {
					plural = "s"
				}
				table2Name := table2Meta.Alias.Table()
				args = append(args, table2Name)
				args = append(args, crdbRegionColName2)
				args = append(args, plural)
				args = append(args, b.indexColumnNames(table2Meta, index, 1))
				args = append(args, sqlerrors.EnforceHomeRegionFurtherInfo)
				err = pgerror.Newf(pgcode.QueryHasNoHomeRegion,
					"Query has no home region. Try adding a filter on %s.%s and/or on key column%s (%s). Try adding a filter on %s.%s and/or on key column%s (%s). %s", args...)
			}
		}
	}
	return err
}

func (b *Builder) handleRemoteLookupJoinError(join *memo.LookupJoinExpr) (err error) {
	if join.ChildOfLocalityOptimizedSearch {
		// If this join is a child of a locality-optimized search operation, it
		// should not be statically errored out. It could dynamically error out if
		// the query can't be answered by executing the first branch of the LOS.
		return nil
	}
	lookupTableMeta := join.Memo().Metadata().TableMeta(join.Table)
	lookupTable := lookupTableMeta.Table

	var input opt.Expr
	input = join.Input
	for input.ChildCount() == 1 || input.Op() == opt.ProjectOp {
		input = input.Child(0)
	}
	gatewayRegion, foundLocalRegion := b.evalCtx.Locality.Find("region")
	inputTableName := ""
	// ScanExprs from global tables will have filled in a provided distribution
	// of the gateway region by now.
	var queryHomeRegion string
	var queryHasHomeRegion bool
	if rel, ok := input.(memo.RelExpr); ok {
		queryHomeRegion, queryHasHomeRegion = rel.ProvidedPhysical().Distribution.GetSingleRegion()
	} else if _, ok = input.(*memo.ScalarListExpr); ok {
		// A list of scalar constants doesn't access remote regions.
		// If these aren't constants, such as scalar subqueries, checks for a home
		// region are done elsewhere.
		queryHasHomeRegion = true
		queryHomeRegion = gatewayRegion
	} else {
		return errors.AssertionFailedf("unexpected expression kind while checking home region of input to lookup join: %v", input)
	}

	var inputTableMeta *opt.TableMeta
	var inputTable cat.Table
	var inputIndexOrdinal cat.IndexOrdinal
	switch t := input.(type) {
	case *memo.ScanExpr:
		inputTableMeta = join.Memo().Metadata().TableMeta(t.Table)
		inputTable = inputTableMeta.Table
		inputTableName = string(inputTable.Name())
		inputIndexOrdinal = t.Index
	}

	homeRegion := ""
	if lookupTable.IsGlobalTable() || join.LocalityOptimized || join.ChildOfLocalityOptimizedSearch {
		// HomeRegion() does not automatically fill in the home region of a global
		// table as the gateway region, so let's manually set it here.
		// Locality optimized joins are considered local for static
		// enforce_home_region checks.
		homeRegion = gatewayRegion
	} else {
		homeRegion, _ = lookupTable.HomeRegion()
	}
	if homeRegion == "" {
		if lookupTable.IsRegionalByRow() {
			if len(join.KeyCols) > 0 {
				inputExpr := join.Input
				firstKeyColID := join.KeyCols[0]
				if invertedJoinExpr, ok := inputExpr.(*memo.InvertedJoinExpr); ok {
					if constExpr, ok := invertedJoinExpr.GetConstExprFromFilter(firstKeyColID); ok {
						if regionName, ok := distribution.GetDEnumAsStringFromConstantExpr(constExpr); ok {
							homeRegion = regionName
						}
					}
				} else if projectExpr, ok := inputExpr.(*memo.ProjectExpr); ok {
					homeRegion = projectExpr.GetProjectedEnumConstant(firstKeyColID)
				}
			} else if join.LookupColsAreTableKey &&
				len(join.LookupExpr) > 0 {
				if filterIdx, ok := join.GetConstPrefixFilter(join.Memo().Metadata()); ok {
					firstIndexColEqExpr := join.LookupJoinPrivate.LookupExpr[filterIdx].Condition
					if firstIndexColEqExpr.Op() == opt.EqOp {
						if regionName, ok := distribution.GetDEnumAsStringFromConstantExpr(firstIndexColEqExpr.Child(1)); ok {
							homeRegion = regionName
						}
					}
				}
			}
		}
	}
	// If the specialized methods of setting the home region above don't succeed,
	// see if `GetLookupJoinLookupTableDistribution` can find the lookup table's
	// distribution.
	// TODO(msirek): Check if the specialized methods above are even needed any
	// more.
	if homeRegion == "" && b.optimizer != nil && b.optimizer.Coster() != nil {
		_, physicalDistribution := distribution.BuildLookupJoinLookupTableDistribution(
			b.ctx, b.evalCtx, join, join.RequiredPhysical(), b.optimizer.MaybeGetBestCostRelation,
		)
		if len(physicalDistribution.Regions) == 1 {
			homeRegion = physicalDistribution.Regions[0]
		}
	}
	if homeRegion != "" {
		if foundLocalRegion {
			if queryHasHomeRegion {
				if homeRegion != queryHomeRegion {
					if inputTableName == "" {
						return pgerror.Newf(pgcode.QueryHasNoHomeRegion,
							`Query has no home region. The home region ('%s') of lookup table '%s' does not match the home region ('%s') of the other relation in the join. %s'`,
							homeRegion,
							lookupTable.Name(),
							queryHomeRegion,
							sqlerrors.EnforceHomeRegionFurtherInfo,
						)
					}
					return pgerror.Newf(pgcode.QueryHasNoHomeRegion,
						`Query has no home region. The home region ('%s') of table '%s' does not match the home region ('%s') of lookup table '%s'. %s`,
						queryHomeRegion,
						inputTableName,
						homeRegion,
						string(lookupTable.Name()),
						sqlerrors.EnforceHomeRegionFurtherInfo,
					)
				} else if gatewayRegion != homeRegion {
					return pgerror.Newf(pgcode.QueryNotRunningInHomeRegion,
						`%s. Try running the query from region '%s'. %s`,
						execinfra.QueryNotRunningInHomeRegionMessagePrefix,
						homeRegion,
						sqlerrors.EnforceHomeRegionFurtherInfo,
					)
				}
			} else {
				return b.filterSuggestionError(inputTableMeta, inputIndexOrdinal, nil /* table2Meta */, 0 /* indexOrdinal2 */)
			}
		} else {
			return errors.AssertionFailedf("The gateway region could not be determined while enforcing query home region.")
		}
	} else {
		if !queryHasHomeRegion {
			return b.filterSuggestionError(lookupTableMeta, join.Index, inputTableMeta, inputIndexOrdinal)
		}
		return b.filterSuggestionError(lookupTableMeta, join.Index, nil /* table2Meta */, 0 /* indexOrdinal2 */)
	}
	return nil
}

func (b *Builder) buildLookupJoin(
	join *memo.LookupJoinExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	md := b.mem.Metadata()
	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.JoinAlgoLookupUseCounter)
		telemetry.Inc(opt.JoinTypeToUseCounter(join.JoinType))

		if join.UsesPartialIndex(md) {
			telemetry.Inc(sqltelemetry.PartialIndexLookupJoinUseCounter)
		}
	}
	saveDoScanExprCollection := false
	enforceHomeRegion := b.evalCtx.SessionData().EnforceHomeRegion && b.IsANSIDML
	if enforceHomeRegion {
		saveDoScanExprCollection = b.doScanExprCollection
		var rel opt.Expr
		rel = join.Input
		for rel.ChildCount() == 1 || rel.Op() == opt.ProjectOp {
			rel = rel.Child(0)
		}
		if rel.Op() == opt.ScanOp {
			b.doScanExprCollection = false
		}
	}
	input, inputCols, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	if enforceHomeRegion {
		b.doScanExprCollection = saveDoScanExprCollection
		// TODO(msirek): Update this in phase 2 or 3 when we can dynamically
		//               determine if the lookup will be local, or after #69617 is
		//               merged.
		err = b.handleRemoteLookupJoinError(join)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}
	keyCols := make([]exec.NodeColumnOrdinal, len(join.KeyCols))
	for i, c := range join.KeyCols {
		keyCols[i], err = getNodeColumnOrdinal(inputCols, c)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}

	joinInputCols := join.Input.Relational().OutputCols
	lookupCols := join.Cols.Difference(joinInputCols)
	if join.IsFirstJoinInPairedJoiner {
		lookupCols.Remove(join.ContinuationCol)
	}

	lookupOrdinals, lookupColMap := b.getColumns(lookupCols, join.Table)

	// leftAndRightCols are the columns used in expressions evaluated by this
	// join.
	leftAndRightCols := b.joinOutputMap(inputCols, lookupColMap)

	// lookupColMap is no longer used, so it can be freed.
	b.colOrdsAlloc.Free(lookupColMap)

	// Create the output column mapping.
	switch {
	case join.IsFirstJoinInPairedJoiner:
		// For the first join in a paired join, outputCols needs to include the
		// continuation column since it will be in the result output by this
		// join. The first join in a paired join is always a left-join or an
		// inner join, so the output columns always include the left and right
		// columns.
		if join.JoinType != opt.LeftJoinOp && join.JoinType != opt.InnerJoinOp {
			return execPlan{}, colOrdMap{}, errors.AssertionFailedf(
				"unexpected join type %s for lower join", join.JoinType,
			)
		}
		outputCols = b.colOrdsAlloc.Copy(leftAndRightCols)

		maxOrd := outputCols.MaxOrd()
		if maxOrd == -1 {
			return execPlan{}, colOrdMap{}, errors.AssertionFailedf("outputCols should not be empty")
		}
		// Assign the continuation column the next unused value in the map.
		outputCols.Set(join.ContinuationCol, maxOrd+1)

		// leftAndRightCols is only needed for the lifetime of the function, so
		// free it afterward.
		defer b.colOrdsAlloc.Free(leftAndRightCols)

		// inputCols is no longer used, so it can be freed.
		b.colOrdsAlloc.Free(inputCols)

	case join.JoinType == opt.SemiJoinOp || join.JoinType == opt.AntiJoinOp:
		// For semi and anti joins, only the left columns are output.
		outputCols = inputCols

		// leftAndRightCols is only needed for the lifetime of the function, so
		// free it afterward.
		defer b.colOrdsAlloc.Free(leftAndRightCols)

	default:
		// For all other joins, the left and right columns are output.
		outputCols = leftAndRightCols

		// inputCols is no longer used, so it can be freed.
		b.colOrdsAlloc.Free(inputCols)
	}

	ctx := makeBuildScalarCtx(leftAndRightCols)
	var lookupExpr, remoteLookupExpr tree.TypedExpr
	if len(join.LookupExpr) > 0 {
		var err error
		lookupExpr, err = b.buildScalar(&ctx, &join.LookupExpr)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}
	if len(join.RemoteLookupExpr) > 0 {
		var err error
		remoteLookupExpr, err = b.buildScalar(&ctx, &join.RemoteLookupExpr)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}
	var onExpr tree.TypedExpr
	if len(join.On) > 0 {
		var err error
		onExpr, err = b.buildScalar(&ctx, &join.On)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}

	tab := md.Table(join.Table)
	idx := tab.Index(join.Index)
	b.IndexesUsed.add(tab.ID(), idx.ID())

	locking, err := b.buildLocking(join.Table, join.Locking)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	joinType, err := joinOpToJoinType(join.JoinType)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	b.recordJoinType(joinType)
	b.recordJoinAlgorithm(exec.LookupJoin)
	reqOrdering, err := reqOrdering(join, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var res execPlan
	res.root, err = b.factory.ConstructLookupJoin(
		joinType,
		input.root,
		tab,
		idx,
		keyCols,
		join.LookupColsAreTableKey,
		lookupExpr,
		remoteLookupExpr,
		lookupOrdinals,
		onExpr,
		join.IsFirstJoinInPairedJoiner,
		join.IsSecondJoinInPairedJoiner,
		reqOrdering,
		locking,
		join.RequiredPhysical().LimitHintInt64(),
		join.RemoteOnlyLookups,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Apply a post-projection if Cols doesn't contain all input columns.
	//
	// NB: For paired-joins, this is where the continuation column and the PK
	// columns for the right side, which were part of the inputCols, are projected
	// away.
	if !joinInputCols.SubsetOf(join.Cols) {
		outCols := join.Cols
		if join.JoinType == opt.SemiJoinOp || join.JoinType == opt.AntiJoinOp {
			outCols = join.Cols.Intersection(joinInputCols)
		}
		// applySimpleProject creates a new map for outputCols, so outputCols
		// can be freed.
		defer b.colOrdsAlloc.Free(outputCols)
		return b.applySimpleProject(res, outputCols, join, outCols, join.ProvidedPhysical().Ordering)
	}
	return res, outputCols, nil
}

func (b *Builder) handleRemoteInvertedJoinError(join *memo.InvertedJoinExpr) (err error) {
	lookupTableMeta := join.Memo().Metadata().TableMeta(join.Table)
	lookupTable := lookupTableMeta.Table

	var input opt.Expr
	input = join.Input
	for input.ChildCount() == 1 || input.Op() == opt.ProjectOp {
		input = input.Child(0)
	}
	gatewayRegion, foundLocalRegion := b.evalCtx.Locality.Find("region")
	inputTableName := ""
	// ScanExprs from global tables will have filled in a provided distribution
	// of the gateway region by now.
	queryHomeRegion, queryHasHomeRegion := input.(memo.RelExpr).ProvidedPhysical().Distribution.GetSingleRegion()
	var inputTableMeta *opt.TableMeta
	var inputTable cat.Table
	var inputIndexOrdinal cat.IndexOrdinal
	switch t := input.(type) {
	case *memo.ScanExpr:
		inputTableMeta = join.Memo().Metadata().TableMeta(t.Table)
		inputTable = inputTableMeta.Table
		inputTableName = string(inputTable.Name())
		inputIndexOrdinal = t.Index
	}

	homeRegion := ""
	if lookupTable.IsGlobalTable() {
		// HomeRegion() does not automatically fill in the home region of a global
		// table as the gateway region, so let's manually set it here.
		homeRegion = gatewayRegion
	} else {
		homeRegion, _ = lookupTable.HomeRegion()
	}
	if homeRegion == "" {
		if lookupTable.IsRegionalByRow() {
			if len(join.PrefixKeyCols) > 0 {
				if projectExpr, ok := join.Input.(*memo.ProjectExpr); ok {
					colID := join.PrefixKeyCols[0]
					homeRegion = projectExpr.GetProjectedEnumConstant(colID)
				}
			}
		}
	}

	if homeRegion != "" {
		if foundLocalRegion {
			if queryHasHomeRegion {
				if homeRegion != queryHomeRegion {
					if inputTableName == "" {
						return pgerror.Newf(pgcode.QueryHasNoHomeRegion,
							`Query has no home region. The home region ('%s') of lookup table '%s' does not match the home region ('%s') of the other relation in the join. %s`,
							homeRegion,
							lookupTable.Name(),
							queryHomeRegion,
							sqlerrors.EnforceHomeRegionFurtherInfo,
						)
					}
					return pgerror.Newf(pgcode.QueryHasNoHomeRegion,
						`Query has no home region. The home region ('%s') of table '%s' does not match the home region ('%s') of lookup table '%s'. %s`,
						queryHomeRegion,
						inputTableName,
						homeRegion,
						string(lookupTable.Name()),
						sqlerrors.EnforceHomeRegionFurtherInfo,
					)
				} else if gatewayRegion != homeRegion {
					return pgerror.Newf(pgcode.QueryNotRunningInHomeRegion,
						`%s. Try running the query from region '%s'. %s`,
						execinfra.QueryNotRunningInHomeRegionMessagePrefix,
						homeRegion,
						sqlerrors.EnforceHomeRegionFurtherInfo,
					)
				}
			} else {
				return b.filterSuggestionError(inputTableMeta, inputIndexOrdinal, nil /* table2Meta */, 0 /* indexOrdinal2 */)
			}
		} else {
			return errors.AssertionFailedf("The gateway region could not be determined while enforcing query home region.")
		}
	} else {
		if !queryHasHomeRegion {
			return b.filterSuggestionError(lookupTableMeta, join.Index, inputTableMeta, inputIndexOrdinal)
		}
		return b.filterSuggestionError(lookupTableMeta, join.Index, nil /* table2Meta */, 0 /* indexOrdinal2 */)
	}
	return nil
}

func (b *Builder) buildInvertedJoin(
	join *memo.InvertedJoinExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	enforceHomeRegion := b.evalCtx.SessionData().EnforceHomeRegion && b.IsANSIDML
	saveDoScanExprCollection := false
	if enforceHomeRegion {
		saveDoScanExprCollection = b.doScanExprCollection
		var rel opt.Expr
		rel = join.Input
		for rel.ChildCount() == 1 || rel.Op() == opt.ProjectOp {
			rel = rel.Child(0)
		}
		if rel.Op() == opt.ScanOp {
			b.doScanExprCollection = false
		}
	}
	input, inputCols, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	if enforceHomeRegion {
		b.doScanExprCollection = saveDoScanExprCollection
		// TODO(msirek): Update this in phase 2 or 3 when we can dynamically
		//               determine if the lookup will be local, or after #69617 is
		//               merged.
		err = b.handleRemoteInvertedJoinError(join)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}
	md := b.mem.Metadata()
	tab := md.Table(join.Table)
	idx := tab.Index(join.Index)
	b.IndexesUsed.add(tab.ID(), idx.ID())

	prefixEqCols := make([]exec.NodeColumnOrdinal, len(join.PrefixKeyCols))
	for i, c := range join.PrefixKeyCols {
		prefixEqCols[i], err = getNodeColumnOrdinal(inputCols, c)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
	}

	joinInputCols := join.Input.Relational().OutputCols
	lookupCols := join.Cols.Difference(joinInputCols)
	if join.IsFirstJoinInPairedJoiner {
		lookupCols.Remove(join.ContinuationCol)
	}

	// Add the inverted column. Its source column will be referenced in the
	// inverted expression and needs a corresponding indexed var. It will be
	// projected away below.
	invertedColumn := idx.InvertedColumn()
	invertedColID := join.Table.ColumnID(invertedColumn.Ordinal())
	lookupCols.Add(invertedColID)

	lookupOrdinals, lookupColMap := b.getColumns(lookupCols, join.Table)
	// leftAndRightCols are the columns used in expressions evaluated by this join.
	leftAndRightCols := b.joinOutputMap(inputCols, lookupColMap)

	// Create the output column mapping.
	switch {
	case join.IsFirstJoinInPairedJoiner:
		// For the first join in a paired join, outputCols needs to include the
		// continuation column since it will be in the result output by this
		// join. The first join in a paired join is always a left-join or an
		// inner join, so the output columns always include the left and right
		// columns.
		if join.JoinType != opt.LeftJoinOp && join.JoinType != opt.InnerJoinOp {
			return execPlan{}, colOrdMap{}, errors.AssertionFailedf(
				"unexpected join type %s for lower join", join.JoinType,
			)
		}
		outputCols = b.colOrdsAlloc.Copy(leftAndRightCols)

		maxOrd := outputCols.MaxOrd()
		if maxOrd == -1 {
			return execPlan{}, colOrdMap{}, errors.AssertionFailedf("outputCols should not be empty")
		}
		// Assign the continuation column the next unused value in the map.
		outputCols.Set(join.ContinuationCol, maxOrd+1)

		// leftAndRightCols is only needed for the lifetime of the function, so free
		// it afterward.
		defer b.colOrdsAlloc.Free(leftAndRightCols)

		// inputCols is no longer used, so it can be freed.
		b.colOrdsAlloc.Free(inputCols)

	case join.JoinType == opt.SemiJoinOp || join.JoinType == opt.AntiJoinOp:
		// For semi and anti join, only the left columns are output.
		outputCols = inputCols

		// leftAndRightCols is only needed for the lifetime of the function, so free
		// it afterward.
		defer b.colOrdsAlloc.Free(leftAndRightCols)

	default:
		// For all other joins, the left and right columns are output.
		outputCols = leftAndRightCols

		// inputCols is no longer used, so it can be freed.
		b.colOrdsAlloc.Free(inputCols)
	}

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, leftAndRightCols.MaxOrd()+1),
		ivarMap: b.colOrdsAlloc.Copy(leftAndRightCols),
	}
	onExpr, err := b.buildScalar(&ctx, &join.On)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// The inverted filter refers to the inverted column's source column, but it
	// is actually evaluated implicitly using the inverted column; the inverted
	// column's source column is not even accessible here.
	//
	// TODO(radu): this is sketchy. The inverted column should not even have the
	// geospatial type (which would make the expression invalid in terms of
	// typing). Perhaps we need to pass this information in a more specific way
	// and not as a generic expression?
	ord, _ := ctx.ivarMap.Get(invertedColID)
	ctx.ivarMap.Set(join.Table.ColumnID(invertedColumn.InvertedSourceColumnOrdinal()), ord)
	invertedExpr, err := b.buildScalar(&ctx, join.InvertedExpr)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	locking, err := b.buildLocking(join.Table, join.Locking)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	joinType, err := joinOpToJoinType(join.JoinType)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	b.recordJoinType(joinType)
	b.recordJoinAlgorithm(exec.InvertedJoin)
	reqOrdering, err := reqOrdering(join, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var res execPlan
	res.root, err = b.factory.ConstructInvertedJoin(
		joinType,
		invertedExpr,
		input.root,
		tab,
		idx,
		prefixEqCols,
		lookupOrdinals,
		onExpr,
		join.IsFirstJoinInPairedJoiner,
		reqOrdering,
		locking,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Apply a post-projection to remove the inverted column.
	return b.applySimpleProject(res, outputCols, join, join.Cols, join.ProvidedPhysical().Ordering)
}

func (b *Builder) buildZigzagJoin(
	join *memo.ZigzagJoinExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	md := b.mem.Metadata()

	leftTable := md.Table(join.LeftTable)
	rightTable := md.Table(join.RightTable)
	leftIndex := leftTable.Index(join.LeftIndex)
	rightIndex := rightTable.Index(join.RightIndex)
	b.IndexesUsed.add(leftTable.ID(), leftIndex.ID())
	b.IndexesUsed.add(rightTable.ID(), rightIndex.ID())

	leftEqCols := make([]exec.TableColumnOrdinal, len(join.LeftEqCols))
	rightEqCols := make([]exec.TableColumnOrdinal, len(join.RightEqCols))
	for i := range join.LeftEqCols {
		leftEqCols[i] = exec.TableColumnOrdinal(join.LeftTable.ColumnOrdinal(join.LeftEqCols[i]))
		rightEqCols[i] = exec.TableColumnOrdinal(join.RightTable.ColumnOrdinal(join.RightEqCols[i]))
	}

	// Determine the columns that are needed from each side.
	leftCols := md.TableMeta(join.LeftTable).IndexColumns(join.LeftIndex).Intersection(join.Cols)
	rightCols := md.TableMeta(join.RightTable).IndexColumns(join.RightIndex).Intersection(join.Cols)
	// Remove duplicate columns, if any.
	rightCols.DifferenceWith(leftCols)
	// Make sure each side's columns always include the equality columns.
	for i := range join.LeftEqCols {
		leftCols.Add(join.LeftEqCols[i])
		rightCols.Add(join.RightEqCols[i])
	}
	// Make sure each side's columns always include the fixed index prefix
	// columns.
	leftNumFixed := len(join.FixedVals[0].(*memo.TupleExpr).Elems)
	for i := 0; i < leftNumFixed; i++ {
		leftCols.Add(join.LeftTable.IndexColumnID(leftIndex, i))
	}
	rightNumFixed := len(join.FixedVals[1].(*memo.TupleExpr).Elems)
	for i := 0; i < rightNumFixed; i++ {
		rightCols.Add(join.RightTable.IndexColumnID(rightIndex, i))
	}

	// TODO(mgartner): Free leftColMap, rightColMap, and other ordColMaps below
	// when possible.
	leftOrdinals, leftColMap := b.getColumns(leftCols, join.LeftTable)
	rightOrdinals, rightColMap := b.getColumns(rightCols, join.RightTable)

	leftLocking, err := b.buildLocking(join.LeftTable, join.LeftLocking)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	rightLocking, err := b.buildLocking(join.RightTable, join.RightLocking)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	allCols := b.joinOutputMap(leftColMap, rightColMap)
	outputCols = allCols

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, leftColMap.MaxOrd()+rightColMap.MaxOrd()+2),
		ivarMap: allCols,
	}
	onExpr, err := b.buildScalar(&ctx, &join.On)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Build the fixed value scalars.
	tupleToExprs := func(tup *memo.TupleExpr) ([]tree.TypedExpr, error) {
		res := make([]tree.TypedExpr, len(tup.Elems))
		for i := range res {
			res[i], err = b.buildScalar(&ctx, tup.Elems[i])
			if err != nil {
				return nil, err
			}
		}
		return res, nil
	}

	leftFixedVals, err := tupleToExprs(join.FixedVals[0].(*memo.TupleExpr))
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	rightFixedVals, err := tupleToExprs(join.FixedVals[1].(*memo.TupleExpr))
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	b.recordJoinAlgorithm(exec.ZigZagJoin)
	reqOrdering, err := reqOrdering(join, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var res execPlan
	res.root, err = b.factory.ConstructZigzagJoin(
		leftTable,
		leftIndex,
		leftOrdinals,
		leftFixedVals,
		leftEqCols,
		leftLocking,
		rightTable,
		rightIndex,
		rightOrdinals,
		rightFixedVals,
		rightEqCols,
		rightLocking,
		onExpr,
		reqOrdering,
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Apply a post-projection to retain only the columns we need.
	return b.applySimpleProject(res, outputCols, join, join.Cols, join.ProvidedPhysical().Ordering)
}

func (b *Builder) buildLocking(toLock opt.TableID, locking opt.Locking) (opt.Locking, error) {
	if b.forceForUpdateLocking.Contains(int(toLock)) {
		locking = locking.Max(forUpdateLocking)
	}
	if locking.IsLocking() {
		// Raise error if row-level locking is part of a read-only transaction.
		if b.evalCtx.TxnReadOnly {
			return opt.Locking{}, pgerror.Newf(pgcode.ReadOnlySQLTransaction,
				"cannot execute SELECT %s in a read-only transaction", locking.Strength.String(),
			)
		}
		if locking.Form == tree.LockPredicate {
			return opt.Locking{}, unimplemented.NewWithIssuef(
				110873, "explicit unique checks are not yet supported under read committed isolation",
			)
		}
		// Check if we can actually use shared locks here, or we need to use
		// non-locking reads instead.
		if locking.Strength == tree.ForShare || locking.Strength == tree.ForKeyShare {
			// Shared locks behavior for serializable transactions is dictated by
			// session setting.
			if b.evalCtx.TxnIsoLevel == isolation.Serializable &&
				!b.evalCtx.SessionData().SharedLockingForSerializable {
				// Reset locking information as we've determined we're going to be
				// performing a non-locking read.
				return opt.Locking{}, nil // early return; do not set PlanFlagContainsLocking
			}
		}
		b.flags.Set(exec.PlanFlagContainsLocking)
	}
	return locking, nil
}

func (b *Builder) buildMax1Row(
	max1Row *memo.Max1RowExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	input, inputCols, err := b.buildRelational(max1Row.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	var ep execPlan
	ep.root, err = b.factory.ConstructMax1Row(input.root, max1Row.ErrorText)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	return ep, inputCols, nil
}

func (b *Builder) buildWith(with *memo.WithExpr) (_ execPlan, outputCols colOrdMap, err error) {
	value, valuesCols, err := b.buildRelational(with.Binding)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	var label bytes.Buffer
	fmt.Fprintf(&label, "buffer %d", with.ID)
	if with.Name != "" {
		fmt.Fprintf(&label, " (%s)", with.Name)
	}

	buffer, err := b.factory.ConstructBuffer(value.root, label.String())
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// TODO(justin): if the binding here has a spoolNode at its root, we can
	// remove it, since subquery execution also guarantees complete execution.

	// Add the buffer as a subquery so it gets executed ahead of time, and is
	// available to be referenced by other queries.
	b.subqueries = append(b.subqueries, exec.Subquery{
		ExprNode: with.OriginalExpr,
		// TODO(justin): this is wasteful: both the subquery and the bufferNode
		// will buffer up all the results.  This should be fixed by either making
		// the buffer point directly to the subquery results or adding a new
		// subquery mode that reads and discards all rows. This could possibly also
		// be fixed by ensuring that bufferNode exhausts its input (and forcing it
		// to behave like a spoolNode) and using the EXISTS mode.
		Mode:     exec.SubqueryAllRows,
		Root:     buffer,
		RowCount: int64(with.Relational().Statistics().RowCountIfAvailable()),
	})

	b.addBuiltWithExpr(with.ID, valuesCols, buffer)

	return b.buildRelational(with.Main)
}

func (b *Builder) buildRecursiveCTE(
	rec *memo.RecursiveCTEExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	initial, initialCols, err := b.buildRelational(rec.Initial)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Make sure we have the columns in the correct order.
	initial, initialCols, err = b.ensureColumns(
		initial, initialCols, rec.Initial, rec.InitialCols,
		nil /* ordering */, true, /* reuseInputCols */
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Renumber the columns so they match the columns expected by the recursive
	// query.
	initialCols.Clear()
	for i, col := range rec.OutCols {
		initialCols.Set(col, i)
	}

	// To implement exec.RecursiveCTEIterationFn, we create a special Builder.

	innerBldTemplate := &Builder{
		ctx:     b.ctx,
		mem:     b.mem,
		catalog: b.catalog,
		semaCtx: b.semaCtx,
		evalCtx: b.evalCtx,
		// If the recursive query itself contains CTEs, building it in the function
		// below will add to withExprs. Cap the slice to force reallocation on any
		// appends, so that they don't overwrite overwrite later appends by our
		// original builder.
		withExprs: b.withExprs[:len(b.withExprs):len(b.withExprs)],
	}

	fn := func(ef exec.Factory, bufferRef exec.Node) (exec.Plan, error) {
		// Use a separate builder each time.
		innerBld := *innerBldTemplate
		innerBld.factory = ef
		innerBld.addBuiltWithExpr(rec.WithID, initialCols, bufferRef)
		// TODO(mgartner): I think colOrdsAlloc can be reused for each recursive
		// iteration.
		innerBld.colOrdsAlloc.Init(innerBld.mem.Metadata().MaxColumn())
		plan, planCols, err := innerBld.build(rec.Recursive)
		if err != nil {
			return nil, err
		}
		// Ensure columns are output in the same order.
		plan, _, err = innerBld.ensureColumns(
			plan, planCols, rec.Recursive, rec.RecursiveCols,
			opt.Ordering{}, true, /* reuseInputCols */
		)
		if err != nil {
			return nil, err
		}
		rootRowCount := int64(rec.Recursive.Relational().Statistics().RowCountIfAvailable())
		return innerBld.factory.ConstructPlan(
			plan.root, innerBld.subqueries, innerBld.cascades, innerBld.checks, rootRowCount,
			innerBld.flags,
		)
	}

	label := fmt.Sprintf("working buffer (%s)", rec.Name)
	var ep execPlan
	ep.root, err = b.factory.ConstructRecursiveCTE(initial.root, fn, label, rec.Deduplicate)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	outputCols = b.colOrdsAlloc.Alloc()
	for i, col := range rec.OutCols {
		outputCols.Set(col, i)
	}
	return ep, outputCols, nil
}

func (b *Builder) buildWithScan(
	withScan *memo.WithScanExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	e := b.findBuiltWithExpr(withScan.With)
	if e == nil {
		return execPlan{}, colOrdMap{}, errors.AssertionFailedf(
			"couldn't find With expression with ID %d", withScan.With,
		)
	}

	var label bytes.Buffer
	fmt.Fprintf(&label, "buffer %d", withScan.With)
	if withScan.Name != "" {
		fmt.Fprintf(&label, " (%s)", withScan.Name)
	}

	node, err := b.factory.ConstructScanBuffer(e.bufferNode, label.String())
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	res := execPlan{root: node}

	// Apply any necessary projection to produce the InCols in the given order.
	res, _, err = b.ensureColumns(
		res, e.outputCols, withScan, withScan.InCols,
		withScan.ProvidedPhysical().Ordering, false, /* reuseInputCols */
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Renumber the columns.
	outputCols = b.colOrdsAlloc.Alloc()
	for i, col := range withScan.OutCols {
		outputCols.Set(col, i)
	}

	return res, outputCols, nil
}

func (b *Builder) buildProjectSet(
	projectSet *memo.ProjectSetExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	// TODO(mgartner): Free inputCols and other ordColMaps below when possible.
	input, inputCols, err := b.buildRelational(projectSet.Input)

	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	zip := projectSet.Zip
	md := b.mem.Metadata()
	scalarCtx := makeBuildScalarCtx(inputCols)

	exprs := make(tree.TypedExprs, len(zip))
	zipCols := make(colinfo.ResultColumns, 0, len(zip))
	numColsPerGen := make([]int, len(zip))

	n := inputCols.MaxOrd() + 1
	for i := range zip {
		item := &zip[i]
		exprs[i], err = b.buildScalar(&scalarCtx, item.Fn)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}

		for _, col := range item.Cols {
			colMeta := md.ColumnMeta(col)
			zipCols = append(zipCols, colinfo.ResultColumn{Name: colMeta.Alias, Typ: colMeta.Type})

			inputCols.Set(col, n)
			n++
		}

		numColsPerGen[i] = len(item.Cols)
	}

	var ep execPlan
	ep.root, err = b.factory.ConstructProjectSet(input.root, exprs, zipCols, numColsPerGen)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	return ep, inputCols, nil
}

func (b *Builder) buildCall(c *memo.CallExpr) (_ execPlan, outputCols colOrdMap, err error) {
	udf := c.Proc.(*memo.UDFCallExpr)
	if udf.Def == nil {
		return execPlan{}, colOrdMap{}, errors.AssertionFailedf("expected non-nil UDF definition")
	}

	// Build the argument expressions.
	var args tree.TypedExprs
	ctx := buildScalarCtx{}
	if len(udf.Args) > 0 {
		args = make(tree.TypedExprs, len(udf.Args))
		for i := range udf.Args {
			args[i], err = b.buildScalar(&ctx, udf.Args[i])
			if err != nil {
				return execPlan{}, colOrdMap{}, err
			}
		}
	}

	for _, s := range udf.Def.Body {
		if s.Relational().CanMutate {
			b.flags.Set(exec.PlanFlagContainsMutation)
			break
		}
	}

	// Create a tree.RoutinePlanFn that can plan the statements in the UDF body.
	planGen := b.buildRoutinePlanGenerator(
		udf.Def.Params,
		udf.Def.Body,
		udf.Def.BodyProps,
		udf.Def.BodyStmts,
		false, /* allowOuterWithRefs */
		nil,   /* wrapRootExpr */
	)

	r := tree.NewTypedRoutineExpr(
		udf.Def.Name,
		args,
		planGen,
		udf.Typ,
		true, /* enableStepping */
		udf.Def.CalledOnNullInput,
		udf.Def.MultiColDataSource,
		udf.Def.SetReturning,
		false, /* tailCall */
		true,  /* procedure */
		false, /* blockStart */
		nil,   /* blockState */
		nil,   /* cursorDeclaration */
	)

	var ep execPlan
	ep.root, err = b.factory.ConstructCall(r)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Renumber the columns.
	outputCols = b.colOrdsAlloc.Alloc()
	for i, col := range c.Columns {
		outputCols.Set(col, i)
	}
	return ep, outputCols, nil
}

func (b *Builder) resultColumn(id opt.ColumnID) colinfo.ResultColumn {
	colMeta := b.mem.Metadata().ColumnMeta(id)
	return colinfo.ResultColumn{
		Name: colMeta.Alias,
		Typ:  colMeta.Type,
	}
}

// extractFromOffset extracts the start bound expression of a window function
// that uses the OFFSET windowing mode for its start bound.
func (b *Builder) extractFromOffset(e opt.ScalarExpr) (_ opt.ScalarExpr, ok bool) {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return nil, false
	}
	if modifier, ok := e.(*memo.WindowFromOffsetExpr); ok {
		return modifier.Offset, true
	}
	return b.extractFromOffset(e.Child(0).(opt.ScalarExpr))
}

// extractToOffset extracts the end bound expression of a window function
// that uses the OFFSET windowing mode for its end bound.
func (b *Builder) extractToOffset(e opt.ScalarExpr) (_ opt.ScalarExpr, ok bool) {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return nil, false
	}
	if modifier, ok := e.(*memo.WindowToOffsetExpr); ok {
		return modifier.Offset, true
	}
	return b.extractToOffset(e.Child(0).(opt.ScalarExpr))
}

// extractFilter extracts a FILTER expression from a window function tower.
// Returns the expression and true if there was a filter, and false otherwise.
func (b *Builder) extractFilter(e opt.ScalarExpr) (opt.ScalarExpr, bool) {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return nil, false
	}
	if filter, ok := e.(*memo.AggFilterExpr); ok {
		return filter.Filter, true
	}
	return b.extractFilter(e.Child(0).(opt.ScalarExpr))
}

// extractWindowFunction extracts the window function being computed from a
// potential tower of modifiers attached to the Function field of a
// WindowsItem.
func (b *Builder) extractWindowFunction(e opt.ScalarExpr) opt.ScalarExpr {
	if opt.IsWindowOp(e) || opt.IsAggregateOp(e) {
		return e
	}
	return b.extractWindowFunction(e.Child(0).(opt.ScalarExpr))
}

func (b *Builder) isOffsetMode(boundType treewindow.WindowFrameBoundType) bool {
	return boundType == treewindow.OffsetPreceding || boundType == treewindow.OffsetFollowing
}

func (b *Builder) buildFrame(inputCols colOrdMap, w *memo.WindowsItem) (*tree.WindowFrame, error) {
	scalarCtx := makeBuildScalarCtx(inputCols)
	newDef := &tree.WindowFrame{
		Mode: w.Frame.Mode,
		Bounds: tree.WindowFrameBounds{
			StartBound: &tree.WindowFrameBound{
				BoundType: w.Frame.StartBoundType,
			},
			EndBound: &tree.WindowFrameBound{
				BoundType: w.Frame.EndBoundType,
			},
		},
		Exclusion: w.Frame.FrameExclusion,
	}
	if boundExpr, ok := b.extractFromOffset(w.Function); ok {
		if !b.isOffsetMode(w.Frame.StartBoundType) {
			return nil, errors.AssertionFailedf("expected offset to only be present in offset mode")
		}
		offset, err := b.buildScalar(&scalarCtx, boundExpr)
		if err != nil {
			return nil, err
		}
		if offset == tree.DNull {
			return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "frame starting offset must not be null")
		}
		newDef.Bounds.StartBound.OffsetExpr = offset
	}

	if boundExpr, ok := b.extractToOffset(w.Function); ok {
		if !b.isOffsetMode(newDef.Bounds.EndBound.BoundType) {
			return nil, errors.AssertionFailedf("expected offset to only be present in offset mode")
		}
		offset, err := b.buildScalar(&scalarCtx, boundExpr)
		if err != nil {
			return nil, err
		}
		if offset == tree.DNull {
			return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "frame ending offset must not be null")
		}
		newDef.Bounds.EndBound.OffsetExpr = offset
	}
	return newDef, nil
}

func (b *Builder) buildWindow(w *memo.WindowExpr) (_ execPlan, outputCols colOrdMap, err error) {
	// TODO(mgartner): Free inputCols and other ordColMaps below when possible.
	input, inputCols, err := b.buildRelational(w.Input)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	// Rearrange the input so that the input has all the passthrough columns
	// followed by all the argument columns.

	passthrough := w.Input.Relational().OutputCols

	desiredCols := opt.ColList{}
	passthrough.ForEach(func(i opt.ColumnID) {
		desiredCols = append(desiredCols, i)
	})

	// TODO(justin): this call to ensureColumns is kind of unfortunate because it
	// can result in an extra render beneath each window function. Figure out a
	// way to alleviate this.
	input, inputCols, err = b.ensureColumns(
		input, inputCols, w, desiredCols, opt.Ordering{}, true, /* reuseInputCols */
	)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	ctx := makeBuildScalarCtx(inputCols)

	ord := w.Ordering.ToOrdering()

	orderingExprs := make(tree.OrderBy, len(ord))
	for i, c := range ord {
		direction := tree.Ascending
		if c.Descending() {
			direction = tree.Descending
		}
		indexedVar, err := b.indexedVar(&ctx, b.mem.Metadata(), c.ID())
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		orderingExprs[i] = &tree.Order{
			Expr:      indexedVar,
			Direction: direction,
		}
	}

	partitionIdxs := make([]exec.NodeColumnOrdinal, w.Partition.Len())
	partitionExprs := make(tree.Exprs, w.Partition.Len())

	i := 0
	for col, ok := w.Partition.Next(0); ok; col, ok = w.Partition.Next(col + 1) {
		ordinal, _ := inputCols.Get(col)
		partitionIdxs[i] = exec.NodeColumnOrdinal(ordinal)
		indexedVar, err := b.indexedVar(&ctx, b.mem.Metadata(), col)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		partitionExprs[i] = indexedVar
		i++
	}

	argIdxs := make([][]exec.NodeColumnOrdinal, len(w.Windows))
	filterIdxs := make([]int, len(w.Windows))
	exprs := make([]*tree.FuncExpr, len(w.Windows))
	windowVals := make([]tree.WindowDef, len(w.Windows))

	for i := range w.Windows {
		item := &w.Windows[i]
		fn := b.extractWindowFunction(item.Function)
		name, overload := memo.FindWindowOverload(fn)
		if !b.disableTelemetry {
			telemetry.Inc(sqltelemetry.WindowFunctionCounter(name))
		}
		props, _ := builtinsregistry.GetBuiltinProperties(name)

		args := make([]tree.TypedExpr, fn.ChildCount())
		argIdxs[i] = make([]exec.NodeColumnOrdinal, fn.ChildCount())
		for j, n := 0, fn.ChildCount(); j < n; j++ {
			col := fn.Child(j).(*memo.VariableExpr).Col
			indexedVar, err := b.indexedVar(&ctx, b.mem.Metadata(), col)
			if err != nil {
				return execPlan{}, colOrdMap{}, err
			}
			args[j] = indexedVar
			idx, _ := inputCols.Get(col)
			argIdxs[i][j] = exec.NodeColumnOrdinal(idx)
		}

		frame, err := b.buildFrame(inputCols, item)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}

		var builtFilter tree.TypedExpr
		filter, ok := b.extractFilter(item.Function)
		if ok {
			f, ok := filter.(*memo.VariableExpr)
			if !ok {
				return execPlan{}, colOrdMap{},
					errors.AssertionFailedf("expected FILTER expression to be a VariableExpr")
			}
			filterIdxs[i], _ = inputCols.Get(f.Col)

			builtFilter, err = b.buildScalar(&ctx, filter)
			if err != nil {
				return execPlan{}, colOrdMap{}, err
			}
		} else {
			filterIdxs[i] = -1
		}

		windowVals[i] = tree.WindowDef{
			Partitions: partitionExprs,
			OrderBy:    orderingExprs,
			Frame:      frame,
		}
		wrappedFn, err := b.wrapBuiltinFunction(name)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		exprs[i] = tree.NewTypedFuncExpr(
			wrappedFn,
			0,
			args,
			builtFilter,
			&windowVals[i],
			overload.FixedReturnType(),
			props,
			overload,
		)
	}

	resultCols := make(colinfo.ResultColumns, w.Relational().OutputCols.Len())

	// All the passthrough cols will keep their ordinal index.
	passthrough.ForEach(func(col opt.ColumnID) {
		ordinal, _ := inputCols.Get(col)
		resultCols[ordinal] = b.resultColumn(col)
	})

	outputCols = b.colOrdsAlloc.Alloc()
	inputCols.ForEach(func(col opt.ColumnID, ord int) {
		if passthrough.Contains(col) {
			outputCols.Set(col, ord)
		}
	})

	outputIdxs := make([]int, len(w.Windows))

	// Because of the way we arranged the input columns, we will be outputting
	// the window columns at the end (which is exactly what the execution engine
	// will do as well).
	windowStart := passthrough.Len()
	for i := range w.Windows {
		resultCols[windowStart+i] = b.resultColumn(w.Windows[i].Col)
		outputCols.Set(w.Windows[i].Col, windowStart+i)
		outputIdxs[i] = windowStart + i
	}

	sqlOrdering, err := sqlOrdering(ord, inputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	var ep execPlan
	ep.root, err = b.factory.ConstructWindow(input.root, exec.WindowInfo{
		Cols:       resultCols,
		Exprs:      exprs,
		OutputIdxs: outputIdxs,
		ArgIdxs:    argIdxs,
		FilterIdxs: filterIdxs,
		Partition:  partitionIdxs,
		Ordering:   sqlOrdering,
	})
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	return ep, outputCols, nil
}

func (b *Builder) buildSequenceSelect(
	seqSel *memo.SequenceSelectExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	seq := b.mem.Metadata().Sequence(seqSel.Sequence)
	var ep execPlan
	ep.root, err = b.factory.ConstructSequenceSelect(seq)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	outputCols = b.colOrdsAlloc.Alloc()
	for i, c := range seqSel.Cols {
		outputCols.Set(c, i)
	}

	return ep, outputCols, nil
}

func (b *Builder) applySaveTable(
	input execPlan, inputCols colOrdMap, e memo.RelExpr, saveTableName string,
) (execPlan, error) {
	name := tree.NewTableNameWithSchema(tree.Name(opt.SaveTablesDatabase), catconstants.PublicSchemaName, tree.Name(saveTableName))

	// Ensure that the column names are unique and match the names used by the
	// opttester.
	outputCols := e.Relational().OutputCols
	colNames := make([]string, outputCols.Len())
	colNameGen := memo.NewColumnNameGenerator(e)
	for col, ok := outputCols.Next(0); ok; col, ok = outputCols.Next(col + 1) {
		ord, _ := inputCols.Get(col)
		colNames[ord] = colNameGen.GenerateName(col)
	}

	var err error
	input.root, err = b.factory.ConstructSaveTable(input.root, name, colNames)
	if err != nil {
		return execPlan{}, err
	}
	return input, err
}

func (b *Builder) buildOpaque(
	opaque *memo.OpaqueRelPrivate,
) (_ execPlan, outputCols colOrdMap, err error) {
	var ep execPlan
	ep.root, err = b.factory.ConstructOpaque(opaque.Metadata)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}

	outputCols = b.colOrdsAlloc.Alloc()
	for i, c := range opaque.Columns {
		outputCols.Set(c, i)
	}

	return ep, outputCols, nil
}

func (b *Builder) buildBarrier(
	barrier *memo.BarrierExpr,
) (_ execPlan, outputCols colOrdMap, err error) {
	// BarrierExpr is only used as an optimization barrier. In the execution plan,
	// it is replaced with its input.
	return b.buildRelational(barrier.Input)
}

// needProjection figures out what projection is needed on top of the input plan
// to produce the given list of columns. If the input plan already produces
// the columns (in the same order), returns needProj=false.
func (b *Builder) needProjection(
	inputCols colOrdMap, colList opt.ColList,
) (_ []exec.NodeColumnOrdinal, needProj bool, err error) {
	if inputCols.MaxOrd()+1 == len(colList) {
		identity := true
		for i, col := range colList {
			if ord, ok := inputCols.Get(col); !ok || ord != i {
				identity = false
				break
			}
		}
		if identity {
			return nil, false, nil
		}
	}
	cols := make([]exec.NodeColumnOrdinal, 0, len(colList))
	for _, col := range colList {
		if col != 0 {
			ord, err := getNodeColumnOrdinal(inputCols, col)
			if err != nil {
				return nil, false, err
			}
			cols = append(cols, ord)
		}
	}
	return cols, true, nil
}

// ensureColumns applies a projection as necessary to make the output match the
// given list of columns; colNames is optional. If reuseInputCols is true, then
// inputCols will be reused to build outputCols, and the caller must no longer
// use inputCols.
func (b *Builder) ensureColumns(
	input execPlan,
	inputCols colOrdMap,
	inputExpr memo.RelExpr,
	colList opt.ColList,
	provided opt.Ordering,
	reuseInputCols bool,
) (_ execPlan, outputCols colOrdMap, err error) {
	cols, needProj, err := b.needProjection(inputCols, colList)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	if !needProj {
		return input, inputCols, nil
	}
	// Since we are constructing a simple project on top of the main operator,
	// we need to explicitly annotate the latter with estimates since the code
	// in buildRelational() will attach them to the project.
	b.maybeAnnotateWithEstimates(input.root, inputExpr)
	if reuseInputCols {
		outputCols = inputCols
		outputCols.Clear()
	} else {
		outputCols = b.colOrdsAlloc.Alloc()
	}
	for i, col := range colList {
		outputCols.Set(col, i)
	}
	sqlOrdering, err := sqlOrdering(provided, outputCols)
	if err != nil {
		return execPlan{}, colOrdMap{}, err
	}
	reqOrdering := exec.OutputOrdering(sqlOrdering)
	var res execPlan
	res.root, err = b.factory.ConstructSimpleProject(input.root, cols, reqOrdering)
	return res, outputCols, err
}

// applyPresentation adds a projection to a plan to satisfy a required
// Presentation property.
func (b *Builder) applyPresentation(
	input execPlan, inputCols colOrdMap, pres physical.Presentation,
) (_ execPlan, outputCols colOrdMap, err error) {
	cols := make([]exec.NodeColumnOrdinal, len(pres))
	colNames := make([]string, len(pres))
	var res execPlan
	outputCols = b.colOrdsAlloc.Alloc()
	for i := range pres {
		ord, err := getNodeColumnOrdinal(inputCols, pres[i].ID)
		if err != nil {
			return execPlan{}, colOrdMap{}, err
		}
		cols[i] = ord
		outputCols.Set(pres[i].ID, i)
		colNames[i] = pres[i].Alias
	}
	res.root, err = b.factory.ConstructSerializingProject(input.root, cols, colNames)
	return res, outputCols, err
}

// getEnvData consolidates the information that must be presented in
// EXPLAIN (opt, env).
func (b *Builder) getEnvData() (exec.ExplainEnvData, error) {
	envOpts := exec.ExplainEnvData{ShowEnv: true}
	var err error
	envOpts.Tables, envOpts.Sequences, envOpts.Views, err = b.mem.Metadata().AllDataSourceNames(
		b.ctx, b.catalog,
		func(ds cat.DataSource) (cat.DataSourceName, error) {
			return b.catalog.FullyQualifiedName(b.ctx, ds)
		},
		true, /* includeVirtualTables */
	)
	return envOpts, err
}

// statementTag returns a string that can be used in an error message regarding
// the given expression.
func (b *Builder) statementTag(expr memo.RelExpr) string {
	switch expr.Op() {
	case opt.OpaqueRelOp, opt.OpaqueMutationOp, opt.OpaqueDDLOp:
		return expr.Private().(*memo.OpaqueRelPrivate).Metadata.String()
	case opt.LockOp:
		return "SELECT " + expr.Private().(*memo.LockPrivate).Locking.Strength.String()
	default:
		return expr.Op().SyntaxTag()
	}
}

// recordJoinType increments the counter for the given join type for telemetry
// reporting.
func (b *Builder) recordJoinType(joinType descpb.JoinType) {
	if b.JoinTypeCounts == nil {
		const numJoinTypes = 7
		b.JoinTypeCounts = make(map[descpb.JoinType]int, numJoinTypes)
	}
	// Don't bother distinguishing between left and right.
	switch joinType {
	case descpb.RightOuterJoin:
		joinType = descpb.LeftOuterJoin
	case descpb.RightSemiJoin:
		joinType = descpb.LeftSemiJoin
	case descpb.RightAntiJoin:
		joinType = descpb.LeftAntiJoin
	}
	b.JoinTypeCounts[joinType]++
}

// recordJoinAlgorithm increments the counter for the given join algorithm for
// telemetry reporting.
func (b *Builder) recordJoinAlgorithm(joinAlgorithm exec.JoinAlgorithm) {
	if b.JoinAlgorithmCounts == nil {
		b.JoinAlgorithmCounts = make(map[exec.JoinAlgorithm]int, exec.NumJoinAlgorithms)
	}
	b.JoinAlgorithmCounts[joinAlgorithm]++
}

// boundedStalenessAllowList contains the operators that may be used with
// bounded staleness queries.
var boundedStalenessAllowList = map[opt.Operator]struct{}{
	opt.ValuesOp:           {},
	opt.ScanOp:             {},
	opt.PlaceholderScanOp:  {},
	opt.SelectOp:           {},
	opt.ProjectOp:          {},
	opt.GroupByOp:          {},
	opt.ScalarGroupByOp:    {},
	opt.DistinctOnOp:       {},
	opt.DistributeOp:       {},
	opt.EnsureDistinctOnOp: {},
	opt.LimitOp:            {},
	opt.OffsetOp:           {},
	opt.SortOp:             {},
	opt.OrdinalityOp:       {},
	opt.Max1RowOp:          {},
	opt.ProjectSetOp:       {},
	opt.WindowOp:           {},
	opt.ExplainOp:          {},
}
