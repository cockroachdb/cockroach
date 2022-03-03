// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execbuilder

import (
	"bytes"
	"context"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type execPlan struct {
	root exec.Node

	// outputCols is a map from opt.ColumnID to exec.NodeColumnOrdinal. It maps
	// columns in the output set of a relational expression to indices in the
	// result columns of the exec.Node.
	//
	// The reason we need to keep track of this (instead of using just the
	// relational properties) is that the relational properties don't force a
	// single "schema": any ordering of the output columns is possible. We choose
	// the schema that is most convenient: for scans, we use the table's column
	// ordering. Consider:
	//   SELECT a, b FROM t WHERE a = b
	// and the following two cases:
	//   1. The table is defined as (k INT PRIMARY KEY, a INT, b INT). The scan will
	//      return (k, a, b).
	//   2. The table is defined as (k INT PRIMARY KEY, b INT, a INT). The scan will
	//      return (k, b, a).
	// In these two cases, the relational properties are effectively the same.
	//
	// An alternative to this would be to always use a "canonical" schema, for
	// example the output columns in increasing index order. However, this would
	// require a lot of otherwise unnecessary projections.
	//
	// Note: conceptually, this could be a ColList; however, the map is more
	// convenient when converting VariableOps to IndexedVars.
	outputCols opt.ColMap
}

// numOutputCols returns the number of columns emitted by the execPlan's Node.
// This will typically be equal to ep.outputCols.Len(), but might be different
// if the node outputs the same optimizer ColumnID multiple times.
// TODO(justin): we should keep track of this instead of computing it each time.
func (ep *execPlan) numOutputCols() int {
	return numOutputColsInMap(ep.outputCols)
}

// numOutputColsInMap returns the number of slots required to fill in all of
// the columns referred to by this ColMap.
func numOutputColsInMap(m opt.ColMap) int {
	max, ok := m.MaxValue()
	if !ok {
		return 0
	}
	return max + 1
}

// makeBuildScalarCtx returns a buildScalarCtx that can be used with expressions
// that refer the output columns of this plan.
func (ep *execPlan) makeBuildScalarCtx() buildScalarCtx {
	return buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, ep.numOutputCols()),
		ivarMap: ep.outputCols,
	}
}

// getNodeColumnOrdinal takes a column that is known to be produced by the execPlan
// and returns the ordinal index of that column in the result columns of the
// node.
func (ep *execPlan) getNodeColumnOrdinal(col opt.ColumnID) exec.NodeColumnOrdinal {
	ord, ok := ep.outputCols.Get(int(col))
	if !ok {
		panic(errors.AssertionFailedf("column %d not in input", redact.Safe(col)))
	}
	return exec.NodeColumnOrdinal(ord)
}

func (ep *execPlan) getNodeColumnOrdinalSet(cols opt.ColSet) exec.NodeColumnOrdinalSet {
	var res exec.NodeColumnOrdinalSet
	cols.ForEach(func(colID opt.ColumnID) {
		res.Add(int(ep.getNodeColumnOrdinal(colID)))
	})
	return res
}

// reqOrdering converts the provided ordering of a relational expression to an
// OutputOrdering (according to the outputCols map).
func (ep *execPlan) reqOrdering(expr memo.RelExpr) exec.OutputOrdering {
	return exec.OutputOrdering(ep.sqlOrdering(expr.ProvidedPhysical().Ordering))
}

// sqlOrdering converts an Ordering to a ColumnOrdering (according to the
// outputCols map).
func (ep *execPlan) sqlOrdering(ordering opt.Ordering) colinfo.ColumnOrdering {
	if ordering.Empty() {
		return nil
	}
	colOrder := make(colinfo.ColumnOrdering, len(ordering))
	for i := range ordering {
		colOrder[i].ColIdx = int(ep.getNodeColumnOrdinal(ordering[i].ID()))
		if ordering[i].Descending() {
			colOrder[i].Direction = encoding.Descending
		} else {
			colOrder[i].Direction = encoding.Ascending
		}
	}

	return colOrder
}

func (b *Builder) buildRelational(e memo.RelExpr) (execPlan, error) {
	var ep execPlan
	var err error

	if opt.IsDDLOp(e) {
		// Mark the statement as containing DDL for use
		// in the SQL executor.
		b.IsDDL = true

		// This will set the system DB trigger for transactions containing
		// schema-modifying statements that have no effect, such as
		// `BEGIN; INSERT INTO ...; CREATE TABLE IF NOT EXISTS ...; COMMIT;`
		// where the table already exists. This will generate some false schema
		// cache refreshes, but that's expected to be quite rare in practice.
		if !b.evalCtx.Settings.Version.IsActive(
			b.evalCtx.Ctx(), clusterversion.DisableSystemConfigGossipTrigger,
		) {
			if err := b.evalCtx.Txn.DeprecatedSetSystemConfigTrigger(b.evalCtx.Codec.ForSystemTenant()); err != nil {
				return execPlan{}, errors.WithSecondaryError(
					unimplemented.NewWithIssuef(26508,
						"the first schema change statement in a transaction must precede any writes"),
					err)
			}
		}

	}

	if opt.IsMutationOp(e) {
		b.ContainsMutation = true
		// Raise error if mutation op is part of a read-only transaction.
		if b.evalCtx.TxnReadOnly {
			return execPlan{}, pgerror.Newf(pgcode.ReadOnlySQLTransaction,
				"cannot execute %s in a read-only transaction", b.statementTag(e))
		}
	}

	// Raise error if bounded staleness is used incorrectly.
	if b.boundedStaleness() {
		if _, ok := boundedStalenessAllowList[e.Op()]; !ok {
			return execPlan{}, unimplemented.NewWithIssuef(67562,
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
		ep, err = b.buildValues(t)

	case *memo.ScanExpr:
		ep, err = b.buildScan(t)

	case *memo.PlaceholderScanExpr:
		ep, err = b.buildPlaceholderScan(t)

	case *memo.SelectExpr:
		ep, err = b.buildSelect(t)

	case *memo.ProjectExpr:
		ep, err = b.buildProject(t)

	case *memo.GroupByExpr, *memo.ScalarGroupByExpr:
		ep, err = b.buildGroupBy(e)

	case *memo.DistinctOnExpr, *memo.EnsureDistinctOnExpr, *memo.UpsertDistinctOnExpr,
		*memo.EnsureUpsertDistinctOnExpr:
		ep, err = b.buildDistinct(t)

	case *memo.TopKExpr:
		ep, err = b.buildTopK(t)

	case *memo.LimitExpr, *memo.OffsetExpr:
		ep, err = b.buildLimitOffset(e)

	case *memo.SortExpr:
		ep, err = b.buildSort(t)

	case *memo.DistributeExpr:
		ep, err = b.buildDistribute(t)

	case *memo.IndexJoinExpr:
		ep, err = b.buildIndexJoin(t)

	case *memo.LookupJoinExpr:
		ep, err = b.buildLookupJoin(t)

	case *memo.InvertedJoinExpr:
		ep, err = b.buildInvertedJoin(t)

	case *memo.ZigzagJoinExpr:
		ep, err = b.buildZigzagJoin(t)

	case *memo.OrdinalityExpr:
		ep, err = b.buildOrdinality(t)

	case *memo.MergeJoinExpr:
		ep, err = b.buildMergeJoin(t)

	case *memo.Max1RowExpr:
		ep, err = b.buildMax1Row(t)

	case *memo.ProjectSetExpr:
		ep, err = b.buildProjectSet(t)

	case *memo.WindowExpr:
		ep, err = b.buildWindow(t)

	case *memo.SequenceSelectExpr:
		ep, err = b.buildSequenceSelect(t)

	case *memo.InsertExpr:
		ep, err = b.buildInsert(t)

	case *memo.InvertedFilterExpr:
		ep, err = b.buildInvertedFilter(t)

	case *memo.UpdateExpr:
		ep, err = b.buildUpdate(t)

	case *memo.UpsertExpr:
		ep, err = b.buildUpsert(t)

	case *memo.DeleteExpr:
		ep, err = b.buildDelete(t)

	case *memo.CreateTableExpr:
		ep, err = b.buildCreateTable(t)

	case *memo.CreateViewExpr:
		ep, err = b.buildCreateView(t)

	case *memo.WithExpr:
		ep, err = b.buildWith(t)

	case *memo.WithScanExpr:
		ep, err = b.buildWithScan(t)

	case *memo.RecursiveCTEExpr:
		ep, err = b.buildRecursiveCTE(t)

	case *memo.ExplainExpr:
		ep, err = b.buildExplain(t)

	case *memo.ShowTraceForSessionExpr:
		ep, err = b.buildShowTrace(t)

	case *memo.OpaqueRelExpr, *memo.OpaqueMutationExpr, *memo.OpaqueDDLExpr:
		ep, err = b.buildOpaque(t.Private().(*memo.OpaqueRelPrivate))

	case *memo.AlterTableSplitExpr:
		ep, err = b.buildAlterTableSplit(t)

	case *memo.AlterTableUnsplitExpr:
		ep, err = b.buildAlterTableUnsplit(t)

	case *memo.AlterTableUnsplitAllExpr:
		ep, err = b.buildAlterTableUnsplitAll(t)

	case *memo.AlterTableRelocateExpr:
		ep, err = b.buildAlterTableRelocate(t)

	case *memo.AlterRangeRelocateExpr:
		ep, err = b.buildAlterRangeRelocate(t)

	case *memo.ControlJobsExpr:
		ep, err = b.buildControlJobs(t)

	case *memo.ControlSchedulesExpr:
		ep, err = b.buildControlSchedules(t)

	case *memo.CancelQueriesExpr:
		ep, err = b.buildCancelQueries(t)

	case *memo.CancelSessionsExpr:
		ep, err = b.buildCancelSessions(t)

	case *memo.CreateStatisticsExpr:
		ep, err = b.buildCreateStatistics(t)

	case *memo.ExportExpr:
		ep, err = b.buildExport(t)

	default:
		switch {
		case opt.IsSetOp(e):
			ep, err = b.buildSetOp(e)

		case opt.IsJoinNonApplyOp(e):
			ep, err = b.buildHashJoin(e)

		case opt.IsJoinApplyOp(e):
			ep, err = b.buildApplyJoin(e)

		default:
			err = errors.AssertionFailedf("no execbuild for %T", t)
		}
	}
	if err != nil {
		return execPlan{}, err
	}

	// In test builds, assert that the exec plan output columns match the opt
	// plan output columns.
	if buildutil.CrdbTestBuild {
		optCols := e.Relational().OutputCols
		var execCols opt.ColSet
		ep.outputCols.ForEach(func(key, val int) {
			execCols.Add(opt.ColumnID(key))
		})
		if !execCols.Equals(optCols) {
			return execPlan{}, errors.AssertionFailedf(
				"exec columns do not match opt columns: expected %v, got %v. op: %T", optCols, execCols, e)
		}
	}

	// If we are building against an ExplainFactory, annotate the nodes with more
	// information.
	if ef, ok := b.factory.(exec.ExplainFactory); ok {
		stats := &e.Relational().Stats
		val := exec.EstimatedStats{
			TableStatsAvailable: stats.Available,
			RowCount:            stats.RowCount,
			Cost:                float64(e.Cost()),
		}
		if scan, ok := e.(*memo.ScanExpr); ok {
			tab := b.mem.Metadata().Table(scan.Table)
			if tab.StatisticCount() > 0 {
				// The first stat is the most recent one.
				stat := tab.Statistic(0)
				val.TableStatsRowCount = stat.RowCount()
				if val.TableStatsRowCount == 0 {
					val.TableStatsRowCount = 1
				}
				val.TableStatsCreatedAt = stat.CreatedAt()
				val.LimitHint = scan.RequiredPhysical().LimitHint
			}
		}
		ef.AnnotateNode(ep.root, exec.EstimatedStatsID, &val)
	}

	if saveTableName != "" {
		ep, err = b.applySaveTable(ep, e, saveTableName)
		if err != nil {
			return execPlan{}, err
		}
	}

	// Wrap the expression in a render expression if presentation requires it.
	if p := e.RequiredPhysical(); !p.Presentation.Any() {
		ep, err = b.applyPresentation(ep, p.Presentation)
	}
	return ep, err
}

func (b *Builder) buildValues(values *memo.ValuesExpr) (execPlan, error) {
	rows, err := b.buildValuesRows(values)
	if err != nil {
		return execPlan{}, err
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

func (b *Builder) constructValues(rows [][]tree.TypedExpr, cols opt.ColList) (execPlan, error) {
	md := b.mem.Metadata()
	resultCols := make(colinfo.ResultColumns, len(cols))
	for i, col := range cols {
		colMeta := md.ColumnMeta(col)
		resultCols[i].Name = colMeta.Alias
		resultCols[i].Typ = colMeta.Type
	}
	node, err := b.factory.ConstructValues(rows, resultCols)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{root: node}
	for i, col := range cols {
		ep.outputCols.Set(int(col), i)
	}

	return ep, nil
}

// getColumns returns the set of column ordinals in the table for the set of
// column IDs, along with a mapping from the column IDs to output ordinals
// (starting with 0).
func (b *Builder) getColumns(
	cols opt.ColSet, tableID opt.TableID,
) (exec.TableColumnOrdinalSet, opt.ColMap) {
	var needed exec.TableColumnOrdinalSet
	var output opt.ColMap

	columnCount := b.mem.Metadata().Table(tableID).ColumnCount()
	n := 0
	for i := 0; i < columnCount; i++ {
		colID := tableID.ColumnID(i)
		if cols.Contains(colID) {
			needed.Add(i)
			output.Set(int(colID), n)
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

	return c.CalculateMaxResults(b.evalCtx, indexCols, relProps.NotNullCols)
}

// scanParams populates ScanParams and the output column mapping.
func (b *Builder) scanParams(
	tab cat.Table, scan *memo.ScanPrivate, relProps *props.Relational, reqProps *physical.Required,
) (exec.ScanParams, opt.ColMap, error) {
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
				(b.ContainsLargeFullTableScan || b.ContainsLargeFullIndexScan) {
				err = errors.WithHint(err,
					"try overriding the `disallow_full_table_scans` or increasing the `large_full_scan_rows` cluster/session settings",
				)
			}
		}

		return exec.ScanParams{}, opt.ColMap{}, err
	}

	locking := scan.Locking
	if b.forceForUpdateLocking {
		locking = forUpdateLocking
	}

	// Raise error if row-level locking is part of a read-only transaction.
	if locking != nil && locking.Strength > tree.ForNone && b.evalCtx.TxnReadOnly {
		return exec.ScanParams{}, opt.ColMap{}, pgerror.Newf(pgcode.ReadOnlySQLTransaction,
			"cannot execute %s in a read-only transaction", locking.Strength.String())
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
	var rowCount float64
	if relProps.Stats.Available {
		rowCount = relProps.Stats.RowCount
	}

	if scan.PartitionConstrainedScan {
		sqltelemetry.IncrementPartitioningCounter(sqltelemetry.PartitionConstrainedScan)
	}

	softLimit := int64(math.Ceil(reqProps.LimitHint))
	hardLimit := scan.HardLimit.RowCount()

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
			maxResults, ok := b.indexConstraintMaxResults(scan, relProps)
			valid = ok && maxResults == 1
		}
		if !valid {
			return exec.ScanParams{}, opt.ColMap{}, unimplemented.NewWithIssuef(67562,
				"cannot use bounded staleness for queries that may touch more than one range or require an index join",
			)
		}
		b.containsBoundedStalenessScan = true
	}

	parallelize := false
	if hardLimit == 0 && softLimit == 0 {
		maxResults, ok := b.indexConstraintMaxResults(scan, relProps)
		if ok && maxResults < ParallelScanResultThreshold {
			// Don't set the flag when we have a single span which returns a single
			// row: it does nothing in this case except litter EXPLAINs.
			// There are still cases where the flag doesn't do anything when the spans
			// cover a single range, but there is nothing we can do about that.
			if !(maxResults == 1 && scan.Constraint.Spans.Count() == 1) {
				parallelize = true
			}
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
		return exec.ScanParams{}, opt.ColMap{}, errors.AssertionFailedf("scan can't provide required ordering")
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

func (b *Builder) buildScan(scan *memo.ScanExpr) (execPlan, error) {
	md := b.mem.Metadata()
	tab := md.Table(scan.Table)

	if !b.disableTelemetry && scan.PartialIndexPredicate(md) != nil {
		telemetry.Inc(sqltelemetry.PartialIndexScanUseCounter)
	}

	if scan.Flags.ForceZigzag {
		return execPlan{}, fmt.Errorf("could not produce a query plan conforming to the FORCE_ZIGZAG hint")
	}

	isUnfiltered := scan.IsUnfiltered(md)
	if scan.Flags.NoFullScan {
		// Normally a full scan of a partial index would be allowed with the
		// NO_FULL_SCAN hint (isUnfiltered is false for partial indexes), but if the
		// user has explicitly forced the partial index *and* used NO_FULL_SCAN, we
		// disallow the full index scan.
		if isUnfiltered || (scan.Flags.ForceIndex && scan.IsFullIndexScan(md)) {
			return execPlan{}, fmt.Errorf("could not produce a query plan conforming to the NO_FULL_SCAN hint")
		}
	}

	// Save if we planned a full table/index scan on the builder so that the
	// planner can be made aware later. We only do this for non-virtual tables.
	if !tab.IsVirtualTable() && isUnfiltered {
		stats := scan.Relational().Stats
		large := !stats.Available || stats.RowCount > b.evalCtx.SessionData().LargeFullScanRows
		if scan.Index == cat.PrimaryIndex {
			b.ContainsFullTableScan = true
			b.ContainsLargeFullTableScan = b.ContainsLargeFullTableScan || large
		} else {
			b.ContainsFullIndexScan = true
			b.ContainsLargeFullIndexScan = b.ContainsLargeFullIndexScan || large
		}
	}

	params, outputCols, err := b.scanParams(tab, &scan.ScanPrivate, scan.Relational(), scan.RequiredPhysical())
	if err != nil {
		return execPlan{}, err
	}
	res := execPlan{outputCols: outputCols}
	root, err := b.factory.ConstructScan(
		tab,
		tab.Index(scan.Index),
		params,
		res.reqOrdering(scan),
	)
	if err != nil {
		return execPlan{}, err
	}

	res.root = root
	return res, nil
}

func (b *Builder) buildPlaceholderScan(scan *memo.PlaceholderScanExpr) (execPlan, error) {
	if scan.Constraint != nil || scan.InvertedConstraint != nil {
		return execPlan{}, errors.AssertionFailedf("PlaceholderScan cannot have constraints")
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
	keyCtx := constraint.MakeKeyContext(&columns, b.evalCtx)

	values := make([]tree.Datum, len(scan.Span))
	for i, expr := range scan.Span {
		// The expression is either a placeholder or a constant.
		if p, ok := expr.(*memo.PlaceholderExpr); ok {
			val, err := p.Value.(*tree.Placeholder).Eval(b.evalCtx)
			if err != nil {
				return execPlan{}, err
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
	private.SetConstraint(b.evalCtx, &c)

	params, outputCols, err := b.scanParams(tab, &private, scan.Relational(), scan.RequiredPhysical())
	if err != nil {
		return execPlan{}, err
	}
	res := execPlan{outputCols: outputCols}
	root, err := b.factory.ConstructScan(
		tab,
		tab.Index(scan.Index),
		params,
		res.reqOrdering(scan),
	)
	if err != nil {
		return execPlan{}, err
	}

	res.root = root
	return res, nil
}

func (b *Builder) buildSelect(sel *memo.SelectExpr) (execPlan, error) {
	input, err := b.buildRelational(sel.Input)
	if err != nil {
		return execPlan{}, err
	}
	filter, err := b.buildScalarWithMap(input.outputCols, &sel.Filters)
	if err != nil {
		return execPlan{}, err
	}
	// A filtering node does not modify the schema.
	res := execPlan{outputCols: input.outputCols}
	reqOrder := res.reqOrdering(sel)
	res.root, err = b.factory.ConstructFilter(input.root, filter, reqOrder)
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

func (b *Builder) buildInvertedFilter(invFilter *memo.InvertedFilterExpr) (execPlan, error) {
	input, err := b.buildRelational(invFilter.Input)
	if err != nil {
		return execPlan{}, err
	}
	// A filtering node does not modify the schema.
	res := execPlan{outputCols: input.outputCols}
	invertedCol := input.getNodeColumnOrdinal(invFilter.InvertedColumn)
	var typedPreFilterExpr tree.TypedExpr
	var typ *types.T
	if invFilter.PreFiltererState != nil && invFilter.PreFiltererState.Expr != nil {
		// The expression has a single variable, corresponding to the indexed
		// column. We assign it an ordinal of 0.
		var colMap opt.ColMap
		colMap.Set(int(invFilter.PreFiltererState.Col), 0)
		ctx := buildScalarCtx{
			ivh:     tree.MakeIndexedVarHelper(nil /* container */, colMap.Len()),
			ivarMap: colMap,
		}
		typedPreFilterExpr, err = b.buildScalar(&ctx, invFilter.PreFiltererState.Expr)
		if err != nil {
			return execPlan{}, err
		}
		typ = invFilter.PreFiltererState.Typ
	}
	res.root, err = b.factory.ConstructInvertedFilter(
		input.root, invFilter.InvertedExpression, typedPreFilterExpr, typ, invertedCol)
	if err != nil {
		return execPlan{}, err
	}
	// Apply a post-projection to remove the inverted column.
	//
	// TODO(rytaft): the invertedFilter used to do this post-projection, but we
	// had difficulty integrating that behavior. Investigate and restore that
	// original behavior.
	return b.applySimpleProject(
		res, invFilter.Relational().OutputCols, invFilter.ProvidedPhysical().Ordering,
	)
}

// applySimpleProject adds a simple projection on top of an existing plan.
func (b *Builder) applySimpleProject(
	input execPlan, cols opt.ColSet, providedOrd opt.Ordering,
) (execPlan, error) {
	// We have only pass-through columns.
	colList := make([]exec.NodeColumnOrdinal, 0, cols.Len())
	var res execPlan
	cols.ForEach(func(i opt.ColumnID) {
		res.outputCols.Set(int(i), len(colList))
		colList = append(colList, input.getNodeColumnOrdinal(i))
	})
	var err error
	res.root, err = b.factory.ConstructSimpleProject(
		input.root, colList, exec.OutputOrdering(res.sqlOrdering(providedOrd)),
	)
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

func (b *Builder) buildProject(prj *memo.ProjectExpr) (execPlan, error) {
	md := b.mem.Metadata()
	input, err := b.buildRelational(prj.Input)
	if err != nil {
		return execPlan{}, err
	}

	projections := prj.Projections
	if len(projections) == 0 {
		// We have only pass-through columns.
		return b.applySimpleProject(input, prj.Passthrough, prj.ProvidedPhysical().Ordering)
	}

	var res execPlan
	exprs := make(tree.TypedExprs, 0, len(projections)+prj.Passthrough.Len())
	cols := make(colinfo.ResultColumns, 0, len(exprs))
	ctx := input.makeBuildScalarCtx()
	for i := range projections {
		item := &projections[i]
		expr, err := b.buildScalar(&ctx, item.Element)
		if err != nil {
			return execPlan{}, err
		}
		res.outputCols.Set(int(item.Col), i)
		exprs = append(exprs, expr)
		cols = append(cols, colinfo.ResultColumn{
			Name: md.ColumnMeta(item.Col).Alias,
			Typ:  item.Typ,
		})
	}
	prj.Passthrough.ForEach(func(colID opt.ColumnID) {
		res.outputCols.Set(int(colID), len(exprs))
		indexedVar := b.indexedVar(&ctx, md, colID)
		exprs = append(exprs, indexedVar)
		meta := md.ColumnMeta(colID)
		cols = append(cols, colinfo.ResultColumn{
			Name: meta.Alias,
			Typ:  meta.Type,
		})
	})
	reqOrdering := res.reqOrdering(prj)
	res.root, err = b.factory.ConstructRender(input.root, cols, exprs, reqOrdering)
	if err != nil {
		return execPlan{}, err
	}
	return res, nil
}

func (b *Builder) buildApplyJoin(join memo.RelExpr) (execPlan, error) {
	switch join.Op() {
	case opt.InnerJoinApplyOp, opt.LeftJoinApplyOp, opt.SemiJoinApplyOp, opt.AntiJoinApplyOp:
	default:
		return execPlan{}, fmt.Errorf("couldn't execute correlated subquery with op %s", join.Op())
	}
	joinType := joinOpToJoinType(join.Op())
	leftExpr := join.Child(0).(memo.RelExpr)
	leftProps := leftExpr.Relational()
	rightExpr := join.Child(1).(memo.RelExpr)
	rightProps := rightExpr.Relational()
	filters := join.Child(2).(*memo.FiltersExpr)

	// We will pre-populate the withExprs of the right-hand side execbuilder.
	withExprs := make([]builtWithExpr, len(b.withExprs))
	copy(withExprs, b.withExprs)

	leftPlan, err := b.buildRelational(leftExpr)
	if err != nil {
		return execPlan{}, err
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
	var leftBoundColMap opt.ColMap
	for col, ok := leftBoundCols.Next(0); ok; col, ok = leftBoundCols.Next(col + 1) {
		v, ok := leftPlan.outputCols.Get(int(col))
		if !ok {
			return execPlan{}, fmt.Errorf("couldn't find binding column %d in left output columns", col)
		}
		leftBoundColMap.Set(int(col), v)
	}

	// Now, the cool part! We set up an ApplyJoinPlanRightSideFn which plans the
	// right side given a particular left side row. We do this planning in a
	// separate memo, but we use the same exec.Factory.
	//
	// Note: we put o outside of the function so we allocate it only once.
	var o xform.Optimizer
	planRightSideFn := func(ef exec.Factory, leftRow tree.Datums) (exec.Plan, error) {
		o.Init(b.evalCtx, b.catalog)
		f := o.Factory()

		// Copy the right expression into a new memo, replacing each bound column
		// with the corresponding value from the left row.
		var replaceFn norm.ReplaceFunc
		replaceFn = func(e opt.Expr) opt.Expr {
			switch t := e.(type) {
			case *memo.VariableExpr:
				if leftOrd, ok := leftBoundColMap.Get(int(t.Col)); ok {
					return f.ConstructConstVal(leftRow[leftOrd], t.Typ)
				}

			case *memo.WithScanExpr:
				// Allow referring to "outer" With expressions. The bound expressions
				// are not part of this Memo but they are used only for their relational
				// properties, which should be valid.
				for i := range withExprs {
					if withExprs[i].id == t.With {
						memoExpr := b.mem.Metadata().WithBinding(t.With)
						f.Metadata().AddWithBinding(t.With, memoExpr)
						break
					}
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

		eb := New(ef, &o, f.Memo(), b.catalog, newRightSide, b.evalCtx, false /* allowAutoCommit */)
		eb.disableTelemetry = true
		eb.withExprs = withExprs
		plan, err := eb.Build()
		if err != nil {
			if errors.IsAssertionFailure(err) {
				// Enhance the error with the EXPLAIN (OPT, VERBOSE) of the inner
				// expression.
				fmtFlags := memo.ExprFmtHideQualifications | memo.ExprFmtHideScalars | memo.ExprFmtHideTypes
				explainOpt := o.FormatExpr(newRightSide, fmtFlags)
				err = errors.WithDetailf(err, "newRightSide:\n%s", explainOpt)
			}
			return nil, err
		}
		return plan, nil
	}

	// The right plan will always produce the columns in the presentation, in
	// the same order.
	var rightOutputCols opt.ColMap
	for i := range rightRequiredProps.Presentation {
		rightOutputCols.Set(int(rightRequiredProps.Presentation[i].ID), i)
	}
	allCols := joinOutputMap(leftPlan.outputCols, rightOutputCols)

	var onExpr tree.TypedExpr
	if len(*filters) != 0 {
		scalarCtx := buildScalarCtx{
			ivh:     tree.MakeIndexedVarHelper(nil /* container */, numOutputColsInMap(allCols)),
			ivarMap: allCols,
		}
		onExpr, err = b.buildScalar(&scalarCtx, filters)
		if err != nil {
			return execPlan{}, err
		}
	}

	var outputCols opt.ColMap
	if !joinType.ShouldIncludeRightColsInOutput() {
		outputCols = leftPlan.outputCols
	} else {
		outputCols = allCols
	}

	ep := execPlan{outputCols: outputCols}

	ep.root, err = b.factory.ConstructApplyJoin(
		joinType,
		leftPlan.root,
		b.presentationToResultColumns(rightRequiredProps.Presentation),
		onExpr,
		planRightSideFn,
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
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

func (b *Builder) buildHashJoin(join memo.RelExpr) (execPlan, error) {
	if f := join.Private().(*memo.JoinPrivate).Flags; f.Has(memo.DisallowHashJoinStoreRight) {
		// We need to do a bit of reverse engineering here to determine what the
		// hint was.
		hint := tree.AstLookup
		if !f.Has(memo.DisallowMergeJoin) {
			hint = tree.AstMerge
		} else if !f.Has(memo.DisallowInvertedJoinIntoLeft) || !f.Has(memo.DisallowInvertedJoinIntoRight) {
			hint = tree.AstInverted
		}

		return execPlan{}, errors.Errorf(
			"could not produce a query plan conforming to the %s JOIN hint", hint,
		)
	}

	joinType := joinOpToJoinType(join.Op())
	leftExpr := join.Child(0).(memo.RelExpr)
	rightExpr := join.Child(1).(memo.RelExpr)
	filters := join.Child(2).(*memo.FiltersExpr)
	if joinType == descpb.LeftSemiJoin || joinType == descpb.LeftAntiJoin {
		// We have a partial join, and we want to make sure that the relation
		// with smaller cardinality is on the right side. Note that we assumed
		// it during the costing.
		// TODO(raduberinde): we might also need to look at memo.JoinFlags when
		// choosing a side.
		leftRowCount := leftExpr.Relational().Stats.RowCount
		rightRowCount := rightExpr.Relational().Stats.RowCount
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
	if !b.disableTelemetry {
		if len(leftEq) > 0 {
			telemetry.Inc(sqltelemetry.JoinAlgoHashUseCounter)
		} else {
			telemetry.Inc(sqltelemetry.JoinAlgoCrossUseCounter)
		}
		telemetry.Inc(opt.JoinTypeToUseCounter(join.Op()))
	}

	left, right, onExpr, outputCols, err := b.initJoinBuild(
		leftExpr,
		rightExpr,
		memo.ExtractRemainingJoinFilters(*filters, leftEq, rightEq),
		joinType,
	)
	if err != nil {
		return execPlan{}, err
	}
	ep := execPlan{outputCols: outputCols}

	// Convert leftEq/rightEq to ordinals.
	eqColsBuf := make([]exec.NodeColumnOrdinal, 2*len(leftEq))
	leftEqOrdinals := eqColsBuf[:len(leftEq):len(leftEq)]
	rightEqOrdinals := eqColsBuf[len(leftEq):]
	for i := range leftEq {
		leftEqOrdinals[i] = left.getNodeColumnOrdinal(leftEq[i])
		rightEqOrdinals[i] = right.getNodeColumnOrdinal(rightEq[i])
	}

	leftEqColsAreKey := leftExpr.Relational().FuncDeps.ColsAreStrictKey(leftEq.ToSet())
	rightEqColsAreKey := rightExpr.Relational().FuncDeps.ColsAreStrictKey(rightEq.ToSet())

	ep.root, err = b.factory.ConstructHashJoin(
		joinType,
		left.root, right.root,
		leftEqOrdinals, rightEqOrdinals,
		leftEqColsAreKey, rightEqColsAreKey,
		onExpr,
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

func (b *Builder) buildMergeJoin(join *memo.MergeJoinExpr) (execPlan, error) {
	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.JoinAlgoMergeUseCounter)
		telemetry.Inc(opt.JoinTypeToUseCounter(join.JoinType))
	}

	joinType := joinOpToJoinType(join.JoinType)
	leftExpr, rightExpr := join.Left, join.Right
	leftEq, rightEq := join.LeftEq, join.RightEq

	if joinType == descpb.LeftSemiJoin || joinType == descpb.LeftAntiJoin {
		// We have a partial join, and we want to make sure that the relation
		// with smaller cardinality is on the right side. Note that we assumed
		// it during the costing.
		// TODO(raduberinde): we might also need to look at memo.JoinFlags when
		// choosing a side.
		leftRowCount := leftExpr.Relational().Stats.RowCount
		rightRowCount := rightExpr.Relational().Stats.RowCount
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

	left, right, onExpr, outputCols, err := b.initJoinBuild(
		leftExpr, rightExpr, join.On, joinType,
	)
	if err != nil {
		return execPlan{}, err
	}
	leftOrd := left.sqlOrdering(leftEq)
	rightOrd := right.sqlOrdering(rightEq)
	ep := execPlan{outputCols: outputCols}
	reqOrd := ep.reqOrdering(join)
	leftEqColsAreKey := leftExpr.Relational().FuncDeps.ColsAreStrictKey(leftEq.ColSet())
	rightEqColsAreKey := rightExpr.Relational().FuncDeps.ColsAreStrictKey(rightEq.ColSet())
	ep.root, err = b.factory.ConstructMergeJoin(
		joinType,
		left.root, right.root,
		onExpr,
		leftOrd, rightOrd, reqOrd,
		leftEqColsAreKey, rightEqColsAreKey,
	)
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

// initJoinBuild builds the inputs to the join as well as the ON expression.
func (b *Builder) initJoinBuild(
	leftChild memo.RelExpr,
	rightChild memo.RelExpr,
	filters memo.FiltersExpr,
	joinType descpb.JoinType,
) (leftPlan, rightPlan execPlan, onExpr tree.TypedExpr, outputCols opt.ColMap, _ error) {
	leftPlan, err := b.buildRelational(leftChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
	}
	rightPlan, err = b.buildRelational(rightChild)
	if err != nil {
		return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
	}

	allCols := joinOutputMap(leftPlan.outputCols, rightPlan.outputCols)

	if len(filters) != 0 {
		onExpr, err = b.buildScalarWithMap(allCols, &filters)
		if err != nil {
			return execPlan{}, execPlan{}, nil, opt.ColMap{}, err
		}
	}

	if !joinType.ShouldIncludeLeftColsInOutput() {
		return leftPlan, rightPlan, onExpr, rightPlan.outputCols, nil
	}
	if !joinType.ShouldIncludeRightColsInOutput() {
		return leftPlan, rightPlan, onExpr, leftPlan.outputCols, nil
	}
	return leftPlan, rightPlan, onExpr, allCols, nil
}

// joinOutputMap determines the outputCols map for a (non-semi/anti) join, given
// the outputCols maps for its inputs.
func joinOutputMap(left, right opt.ColMap) opt.ColMap {
	numLeftCols := numOutputColsInMap(left)

	res := left.Copy()
	right.ForEach(func(colIdx, rightIdx int) {
		res.Set(colIdx, rightIdx+numLeftCols)
	})
	return res
}

func joinOpToJoinType(op opt.Operator) descpb.JoinType {
	switch op {
	case opt.InnerJoinOp, opt.InnerJoinApplyOp:
		return descpb.InnerJoin

	case opt.LeftJoinOp, opt.LeftJoinApplyOp:
		return descpb.LeftOuterJoin

	case opt.RightJoinOp:
		return descpb.RightOuterJoin

	case opt.FullJoinOp:
		return descpb.FullOuterJoin

	case opt.SemiJoinOp, opt.SemiJoinApplyOp:
		return descpb.LeftSemiJoin

	case opt.AntiJoinOp, opt.AntiJoinApplyOp:
		return descpb.LeftAntiJoin

	default:
		panic(errors.AssertionFailedf("not a join op %s", redact.Safe(op)))
	}
}

func (b *Builder) buildGroupBy(groupBy memo.RelExpr) (execPlan, error) {
	input, err := b.buildGroupByInput(groupBy)
	if err != nil {
		return execPlan{}, err
	}

	var ep execPlan
	groupingCols := groupBy.Private().(*memo.GroupingPrivate).GroupingCols
	groupingColIdx := make([]exec.NodeColumnOrdinal, 0, groupingCols.Len())
	for i, ok := groupingCols.Next(0); ok; i, ok = groupingCols.Next(i + 1) {
		ep.outputCols.Set(int(i), len(groupingColIdx))
		groupingColIdx = append(groupingColIdx, input.getNodeColumnOrdinal(i))
	}

	aggregations := *groupBy.Child(1).(*memo.AggregationsExpr)
	aggInfos := make([]exec.AggInfo, len(aggregations))
	for i := range aggregations {
		item := &aggregations[i]
		agg := item.Agg

		var filterOrd exec.NodeColumnOrdinal = tree.NoColumnIdx
		if aggFilter, ok := agg.(*memo.AggFilterExpr); ok {
			filter, ok := aggFilter.Filter.(*memo.VariableExpr)
			if !ok {
				return execPlan{}, errors.AssertionFailedf("only VariableOp args supported")
			}
			filterOrd = input.getNodeColumnOrdinal(filter.Col)
			agg = aggFilter.Input
		}

		distinct := false
		if aggDistinct, ok := agg.(*memo.AggDistinctExpr); ok {
			distinct = true
			agg = aggDistinct.Input
		}

		name, _ := memo.FindAggregateOverload(agg)

		// Accumulate variable arguments in argCols and constant arguments in
		// constArgs. Constant arguments must follow variable arguments.
		var argCols []exec.NodeColumnOrdinal
		var constArgs tree.Datums
		for j, n := 0, agg.ChildCount(); j < n; j++ {
			child := agg.Child(j)
			if variable, ok := child.(*memo.VariableExpr); ok {
				if len(constArgs) != 0 {
					return execPlan{}, errors.Errorf("constant args must come after variable args")
				}
				argCols = append(argCols, input.getNodeColumnOrdinal(variable.Col))
			} else {
				if len(argCols) == 0 {
					return execPlan{}, errors.Errorf("a constant arg requires at least one variable arg")
				}
				constArgs = append(constArgs, memo.ExtractConstDatum(child))
			}
		}

		aggInfos[i] = exec.AggInfo{
			FuncName:   name,
			Distinct:   distinct,
			ResultType: item.Agg.DataType(),
			ArgCols:    argCols,
			ConstArgs:  constArgs,
			Filter:     filterOrd,
		}
		ep.outputCols.Set(int(item.Col), len(groupingColIdx)+i)
	}

	if groupBy.Op() == opt.ScalarGroupByOp {
		ep.root, err = b.factory.ConstructScalarGroupBy(input.root, aggInfos)
	} else {
		groupBy := groupBy.(*memo.GroupByExpr)
		groupingColOrder := input.sqlOrdering(ordering.StreamingGroupingColOrdering(
			&groupBy.GroupingPrivate, &groupBy.RequiredPhysical().Ordering,
		))
		reqOrdering := ep.reqOrdering(groupBy)
		orderType := exec.GroupingOrderType(groupBy.GroupingOrderType(&groupBy.RequiredPhysical().Ordering))
		ep.root, err = b.factory.ConstructGroupBy(
			input.root, groupingColIdx, groupingColOrder, aggInfos, reqOrdering, orderType,
		)
	}
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

func (b *Builder) buildDistinct(distinct memo.RelExpr) (execPlan, error) {
	private := distinct.Private().(*memo.GroupingPrivate)

	if private.GroupingCols.Empty() {
		// A DistinctOn with no grouping columns should have been converted to a
		// LIMIT 1 or Max1Row by normalization rules.
		return execPlan{}, fmt.Errorf("cannot execute distinct on no columns")
	}
	input, err := b.buildGroupByInput(distinct)
	if err != nil {
		return execPlan{}, err
	}

	distinctCols := input.getNodeColumnOrdinalSet(private.GroupingCols)
	var orderedCols exec.NodeColumnOrdinalSet
	ordering := ordering.StreamingGroupingColOrdering(
		private, &distinct.RequiredPhysical().Ordering,
	)
	for i := range ordering {
		orderedCols.Add(int(input.getNodeColumnOrdinal(ordering[i].ID())))
	}
	ep := execPlan{outputCols: input.outputCols}

	reqOrdering := ep.reqOrdering(distinct)
	ep.root, err = b.factory.ConstructDistinct(
		input.root, distinctCols, orderedCols, reqOrdering,
		private.NullsAreDistinct, private.ErrorOnDup)
	if err != nil {
		return execPlan{}, err
	}

	// buildGroupByInput can add extra sort column(s), so discard those if they
	// are present by using an additional projection.
	outCols := distinct.Relational().OutputCols
	if input.outputCols.Len() == outCols.Len() {
		return ep, nil
	}
	return b.ensureColumns(ep, outCols.ToList(), distinct.ProvidedPhysical().Ordering)
}

func (b *Builder) buildGroupByInput(groupBy memo.RelExpr) (execPlan, error) {
	groupByInput := groupBy.Child(0).(memo.RelExpr)
	input, err := b.buildRelational(groupByInput)
	if err != nil {
		return execPlan{}, err
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
		neededCols.UnionWith(memo.ExtractAggInputColumns(aggs[i].Agg))
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
		return input, nil
	}

	// The input is producing columns that are not useful; set up a projection.
	cols := make([]exec.NodeColumnOrdinal, 0, neededCols.Len())
	var newOutputCols opt.ColMap
	for colID, ok := neededCols.Next(0); ok; colID, ok = neededCols.Next(colID + 1) {
		ordinal, ordOk := input.outputCols.Get(int(colID))
		if !ordOk {
			panic(errors.AssertionFailedf("needed column not produced by group-by input"))
		}
		newOutputCols.Set(int(colID), len(cols))
		cols = append(cols, exec.NodeColumnOrdinal(ordinal))
	}

	input.outputCols = newOutputCols
	reqOrdering := input.reqOrdering(groupByInput)
	input.root, err = b.factory.ConstructSimpleProject(input.root, cols, reqOrdering)
	if err != nil {
		return execPlan{}, err
	}
	return input, nil
}

func (b *Builder) buildSetOp(set memo.RelExpr) (execPlan, error) {
	leftExpr := set.Child(0).(memo.RelExpr)
	left, err := b.buildRelational(leftExpr)
	if err != nil {
		return execPlan{}, err
	}
	rightExpr := set.Child(1).(memo.RelExpr)
	right, err := b.buildRelational(rightExpr)
	if err != nil {
		return execPlan{}, err
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
	left, err = b.ensureColumns(left, private.LeftCols, leftExpr.ProvidedPhysical().Ordering)
	if err != nil {
		return execPlan{}, err
	}
	right, err = b.ensureColumns(right, private.RightCols, rightExpr.ProvidedPhysical().Ordering)
	if err != nil {
		return execPlan{}, err
	}

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
		panic(errors.AssertionFailedf("invalid operator %s", redact.Safe(set.Op())))
	}

	hardLimit := uint64(0)
	if set.Op() == opt.LocalityOptimizedSearchOp {
		if !b.disableTelemetry {
			telemetry.Inc(sqltelemetry.LocalityOptimizedSearchUseCounter)
		}

		// If we are performing locality optimized search, set a limit equal to
		// the maximum possible number of rows. This will tell the execution engine
		// not to execute the right child if the limit is reached by the left
		// child.
		// TODO(rytaft): Store the limit in the expression.
		hardLimit = uint64(set.Relational().Cardinality.Max)
	}

	ep := execPlan{}
	for i, col := range private.OutCols {
		ep.outputCols.Set(int(col), i)
	}
	streamingOrdering := ep.sqlOrdering(
		ordering.StreamingSetOpOrdering(set, &set.RequiredPhysical().Ordering),
	)
	reqOrdering := ep.reqOrdering(set)

	if typ == tree.UnionOp && all {
		ep.root, err = b.factory.ConstructUnionAll(left.root, right.root, reqOrdering, hardLimit)
	} else if len(streamingOrdering) > 0 {
		ep.root, err = b.factory.ConstructStreamingSetOp(typ, all, left.root, right.root, streamingOrdering, reqOrdering)
	} else {
		if len(reqOrdering) > 0 {
			return execPlan{}, errors.AssertionFailedf("hash set op is not supported with a required ordering")
		}
		ep.root, err = b.factory.ConstructHashSetOp(typ, all, left.root, right.root)
	}
	if err != nil {
		return execPlan{}, err
	}
	return ep, nil
}

// buildTopK builds a plan for a TopKOp, which is like a combined SortOp and LimitOp.
func (b *Builder) buildTopK(e *memo.TopKExpr) (execPlan, error) {
	inputExpr := e.Input
	input, err := b.buildRelational(inputExpr)
	if err != nil {
		return execPlan{}, err
	}
	ordering := e.Ordering.ToOrdering()
	inputOrdering := e.Input.ProvidedPhysical().Ordering
	alreadyOrderedPrefix := 0
	for i := range inputOrdering {
		if i == len(ordering) {
			return execPlan{}, errors.AssertionFailedf("sort ordering already provided by input")
		}
		if inputOrdering[i] != ordering[i] {
			break
		}
		alreadyOrderedPrefix = i + 1
	}
	node, err := b.factory.ConstructTopK(
		input.root,
		e.K,
		exec.OutputOrdering(input.sqlOrdering(ordering)),
		alreadyOrderedPrefix)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

// buildLimitOffset builds a plan for a LimitOp or OffsetOp
func (b *Builder) buildLimitOffset(e memo.RelExpr) (execPlan, error) {
	input, err := b.buildRelational(e.Child(0).(memo.RelExpr))
	if err != nil {
		return execPlan{}, err
	}
	// LIMIT/OFFSET expression should never need buildScalarContext, because it
	// can't refer to the input expression.
	expr, err := b.buildScalar(nil, e.Child(1).(opt.ScalarExpr))
	if err != nil {
		return execPlan{}, err
	}
	var node exec.Node
	if e.Op() == opt.LimitOp {
		node, err = b.factory.ConstructLimit(input.root, expr, nil)
	} else {
		node, err = b.factory.ConstructLimit(input.root, nil, expr)
	}
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

func (b *Builder) buildSort(sort *memo.SortExpr) (execPlan, error) {
	input, err := b.buildRelational(sort.Input)
	if err != nil {
		return execPlan{}, err
	}

	ordering := sort.ProvidedPhysical().Ordering
	inputOrdering := sort.Input.ProvidedPhysical().Ordering
	alreadyOrderedPrefix := 0
	for i := range inputOrdering {
		if i == len(ordering) {
			return execPlan{}, errors.AssertionFailedf("sort ordering already provided by input")
		}
		if inputOrdering[i] != ordering[i] {
			break
		}
		alreadyOrderedPrefix = i + 1
	}

	node, err := b.factory.ConstructSort(
		input.root,
		exec.OutputOrdering(input.sqlOrdering(ordering)),
		alreadyOrderedPrefix,
	)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

func (b *Builder) buildDistribute(distribute *memo.DistributeExpr) (execPlan, error) {
	input, err := b.buildRelational(distribute.Input)
	if err != nil {
		return execPlan{}, err
	}

	distribution := distribute.ProvidedPhysical().Distribution
	inputDistribution := distribute.Input.ProvidedPhysical().Distribution
	if distribution.Equals(inputDistribution) {
		// Don't bother creating a no-op distribution. This likely exists because
		// the input is a Sort expression, and this is an artifact of how physical
		// properties are enforced.
		return input, err
	}

	// TODO(rytaft): This is currently a no-op. We should pass this distribution
	// info to the DistSQL planner.
	return input, err
}

func (b *Builder) buildOrdinality(ord *memo.OrdinalityExpr) (execPlan, error) {
	input, err := b.buildRelational(ord.Input)
	if err != nil {
		return execPlan{}, err
	}

	colName := b.mem.Metadata().ColumnMeta(ord.ColID).Alias

	node, err := b.factory.ConstructOrdinality(input.root, colName)
	if err != nil {
		return execPlan{}, err
	}

	// We have one additional ordinality column, which is ordered at the end of
	// the list.
	outputCols := input.outputCols.Copy()
	outputCols.Set(int(ord.ColID), outputCols.Len())

	return execPlan{root: node, outputCols: outputCols}, nil
}

func (b *Builder) buildIndexJoin(join *memo.IndexJoinExpr) (execPlan, error) {
	input, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, err
	}

	md := b.mem.Metadata()
	tab := md.Table(join.Table)

	// TODO(radu): the distsql implementation of index join assumes that the input
	// starts with the PK columns in order (#40749).
	pri := tab.Index(cat.PrimaryIndex)
	keyCols := make([]exec.NodeColumnOrdinal, pri.KeyColumnCount())
	for i := range keyCols {
		keyCols[i] = input.getNodeColumnOrdinal(join.Table.ColumnID(pri.Column(i).Ordinal()))
	}

	cols := join.Cols
	needed, output := b.getColumns(cols, join.Table)
	res := execPlan{outputCols: output}
	res.root, err = b.factory.ConstructIndexJoin(
		input.root, tab, keyCols, needed, res.reqOrdering(join),
	)
	if err != nil {
		return execPlan{}, err
	}

	return res, nil
}

func (b *Builder) buildLookupJoin(join *memo.LookupJoinExpr) (execPlan, error) {
	md := b.mem.Metadata()

	if !b.disableTelemetry {
		telemetry.Inc(sqltelemetry.JoinAlgoLookupUseCounter)
		telemetry.Inc(opt.JoinTypeToUseCounter(join.JoinType))

		if join.UsesPartialIndex(md) {
			telemetry.Inc(sqltelemetry.PartialIndexLookupJoinUseCounter)
		}
	}

	input, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, err
	}

	keyCols := make([]exec.NodeColumnOrdinal, len(join.KeyCols))
	for i, c := range join.KeyCols {
		keyCols[i] = input.getNodeColumnOrdinal(c)
	}

	inputCols := join.Input.Relational().OutputCols
	lookupCols := join.Cols.Difference(inputCols)
	if join.IsFirstJoinInPairedJoiner {
		lookupCols.Remove(join.ContinuationCol)
	}

	lookupOrdinals, lookupColMap := b.getColumns(lookupCols, join.Table)
	// allExprCols are the columns used in expressions evaluated by this join.
	allExprCols := joinOutputMap(input.outputCols, lookupColMap)
	allCols := allExprCols
	if join.IsFirstJoinInPairedJoiner {
		// allCols needs to include the continuation column since it will be
		// in the result output by this join.
		allCols = allExprCols.Copy()
		maxValue, ok := allCols.MaxValue()
		if !ok {
			return execPlan{}, errors.AssertionFailedf("allCols should not be empty")
		}
		// Assign the continuation column the next unused value in the map.
		allCols.Set(int(join.ContinuationCol), maxValue+1)
	}
	res := execPlan{outputCols: allCols}
	if join.JoinType == opt.SemiJoinOp || join.JoinType == opt.AntiJoinOp {
		// For semi and anti join, only the left columns are output.
		res.outputCols = input.outputCols
	}

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, allExprCols.Len()),
		ivarMap: allExprCols,
	}
	var lookupExpr, remoteLookupExpr tree.TypedExpr
	if len(join.LookupExpr) > 0 {
		var err error
		lookupExpr, err = b.buildScalar(&ctx, &join.LookupExpr)
		if err != nil {
			return execPlan{}, err
		}
	}
	if len(join.RemoteLookupExpr) > 0 {
		var err error
		remoteLookupExpr, err = b.buildScalar(&ctx, &join.RemoteLookupExpr)
		if err != nil {
			return execPlan{}, err
		}
	}
	var onExpr tree.TypedExpr
	if len(join.On) > 0 {
		var err error
		onExpr, err = b.buildScalar(&ctx, &join.On)
		if err != nil {
			return execPlan{}, err
		}
	}

	tab := md.Table(join.Table)
	idx := tab.Index(join.Index)

	var locking *tree.LockingItem
	if b.forceForUpdateLocking {
		locking = forUpdateLocking
	}

	res.root, err = b.factory.ConstructLookupJoin(
		joinOpToJoinType(join.JoinType),
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
		res.reqOrdering(join),
		locking,
	)
	if err != nil {
		return execPlan{}, err
	}

	// Apply a post-projection if Cols doesn't contain all input columns.
	//
	// NB: For paired-joins, this is where the continuation column and the PK
	// columns for the right side, which were part of the inputCols, are projected
	// away.
	if !inputCols.SubsetOf(join.Cols) {
		outCols := join.Cols
		if join.JoinType == opt.SemiJoinOp || join.JoinType == opt.AntiJoinOp {
			outCols = join.Cols.Intersection(inputCols)
		}
		return b.applySimpleProject(res, outCols, join.ProvidedPhysical().Ordering)
	}
	return res, nil
}

func (b *Builder) buildInvertedJoin(join *memo.InvertedJoinExpr) (execPlan, error) {
	input, err := b.buildRelational(join.Input)
	if err != nil {
		return execPlan{}, err
	}

	md := b.mem.Metadata()
	tab := md.Table(join.Table)
	idx := tab.Index(join.Index)

	prefixEqCols := make([]exec.NodeColumnOrdinal, len(join.PrefixKeyCols))
	for i, c := range join.PrefixKeyCols {
		prefixEqCols[i] = input.getNodeColumnOrdinal(c)
	}

	inputCols := join.Input.Relational().OutputCols
	lookupCols := join.Cols.Difference(inputCols)
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
	// allExprCols are the columns used in expressions evaluated by this join.
	allExprCols := joinOutputMap(input.outputCols, lookupColMap)

	allCols := allExprCols
	if join.IsFirstJoinInPairedJoiner {
		// allCols needs to include the continuation column since it will be
		// in the result output by this join.
		allCols = allExprCols.Copy()
		maxValue, ok := allCols.MaxValue()
		if !ok {
			return execPlan{}, errors.AssertionFailedf("allCols should not be empty")
		}
		// Assign the continuation column the next unused value in the map.
		allCols.Set(int(join.ContinuationCol), maxValue+1)
	}
	res := execPlan{outputCols: allCols}
	if join.JoinType == opt.SemiJoinOp || join.JoinType == opt.AntiJoinOp {
		// For semi and anti join, only the left columns are output.
		res.outputCols = input.outputCols
	}

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, allExprCols.Len()),
		ivarMap: allExprCols.Copy(),
	}
	onExpr, err := b.buildScalar(&ctx, &join.On)
	if err != nil {
		return execPlan{}, err
	}

	// The inverted filter refers to the inverted column's source column, but it
	// is actually evaluated implicitly using the inverted column; the inverted
	// column's source column is not even accessible here.
	//
	// TODO(radu): this is sketchy. The inverted column should not even have the
	// geospatial type (which would make the expression invalid in terms of
	// typing). Perhaps we need to pass this information in a more specific way
	// and not as a generic expression?
	ord, _ := ctx.ivarMap.Get(int(invertedColID))
	ctx.ivarMap.Set(int(join.Table.ColumnID(invertedColumn.InvertedSourceColumnOrdinal())), ord)
	invertedExpr, err := b.buildScalar(&ctx, join.InvertedExpr)
	if err != nil {
		return execPlan{}, err
	}

	res.root, err = b.factory.ConstructInvertedJoin(
		joinOpToJoinType(join.JoinType),
		invertedExpr,
		input.root,
		tab,
		idx,
		prefixEqCols,
		lookupOrdinals,
		onExpr,
		join.IsFirstJoinInPairedJoiner,
		res.reqOrdering(join),
	)
	if err != nil {
		return execPlan{}, err
	}

	// Apply a post-projection to remove the inverted column.
	return b.applySimpleProject(res, join.Cols, join.ProvidedPhysical().Ordering)
}

func (b *Builder) buildZigzagJoin(join *memo.ZigzagJoinExpr) (execPlan, error) {
	md := b.mem.Metadata()

	leftTable := md.Table(join.LeftTable)
	rightTable := md.Table(join.RightTable)
	leftIndex := leftTable.Index(join.LeftIndex)
	rightIndex := rightTable.Index(join.RightIndex)

	leftEqCols := make([]exec.TableColumnOrdinal, len(join.LeftEqCols))
	rightEqCols := make([]exec.TableColumnOrdinal, len(join.RightEqCols))
	for i := range join.LeftEqCols {
		leftEqCols[i] = exec.TableColumnOrdinal(join.LeftTable.ColumnOrdinal(join.LeftEqCols[i]))
		rightEqCols[i] = exec.TableColumnOrdinal(join.RightTable.ColumnOrdinal(join.RightEqCols[i]))
	}
	leftCols := md.TableMeta(join.LeftTable).IndexColumns(join.LeftIndex).Intersection(join.Cols)
	rightCols := md.TableMeta(join.RightTable).IndexColumns(join.RightIndex).Intersection(join.Cols)
	// Remove duplicate columns, if any.
	rightCols.DifferenceWith(leftCols)

	leftOrdinals, leftColMap := b.getColumns(leftCols, join.LeftTable)
	rightOrdinals, rightColMap := b.getColumns(rightCols, join.RightTable)

	allCols := joinOutputMap(leftColMap, rightColMap)

	res := execPlan{outputCols: allCols}

	ctx := buildScalarCtx{
		ivh:     tree.MakeIndexedVarHelper(nil /* container */, leftColMap.Len()+rightColMap.Len()),
		ivarMap: allCols,
	}
	onExpr, err := b.buildScalar(&ctx, &join.On)
	if err != nil {
		return execPlan{}, err
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
		return execPlan{}, err
	}
	rightFixedVals, err := tupleToExprs(join.FixedVals[1].(*memo.TupleExpr))
	if err != nil {
		return execPlan{}, err
	}

	res.root, err = b.factory.ConstructZigzagJoin(
		leftTable,
		leftIndex,
		leftOrdinals,
		leftFixedVals,
		leftEqCols,
		rightTable,
		rightIndex,
		rightOrdinals,
		rightFixedVals,
		rightEqCols,
		onExpr,
		res.reqOrdering(join),
	)
	if err != nil {
		return execPlan{}, err
	}

	return res, nil
}

func (b *Builder) buildMax1Row(max1Row *memo.Max1RowExpr) (execPlan, error) {
	input, err := b.buildRelational(max1Row.Input)
	if err != nil {
		return execPlan{}, err
	}

	node, err := b.factory.ConstructMax1Row(input.root, max1Row.ErrorText)
	if err != nil {
		return execPlan{}, err
	}
	return execPlan{root: node, outputCols: input.outputCols}, nil
}

func (b *Builder) buildWith(with *memo.WithExpr) (execPlan, error) {
	value, err := b.buildRelational(with.Binding)
	if err != nil {
		return execPlan{}, err
	}

	var label bytes.Buffer
	fmt.Fprintf(&label, "buffer %d", with.ID)
	if with.Name != "" {
		fmt.Fprintf(&label, " (%s)", with.Name)
	}

	buffer, err := b.factory.ConstructBuffer(value.root, label.String())
	if err != nil {
		return execPlan{}, err
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
		RowCount: int64(with.Relational().Stats.RowCountIfAvailable()),
	})

	b.addBuiltWithExpr(with.ID, value.outputCols, buffer)

	return b.buildRelational(with.Main)
}

func (b *Builder) buildRecursiveCTE(rec *memo.RecursiveCTEExpr) (execPlan, error) {
	initial, err := b.buildRelational(rec.Initial)
	if err != nil {
		return execPlan{}, err
	}

	// Make sure we have the columns in the correct order.
	initial, err = b.ensureColumns(initial, rec.InitialCols, nil /* ordering */)
	if err != nil {
		return execPlan{}, err
	}

	// Renumber the columns so they match the columns expected by the recursive
	// query.
	initial.outputCols = util.FastIntMap{}
	for i, col := range rec.OutCols {
		initial.outputCols.Set(int(col), i)
	}

	// To implement exec.RecursiveCTEIterationFn, we create a special Builder.

	innerBldTemplate := &Builder{
		mem:     b.mem,
		catalog: b.catalog,
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
		innerBld.addBuiltWithExpr(rec.WithID, initial.outputCols, bufferRef)
		plan, err := innerBld.build(rec.Recursive)
		if err != nil {
			return nil, err
		}
		// Ensure columns are output in the same order.
		plan, err = innerBld.ensureColumns(plan, rec.RecursiveCols, opt.Ordering{})
		if err != nil {
			return nil, err
		}
		rootRowCount := int64(rec.Recursive.Relational().Stats.RowCountIfAvailable())
		return innerBld.factory.ConstructPlan(plan.root, innerBld.subqueries, innerBld.cascades, innerBld.checks, rootRowCount)
	}

	label := fmt.Sprintf("working buffer (%s)", rec.Name)
	var ep execPlan
	ep.root, err = b.factory.ConstructRecursiveCTE(initial.root, fn, label, rec.Deduplicate)
	if err != nil {
		return execPlan{}, err
	}
	for i, col := range rec.OutCols {
		ep.outputCols.Set(int(col), i)
	}
	return ep, nil
}

func (b *Builder) buildWithScan(withScan *memo.WithScanExpr) (execPlan, error) {
	e := b.findBuiltWithExpr(withScan.With)
	if e == nil {
		return execPlan{}, errors.AssertionFailedf(
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
		return execPlan{}, err
	}
	res := execPlan{root: node, outputCols: e.outputCols}

	// Apply any necessary projection to produce the InCols in the given order.
	res, err = b.ensureColumns(res, withScan.InCols, withScan.ProvidedPhysical().Ordering)
	if err != nil {
		return execPlan{}, err
	}

	// Renumber the columns.
	res.outputCols = opt.ColMap{}
	for i, col := range withScan.OutCols {
		res.outputCols.Set(int(col), i)
	}

	return res, nil
}

func (b *Builder) buildProjectSet(projectSet *memo.ProjectSetExpr) (execPlan, error) {
	input, err := b.buildRelational(projectSet.Input)
	if err != nil {
		return execPlan{}, err
	}

	zip := projectSet.Zip
	md := b.mem.Metadata()
	scalarCtx := input.makeBuildScalarCtx()

	exprs := make(tree.TypedExprs, len(zip))
	zipCols := make(colinfo.ResultColumns, 0, len(zip))
	numColsPerGen := make([]int, len(zip))

	ep := execPlan{outputCols: input.outputCols}
	n := ep.numOutputCols()

	for i := range zip {
		item := &zip[i]
		exprs[i], err = b.buildScalar(&scalarCtx, item.Fn)
		if err != nil {
			return execPlan{}, err
		}

		for _, col := range item.Cols {
			colMeta := md.ColumnMeta(col)
			zipCols = append(zipCols, colinfo.ResultColumn{Name: colMeta.Alias, Typ: colMeta.Type})

			ep.outputCols.Set(int(col), n)
			n++
		}

		numColsPerGen[i] = len(item.Cols)
	}

	ep.root, err = b.factory.ConstructProjectSet(input.root, exprs, zipCols, numColsPerGen)
	if err != nil {
		return execPlan{}, err
	}

	return ep, nil
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

func (b *Builder) buildFrame(input execPlan, w *memo.WindowsItem) (*tree.WindowFrame, error) {
	scalarCtx := input.makeBuildScalarCtx()
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
			panic(errors.AssertionFailedf("expected offset to only be present in offset mode"))
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
			panic(errors.AssertionFailedf("expected offset to only be present in offset mode"))
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

func (b *Builder) buildWindow(w *memo.WindowExpr) (execPlan, error) {
	input, err := b.buildRelational(w.Input)
	if err != nil {
		return execPlan{}, err
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
	input, err = b.ensureColumns(input, desiredCols, opt.Ordering{})
	if err != nil {
		return execPlan{}, err
	}

	ctx := input.makeBuildScalarCtx()

	ord := w.Ordering.ToOrdering()

	orderingExprs := make(tree.OrderBy, len(ord))
	for i, c := range ord {
		direction := tree.Ascending
		if c.Descending() {
			direction = tree.Descending
		}
		orderingExprs[i] = &tree.Order{
			Expr:      b.indexedVar(&ctx, b.mem.Metadata(), c.ID()),
			Direction: direction,
		}
	}

	partitionIdxs := make([]exec.NodeColumnOrdinal, w.Partition.Len())
	partitionExprs := make(tree.Exprs, w.Partition.Len())

	i := 0
	w.Partition.ForEach(func(col opt.ColumnID) {
		ordinal, _ := input.outputCols.Get(int(col))
		partitionIdxs[i] = exec.NodeColumnOrdinal(ordinal)
		partitionExprs[i] = b.indexedVar(&ctx, b.mem.Metadata(), col)
		i++
	})

	argIdxs := make([][]exec.NodeColumnOrdinal, len(w.Windows))
	filterIdxs := make([]int, len(w.Windows))
	exprs := make([]*tree.FuncExpr, len(w.Windows))

	for i := range w.Windows {
		item := &w.Windows[i]
		fn := b.extractWindowFunction(item.Function)
		name, overload := memo.FindWindowOverload(fn)
		if !b.disableTelemetry {
			telemetry.Inc(sqltelemetry.WindowFunctionCounter(name))
		}
		props, _ := builtins.GetBuiltinProperties(name)

		args := make([]tree.TypedExpr, fn.ChildCount())
		argIdxs[i] = make([]exec.NodeColumnOrdinal, fn.ChildCount())
		for j, n := 0, fn.ChildCount(); j < n; j++ {
			col := fn.Child(j).(*memo.VariableExpr).Col
			args[j] = b.indexedVar(&ctx, b.mem.Metadata(), col)
			idx, _ := input.outputCols.Get(int(col))
			argIdxs[i][j] = exec.NodeColumnOrdinal(idx)
		}

		frame, err := b.buildFrame(input, item)
		if err != nil {
			return execPlan{}, err
		}

		var builtFilter tree.TypedExpr
		filter, ok := b.extractFilter(item.Function)
		if ok {
			f, ok := filter.(*memo.VariableExpr)
			if !ok {
				panic(errors.AssertionFailedf("expected FILTER expression to be a VariableExpr"))
			}
			filterIdxs[i], _ = input.outputCols.Get(int(f.Col))

			builtFilter, err = b.buildScalar(&ctx, filter)
			if err != nil {
				return execPlan{}, err
			}
		} else {
			filterIdxs[i] = -1
		}

		exprs[i] = tree.NewTypedFuncExpr(
			tree.WrapFunction(name),
			0,
			args,
			builtFilter,
			&tree.WindowDef{
				Partitions: partitionExprs,
				OrderBy:    orderingExprs,
				Frame:      frame,
			},
			overload.FixedReturnType(),
			props,
			overload,
		)
	}

	resultCols := make(colinfo.ResultColumns, w.Relational().OutputCols.Len())

	// All the passthrough cols will keep their ordinal index.
	passthrough.ForEach(func(col opt.ColumnID) {
		ordinal, _ := input.outputCols.Get(int(col))
		resultCols[ordinal] = b.resultColumn(col)
	})

	var outputCols opt.ColMap
	input.outputCols.ForEach(func(key, val int) {
		if passthrough.Contains(opt.ColumnID(key)) {
			outputCols.Set(key, val)
		}
	})

	outputIdxs := make([]int, len(w.Windows))

	// Because of the way we arranged the input columns, we will be outputting
	// the window columns at the end (which is exactly what the execution engine
	// will do as well).
	windowStart := passthrough.Len()
	for i := range w.Windows {
		resultCols[windowStart+i] = b.resultColumn(w.Windows[i].Col)
		outputCols.Set(int(w.Windows[i].Col), windowStart+i)
		outputIdxs[i] = windowStart + i
	}

	node, err := b.factory.ConstructWindow(input.root, exec.WindowInfo{
		Cols:       resultCols,
		Exprs:      exprs,
		OutputIdxs: outputIdxs,
		ArgIdxs:    argIdxs,
		FilterIdxs: filterIdxs,
		Partition:  partitionIdxs,
		Ordering:   input.sqlOrdering(ord),
	})
	if err != nil {
		return execPlan{}, err
	}

	return execPlan{
		root:       node,
		outputCols: outputCols,
	}, nil
}

func (b *Builder) buildSequenceSelect(seqSel *memo.SequenceSelectExpr) (execPlan, error) {
	seq := b.mem.Metadata().Sequence(seqSel.Sequence)
	node, err := b.factory.ConstructSequenceSelect(seq)
	if err != nil {
		return execPlan{}, err
	}

	ep := execPlan{root: node}
	for i, c := range seqSel.Cols {
		ep.outputCols.Set(int(c), i)
	}

	return ep, nil
}

func (b *Builder) applySaveTable(
	input execPlan, e memo.RelExpr, saveTableName string,
) (execPlan, error) {
	name := tree.NewTableNameWithSchema(tree.Name(opt.SaveTablesDatabase), tree.PublicSchemaName, tree.Name(saveTableName))

	// Ensure that the column names are unique and match the names used by the
	// opttester.
	outputCols := e.Relational().OutputCols
	colNames := make([]string, outputCols.Len())
	colNameGen := memo.NewColumnNameGenerator(e)
	for col, ok := outputCols.Next(0); ok; col, ok = outputCols.Next(col + 1) {
		ord, _ := input.outputCols.Get(int(col))
		colNames[ord] = colNameGen.GenerateName(col)
	}

	var err error
	input.root, err = b.factory.ConstructSaveTable(input.root, name, colNames)
	if err != nil {
		return execPlan{}, err
	}
	return input, err
}

func (b *Builder) buildOpaque(opaque *memo.OpaqueRelPrivate) (execPlan, error) {
	node, err := b.factory.ConstructOpaque(opaque.Metadata)
	if err != nil {
		return execPlan{}, err
	}

	ep := execPlan{root: node}
	for i, c := range opaque.Columns {
		ep.outputCols.Set(int(c), i)
	}

	return ep, nil
}

// needProjection figures out what projection is needed on top of the input plan
// to produce the given list of columns. If the input plan already produces
// the columns (in the same order), returns needProj=false.
func (b *Builder) needProjection(
	input execPlan, colList opt.ColList,
) (_ []exec.NodeColumnOrdinal, needProj bool) {
	if input.numOutputCols() == len(colList) {
		identity := true
		for i, col := range colList {
			if ord, ok := input.outputCols.Get(int(col)); !ok || ord != i {
				identity = false
				break
			}
		}
		if identity {
			return nil, false
		}
	}
	cols := make([]exec.NodeColumnOrdinal, 0, len(colList))
	for _, col := range colList {
		if col != 0 {
			cols = append(cols, input.getNodeColumnOrdinal(col))
		}
	}
	return cols, true
}

// ensureColumns applies a projection as necessary to make the output match the
// given list of columns; colNames is optional.
func (b *Builder) ensureColumns(
	input execPlan, colList opt.ColList, provided opt.Ordering,
) (execPlan, error) {
	cols, needProj := b.needProjection(input, colList)
	if !needProj {
		return input, nil
	}
	var res execPlan
	for i, col := range colList {
		res.outputCols.Set(int(col), i)
	}
	reqOrdering := exec.OutputOrdering(res.sqlOrdering(provided))
	var err error
	res.root, err = b.factory.ConstructSimpleProject(input.root, cols, reqOrdering)
	return res, err
}

// applyPresentation adds a projection to a plan to satisfy a required
// Presentation property.
func (b *Builder) applyPresentation(input execPlan, pres physical.Presentation) (execPlan, error) {
	cols := make([]exec.NodeColumnOrdinal, len(pres))
	colNames := make([]string, len(pres))
	var res execPlan
	for i := range pres {
		cols[i] = input.getNodeColumnOrdinal(pres[i].ID)
		res.outputCols.Set(int(pres[i].ID), i)
		colNames[i] = pres[i].Alias
	}
	var err error
	res.root, err = b.factory.ConstructSerializingProject(input.root, cols, colNames)
	return res, err
}

// getEnvData consolidates the information that must be presented in
// EXPLAIN (opt, env).
func (b *Builder) getEnvData() exec.ExplainEnvData {
	envOpts := exec.ExplainEnvData{ShowEnv: true}
	var err error
	envOpts.Tables, envOpts.Sequences, envOpts.Views, err = b.mem.Metadata().AllDataSourceNames(
		func(ds cat.DataSource) (cat.DataSourceName, error) {
			return b.catalog.FullyQualifiedName(context.TODO(), ds)
		},
	)
	if err != nil {
		panic(err)
	}

	return envOpts
}

// statementTag returns a string that can be used in an error message regarding
// the given expression.
func (b *Builder) statementTag(expr memo.RelExpr) string {
	switch expr.Op() {
	case opt.OpaqueRelOp, opt.OpaqueMutationOp, opt.OpaqueDDLOp:
		return expr.Private().(*memo.OpaqueRelPrivate).Metadata.String()

	default:
		return expr.Op().SyntaxTag()
	}
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
