// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bench

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// stubFactory is a do-nothing implementation of exec.Factory, used for testing.
type stubFactory struct{}

var _ exec.Factory = &stubFactory{}

func (f *stubFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScan(
	table cat.Table,
	index cat.Index,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	invertedConstraint invertedexpr.InvertedSpans,
	hardLimit int64,
	softLimit int64,
	reverse bool,
	maxResults uint64,
	reqOrdering exec.OutputOrdering,
	rowCount float64,
	locking *tree.LockingItem,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructInvertedFilter(
	n exec.Node, invFilter *invertedexpr.SpanExpression, invColumn exec.NodeColumnOrdinal,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructRender(
	n exec.Node,
	columns sqlbase.ResultColumns,
	exprs tree.TypedExprs,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left exec.Node,
	rightColumns sqlbase.ResultColumns,
	onCond tree.TypedExpr,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.NodeColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDistinct(
	input exec.Node,
	distinctCols, orderedCols exec.NodeColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
	nullsAreDistinct bool,
	errorOnDup string,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructOrdinality(input exec.Node, colName string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructIndexJoin(
	input exec.Node,
	table cat.Table,
	keyCols []exec.NodeColumnOrdinal,
	tableCols exec.TableColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructLookupJoin(
	joinType sqlbase.JoinType,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	eqCols []exec.NodeColumnOrdinal,
	eqColsAreKey bool,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructInvertedJoin(
	joinType sqlbase.JoinType,
	invertedExpr tree.TypedExpr,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	inputCol exec.NodeColumnOrdinal,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructZigzagJoin(
	leftTable cat.Table,
	leftIndex cat.Index,
	rightTable cat.Table,
	rightIndex cat.Index,
	leftEqCols []exec.NodeColumnOrdinal,
	rightEqCols []exec.NodeColumnOrdinal,
	leftCols exec.NodeColumnOrdinalSet,
	rightCols exec.NodeColumnOrdinalSet,
	onCond tree.TypedExpr,
	fixedVals []exec.Node,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructLimit(
	input exec.Node, limit, offset tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructMax1Row(input exec.Node, errorText string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructWindow(n exec.Node, wi exec.WindowInfo) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) RenameColumns(input exec.Node, colNames []string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, cascades []exec.Cascade, checks []exec.Node,
) (exec.Plan, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructExplainOpt(
	plan string, envOpts exec.ExplainEnvData,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructExplain(
	options *tree.ExplainOptions, stmtType tree.StatementType, plan exec.Plan,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	allowAutoCommit bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructInsertFastPath(
	rows [][]tree.TypedExpr,
	table cat.Table,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	fkChecks []exec.InsertFastPathFKCheck,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructUpdate(
	input exec.Node,
	table cat.Table,
	fetchCols exec.TableColumnOrdinalSet,
	updateCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	passthrough sqlbase.ResultColumns,
	allowAutoCommit bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	canaryCol exec.NodeColumnOrdinal,
	insertCols exec.TableColumnOrdinalSet,
	fetchCols exec.TableColumnOrdinalSet,
	updateCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	allowAutoCommit bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	allowAutoCommit bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructDeleteRange(
	table cat.Table,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	interleavedTables []cat.Table,
	maxReturnedKeys int,
	allowAutoCommit bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSequenceSelect(seq cat.Sequence) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructSaveTable(
	input exec.Node, table *cat.DataSourceName, colNames []string,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructErrorIfRows(
	input exec.Node, mkErr func(tree.Datums) error,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructAlterTableUnsplit(
	index cat.Index, input exec.Node,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructAlterTableUnsplitAll(index cat.Index) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructBuffer(value exec.Node, label string) (exec.BufferNode, error) {
	return struct{ exec.BufferNode }{}, nil
}

func (f *stubFactory) ConstructScanBuffer(ref exec.BufferNode, label string) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCancelQueries(input exec.Node, ifExists bool) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCancelSessions(input exec.Node, ifExists bool) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructCreateView(
	schema cat.Schema,
	viewName string,
	ifNotExists bool,
	replace bool,
	temporary bool,
	viewQuery string,
	columns sqlbase.ResultColumns,
	deps opt.ViewDeps,
) (exec.Node, error) {
	return struct{}{}, nil
}

func (f *stubFactory) ConstructExport(
	input exec.Node, fileName tree.TypedExpr, fileFormat string, options []exec.KVOption,
) (exec.Node, error) {
	return struct{}{}, nil
}
