// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// StubFactory is a do-nothing implementation of Factory, used for testing.
type StubFactory struct{}

var _ Factory = StubFactory{}

// ConstructValues is part of the exec.Factory interface.
func (StubFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructScan is part of the exec.Factory interface.
func (StubFactory) ConstructScan(
	table cat.Table,
	index cat.Index,
	needed TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	invertedConstraint invertedexpr.InvertedSpans,
	hardLimit int64,
	softLimit int64,
	reverse bool,
	parallelize bool,
	reqOrdering OutputOrdering,
	rowCount float64,
	locking *tree.LockingItem,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructFilter is part of the exec.Factory interface.
func (StubFactory) ConstructFilter(
	n Node, filter tree.TypedExpr, reqOrdering OutputOrdering,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructInvertedFilter is part of the exec.Factory interface.
func (StubFactory) ConstructInvertedFilter(
	n Node, invFilter *invertedexpr.SpanExpression, invColumn NodeColumnOrdinal,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructSimpleProject is part of the exec.Factory interface.
func (StubFactory) ConstructSimpleProject(
	n Node, cols []NodeColumnOrdinal, colNames []string, reqOrdering OutputOrdering,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructRender is part of the exec.Factory interface.
func (StubFactory) ConstructRender(
	n Node, columns sqlbase.ResultColumns, exprs tree.TypedExprs, reqOrdering OutputOrdering,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructHashJoin is part of the exec.Factory interface.
func (StubFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right Node,
	leftEqCols, rightEqCols []NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructApplyJoin is part of the exec.Factory interface.
func (StubFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left Node,
	rightColumns sqlbase.ResultColumns,
	onCond tree.TypedExpr,
	planRightSideFn ApplyJoinPlanRightSideFn,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructMergeJoin is part of the exec.Factory interface.
func (StubFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructGroupBy is part of the exec.Factory interface.
func (StubFactory) ConstructGroupBy(
	input Node,
	groupCols []NodeColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	aggregations []AggInfo,
	reqOrdering OutputOrdering,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructScalarGroupBy is part of the exec.Factory interface.
func (StubFactory) ConstructScalarGroupBy(input Node, aggregations []AggInfo) (Node, error) {
	return struct{}{}, nil
}

// ConstructDistinct is part of the exec.Factory interface.
func (StubFactory) ConstructDistinct(
	input Node,
	distinctCols, orderedCols NodeColumnOrdinalSet,
	reqOrdering OutputOrdering,
	nullsAreDistinct bool,
	errorOnDup string,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructSetOp is part of the exec.Factory interface.
func (StubFactory) ConstructSetOp(typ tree.UnionType, all bool, left, right Node) (Node, error) {
	return struct{}{}, nil
}

// ConstructSort is part of the exec.Factory interface.
func (StubFactory) ConstructSort(
	input Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructOrdinality is part of the exec.Factory interface.
func (StubFactory) ConstructOrdinality(input Node, colName string) (Node, error) {
	return struct{}{}, nil
}

// ConstructIndexJoin is part of the exec.Factory interface.
func (StubFactory) ConstructIndexJoin(
	input Node,
	table cat.Table,
	keyCols []NodeColumnOrdinal,
	tableCols TableColumnOrdinalSet,
	reqOrdering OutputOrdering,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructLookupJoin is part of the exec.Factory interface.
func (StubFactory) ConstructLookupJoin(
	joinType sqlbase.JoinType,
	input Node,
	table cat.Table,
	index cat.Index,
	eqCols []NodeColumnOrdinal,
	eqColsAreKey bool,
	lookupCols TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering OutputOrdering,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructInvertedJoin is part of the exec.Factory interface.
func (StubFactory) ConstructInvertedJoin(
	joinType sqlbase.JoinType,
	invertedExpr tree.TypedExpr,
	input Node,
	table cat.Table,
	index cat.Index,
	inputCol NodeColumnOrdinal,
	lookupCols TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering OutputOrdering,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructZigzagJoin is part of the exec.Factory interface.
func (StubFactory) ConstructZigzagJoin(
	leftTable cat.Table,
	leftIndex cat.Index,
	rightTable cat.Table,
	rightIndex cat.Index,
	leftEqCols []NodeColumnOrdinal,
	rightEqCols []NodeColumnOrdinal,
	leftCols NodeColumnOrdinalSet,
	rightCols NodeColumnOrdinalSet,
	onCond tree.TypedExpr,
	fixedVals []Node,
	reqOrdering OutputOrdering,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructLimit is part of the exec.Factory interface.
func (StubFactory) ConstructLimit(input Node, limit, offset tree.TypedExpr) (Node, error) {
	return struct{}{}, nil
}

// ConstructMax1Row is part of the exec.Factory interface.
func (StubFactory) ConstructMax1Row(input Node, errorText string) (Node, error) {
	return struct{}{}, nil
}

// ConstructProjectSet is part of the exec.Factory interface.
func (StubFactory) ConstructProjectSet(
	n Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructWindow is part of the exec.Factory interface.
func (StubFactory) ConstructWindow(n Node, wi WindowInfo) (Node, error) {
	return struct{}{}, nil
}

// RenameColumns is part of the exec.Factory interface.
func (StubFactory) RenameColumns(input Node, colNames []string) (Node, error) {
	return struct{}{}, nil
}

// ConstructPlan is part of the exec.Factory interface.
func (StubFactory) ConstructPlan(
	root Node, subqueries []Subquery, cascades []Cascade, checks []Node,
) (Plan, error) {
	return struct{}{}, nil
}

// ConstructExplainOpt is part of the exec.Factory interface.
func (StubFactory) ConstructExplainOpt(plan string, envOpts ExplainEnvData) (Node, error) {
	return struct{}{}, nil
}

// ConstructExplain is part of the exec.Factory interface.
func (StubFactory) ConstructExplain(
	options *tree.ExplainOptions, stmtType tree.StatementType, plan Plan,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructShowTrace is part of the exec.Factory interface.
func (StubFactory) ConstructShowTrace(typ tree.ShowTraceType, compact bool) (Node, error) {
	return struct{}{}, nil
}

// ConstructInsert is part of the exec.Factory interface.
func (StubFactory) ConstructInsert(
	input Node,
	table cat.Table,
	insertCols TableColumnOrdinalSet,
	returnCols TableColumnOrdinalSet,
	checks CheckOrdinalSet,
	allowAutoCommit bool,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructInsertFastPath is part of the exec.Factory interface.
func (StubFactory) ConstructInsertFastPath(
	rows [][]tree.TypedExpr,
	table cat.Table,
	insertCols TableColumnOrdinalSet,
	returnCols TableColumnOrdinalSet,
	checkCols CheckOrdinalSet,
	fkChecks []InsertFastPathFKCheck,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructUpdate is part of the exec.Factory interface.
func (StubFactory) ConstructUpdate(
	input Node,
	table cat.Table,
	fetchCols TableColumnOrdinalSet,
	updateCols TableColumnOrdinalSet,
	returnCols TableColumnOrdinalSet,
	checks CheckOrdinalSet,
	passthrough sqlbase.ResultColumns,
	allowAutoCommit bool,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructUpsert is part of the exec.Factory interface.
func (StubFactory) ConstructUpsert(
	input Node,
	table cat.Table,
	canaryCol NodeColumnOrdinal,
	insertCols TableColumnOrdinalSet,
	fetchCols TableColumnOrdinalSet,
	updateCols TableColumnOrdinalSet,
	returnCols TableColumnOrdinalSet,
	checks CheckOrdinalSet,
	allowAutoCommit bool,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructDelete is part of the exec.Factory interface.
func (StubFactory) ConstructDelete(
	input Node,
	table cat.Table,
	fetchCols TableColumnOrdinalSet,
	returnCols TableColumnOrdinalSet,
	allowAutoCommit bool,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructDeleteRange is part of the exec.Factory interface.
func (StubFactory) ConstructDeleteRange(
	table cat.Table,
	needed TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	interleavedTables []cat.Table,
	maxReturnedKeys int,
	allowAutoCommit bool,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructCreateTable is part of the exec.Factory interface.
func (StubFactory) ConstructCreateTable(
	input Node, schema cat.Schema, ct *tree.CreateTable,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructSequenceSelect is part of the exec.Factory interface.
func (StubFactory) ConstructSequenceSelect(seq cat.Sequence) (Node, error) {
	return struct{}{}, nil
}

// ConstructSaveTable is part of the exec.Factory interface.
func (StubFactory) ConstructSaveTable(
	input Node, table *cat.DataSourceName, colNames []string,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructErrorIfRows is part of the exec.Factory interface.
func (StubFactory) ConstructErrorIfRows(input Node, mkErr func(tree.Datums) error) (Node, error) {
	return struct{}{}, nil
}

// ConstructOpaque is part of the exec.Factory interface.
func (StubFactory) ConstructOpaque(metadata opt.OpaqueMetadata) (Node, error) {
	return struct{}{}, nil
}

// ConstructAlterTableSplit is part of the exec.Factory interface.
func (StubFactory) ConstructAlterTableSplit(
	index cat.Index, input Node, expiration tree.TypedExpr,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructAlterTableUnsplit is part of the exec.Factory interface.
func (StubFactory) ConstructAlterTableUnsplit(index cat.Index, input Node) (Node, error) {
	return struct{}{}, nil
}

// ConstructAlterTableUnsplitAll is part of the exec.Factory interface.
func (StubFactory) ConstructAlterTableUnsplitAll(index cat.Index) (Node, error) {
	return struct{}{}, nil
}

// ConstructAlterTableRelocate is part of the exec.Factory interface.
func (StubFactory) ConstructAlterTableRelocate(
	index cat.Index, input Node, relocateLease bool,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructBuffer is part of the exec.Factory interface.
func (StubFactory) ConstructBuffer(value Node, label string) (BufferNode, error) {
	return struct{ BufferNode }{}, nil
}

// ConstructScanBuffer is part of the exec.Factory interface.
func (StubFactory) ConstructScanBuffer(ref BufferNode, label string) (Node, error) {
	return struct{}{}, nil
}

// ConstructRecursiveCTE is part of the exec.Factory interface.
func (StubFactory) ConstructRecursiveCTE(
	initial Node, fn RecursiveCTEIterationFn, label string,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructControlJobs is part of the exec.Factory interface.
func (StubFactory) ConstructControlJobs(command tree.JobCommand, input Node) (Node, error) {
	return struct{}{}, nil
}

// ConstructCancelQueries is part of the exec.Factory interface.
func (StubFactory) ConstructCancelQueries(input Node, ifExists bool) (Node, error) {
	return struct{}{}, nil
}

// ConstructCancelSessions is part of the exec.Factory interface.
func (StubFactory) ConstructCancelSessions(input Node, ifExists bool) (Node, error) {
	return struct{}{}, nil
}

// ConstructCreateView is part of the exec.Factory interface.
func (StubFactory) ConstructCreateView(
	schema cat.Schema,
	viewName string,
	ifNotExists bool,
	replace bool,
	temporary bool,
	viewQuery string,
	columns sqlbase.ResultColumns,
	deps opt.ViewDeps,
) (Node, error) {
	return struct{}{}, nil
}

// ConstructExport is part of the exec.Factory interface.
func (StubFactory) ConstructExport(
	input Node, fileName tree.TypedExpr, fileFormat string, options []KVOption,
) (Node, error) {
	return struct{}{}, nil
}
