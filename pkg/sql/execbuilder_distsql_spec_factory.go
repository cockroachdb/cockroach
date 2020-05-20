// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type execBuilderDistSQLSpecFactory struct {
	allowAutoCommit bool
}

var _ exec.Factory = &execBuilderDistSQLSpecFactory{}

func makeExecBuilderDistSQLFactory(allowAutoCommit bool) execBuilderDistSQLSpecFactory {
	return execBuilderDistSQLSpecFactory{
		allowAutoCommit: allowAutoCommit,
	}
}

func (e *execBuilderDistSQLSpecFactory) disableAutoCommit() {
	e.allowAutoCommit = false
}

// Remove unused warning.
var _ = (&execBuilderDistSQLSpecFactory{}).disableAutoCommit

func (e *execBuilderDistSQLSpecFactory) ConstructValues(
	rows [][]tree.TypedExpr, cols sqlbase.ResultColumns,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructScan(
	table cat.Table,
	index cat.Index,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	hardLimit int64,
	softLimit int64,
	reverse bool,
	maxResults uint64,
	reqOrdering exec.OutputOrdering,
	rowCount float64,
	locking *tree.LockingItem,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructFilter(
	n exec.Node, filter tree.TypedExpr, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructSimpleProject(
	n exec.Node, cols []exec.NodeColumnOrdinal, colNames []string, reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructRender(
	n exec.Node,
	columns sqlbase.ResultColumns,
	exprs tree.TypedExprs,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructApplyJoin(
	joinType sqlbase.JoinType,
	left exec.Node,
	rightColumns sqlbase.ResultColumns,
	onCond tree.TypedExpr,
	planRightSideFn exec.ApplyJoinPlanRightSideFn,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructHashJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	leftEqCols, rightEqCols []exec.NodeColumnOrdinal,
	leftEqColsAreKey, rightEqColsAreKey bool,
	extraOnCond tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructMergeJoin(
	joinType sqlbase.JoinType,
	left, right exec.Node,
	onCond tree.TypedExpr,
	leftOrdering, rightOrdering sqlbase.ColumnOrdering,
	reqOrdering exec.OutputOrdering,
	leftEqColsAreKey, rightEqColsAreKey bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructGroupBy(
	input exec.Node,
	groupCols []exec.NodeColumnOrdinal,
	groupColOrdering sqlbase.ColumnOrdering,
	aggregations []exec.AggInfo,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructScalarGroupBy(
	input exec.Node, aggregations []exec.AggInfo,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructDistinct(
	input exec.Node,
	distinctCols, orderedCols exec.NodeColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
	nullsAreDistinct bool,
	errorOnDup string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructSetOp(
	typ tree.UnionType, all bool, left, right exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructSort(
	input exec.Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructOrdinality(
	input exec.Node, colName string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructIndexJoin(
	input exec.Node,
	table cat.Table,
	keyCols []exec.NodeColumnOrdinal,
	tableCols exec.TableColumnOrdinalSet,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructLookupJoin(
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
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructGeoLookupJoin(
	joinType sqlbase.JoinType,
	geoRelationshipType geoindex.RelationshipType,
	input exec.Node,
	table cat.Table,
	index cat.Index,
	geoCol exec.NodeColumnOrdinal,
	lookupCols exec.TableColumnOrdinalSet,
	onCond tree.TypedExpr,
	reqOrdering exec.OutputOrdering,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructZigzagJoin(
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
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructLimit(
	input exec.Node, limit, offset tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructMax1Row(
	input exec.Node, errorText string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructProjectSet(
	n exec.Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructWindow(
	input exec.Node, window exec.WindowInfo,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) RenameColumns(
	input exec.Node, colNames []string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructPlan(
	root exec.Node, subqueries []exec.Subquery, cascades []exec.Cascade, checks []exec.Node,
) (exec.Plan, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructExplainOpt(
	plan string, envOpts exec.ExplainEnvData,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructExplain(
	options *tree.ExplainOptions, stmtType tree.StatementType, plan exec.Plan,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructShowTrace(
	typ tree.ShowTraceType, compact bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructInsert(
	input exec.Node,
	table cat.Table,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructInsertFastPath(
	rows [][]tree.TypedExpr,
	table cat.Table,
	insertCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checkCols exec.CheckOrdinalSet,
	fkChecks []exec.InsertFastPathFKCheck,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructUpdate(
	input exec.Node,
	table cat.Table,
	fetchCols exec.TableColumnOrdinalSet,
	updateCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	passthrough sqlbase.ResultColumns,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructUpsert(
	input exec.Node,
	table cat.Table,
	canaryCol exec.NodeColumnOrdinal,
	insertCols exec.TableColumnOrdinalSet,
	fetchCols exec.TableColumnOrdinalSet,
	updateCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	checks exec.CheckOrdinalSet,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructDelete(
	input exec.Node,
	table cat.Table,
	fetchCols exec.TableColumnOrdinalSet,
	returnCols exec.TableColumnOrdinalSet,
	allowAutoCommit bool,
	skipFKChecks bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructDeleteRange(
	table cat.Table,
	needed exec.TableColumnOrdinalSet,
	indexConstraint *constraint.Constraint,
	interleavedTables []cat.Table,
	maxReturnedKeys int,
	allowAutoCommit bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructCreateTable(
	input exec.Node, schema cat.Schema, ct *tree.CreateTable,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructCreateView(
	schema cat.Schema,
	viewName string,
	ifNotExists bool,
	replace bool,
	temporary bool,
	viewQuery string,
	columns sqlbase.ResultColumns,
	deps opt.ViewDeps,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructSequenceSelect(
	sequence cat.Sequence,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructSaveTable(
	input exec.Node, table *cat.DataSourceName, colNames []string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructErrorIfRows(
	input exec.Node, mkErr func(tree.Datums) error,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructOpaque(
	metadata opt.OpaqueMetadata,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructAlterTableSplit(
	index cat.Index, input exec.Node, expiration tree.TypedExpr,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructAlterTableUnsplit(
	index cat.Index, input exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructAlterTableUnsplitAll(
	index cat.Index,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructAlterTableRelocate(
	index cat.Index, input exec.Node, relocateLease bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructBuffer(
	input exec.Node, label string,
) (exec.BufferNode, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructScanBuffer(
	ref exec.BufferNode, label string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructRecursiveCTE(
	initial exec.Node, fn exec.RecursiveCTEIterationFn, label string,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructControlJobs(
	command tree.JobCommand, input exec.Node,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructCancelQueries(
	input exec.Node, ifExists bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructCancelSessions(
	input exec.Node, ifExists bool,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}

func (e *execBuilderDistSQLSpecFactory) ConstructExport(
	input exec.Node, fileName tree.TypedExpr, fileFormat string, options []exec.KVOption,
) (exec.Node, error) {
	return nil, unimplemented.NewWithIssue(47473, "execbuilder-driven distsql spec creation")
}
