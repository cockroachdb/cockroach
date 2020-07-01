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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Node represents a node in the execution tree
// (currently maps to sql.planNode).
type Node interface{}

// BufferNode is a node returned by ConstructBuffer.
type BufferNode interface {
	Node

	BufferNodeMarker()
}

// Plan represents the plan for a query (currently maps to sql.planTop).
// For simple queries, the plan is associated with a single Node tree.
// For queries containing subqueries, the plan is associated with multiple Node
// trees (see ConstructPlan).
type Plan interface{}

// Factory defines the interface for building an execution plan, which consists
// of a tree of execution nodes (currently a sql.planNode tree).
//
// The tree is always built bottom-up. The Construct methods either construct
// leaf nodes, or they take other nodes previously constructed by this same
// factory as children.
//
// The TypedExprs passed to these functions refer to columns of the input node
// via IndexedVars.
type Factory interface {
	// ConstructValues returns a node that outputs the given rows as results.
	ConstructValues(rows [][]tree.TypedExpr, cols sqlbase.ResultColumns) (Node, error)

	// ConstructScan returns a node that represents a scan of the given index on
	// the given table.
	//   - Only the given set of needed columns are part of the result.
	//   - If indexConstraint or invertedConstraint are not nil, the scan is
	//     restricted to the spans in in the constraint. Only one of the two may
	//     be non-nil.
	//   - If hardLimit > 0, then the scan returns only up to hardLimit rows.
	//   - If softLimit > 0, then the scan may be required to return up to all
	//     of its rows (or up to the hardLimit if it is set), but can be optimized
	//     under the assumption that only softLimit rows will be needed.
	//   - If maxResults > 0, the scan is guaranteed to return at most maxResults
	//     rows.
	//   - If locking is provided, the scan should use the specified row-level
	//     locking mode.
	ConstructScan(
		table cat.Table,
		index cat.Index,
		needed TableColumnOrdinalSet,
		indexConstraint *constraint.Constraint,
		invertedConstraint invertedexpr.InvertedSpans,
		hardLimit int64,
		softLimit int64,
		reverse bool,
		maxResults uint64,
		reqOrdering OutputOrdering,
		rowCount float64,
		locking *tree.LockingItem,
	) (Node, error)

	// ConstructFilter returns a node that applies a filter on the results of
	// the given input node.
	ConstructFilter(n Node, filter tree.TypedExpr, reqOrdering OutputOrdering) (Node, error)

	// ConstructInvertedFilter returns a node that applies a span expression on
	// the results of the given input node.
	ConstructInvertedFilter(
		n Node,
		invFilter *invertedexpr.SpanExpression,
		invColumn NodeColumnOrdinal,
	) (Node, error)

	// ConstructSimpleProject returns a node that applies a "simple" projection on the
	// results of the given input node. A simple projection is one that does not
	// involve new expressions; it's just a reshuffling of columns. This is a
	// more efficient version of ConstructRender.
	// The colNames argument is optional; if it is nil, the names of the
	// corresponding input columns are kept.
	ConstructSimpleProject(
		n Node, cols []NodeColumnOrdinal, colNames []string, reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructRender returns a node that applies a projection on the results of
	// the given input node. The projection can contain new expressions. The input
	// expression slice will be modified.
	ConstructRender(
		n Node, columns sqlbase.ResultColumns, exprs tree.TypedExprs, reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructApplyJoin returns a node that runs an apply join between an input
	// node (the left side of the join) and a RelExpr that has outer columns (the
	// right side of the join) by replacing the outer columns of the right side
	// RelExpr with data from each row of the left side of the join according to
	// the data in leftBoundColMap. The apply join can be any kind of join except
	// for right outer and full outer.
	//
	// To plan the right-hand side, planRightSideFn must be called for each left
	// row. This function generates a plan (using the same factory) that produces
	// the rightColumns (in order).
	//
	// onCond is the join condition.
	ConstructApplyJoin(
		joinType sqlbase.JoinType,
		left Node,
		rightColumns sqlbase.ResultColumns,
		onCond tree.TypedExpr,
		planRightSideFn ApplyJoinPlanRightSideFn,
	) (Node, error)

	// ConstructHashJoin returns a node that runs a hash-join between the results
	// of two input nodes.
	//
	// The leftEqColsAreKey/rightEqColsAreKey flags, if set, indicate that the
	// equality columns form a key in the left/right input.
	//
	// The extraOnCond expression can refer to columns from both inputs using
	// IndexedVars (first the left columns, then the right columns).
	ConstructHashJoin(
		joinType sqlbase.JoinType,
		left, right Node,
		leftEqCols, rightEqCols []NodeColumnOrdinal,
		leftEqColsAreKey, rightEqColsAreKey bool,
		extraOnCond tree.TypedExpr,
	) (Node, error)

	// ConstructMergeJoin returns a node that (under distsql) runs a merge join.
	// The ON expression can refer to columns from both inputs using IndexedVars
	// (first the left columns, then the right columns). In addition, the i-th
	// column in leftOrdering is constrained to equal the i-th column in
	// rightOrdering. The directions must match between the two orderings.
	ConstructMergeJoin(
		joinType sqlbase.JoinType,
		left, right Node,
		onCond tree.TypedExpr,
		leftOrdering, rightOrdering sqlbase.ColumnOrdering,
		reqOrdering OutputOrdering,
		leftEqColsAreKey, rightEqColsAreKey bool,
	) (Node, error)

	// ConstructGroupBy returns a node that runs an aggregation. A set of
	// aggregations is performed for each group of values on the groupCols.
	//
	// If the input is guaranteed to have an ordering on grouping columns, a
	// "streaming" aggregation is performed (i.e. aggregation happens separately
	// for each distinct set of values on the set of columns in the ordering).
	ConstructGroupBy(
		input Node,
		groupCols []NodeColumnOrdinal,
		groupColOrdering sqlbase.ColumnOrdering,
		aggregations []AggInfo,
		reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructScalarGroupBy returns a node that runs a scalar aggregation, i.e.
	// one which performs a set of aggregations on all the input rows (as a single
	// group) and has exactly one result row (even when there are no input rows).
	ConstructScalarGroupBy(input Node, aggregations []AggInfo) (Node, error)

	// ConstructDistinct returns a node that filters out rows such that only the
	// first row is kept for each set of values along the distinct columns.
	// The orderedCols are a subset of distinctCols; the input is required to be
	// ordered along these columns (i.e. all rows with the same values on these
	// columns are a contiguous part of the input).
	ConstructDistinct(
		input Node,
		distinctCols, orderedCols NodeColumnOrdinalSet,
		reqOrdering OutputOrdering,
		nullsAreDistinct bool,
		errorOnDup string,
	) (Node, error)

	// ConstructSetOp returns a node that performs a UNION / INTERSECT / EXCEPT
	// operation (either the ALL or the DISTINCT version). The left and right
	// nodes must have the same number of columns.
	ConstructSetOp(typ tree.UnionType, all bool, left, right Node) (Node, error)

	// ConstructSort returns a node that performs a resorting of the rows produced
	// by the input node.
	//
	// When the input is partially sorted we can execute a "segmented" sort. In
	// this case alreadyOrderedPrefix is non-zero and the input is ordered by
	// ordering[:alreadyOrderedPrefix].
	ConstructSort(input Node, ordering sqlbase.ColumnOrdering, alreadyOrderedPrefix int) (Node, error)

	// ConstructOrdinality returns a node that appends an ordinality column to
	// each row in the input node.
	ConstructOrdinality(input Node, colName string) (Node, error)

	// ConstructIndexJoin returns a node that performs an index join. The input
	// contains the primary key (on the columns identified as keyCols).
	//
	// The index join produces the given table columns (in ordinal order).
	ConstructIndexJoin(
		input Node,
		table cat.Table,
		keyCols []NodeColumnOrdinal,
		tableCols TableColumnOrdinalSet,
		reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructLookupJoin returns a node that performs a lookup join.
	// The eqCols are columns from the input used as keys for the columns of the
	// index (or a prefix of them); lookupCols are ordinals for the table columns
	// we are retrieving.
	//
	// The node produces the columns in the input and (unless join type is
	// LeftSemiJoin or LeftAntiJoin) the lookupCols, ordered by ordinal. The ON
	// condition can refer to these using IndexedVars.
	ConstructLookupJoin(
		joinType sqlbase.JoinType,
		input Node,
		table cat.Table,
		index cat.Index,
		eqCols []NodeColumnOrdinal,
		eqColsAreKey bool,
		lookupCols TableColumnOrdinalSet,
		onCond tree.TypedExpr,
		reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructInvertedJoin returns a node that performs an inverted join.
	// invertedExpr is used along with inputCol (a column from the input) to
	// find the keys to look up in the index; lookupCols are ordinals for the
	// table columns we are retrieving.
	//
	// The node produces the columns in the input and (unless join type is
	// LeftSemiJoin or LeftAntiJoin) the lookupCols, ordered by ordinal. The ON
	// condition can refer to these using IndexedVars. Note that lookupCols
	// includes the inverted column.
	ConstructInvertedJoin(
		joinType sqlbase.JoinType,
		invertedExpr tree.TypedExpr,
		input Node,
		table cat.Table,
		index cat.Index,
		inputCol NodeColumnOrdinal,
		lookupCols TableColumnOrdinalSet,
		onCond tree.TypedExpr,
		reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructZigzagJoin returns a node that performs a zigzag join.
	// Each side of the join has two kinds of columns that form a prefix
	// of the specified index: fixed columns (with values specified in
	// fixedVals), and equal columns (with column ordinals specified in
	// {left,right}EqCols). The lengths of leftEqCols and rightEqCols
	// must match.
	ConstructZigzagJoin(
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
	) (Node, error)

	// ConstructLimit returns a node that implements LIMIT and/or OFFSET on the
	// results of the given node. If one or the other is not needed, then it is
	// set to nil.
	ConstructLimit(input Node, limit, offset tree.TypedExpr) (Node, error)

	// ConstructMax1Row returns a node that permits at most one row from the
	// given input node, returning an error with the given text at runtime if
	// the node tries to return more than one row.
	ConstructMax1Row(input Node, errorText string) (Node, error)

	// ConstructProjectSet returns a node that performs a lateral cross join
	// between the output of the given node and the functional zip of the given
	// expressions.
	ConstructProjectSet(
		n Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
	) (Node, error)

	// ConstructWindow returns a node that executes a window function over the
	// given node.
	ConstructWindow(input Node, window WindowInfo) (Node, error)

	// RenameColumns modifies the column names of a node.
	RenameColumns(input Node, colNames []string) (Node, error)

	// ConstructPlan creates a plan enclosing the given plan and (optionally)
	// subqueries, cascades, and checks.
	//
	// Subqueries are executed before the root tree, which can refer to subquery
	// results using tree.Subquery nodes.
	//
	// Cascades are executed after the root tree. They can return more cascades
	// and checks which should also be executed.
	//
	// Checks are executed after all cascades have been executed. They don't
	// return results but can generate errors (e.g. foreign key check failures).
	ConstructPlan(
		root Node, subqueries []Subquery, cascades []Cascade, checks []Node,
	) (Plan, error)

	// ConstructExplain returns a node that implements EXPLAIN (OPT), showing
	// information about the given plan.
	ConstructExplainOpt(plan string, envOpts ExplainEnvData) (Node, error)

	// ConstructExplain returns a node that implements EXPLAIN, showing
	// information about the given plan.
	ConstructExplain(
		options *tree.ExplainOptions, stmtType tree.StatementType, plan Plan,
	) (Node, error)

	// ConstructShowTrace returns a node that implements a SHOW TRACE
	// FOR SESSION statement.
	ConstructShowTrace(typ tree.ShowTraceType, compact bool) (Node, error)

	// ConstructInsert creates a node that implements an INSERT statement. The
	// input columns are inserted into a subset of columns in the table, in the
	// same order they're defined. The insertCols set contains the ordinal
	// positions of columns in the table into which values are inserted. All
	// columns are expected to be present except delete-only mutation columns,
	// since those do not need to participate in an insert operation.
	//
	// If allowAutoCommit is set, the operator is allowed to commit the
	// transaction (if appropriate, i.e. if it is in an implicit transaction).
	// This is false if there are multiple mutations in a statement, or the output
	// of the mutation is processed through side-effecting expressions.
	ConstructInsert(
		input Node,
		table cat.Table,
		insertCols TableColumnOrdinalSet,
		returnCols TableColumnOrdinalSet,
		checkCols CheckOrdinalSet,
		allowAutoCommit bool,
	) (Node, error)

	// ConstructInsertFastPath creates a node that implements a special (but very
	// common) case of insert, satisfying the following conditions:
	//  - the input is Values with at most InsertFastPathMaxRows, and there are no
	//    subqueries;
	//  - there are no other mutations in the statement, and the output of the
	//    insert is not processed through side-effecting expressions (see
	//    allowAutoCommit flag for ConstructInsert);
	//  - there are no self-referencing foreign keys;
	//  - all FK checks can be performed using direct lookups into unique indexes.
	//
	// In this case, the foreign-key checks can run before (or even concurrently
	// with) the insert. If they are run before, the insert is allowed to
	// auto-commit.
	ConstructInsertFastPath(
		rows [][]tree.TypedExpr,
		table cat.Table,
		insertCols TableColumnOrdinalSet,
		returnCols TableColumnOrdinalSet,
		checkCols CheckOrdinalSet,
		fkChecks []InsertFastPathFKCheck,
	) (Node, error)

	// ConstructUpdate creates a node that implements an UPDATE statement. The
	// input contains columns that were fetched from the target table, and that
	// provide existing values that can be used to formulate the new encoded
	// value that will be written back to the table (updating any column in a
	// family requires having the values of all other columns). The input also
	// contains computed columns that provide new values for any updated columns.
	//
	// The fetchCols and updateCols sets contain the ordinal positions of the
	// fetch and update columns in the target table. The input must contain those
	// columns in the same order as they appear in the table schema, with the
	// fetch columns first and the update columns second.
	//
	// The passthrough parameter contains all the result columns that are part of
	// the input node that the update node needs to return (passing through from
	// the input). The pass through columns are used to return any column from the
	// FROM tables that are referenced in the RETURNING clause.
	//
	// If allowAutoCommit is set, the operator is allowed to commit the
	// transaction (if appropriate, i.e. if it is in an implicit transaction).
	// This is false if there are multiple mutations in a statement, or the output
	// of the mutation is processed through side-effecting expressions.
	ConstructUpdate(
		input Node,
		table cat.Table,
		fetchCols TableColumnOrdinalSet,
		updateCols TableColumnOrdinalSet,
		returnCols TableColumnOrdinalSet,
		checks CheckOrdinalSet,
		passthrough sqlbase.ResultColumns,
		allowAutoCommit bool,
	) (Node, error)

	// ConstructUpsert creates a node that implements an INSERT..ON CONFLICT or
	// UPSERT statement. For each input row, Upsert will test the canaryCol. If
	// it is null, then it will insert a new row. If not-null, then Upsert will
	// update an existing row. The input is expected to contain the columns to be
	// inserted, followed by the columns containing existing values, and finally
	// the columns containing new values.
	//
	// The length of each group of input columns can be up to the number of
	// columns in the given table. The insertCols, fetchCols, and updateCols sets
	// contain the ordinal positions of the table columns that are involved in
	// the Upsert. For example:
	//
	//   CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
	//   INSERT INTO abc VALUES (10, 20, 30) ON CONFLICT (a) DO UPDATE SET b=25
	//
	//   insertCols = {0, 1, 2}
	//   fetchCols  = {0, 1, 2}
	//   updateCols = {1}
	//
	// The input is expected to first have 3 columns that will be inserted into
	// columns {0, 1, 2} of the table. The next 3 columns contain the existing
	// values of columns {0, 1, 2} of the table. The last column contains the
	// new value for column {1} of the table.
	//
	// If allowAutoCommit is set, the operator is allowed to commit the
	// transaction (if appropriate, i.e. if it is in an implicit transaction).
	// This is false if there are multiple mutations in a statement, or the output
	// of the mutation is processed through side-effecting expressions.
	ConstructUpsert(
		input Node,
		table cat.Table,
		canaryCol NodeColumnOrdinal,
		insertCols TableColumnOrdinalSet,
		fetchCols TableColumnOrdinalSet,
		updateCols TableColumnOrdinalSet,
		returnCols TableColumnOrdinalSet,
		checks CheckOrdinalSet,
		allowAutoCommit bool,
	) (Node, error)

	// ConstructDelete creates a node that implements a DELETE statement. The
	// input contains columns that were fetched from the target table, and that
	// will be deleted.
	//
	// The fetchCols set contains the ordinal positions of the fetch columns in
	// the target table. The input must contain those columns in the same order
	// as they appear in the table schema.
	//
	// If allowAutoCommit is set, the operator is allowed to commit the
	// transaction (if appropriate, i.e. if it is in an implicit transaction).
	// This is false if there are multiple mutations in a statement, or the output
	// of the mutation is processed through side-effecting expressions.
	ConstructDelete(
		input Node,
		table cat.Table,
		fetchCols TableColumnOrdinalSet,
		returnCols TableColumnOrdinalSet,
		allowAutoCommit bool,
	) (Node, error)

	// ConstructDeleteRange creates a node that efficiently deletes contiguous
	// rows stored in the given table's primary index. This fast path is only
	// possible when certain conditions hold true:
	//  - there are no secondary indexes;
	//  - the input to the delete is a scan (without limits);
	//  - the table is not involved in interleaving, or it is at the root of an
	//    interleaving hierarchy with cascading FKs such that a delete of a row
	//    cascades and deletes all interleaved rows corresponding to that row;
	//  - there are no inbound FKs to the table (other than within the
	//    interleaving as described above).
	//
	// See the comment for ConstructScan for descriptions of the needed and
	// indexConstraint parameters, since DeleteRange combines Delete + Scan into a
	// single operator.
	//
	// If any interleavedTables are passed, they are all the descendant tables in
	// an interleaving hierarchy we are deleting from.
	ConstructDeleteRange(
		table cat.Table,
		needed TableColumnOrdinalSet,
		indexConstraint *constraint.Constraint,
		interleavedTables []cat.Table,
		maxReturnedKeys int,
		allowAutoCommit bool,
	) (Node, error)

	// ConstructCreateTable returns a node that implements a CREATE TABLE
	// statement.
	ConstructCreateTable(input Node, schema cat.Schema, ct *tree.CreateTable) (Node, error)

	// ConstructCreateView returns a node that implements a CREATE VIEW
	// statement.
	ConstructCreateView(
		schema cat.Schema,
		viewName string,
		ifNotExists bool,
		replace bool,
		temporary bool,
		viewQuery string,
		columns sqlbase.ResultColumns,
		deps opt.ViewDeps,
	) (Node, error)

	// ConstructSequenceSelect creates a node that implements a scan of a sequence
	// as a data source.
	ConstructSequenceSelect(sequence cat.Sequence) (Node, error)

	// ConstructSaveTable wraps the input into a node that passes through all the
	// rows, but also creates a table and inserts all the rows into it.
	ConstructSaveTable(input Node, table *cat.DataSourceName, colNames []string) (Node, error)

	// ConstructErrorIfRows wraps the input into a node which itself returns no
	// results, but errors out if the input returns any rows. The mkErr function
	// is used to create the error.
	ConstructErrorIfRows(input Node, mkErr func(tree.Datums) error) (Node, error)

	// ConstructOpaque creates a node for an opaque operator.
	ConstructOpaque(metadata opt.OpaqueMetadata) (Node, error)

	// ConstructAlterTableSplit creates a node that implements ALTER TABLE/INDEX
	// SPLIT AT.
	ConstructAlterTableSplit(index cat.Index, input Node, expiration tree.TypedExpr) (Node, error)

	// ConstructAlterTableUnsplit creates a node that implements ALTER TABLE/INDEX
	// UNSPLIT AT.
	ConstructAlterTableUnsplit(index cat.Index, input Node) (Node, error)

	// ConstructAlterTableUnsplitAll creates a node that implements ALTER TABLE/INDEX
	// UNSPLIT ALL.
	ConstructAlterTableUnsplitAll(index cat.Index) (Node, error)

	// ConstructAlterTableRelocate creates a node that implements ALTER TABLE/INDEX
	// UNSPLIT AT.
	ConstructAlterTableRelocate(index cat.Index, input Node, relocateLease bool) (Node, error)

	// ConstructBuffer constructs a node whose input can be referenced from
	// elsewhere in the query.
	ConstructBuffer(input Node, label string) (BufferNode, error)

	// ConstructScanBuffer constructs a node which refers to a node constructed by
	// ConstructBuffer or passed to RecursiveCTEIterationFn.
	ConstructScanBuffer(ref BufferNode, label string) (Node, error)

	// ConstructRecursiveCTE constructs a node that executes a recursive CTE:
	//   * the initial plan is run first; the results are emitted and also saved
	//     in a buffer.
	//   * so long as the last buffer is not empty:
	//     - the RecursiveCTEIterationFn is used to create a plan for the
	//       recursive side; a reference to the last buffer is passed to this
	//       function. The returned plan uses this reference with a
	//       ConstructScanBuffer call.
	//     - the plan is executed; the results are emitted and also saved in a new
	//       buffer for the next iteration.
	ConstructRecursiveCTE(initial Node, fn RecursiveCTEIterationFn, label string) (Node, error)

	// ConstructControlJobs creates a node that implements PAUSE/CANCEL/RESUME
	// JOBS.
	ConstructControlJobs(command tree.JobCommand, input Node) (Node, error)

	// ConstructCancelQueries creates a node that implements CANCEL QUERIES.
	ConstructCancelQueries(input Node, ifExists bool) (Node, error)

	// ConstructCancelSessions creates a node that implements CANCEL SESSIONS.
	ConstructCancelSessions(input Node, ifExists bool) (Node, error)

	// ConstructExport creates a node that implements EXPORT.
	ConstructExport(
		input Node,
		fileName tree.TypedExpr,
		fileFormat string,
		options []KVOption,
	) (Node, error)
}

// OutputOrdering indicates the required output ordering on a Node that is being
// created. It refers to the output columns of the node by ordinal.
//
// This ordering is used for distributed execution planning, to know how to
// merge results from different nodes. For example, scanning a table can be
// executed as multiple hosts scanning different pieces of the table. When the
// results from the nodes get merged, we they are merged according to the output
// ordering.
//
// The node must be able to support this output ordering given its other
// configuration parameters.
type OutputOrdering sqlbase.ColumnOrdering

// Subquery encapsulates information about a subquery that is part of a plan.
type Subquery struct {
	// ExprNode is a reference to a AST node that can be used for printing the SQL
	// of the subquery (for EXPLAIN).
	ExprNode tree.NodeFormatter
	Mode     SubqueryMode
	// Root is the root Node of the plan for this subquery. This Node returns
	// results as required for the specific Type.
	Root Node
}

// SubqueryMode indicates how the results of the subquery are to be processed.
type SubqueryMode int

const (
	// SubqueryExists - the value of the subquery is a boolean: true if the
	// subquery returns any rows, false otherwise.
	SubqueryExists SubqueryMode = iota
	// SubqueryOneRow - the subquery expects at most one row; the result is that
	// row (as a single value or a tuple), or NULL if there were no rows.
	SubqueryOneRow
	// SubqueryAnyRows - the subquery is an argument to ANY. Any number of rows
	// expected; the result is a sorted, distinct tuple of rows (i.e. it has been
	// normalized). As a special case, if there is only one column selected, the
	// result is a tuple of the selected values (instead of a tuple of 1-tuples).
	SubqueryAnyRows
	// SubqueryAllRows - the subquery is an argument to ARRAY. The result is a
	// tuple of rows.
	SubqueryAllRows
)

// TableColumnOrdinal is the 0-based ordinal index of a cat.Table column.
// It is used when operations involve a table directly (e.g. scans, index/lookup
// joins, mutations).
type TableColumnOrdinal int32

// TableColumnOrdinalSet contains a set of TableColumnOrdinal values.
type TableColumnOrdinalSet = util.FastIntSet

// NodeColumnOrdinal is the 0-based ordinal index of a column produced by a
// Node. It is used when referring to a column in an input to an operator.
type NodeColumnOrdinal int32

// NodeColumnOrdinalSet contains a set of NodeColumnOrdinal values.
type NodeColumnOrdinalSet = util.FastIntSet

// CheckOrdinalSet contains the ordinal positions of a set of check constraints
// taken from the opt.Table.Check collection.
type CheckOrdinalSet = util.FastIntSet

// AggInfo represents an aggregation (see ConstructGroupBy).
type AggInfo struct {
	FuncName   string
	Distinct   bool
	ResultType *types.T
	ArgCols    []NodeColumnOrdinal

	// ConstArgs is the list of any constant arguments to the aggregate,
	// for instance, the separator in string_agg.
	ConstArgs []tree.Datum

	// Filter is the index of the column, if any, which should be used as the
	// FILTER condition for the aggregate. If there is no filter, Filter is -1.
	Filter NodeColumnOrdinal
}

// WindowInfo represents the information about a window function that must be
// passed through to the execution engine.
type WindowInfo struct {
	// Cols is the set of columns that are returned from the windowing operator.
	Cols sqlbase.ResultColumns

	// TODO(justin): refactor this to be a single array of structs.

	// Exprs is the list of window function expressions.
	Exprs []*tree.FuncExpr

	// OutputIdxs are the indexes that the various window functions being computed
	// should put their output in.
	OutputIdxs []int

	// ArgIdxs is the list of column ordinals each function takes as arguments,
	// in the same order as Exprs.
	ArgIdxs [][]NodeColumnOrdinal

	// FilterIdxs is the list of column indices to use as filters.
	FilterIdxs []int

	// Partition is the set of input columns to partition on.
	Partition []NodeColumnOrdinal

	// Ordering is the set of input columns to order on.
	Ordering sqlbase.ColumnOrdering

	// RangeOffsetColumn is the column ID of a single column from ORDER BY clause
	// when window frame has RANGE mode of framing and at least one 'offset'
	// boundary. We store it separately because the ordering might be simplified
	// (when that single column is in Partition), but the execution still needs
	// to know the original ordering.
	RangeOffsetColumn NodeColumnOrdinal
}

// ExplainEnvData represents the data that's going to be displayed in EXPLAIN (env).
type ExplainEnvData struct {
	ShowEnv   bool
	Tables    []tree.TableName
	Sequences []tree.TableName
	Views     []tree.TableName
}

// KVOption represents information about a statement option
// (see tree.KVOptions).
type KVOption struct {
	Key string
	// If there is no value, Value is DNull.
	Value tree.TypedExpr
}

// RecursiveCTEIterationFn creates a plan for an iteration of WITH RECURSIVE,
// given the result of the last iteration (as a BufferNode).
type RecursiveCTEIterationFn func(bufferRef BufferNode) (Plan, error)

// ApplyJoinPlanRightSideFn creates a plan for an iteration of ApplyJoin, given
// a row produced from the left side. The plan is guaranteed to produce the
// rightColumns passed to ConstructApplyJoin (in order).
type ApplyJoinPlanRightSideFn func(leftRow tree.Datums) (Plan, error)

// Cascade describes a cascading query. The query uses a BufferNode as an input;
// it should only be triggered if this buffer is not empty.
type Cascade struct {
	// FKName is the name of the foreign key constraint.
	FKName string

	// Buffer is the Node returned by ConstructBuffer which stores the input to
	// the mutation.
	Buffer BufferNode

	// PlanFn builds the cascade query and creates the plan for it.
	// Note that the generated Plan can in turn contain more cascades (as well as
	// checks, which should run after all cascades are executed).
	//
	// The bufferRef is a reference that can be used with ConstructWithBuffer to
	// read the mutation input. It is conceptually the same as the Buffer field;
	// however, we allow the execution engine to provide a different copy or
	// implementation of the node (e.g. to facilitate early cleanup of the
	// original plan).
	//
	// This method does not mutate any captured state; it is ok to call PlanFn
	// methods concurrently (provided that they don't use a single non-thread-safe
	// execFactory).
	PlanFn func(
		ctx context.Context,
		semaCtx *tree.SemaContext,
		evalCtx *tree.EvalContext,
		execFactory Factory,
		bufferRef BufferNode,
		numBufferedRows int,
	) (Plan, error)
}

// InsertFastPathMaxRows is the maximum number of rows for which we can use the
// insert fast path.
const InsertFastPathMaxRows = 10000

// InsertFastPathFKCheck contains information about a foreign key check to be
// performed by the insert fast-path (see ConstructInsertFastPath). It
// identifies the index into which we can perform the lookup.
type InsertFastPathFKCheck struct {
	ReferencedTable cat.Table
	ReferencedIndex cat.Index

	// InsertCols contains the FK columns from the origin table, in the order of
	// the ReferencedIndex columns. For each, the value in the array indicates the
	// index of the column in the input table.
	InsertCols []TableColumnOrdinal

	MatchMethod tree.CompositeKeyMatchMethod

	// MkErr is called when a violation is detected (i.e. the index has no entries
	// for a given inserted row). The values passed correspond to InsertCols
	// above.
	MkErr func(tree.Datums) error
}
