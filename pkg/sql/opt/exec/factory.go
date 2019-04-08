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

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// Node represents a node in the execution tree
// (currently maps to sql.planNode).
type Node interface{}

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
	//   - If indexConstraint is not nil, the scan is restricted to the spans in
	//     in the constraint.
	//   - If hardLimit > 0, then only up to hardLimit rows can be returned from
	//     the scan.
	//   - If maxResults > 0, the scan is guaranteed to return at most maxResults
	//     rows.
	ConstructScan(
		table cat.Table,
		index cat.Index,
		needed ColumnOrdinalSet,
		indexConstraint *constraint.Constraint,
		hardLimit int64,
		reverse bool,
		maxResults uint64,
		reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructVirtualScan returns a node that represents the scan of a virtual
	// table. Virtual tables are system tables that are populated "on the fly"
	// with rows synthesized from system metadata and other state.
	ConstructVirtualScan(table cat.Table) (Node, error)

	// ConstructFilter returns a node that applies a filter on the results of
	// the given input node.
	ConstructFilter(n Node, filter tree.TypedExpr, reqOrdering OutputOrdering) (Node, error)

	// ConstructSimpleProject returns a node that applies a "simple" projection on the
	// results of the given input node. A simple projection is one that does not
	// involve new expressions; it's just a reshuffling of columns. This is a
	// more efficient version of ConstructRender.
	// The colNames argument is optional; if it is nil, the names of the
	// corresponding input columns are kept.
	ConstructSimpleProject(
		n Node, cols []ColumnOrdinal, colNames []string, reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructRender returns a node that applies a projection on the results of
	// the given input node. The projection can contain new expressions.
	ConstructRender(
		n Node, exprs tree.TypedExprs, colNames []string, reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructApplyJoin returns a node that runs an apply join between an input
	// node (the left side of the join) and a RelExpr that has outer columns (the
	// right side of the join) by replacing the outer columns of the right side
	// RelExpr with data from each row of the left side of the join according to
	// the data in leftBoundColMap. The apply join can be any kind of join except
	// for right outer and full outer.
	//
	// leftBoundColMap is a map from opt.ColumnID to opt.ColumnOrdinal that maps
	// a column bound by the left side of the apply join to the column ordinal
	// in the left side that contains the binding.
	//
	// memo, rightProps, and right are the memo, required physical properties, and
	// RelExpr of the right side of the join that will be repeatedly modified,
	// re-planned and executed for every row from the left side.
	//
	// fakeRight is a pre-planned node that is the right side of the join with
	// all outer columns replaced by NULL. The physical properties of this node
	// (its output columns, their order and types) are used to pre-determine the
	// runtime indexes and types for the right side of the apply join, since all
	// re-plannings of the right hand side will be pinned to output the exact
	// same output columns in the same order.
	//
	// onCond is the join condition.
	ConstructApplyJoin(
		joinType sqlbase.JoinType,
		left Node,
		leftBoundColMap opt.ColMap,
		memo *memo.Memo,
		rightProps *physical.Required,
		fakeRight Node,
		right memo.RelExpr,
		onCond tree.TypedExpr,
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
		leftEqCols, rightEqCols []ColumnOrdinal,
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
	) (Node, error)

	// ConstructGroupBy returns a node that runs an aggregation. A set of
	// aggregations is performed for each group of values on the groupCols.
	//
	// If the input is guaranteed to have an ordering on grouping columns, a
	// "streaming" aggregation is performed (i.e. aggregation happens separately
	// for each distinct set of values on the orderedGroupCols).
	ConstructGroupBy(
		input Node,
		groupCols []ColumnOrdinal,
		orderedGroupCols ColumnOrdinalSet,
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
	ConstructDistinct(input Node, distinctCols, orderedCols ColumnOrdinalSet) (Node, error)

	// ConstructSetOp returns a node that performs a UNION / INTERSECT / EXCEPT
	// operation (either the ALL or the DISTINCT version). The left and right
	// nodes must have the same number of columns.
	ConstructSetOp(typ tree.UnionType, all bool, left, right Node) (Node, error)

	// ConstructSort returns a node that performs a resorting of the rows produced
	// by the input node.
	ConstructSort(input Node, ordering sqlbase.ColumnOrdering) (Node, error)

	// ConstructOrdinality returns a node that appends an ordinality column to
	// each row in the input node.
	ConstructOrdinality(input Node, colName string) (Node, error)

	// ConstructIndexJoin returns a node that performs an index join.
	// The input must be created by ConstructScan for the same table; cols is the
	// set of columns produced by the index join.
	ConstructIndexJoin(
		input Node, table cat.Table, cols ColumnOrdinalSet, reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructLookupJoin returns a node that preforms a lookup join.
	// The keyCols are columns from the input used as keys for the columns of the
	// index (or a prefix of them); lookupCols are ordinals for the table columns
	// we are retrieving.
	//
	// The node produces the columns in the input and lookupCols (ordered by
	// ordinal). The ON condition can refer to these using IndexedVars.
	ConstructLookupJoin(
		joinType sqlbase.JoinType,
		input Node,
		table cat.Table,
		index cat.Index,
		keyCols []ColumnOrdinal,
		lookupCols ColumnOrdinalSet,
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
		leftEqCols []ColumnOrdinal,
		rightEqCols []ColumnOrdinal,
		leftCols ColumnOrdinalSet,
		rightCols ColumnOrdinalSet,
		onCond tree.TypedExpr,
		fixedVals []Node,
		reqOrdering OutputOrdering,
	) (Node, error)

	// ConstructLimit returns a node that implements LIMIT and/or OFFSET on the
	// results of the given node. If one or the other is not needed, then it is
	// set to nil.
	ConstructLimit(input Node, limit, offset tree.TypedExpr) (Node, error)

	// ConstructMax1Row returns a node that permits at most one row from the
	// given input node, returning an error at runtime if the node tries to return
	// more than one row.
	ConstructMax1Row(input Node) (Node, error)

	// ConstructProjectSet returns a node that performs a lateral cross join
	// between the output of the given node and the functional zip of the given
	// expressions.
	ConstructProjectSet(
		n Node, exprs tree.TypedExprs, zipCols sqlbase.ResultColumns, numColsPerGen []int,
	) (Node, error)

	// RenameColumns modifies the column names of a node.
	RenameColumns(input Node, colNames []string) (Node, error)

	// ConstructPlan creates a plan enclosing the given plan and (optionally)
	// subqueries.
	ConstructPlan(root Node, subqueries []Subquery) (Plan, error)

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
	// since those do not need to participate in an insert operation. The
	// rowsNeeded parameter is true if a RETURNING clause needs the inserted
	// row(s) as output.
	ConstructInsert(
		input Node,
		table cat.Table,
		insertCols ColumnOrdinalSet,
		checks CheckOrdinalSet,
		rowsNeeded bool,
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
	// fetch columns first and the update columns second. The rowsNeeded parameter
	// is true if a RETURNING clause needs the updated row(s) as output.
	ConstructUpdate(
		input Node,
		table cat.Table,
		fetchCols ColumnOrdinalSet,
		updateCols ColumnOrdinalSet,
		checks CheckOrdinalSet,
		rowsNeeded bool,
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
	ConstructUpsert(
		input Node,
		table cat.Table,
		canaryCol ColumnOrdinal,
		insertCols ColumnOrdinalSet,
		fetchCols ColumnOrdinalSet,
		updateCols ColumnOrdinalSet,
		checks CheckOrdinalSet,
		rowsNeeded bool,
	) (Node, error)

	// ConstructDelete creates a node that implements a DELETE statement. The
	// input contains columns that were fetched from the target table, and that
	// will be deleted.
	//
	// The fetchCols set contains the ordinal positions of the fetch columns in
	// the target table. The input must contain those columns in the same order
	// as they appear in the table schema. The rowsNeeded parameter is true if a
	// RETURNING clause needs the deleted row(s) as output.
	ConstructDelete(
		input Node, table cat.Table, fetchCols ColumnOrdinalSet, rowsNeeded bool,
	) (Node, error)

	// ConstructDeleteRange creates a node that efficiently deletes contiguous
	// rows stored in the given table's primary index. This fast path is only
	// possible when certain conditions hold true (see canUseDeleteRange for more
	// details). See the comment for ConstructScan for descriptions of the
	// parameters, since FastDelete combines Delete + Scan into a single operator.
	ConstructDeleteRange(
		table cat.Table,
		needed ColumnOrdinalSet,
		indexConstraint *constraint.Constraint,
	) (Node, error)

	// ConstructCreateTable returns a node that implements a CREATE TABLE
	// statement.
	ConstructCreateTable(input Node, schema cat.Schema, ct *tree.CreateTable) (Node, error)

	// ConstructSequenceSelect creates a node that implements a scan of a sequence
	// as a data source.
	ConstructSequenceSelect(sequence cat.Sequence) (Node, error)
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
	// ExprNode is a reference to a tree.Subquery node that has been created for
	// this query; it is part of a scalar expression inside some Node.
	ExprNode *tree.Subquery
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

// ColumnOrdinal is the 0-based ordinal index of a column produced by a Node.
type ColumnOrdinal int32

// ColumnOrdinalSet contains a set of ColumnOrdinal values as ints.
type ColumnOrdinalSet = util.FastIntSet

// CheckOrdinalSet contains the ordinal positions of a set of check constraints
// taken from the opt.Table.Check collection.
type CheckOrdinalSet = util.FastIntSet

// AggInfo represents an aggregation (see ConstructGroupBy).
type AggInfo struct {
	FuncName   string
	Builtin    *tree.Overload
	Distinct   bool
	ResultType *types.T
	ArgCols    []ColumnOrdinal

	// ConstArgs is the list of any constant arguments to the aggregate,
	// for instance, the separator in string_agg.
	ConstArgs []tree.Datum

	// Filter is the index of the column, if any, which should be used as the
	// FILTER condition for the aggregate. If there is no filter, Filter is -1.
	Filter ColumnOrdinal
}

// ExplainEnvData represents the data that's going to be displayed in EXPLAIN (env).
type ExplainEnvData struct {
	ShowEnv   bool
	Tables    []tree.TableName
	Sequences []tree.TableName
	Views     []tree.TableName
}
