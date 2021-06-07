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
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
)

// Node represents a node in the execution tree
// (currently maps to sql.planNode).
type Node interface{}

// Plan represents the plan for a query (currently maps to sql.planTop).
// For simple queries, the plan is associated with a single Node tree.
// For queries containing subqueries, the plan is associated with multiple Node
// trees (see ConstructPlan).
type Plan interface{}

// ScanParams contains all the parameters for a table scan.
type ScanParams struct {
	// Only columns in this set are scanned and produced.
	NeededCols TableColumnOrdinalSet

	// At most one of IndexConstraint or InvertedConstraint is non-nil, depending
	// on the index type.
	IndexConstraint    *constraint.Constraint
	InvertedConstraint inverted.Spans

	// If non-zero, the scan returns this many rows.
	HardLimit int64

	// If non-zero, the scan may still be required to return up to all its rows
	// (or up to the HardLimit if it is set, but can be optimized under the
	// assumption that only SoftLimit rows will be needed.
	SoftLimit int64

	Reverse bool

	// If true, the scan will scan all spans in parallel. It should only be set to
	// true if there is a known upper bound on the number of rows that will be
	// scanned. It should not be set if there is a hard or soft limit.
	Parallelize bool

	Locking *tree.LockingItem

	EstimatedRowCount float64

	// If true, we are performing a locality optimized search. In order for this
	// to work correctly, the execution engine must create a local DistSQL plan
	// for the main query (subqueries and postqueries need not be local).
	LocalityOptimized bool
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
type OutputOrdering colinfo.ColumnOrdering

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
	Cols colinfo.ResultColumns

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
	Ordering colinfo.ColumnOrdering
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
// given the result of the last iteration (as a node created by
// ConstructBuffer).
type RecursiveCTEIterationFn func(ef Factory, bufferRef Node) (Plan, error)

// ApplyJoinPlanRightSideFn creates a plan for an iteration of ApplyJoin, given
// a row produced from the left side. The plan is guaranteed to produce the
// rightColumns passed to ConstructApplyJoin (in order).
type ApplyJoinPlanRightSideFn func(ef Factory, leftRow tree.Datums) (Plan, error)

// Cascade describes a cascading query. The query uses a node created by
// ConstructBuffer as an input; it should only be triggered if this buffer is
// not empty.
type Cascade struct {
	// FKName is the name of the foreign key constraint.
	FKName string

	// Buffer is the Node returned by ConstructBuffer which stores the input to
	// the mutation. It is nil if the cascade does not require a buffer.
	Buffer Node

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
	// If the cascade does not require input buffering (Buffer is nil), then
	// bufferRef should be nil and numBufferedRows should be 0.
	//
	// This method does not mutate any captured state; it is ok to call PlanFn
	// methods concurrently (provided that they don't use a single non-thread-safe
	// execFactory).
	PlanFn func(
		ctx context.Context,
		semaCtx *tree.SemaContext,
		evalCtx *tree.EvalContext,
		execFactory Factory,
		bufferRef Node,
		numBufferedRows int,
		allowAutoCommit bool,
	) (Plan, error)
}

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
	MkErr MkErrFn
}

// MkErrFn is a function that generates an error which includes values from a
// relevant row.
type MkErrFn func(tree.Datums) error

// ExplainFactory is an extension of Factory used when constructing a plan that
// can be explained. It allows annotation of nodes with extra information.
type ExplainFactory interface {
	Factory

	// AnnotateNode annotates a constructed Node with extra information.
	AnnotateNode(n Node, id ExplainAnnotationID, value interface{})
}

// ExplainAnnotationID identifies the type of a node annotation.
type ExplainAnnotationID int

const (
	// EstimatedStatsID is an annotation with a *EstimatedStats value.
	EstimatedStatsID ExplainAnnotationID = iota

	// ExecutionStatsID is an annotation with a *ExecutionStats value.
	ExecutionStatsID
)

// EstimatedStats contains estimated statistics about a given operator.
type EstimatedStats struct {
	// TableStatsAvailable is true if all the tables involved by this operator
	// (directly or indirectly) had table statistics.
	TableStatsAvailable bool
	// RowCount is the estimated number of rows produced by the operator.
	RowCount float64
	// TableStatsRowCount is set only for scans; it is the estimated total number
	// of rows in the table we are scanning.
	TableStatsRowCount uint64
	// TableStatsCreatedAt is set only for scans; it is the time when the latest
	// table statistics were collected.
	TableStatsCreatedAt time.Time
	// Cost is the estimated cost of the operator. This cost includes the costs of
	// the child operators.
	Cost float64
}

// ExecutionStats contain statistics about a given operator gathered from the
// execution of the query.
//
// TODO(radu): can/should we just use execinfrapb.ComponentStats instead?
type ExecutionStats struct {
	// RowCount is the number of rows produced by the operator.
	RowCount optional.Uint

	// VectorizedBatchCount is the number of vectorized batches produced by the
	// operator.
	VectorizedBatchCount optional.Uint

	KVTime           optional.Duration
	KVContentionTime optional.Duration
	KVBytesRead      optional.Uint
	KVRowsRead       optional.Uint

	// Nodes on which this operator was executed.
	Nodes []string

	// Regions on which this operator was executed.
	// Only being generated on EXPLAIN ANALYZE.
	Regions []string
}

// BuildPlanForExplainFn builds an execution plan against the given
// ExplainFactory.
type BuildPlanForExplainFn func(ef ExplainFactory) (Plan, error)
