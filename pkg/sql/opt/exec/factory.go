// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package exec contains execution-related utilities. (See README.md.)
package exec

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/optional"
)

// Node represents a node in the execution tree
// (currently maps to sql.planNode).
type Node interface{}

// Plan represents the plan for a query (currently maps to sql.planComponents).
// For simple queries, the plan is associated with a single Node tree.
// For queries containing subqueries, the plan is associated with multiple Node
// trees (see ConstructPlan).
type Plan interface{}

// PlanFlags tracks various properties of the built plan.
type PlanFlags uint32

const (
	// PlanFlagIsDDL is set if the statement contains DDL.
	PlanFlagIsDDL = (1 << iota)

	// PlanFlagContainsFullTableScan is set if the statement contains an
	// unconstrained primary index scan. This could be a full scan of any
	// cardinality. Full scans of virtual tables are ignored.
	PlanFlagContainsFullTableScan

	// PlanFlagContainsFullIndexScan is set if the statement contains an
	// unconstrained non-partial secondary index scan. This could be a full scan
	// of any cardinality. Full scans of virtual tables are ignored.
	PlanFlagContainsFullIndexScan

	// PlanFlagContainsLargeFullTableScan is set if the statement contains an
	// unconstrained primary index scan estimated to read more than
	// large_full_scan_rows (or without available stats). Large scans of virtual
	// tables are ignored.
	PlanFlagContainsLargeFullTableScan

	// PlanFlagContainsLargeFullIndexScan is set if the statement contains an
	// unconstrained non-partial secondary index scan estimated to read more than
	// large_full_scan_rows (or without available stats). Large scans of virtual
	// tables are ignored.
	PlanFlagContainsLargeFullIndexScan

	// PlanFlagContainsMutation is set if the whole plan contains any mutations.
	PlanFlagContainsMutation

	// PlanFlagContainsLocking is set if at least one node in the plan uses
	// locking. (Examples of plans using locking include SELECT FOR UPDATE and
	// SELECT FOR SHARE, UPDATE, UPSERT, and FK checks under read committed
	// isolation.)
	PlanFlagContainsLocking

	// PlanFlagCheckContainsLocking is set if at least one node in at least one
	// check plan uses locking. Typically this is set for plans with FK checks
	// under read committed isolation.
	PlanFlagCheckContainsLocking

	// PlanFlagContainsDelete is set if at least one DELETE stmt is found in the
	// whole plan.
	PlanFlagContainsDelete

	// PlanFlagContainsInsert is set if at least one INSERT stmt is found in the
	// whole plan.
	PlanFlagContainsInsert

	// PlanFlagContainsUpdate is set if at least one UPDATE stmt is found in the
	// whole plan.
	PlanFlagContainsUpdate

	// PlanFlagContainsUpsert is set if at least one UPSERT stmt is found in the
	// whole plan.
	PlanFlagContainsUpsert

	// PlanFlagUsesRLS is set if the plan applies row-level security policies.
	PlanFlagUsesRLS
)

// IsSet returns true if the receiver has all of the given flags set.
func (pf PlanFlags) IsSet(flags PlanFlags) bool {
	return (pf & flags) == flags
}

// Set sets all of the given flags in the receiver.
func (pf *PlanFlags) Set(flags PlanFlags) {
	*pf |= flags
}

// Unset unsets all of the given flags in the receiver.
func (pf *PlanFlags) Unset(flags PlanFlags) {
	*pf &^= flags
}

// ScanParams contains all the parameters for a table scan.
type ScanParams struct {
	// Only columns in this set are scanned and produced.
	NeededCols TableColumnOrdinalSet

	// At most one of IndexConstraint or InvertedConstraint is non-nil, depending
	// on the index type.
	IndexConstraint    *constraint.Constraint
	InvertedConstraint inverted.Spans

	// If non-zero, the scan returns this many rows.
	//
	// Additionally, in plan-gist decoding path, this will be set to -1 to
	// indicate presence of a limit, regardless of its value.
	// TODO(yuzefovich): we could refactor this special case by adding an
	// additional boolean that would allow us to switch to using uint64.
	HardLimit int64

	// If non-zero, the scan may still be required to return up to all its rows
	// (or up to the HardLimit if it is set, but can be optimized under the
	// assumption that only SoftLimit rows will be needed.
	SoftLimit uint64

	Reverse bool

	// If true, the scan will scan all spans in parallel. It should only be set
	// to true if there is a known upper bound on the number of rows that will
	// be scanned, or if 'unbounded_parallel_scans' session variable is 'true'.
	// It should not be set if there is a hard or soft limit.
	Parallelize bool

	// Row-level locking properties.
	Locking opt.Locking

	// EstimatedRowCount, if set, is the estimated number of rows that will be
	// scanned, rounded up.
	EstimatedRowCount uint64

	// StatsCreatedAt, if set, is the time when the latest table statistics were
	// collected.
	StatsCreatedAt time.Time

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
	// RowCount is the estimated number of rows that Root will output, negative
	// if the stats weren't available to make a good estimate.
	RowCount int64
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
	// SubqueryDiscardAllRows - the subquery is executed for its side effects
	// (e.g. it is adding to a bufferNode). The result is empty, and will never be
	// used.
	SubqueryDiscardAllRows
)

// TableColumnOrdinal is the 0-based ordinal index of a cat.Table column.
// It is used when operations involve a table directly (e.g. scans, index/lookup
// joins, mutations).
type TableColumnOrdinal int32

// TableColumnOrdinalSet contains a set of TableColumnOrdinal values.
type TableColumnOrdinalSet = intsets.Fast

// NodeColumnOrdinal is the 0-based ordinal index of a column produced by a
// Node. It is used when referring to a column in an input to an operator.
type NodeColumnOrdinal int32

// NodeColumnOrdinalSet contains a set of NodeColumnOrdinal values.
type NodeColumnOrdinalSet = intsets.Fast

// CheckOrdinalSet contains the ordinal positions of a set of check constraints
// taken from the opt.Table.Check collection.
type CheckOrdinalSet = intsets.Fast

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

	// DistsqlBlocklist is set to true when this aggregate function cannot be
	// evaluated in distributed fashion.
	DistsqlBlocklist bool
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
	AddFKs    []*tree.AlterTable
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
type RecursiveCTEIterationFn func(ctx context.Context, ef Factory, bufferRef Node) (Plan, error)

// ApplyJoinPlanRightSideFn creates a plan for an iteration of ApplyJoin, given
// a row produced from the left side. The plan is guaranteed to produce the
// rightColumns passed to ConstructApplyJoin (in order).
type ApplyJoinPlanRightSideFn func(ctx context.Context, ef Factory, leftRow tree.Datums) (Plan, error)

// ApplyJoinRightSideForExplainFn is a function that lazily populates the
// stringified version of the unoptimized right-hand side plan, for EXPLAIN
// purposes.
type ApplyJoinRightSideForExplainFn func(redactableValues bool) string

// PostQuery describes a cascading query or an AFTER trigger action. The query
// uses a node created by ConstructBuffer as an input; it should only be
// triggered if this buffer is not empty.
type PostQuery struct {
	// FKConstraint is used for logging and EXPLAIN purposes. It is nil if this
	// PostQuery describes a set of AFTER triggers.
	FKConstraint cat.ForeignKeyConstraint

	// Triggers is used for logging and EXPLAIN purposes. It is nil if this
	// PostQuery describes a foreign-key cascade action.
	Triggers []cat.Trigger

	// Buffer is the Node returned by ConstructBuffer which stores the input to
	// the mutation. It is nil if the cascade does not require a buffer.
	Buffer Node

	// PlanFn builds the cascade/trigger query and creates the plan for it.
	// Note that the generated Plan can in turn contain more cascades, triggers,
	// and checks.
	//
	// The bufferRef is a reference that can be used with ConstructWithBuffer to
	// read the mutation input. It is conceptually the same as the Buffer field;
	// however, we allow the execution engine to provide a different copy or
	// implementation of the node (e.g. to facilitate early cleanup of the
	// original plan).
	//
	// If the cascade/trigger does not require input buffering (Buffer is nil),
	// then bufferRef should be nil and numBufferedRows should be 0.
	//
	// This method does not mutate any captured state; it is ok to call PlanFn
	// methods concurrently (provided that they don't use a single non-thread-safe
	// execFactory).
	PlanFn func(
		ctx context.Context,
		semaCtx *tree.SemaContext,
		evalCtx *eval.Context,
		execFactory Factory,
		bufferRef Node,
		numBufferedRows int,
		allowAutoCommit bool,
	) (Plan, error)

	// GetExplainPlan returns the explain plan for the cascade or trigger query.
	// It will always return a cached plan if there is one, and the boolean
	// argument controls whether this function can create a new plan (which will
	// be cached going forward). If createPlanIfMissing is false and there is no
	// cached plan, then nil, nil is returned.
	GetExplainPlan func(_ context.Context, createPlanIfMissing bool) (Plan, error)
}

// InsertFastPathCheck contains information about a foreign key or
// uniqueness check to be performed by the insert fast-path (see
// ConstructInsertFastPath). It identifies the index into which we can perform
// the lookup.
type InsertFastPathCheck struct {
	ReferencedTable cat.Table
	ReferencedIndex cat.Index

	// This is the ordinal of the check in the table's unique constraints.
	CheckOrdinal int

	// InsertCols contains the table column ordinals of the referenced index key
	// columns. The position in this slice corresponds with the ordinal of the
	// referenced index column (its position in the index key).
	InsertCols []TableColumnOrdinal

	MatchMethod tree.CompositeKeyMatchMethod

	// Row-level locking properties of the check.
	Locking opt.Locking

	// DatumsFromConstraint contains constant values from the insert row for the
	// columns in the unique constraint. Columns not available directly from the
	// insert row or computed from insert row values (computed column) may be
	// filled in from a CHECK constraint on the column. The number of entries
	// corresponds with the number of KV lookups. For example, when built from
	// analyzing a locality-optimized operation accessing 1 local region and 2
	// remote regions, the resulting DatumsFromConstraint would have 3 entries.
	DatumsFromConstraint []tree.Datums

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

	// PolicyInfoID is an annotation with a *RLSPoliciesApplied value.
	PolicyInfoID
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
	// LimitHint is the "soft limit" of the number of result rows that may be
	// required. See physical.Required for details.
	LimitHint float64
	// Forecast is set only for scans; it is true if the stats for the scan were
	// forecasted rather than collected.
	Forecast bool
	// ForecastAt is set only for scans with forecasted stats; it is the time the
	// forecast was for (which could be in the past, present, or future).
	ForecastAt time.Time
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

	KVTime                optional.Duration
	KVContentionTime      optional.Duration
	KVLockWaitTime        optional.Duration
	KVLatchWaitTime       optional.Duration
	KVBytesRead           optional.Uint
	KVPairsRead           optional.Uint
	KVRowsRead            optional.Uint
	KVBatchRequestsIssued optional.Uint
	UsedStreamer          bool

	// Storage engine iterator statistics
	//
	// These statistics provide observability into the work performed by
	// low-level iterators during the execution of a query. Interpreting these
	// statistics requires some context on the mechanics of MVCC and LSMs.
	//
	//
	// SeekCount and StepCount record the cumulative number of seeks and steps
	// performed on Pebble iterators while servicing a query. Every scan or
	// point lookup within a KV range requires reading from two distinct
	// keyspaces within the storage engine: the MVCC row data keyspace and the
	// lock table. As an example, a typical point lookup will seek once within
	// the MVCC row data and once within the lock table, yielding SeekCount=2.
	//
	// Cockroach's MVCC layer is implemented (mostly) above Pebble, so the keys
	// returned by Pebble iterators can be MVCC garbage. An accumulation of MVCC
	// garbage amplifies the number of top-level Pebble iterator operations
	// performed, inflating {Seek,Step}Count relative to RowCount.
	//
	//
	// InternalSeekCount and InternalStepCount record the cumulative number of
	// low-level seeks and steps performed among LSM internal keys while
	// servicing a query. These internal keys have a many-to-one relationship
	// with the visible keys returned by the Pebble iterator due to the
	// mechanics of a LSM. When mutating the LSM, deleted or overwritten keys
	// are not updated in-place. Instead new 'internal keys' are recorded, and
	// Pebble iterators are responsible for ignoring obsolete, shadowed internal
	// keys. Asynchronous background compactions are responsible for eventually
	// removing obsolete internal keys to reclaim storage space and reduce the
	// amount of work iterators must perform.
	//
	// Internal keys that must be seeked or stepped through can originate from a
	// few sources:
	//   - Tombstones: Pebble models deletions as tombstone internal keys. An
	//     iterator observing a tombstone must skip both the tombstone itself
	//     and any shadowed keys. In Cockroach, these tombstones may be written
	//     within the MVCC keyspace due to transaction aborts and MVCC garbage
	//     collection. These tombstones may be written within the lock table
	//     during intent resolution.
	//   - Overwritten keys: Overwriting existing keys adds new internal keys,
	//     again requiring iterators to skip the obsolete shadowed keys.
	//     Overwritten keys should be rare since (a) CockroachDB's MVCC layer
	//     creates new KV versions (across different transactions) (b)
	//     transactions that write the same key multiple times will cause
	//     overwrites to the MVCC key and the lock table key, but they should be
	//     rare.
	//   - Concurrent writes: While a Pebble iterator is open, new keys may be
	//     committed to the LSM. The internal Pebble iterator will observe these
	//     new keys but skip over them to ensure the Pebble iterator provides a
	//     consistent view of the LSM state.
	//   - LSM snapshots: Cockroach's KV layer sometimes opens a 'LSM snapshot,'
	//     which provides a long-lived consistent view of the LSM at a
	//     particular moment. LSM snapshots prevent compactions from deleting
	//     obsolete keys if they're required for the LSM snapshot's consistent
	//     view. Snapshots don't introduce new obsolete internal keys, just
	//     postpone their reclamation during compactions.
	//   - MVCC range tombstones: Although the MVCC layer is mostly implemented
	//     above the Pebble interface, Pebble iterators perform a role in the
	//     implementation of MVCC range tombstones used for bulk deletions (eg,
	//     table drops, truncates, import cancellation) in 23.1+. In some cases,
	//     Pebble iterators will skip over garbage MVCC keys if they're marked
	//     as garbage by a MVCC range tombstone. Since stepping over these
	//     skipped keys is performed internally within Pebble, these steps are
	//     recorded within InternalStepCounts.
	//
	// Typically, a large amplification of internal iterator seeks/steps
	// relative to top-level seeks/steps is unexpected.

	StepCount         optional.Uint
	InternalStepCount optional.Uint
	SeekCount         optional.Uint
	InternalSeekCount optional.Uint

	ExecTime         optional.Duration
	MaxAllocatedMem  optional.Uint
	MaxAllocatedDisk optional.Uint
	SQLCPUTime       optional.Duration

	// SQLNodes on which this operator was executed.
	SQLNodes []string
	// KVNodes that served read requests.
	KVNodes []string
	// Regions on which this operator was executed. Includes both KV and SQL
	// processing.
	// Only being generated on EXPLAIN ANALYZE.
	Regions []string
	// UsedFollowerRead indicates whether at least some reads were served by the
	// follower replicas.
	UsedFollowerRead bool
}

// RLSPoliciesApplied contains information about the row-level security policies
// that were applied during the query.
type RLSPoliciesApplied struct {
	// PoliciesSkippedForRole is true if the user is a member of a role that is
	// exempt from all policies (e.g., admin).
	PoliciesSkippedForRole bool

	// PoliciesFilteredAllRows is true if RLS was enforced, and although policies
	// were applied, the result was that no rows were returned (typically represented
	// by an empty VALUES node). This is used when it's not possible to attribute
	// the result to specific table-level policy details (e.g., due to an empty
	// VALUES node replacing a scan).
	PoliciesFilteredAllRows bool

	// Policies is the list of policy IDs applied to the scan of a single table.
	// This applies to the table that this annotation was attached to. If this is
	// empty, it either means policies were skipped due to the role, or none were
	// applied.
	Policies opt.PolicyIDSet
}

// BuildPlanForExplainFn builds an execution plan against the given
// base factory.
type BuildPlanForExplainFn func(f Factory) (Plan, error)

// GroupingOrderType is the grouping column order type for group by and distinct
// operations.
type GroupingOrderType int

const (
	// NoStreaming means that the grouping columns have no useful order, so a
	// hash aggregator should be used.
	NoStreaming GroupingOrderType = iota
	// PartialStreaming means that the grouping columns are partially ordered, so
	// some optimizations can be done during aggregation.
	PartialStreaming
	// Streaming means that the grouping columns are fully ordered.
	Streaming
)

// JoinAlgorithm is the type of join algorithm used.
type JoinAlgorithm int8

// The following are all the supported join algorithms.
const (
	HashJoin JoinAlgorithm = iota
	CrossJoin
	IndexJoin
	LookupJoin
	MergeJoin
	InvertedJoin
	ApplyJoin
	ZigZagJoin
	NumJoinAlgorithms
)

// ScanCountType is the type of count of scan operations in a query.
type ScanCountType int

const (
	// ScanCount is the count of all scans in a query.
	ScanCount ScanCountType = iota
	// ScanWithStatsCount is the count of scans with statistics in a query.
	ScanWithStatsCount
	// ScanWithStatsForecastCount is the count of scans which used forecasted
	// statistics in a query.
	ScanWithStatsForecastCount
	// NumScanCountTypes is the total number of types of counts of scans.
	NumScanCountTypes
)
