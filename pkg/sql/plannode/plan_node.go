// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plannode

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/planbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// PlanNode defines the interface for executing a query or portion of a query.
//
// The following methods apply to planNodes and contain special cases
// for each type; they thus need to be extended when adding/removing
// planNode instances:
// - planVisitor.visit()           (walk.go)
// - planNodeNames                 (walk.go)
// - setLimitHint()                (limit_hint.go)
// - planColumns()                 (plan_columns.go)
type PlanNode interface {
	// StartExec initializes execution for the planNode.
	StartExec(params planbase.RunParams) error

	// Next performs one unit of work, returning false if an error is
	// encountered or if there is no more work to do. For statements
	// that return a result set, the Values() method will return one row
	// of results each time that Next() returns true.
	//
	// Available after StartExec(). It is illegal to call Next() after it returns
	// false.
	Next(params planbase.RunParams) (bool, error)

	// Values returns the values at the current row. The result is only valid
	// until the next call to Next().
	//
	// Available after Next().
	Values() tree.Datums

	// Close terminates the planNode execution and releases its resources.
	// This method should be called if the node has been used in any way (any
	// methods on it have been called) after it was constructed. Note that this
	// doesn't imply that StartExec() has been necessarily called.
	//
	// This method must not be called during execution - the planNode
	// tree must remain "live" and readable via walk() even after
	// execution completes.
	//
	// The node must not be used again after this method is called. Some nodes put
	// themselves back into memory pools on Close.
	Close(ctx context.Context)

	InputCount() int
	Input(i int) (PlanNode, error)
	SetInput(i int, p PlanNode) error
}

// MutationPlanNode is a specification of PlanNode for mutations operations
// (those that insert/update/delete/etc rows).
type MutationPlanNode interface {
	PlanNode

	// RowsWritten returns the number of table rows modified by this planNode.
	// It does not include rows written to secondary indexes. It should only be
	// called once Next returns false.
	RowsWritten() int64

	// IndexRowsWritten returns the number of primary and secondary index rows
	// modified by this planNode. It is always >= RowsWritten. It should only be
	// called once Next returns false.
	IndexRowsWritten() int64

	// IndexBytesWritten returns the number of primary and secondary index bytes
	// modified by this planNode. It should only be called once Next returns
	// false.
	IndexBytesWritten() int64

	// ReturnsRowsAffected indicates that the planNode returns the number of
	// rows affected by the mutation, rather than the rows themselves.
	ReturnsRowsAffected() bool

	// KvCPUTime returns the cumulative CPU time (in nanoseconds) that KV reported
	// in BatchResponse headers during the execution of this mutation. It should
	// only be called once Next returns false.
	KvCPUTime() int64
}

// PlanNodeReadingOwnWrites can be implemented by planNodes which do
// not use the standard SQL principle of reading at the snapshot
// established at the start of the transaction. It requests that
// the top-level (shared) `startExec` function disable stepping
// mode for the duration of the node's `StartExec()` call.
//
// This done e.g. for most DDL statements that perform multiple KV
// operations on descriptors, expecting to read their own writes.
//
// Note that only `StartExec()` runs with the modified stepping mode,
// not the `Next()` methods. This interface (and the idea of
// temporarily disabling stepping mode) is neither sensical nor
// applicable to planNodes whose execution is interleaved with
// that of others.
type PlanNodeReadingOwnWrites interface {
	// ReadingOwnWrites is a marker interface.
	ReadingOwnWrites()
}

// Lowercase aliases for backward compatibility within this package
type planNode = PlanNode
type mutationPlanNode = MutationPlanNode
type planNodeReadingOwnWrites = PlanNodeReadingOwnWrites
type runParams = planbase.RunParams
