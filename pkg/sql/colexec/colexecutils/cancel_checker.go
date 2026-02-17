// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colexecutils

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/cancelchecker"
)

// CancelChecker is a colexecop.Operator that checks whether query cancellation
// has occurred. The check happens on every batch.
//
// It also can be used for its utility Check*() methods, but it must be
// initialized still.
type CancelChecker struct {
	colexecop.OneInputNode
	colexecop.InitHelper
	colexecop.NonExplainable

	// Number of times check() has been called since last context cancellation
	// check.
	callsSinceLastCheck uint32

	// sqlCPUHandle is the parent handle from the context. We store it here
	// and defer calling RegisterGoroutine until the first CheckEveryCall,
	// because Init may run on a different goroutine (e.g., the flow
	// coordinator) than the one that later calls CheckEveryCall (a worker
	// goroutine spawned by ParallelUnorderedSynchronizer). RegisterGoroutine
	// binds to the caller's gid, so it must be called on the executing
	// goroutine to get the correct per-goroutine CPU handle.
	sqlCPUHandle *admission.SQLCPUHandle
	cpuHandle    *admission.GoroutineCPUHandle
}

var _ colexecop.Operator = &CancelChecker{}

// NewCancelChecker creates a new CancelChecker.
func NewCancelChecker(op colexecop.Operator) *CancelChecker {
	return &CancelChecker{OneInputNode: colexecop.NewOneInputNode(op)}
}

// Init is part of colexecop.Operator interface.
func (c *CancelChecker) Init(ctx context.Context) {
	if !c.InitHelper.Init(ctx) {
		return
	}
	if c.Input != nil {
		// In some cases, the cancel checker is used as a utility to provide
		// Check*() methods, and the input remains nil then.
		c.Input.Init(c.Ctx)
	}
	// Store the parent handle but don't register yet — Init may be running on
	// a different goroutine than the one that will call CheckEveryCall.
	c.sqlCPUHandle = admission.SQLCPUHandleFromContext(ctx)
}

// Next is part of colexecop.Operator interface.
func (c *CancelChecker) Next() (coldata.Batch, *execinfrapb.ProducerMetadata) {
	c.CheckEveryCall()
	return c.Input.Next()
}

// Interval of Check() calls to wait between checks for context cancellation.
// The value is a power of 2 to allow the compiler to use bitwise AND instead
// of division.
const cancelCheckInterval = 1024

// Check panics with a query canceled error if the associated query has been
// canceled. The check is performed on every cancelCheckInterval'th call. This
// should be used only during long-running operations.
func (c *CancelChecker) Check() {
	if c.callsSinceLastCheck%cancelCheckInterval == 0 {
		c.CheckEveryCall()
	}

	// Increment. This may rollover when the 32-bit capacity is reached, but
	// that's all right.
	c.callsSinceLastCheck++
}

// CheckEveryCall panics with query canceled error (which will be caught at the
// materializer level and will be propagated forward as metadata) if the
// associated query has been canceled. The check is performed on every call.
func (c *CancelChecker) CheckEveryCall() {
	select {
	case <-c.Ctx.Done():
		colexecerror.ExpectedError(cancelchecker.QueryCanceledError)
	default:
	}
	if c.cpuHandle == nil && c.sqlCPUHandle != nil {
		// Lazily register on the goroutine that actually executes the operator.
		c.cpuHandle = c.sqlCPUHandle.RegisterGoroutine()
	}
	if c.cpuHandle != nil {
		err := c.cpuHandle.MeasureAndAdmit(c.Ctx)
		if err != nil {
			colexecerror.ExpectedError(cancelchecker.QueryCanceledError)
		}
	}
}
