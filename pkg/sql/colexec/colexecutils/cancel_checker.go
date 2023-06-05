// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexecutils

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecop"
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
}

// Next is part of colexecop.Operator interface.
func (c *CancelChecker) Next() coldata.Batch {
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
}
