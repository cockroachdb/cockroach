// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecbase/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// CancelChecker is an Operator that checks whether query cancellation has
// occurred. The check happens on every batch.
type CancelChecker struct {
	OneInputNode
	NonExplainable

	// Number of times check() has been called since last context cancellation
	// check.
	callsSinceLastCheck uint32
}

// Init is part of the Operator interface.
func (c *CancelChecker) Init() {
	c.input.Init()
}

var _ colexecbase.Operator = &CancelChecker{}

// NewCancelChecker creates a new CancelChecker.
func NewCancelChecker(op colexecbase.Operator) *CancelChecker {
	return &CancelChecker{OneInputNode: NewOneInputNode(op)}
}

// Next is part of Operator interface.
func (c *CancelChecker) Next(ctx context.Context) coldata.Batch {
	c.checkEveryCall(ctx)
	return c.input.Next(ctx)
}

// Interval of check() calls to wait between checks for context cancellation.
// The value is a power of 2 to allow the compiler to use bitwise AND instead
// of division.
const cancelCheckInterval = 1024

// check panics with a query canceled error if the associated query has been
// canceled. The check is performed on every cancelCheckInterval'th call. This
// should be used only during long-running operations.
func (c *CancelChecker) check(ctx context.Context) {
	if c.callsSinceLastCheck%cancelCheckInterval == 0 {
		c.checkEveryCall(ctx)
	}

	// Increment. This may rollover when the 32-bit capacity is reached, but
	// that's all right.
	c.callsSinceLastCheck++
}

// checkEveryCall panics with query canceled error (which will be caught at the
// materializer level and will be propagated forward as metadata) if the
// associated query has been canceled. The check is performed on every call.
func (c *CancelChecker) checkEveryCall(ctx context.Context) {
	select {
	case <-ctx.Done():
		colexecerror.ExpectedError(sqlbase.QueryCanceledError)
	default:
	}
}
