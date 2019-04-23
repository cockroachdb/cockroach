// Copyright 2019 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// CancelChecker is an Operator that checks whether query cancellation has
// occurred. The check happens on every batch.
type CancelChecker struct {
	Operator

	// Number of times check() has been called since last context cancellation
	// check.
	callsSinceLastCheck uint32
}

var _ Operator = &CancelChecker{}

// NewCancelChecker creates a new CancelChecker.
func NewCancelChecker(op Operator) *CancelChecker {
	return &CancelChecker{Operator: op}
}

// Next is part of Operator interface.
func (c *CancelChecker) Next(ctx context.Context) coldata.Batch {
	c.checkEveryCall(ctx)
	return c.Operator.Next(ctx)
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
		panic(sqlbase.QueryCanceledError)
	default:
	}
}
