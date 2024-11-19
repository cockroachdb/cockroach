// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cancelchecker

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// CancelChecker is a helper object for repeatedly checking whether the associated context
// has been canceled or not. Encapsulates all logic for waiting for cancelCheckInterval
// rows before actually checking for cancellation. The cancellation check
// has a significant time overhead, so it's not checked in every iteration.
// TODO(yuzefovich): audit all processors to make sure that the ones that should
// use the cancel checker actually do so.
type CancelChecker struct {
	// Reference to associated context to check.
	ctx context.Context

	// Number of times Check() has been called since last context cancellation
	// check.
	callsSinceLastCheck uint32

	// The number of Check() calls to skip the context cancellation check.
	checkInterval uint32
}

// The default interval of Check() calls to wait between checks for context
// cancellation. The value is a power of 2 to allow the compiler to use
// bitwise AND instead of division.
const cancelCheckInterval = 1024

// Check returns an error if the associated query has been canceled.
func (c *CancelChecker) Check() error {

	if atomic.LoadUint32(&c.callsSinceLastCheck)%c.checkInterval == 0 {
		select {
		case <-c.ctx.Done():
			// Once the context is canceled, we no longer increment
			// callsSinceLastCheck and will fall into this path on subsequent calls
			// to Check().
			return QueryCanceledError
		default:
		}
	}

	// Increment. This may rollover when the 32-bit capacity is reached,
	// but that's all right.
	atomic.AddUint32(&c.callsSinceLastCheck, 1)
	return nil
}

// Reset resets this cancel checker with a fresh context. Parameter
// checkInterval is optional and specifies an override for the default check
// interval of 1024. Note that checkInterval is expected to be set to a power of
// 2 by the caller, but Reset does no explicit checking of this.
func (c *CancelChecker) Reset(ctx context.Context, checkInterval ...uint32) {
	*c = CancelChecker{
		ctx: ctx,
	}
	if len(checkInterval) > 0 {
		c.checkInterval = checkInterval[0]
	} else {
		c.checkInterval = cancelCheckInterval
	}
}

// QueryCanceledError is an error representing query cancellation.
var QueryCanceledError = pgerror.New(
	pgcode.QueryCanceled, "query execution canceled")
