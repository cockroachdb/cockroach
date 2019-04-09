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

// CancelChecker is an Operator that checks whether a query cancellation has
// occurred. The check happens on every batch.
// TODO(yuzefovich): is every batch an overkill? What about operators that
// spend a lot of time working on the first batch (i.e. sorter)?
type CancelChecker struct {
	Operator
}

var _ Operator = &CancelChecker{}

// NewCancelChecker creates a new CancelChecker.
func NewCancelChecker(op Operator) *CancelChecker {
	return &CancelChecker{Operator: op}
}

// Init is part of Operator interface.
func (c *CancelChecker) Init() {
	c.Operator.Init()
}

// Next is part of Operator interface.
func (c *CancelChecker) Next(ctx context.Context) coldata.Batch {
	checkCancellation(ctx)
	return c.Operator.Next(ctx)
}

// checkCancellation checks whether ctx is done, and if so, it panics with
// query canceled error (which will be caught at the materializer level and
// will be propagated forward as metadata).
func checkCancellation(ctx context.Context) {
	select {
	case <-ctx.Done():
		panic(sqlbase.QueryCanceledError)
	default:
	}
}

const checkInterval = 1024

// checkCancellationWithInterval checks whether ctx is done only if i is
// divisible by checkInterval.
func checkCancellationWithInterval(ctx context.Context, i int) {
	if i%checkInterval == 0 {
		checkCancellation(ctx)
	}
}
