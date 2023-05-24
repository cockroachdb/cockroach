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
	cancelchecker.CancelChecker
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
	c.CancelChecker.Reset(c.Ctx)

	if c.Input != nil {
		// In some cases, the cancel checker is used as a utility to provide
		// Check*() methods, and the input remains nil then.
		c.Input.Init(c.Ctx)
	}
}

// Next is part of colexecop.Operator interface.
func (c *CancelChecker) Next() coldata.Batch {
	c.Check()
	return c.Input.Next()
}

// Check panics with query canceled error (which will be caught at the
// materializer level and will be propagated forward as metadata) if the
// associated query has been canceled.
func (c *CancelChecker) Check() {
	if err := c.CancelChecker.Check(); err != nil {
		colexecerror.ExpectedError(err)
	}
}
