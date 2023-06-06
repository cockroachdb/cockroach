// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cancelchecker

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
)

// CancelChecker is a helper object for repeatedly checking whether the associated context
// has been canceled or not.
// It is cheaper than the repeated checks of ctx.Done() channel.
// TODO(yuzefovich): audit all processors to make sure that the ones that should
// use the cancel checker actually do so.
type CancelChecker struct {
	*ctxutil.FastDoneCheckerContext
}

// Check returns an error if the associated query has been canceled.
func (c *CancelChecker) Check() error {
	if c.ContextDone() {
		return QueryCanceledError
	}
	return nil
}

// Reset resets this cancel checker with a fresh context.
func (c *CancelChecker) Reset(ctx context.Context) {
	var doneChecker ctxutil.FastDoneCheckerContext
	doneChecker.Init(ctx)
	c.FastDoneCheckerContext = &doneChecker
}

// QueryCanceledError is an error representing query cancellation.
var QueryCanceledError = pgerror.New(
	pgcode.QueryCanceled, "query execution canceled")
