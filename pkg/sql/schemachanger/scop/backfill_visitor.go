// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scop

import "context"

// BackfillOp is an operation which can be visited by BackfillVisitor
type BackfillOp interface {
	Op
	Visit(context.Context, BackfillVisitor) error
}

// BackfillVisitor is a visitor for BackfillOp operations.
type BackfillVisitor interface {
	IndexBackfill(context.Context, IndexBackfill) error
}

// Visit is part of the BackfillOp interface.
func (op IndexBackfill) Visit(ctx context.Context, v BackfillVisitor) error {
	return v.IndexBackfill(ctx, op)
}
