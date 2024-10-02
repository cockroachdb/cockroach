// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

// ExecuteStage executes the provided ops. The ops must all be of the same type.
func ExecuteStage(ctx context.Context, deps Dependencies, phase scop.Phase, ops []scop.Op) error {
	if len(ops) == 0 {
		return nil
	}
	typ := ops[0].Type()
	switch typ {
	case scop.MutationType:
		return executeMutationOps(ctx, deps, phase, ops)
	case scop.BackfillType:
		return executeBackfillOps(ctx, deps, ops)
	case scop.ValidationType:
		return executeValidationOps(ctx, deps, ops)
	default:
		return errors.AssertionFailedf("unknown ops type %d", typ)
	}
}
