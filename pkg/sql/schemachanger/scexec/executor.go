// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// ExecuteStage executes the provided ops. The ops must all be of the same type.
func ExecuteStage(ctx context.Context, deps Dependencies, ops scop.Ops) error {
	// It is perfectly valid to have empty stage after optimizations /
	// transformations.
	if ops == nil {
		log.Infof(ctx, "skipping execution, no operations in this stage")
		return nil
	}
	log.Infof(ctx, "executing %d ops of type %s", len(ops.Slice()), ops.Type().String())
	switch typ := ops.Type(); typ {
	case scop.MutationType:
		return executeDescriptorMutationOps(ctx, deps, ops.Slice())
	case scop.BackfillType:
		return executeBackfillOps(ctx, deps, ops.Slice())
	case scop.ValidationType:
		return executeValidationOps(ctx, deps, ops.Slice())
	default:
		return errors.AssertionFailedf("unknown ops type %d", typ)
	}
}
