// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

// executeMutationOps will visit each operation, accumulating
// side effects into a mutationVisitorState object, and then writing out
// those side effects using the provided deps.
func executeMutationOps(
	ctx context.Context, deps Dependencies, phase scop.Phase, ops []scop.Op,
) error {
	// Execute immediate ops first.
	uvs := immediateState{}
	uv := scmutationexec.NewImmediateVisitor(&uvs, deps.Clock(), deps.Catalog())
	for _, op := range ops {
		if iop, ok := op.(scop.ImmediateMutationOp); ok {
			if err := iop.Visit(ctx, uv); err != nil {
				return errors.Wrapf(err, "%T: %v", op, op)
			}
		}
	}
	if err := uvs.exec(ctx, deps.Catalog()); err != nil {
		return err
	}
	// Exit early when in statement phase to not persist any side effects.
	if phase == scop.StatementPhase {
		return nil
	}
	// Execute deferred ops last.
	nvs := deferredState{}
	nv := scmutationexec.NewDeferredVisitor(&nvs)
	for _, op := range ops {
		if dop, ok := op.(scop.DeferredMutationOp); ok {
			if err := dop.Visit(ctx, nv); err != nil {
				return errors.Wrapf(err, "%T: %v", op, op)
			}
		}
	}
	return nvs.exec(
		ctx,
		deps.Catalog(),
		deps.TransactionalJobRegistry(),
		deps.DescriptorMetadataUpdater(ctx),
		deps.StatsRefresher(),
		deps.IndexSpanSplitter(),
	)
}
