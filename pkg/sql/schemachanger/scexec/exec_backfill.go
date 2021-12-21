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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec/scmutationexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func executeBackfillOps(ctx context.Context, deps Dependencies, execute []scop.Op) error {
	// TODO(ajwerner): Run backfills in parallel. Will require some plumbing for
	// checkpointing at the very least.

	for _, op := range execute {
		var err error
		switch op := op.(type) {
		case *scop.BackfillIndex:
			err = executeIndexBackfillOp(ctx, deps, op)
		default:
			panic("unimplemented")
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func executeIndexBackfillOp(ctx context.Context, deps Dependencies, op *scop.BackfillIndex) error {
	// Note that the leasing here is subtle. We'll avoid the cache and ensure that
	// the descriptor is read from the store. That means it will not be leased.
	// This relies on changed to the descriptor not messing with this index
	// backfill.
	desc, err := deps.Catalog().MustReadImmutableDescriptor(ctx, op.TableID)
	if err != nil {
		return err
	}
	table, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return catalog.WrapTableDescRefErr(desc.GetID(), catalog.NewDescriptorTypeError(desc))
	}
	mut, err := scmutationexec.FindMutation(table, scmutationexec.MakeIndexIDMutationSelector(op.IndexID))
	if err != nil {
		return err
	}

	// Must be the right index given the above call.
	idxToBackfill := mut.AsIndex()

	// Split off the index span prior to backfilling.
	if err := deps.IndexSpanSplitter().MaybeSplitIndexSpans(ctx, table, idxToBackfill); err != nil {
		return err
	}
	return deps.IndexBackfiller().BackfillIndex(ctx, deps.JobProgressTracker(), table, table.GetPrimaryIndexID(), idxToBackfill.GetID())
}
