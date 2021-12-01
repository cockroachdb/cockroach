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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
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

// UpdateDescriptorJobIDs updates the job ID for the schema change on the
// specified set of table descriptors.
func UpdateDescriptorJobIDs(
	ctx context.Context,
	cat Catalog,
	descIDs []descpb.ID,
	expectedID jobspb.JobID,
	newID jobspb.JobID,
) error {
	b := cat.NewCatalogChangeBatcher()
	for _, id := range descIDs {
		// Confirm the descriptor is a table, view or sequence
		// since we can only lock those types.
		desc, err := cat.MustReadMutableDescriptor(ctx, id)
		// It's valid for descriptors to get removed after a stage executes, so
		// skip anything that has disappeared.
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			continue
		}
		if err != nil {
			return err
		}

		// Currently, all "locking" schema changes are on tables. This will probably
		// need to be expanded at least to types.
		table, ok := desc.(*tabledesc.Mutable)
		if !ok {
			continue
		}
		if oldID := jobspb.JobID(table.NewSchemaChangeJobID); oldID != expectedID {
			return errors.AssertionFailedf(
				"unexpected schema change job ID %d on table %d, expected %d", oldID, table.GetID(), expectedID)
		}
		table.NewSchemaChangeJobID = int64(newID)
		if err := b.CreateOrUpdateDescriptor(ctx, table); err != nil {
			return err
		}
	}
	return b.ValidateAndRun(ctx)
}
