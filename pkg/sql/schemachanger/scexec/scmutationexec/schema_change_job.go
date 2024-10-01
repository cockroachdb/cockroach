// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

func (i *immediateVisitor) UndoAllInTxnImmediateMutationOpSideEffects(
	ctx context.Context, _ scop.UndoAllInTxnImmediateMutationOpSideEffects,
) error {
	i.Reset()
	return nil
}

func (d *deferredVisitor) CreateSchemaChangerJob(
	ctx context.Context, job scop.CreateSchemaChangerJob,
) error {
	return d.AddNewSchemaChangerJob(
		job.JobID,
		job.Statements,
		job.NonCancelable,
		job.Authorization,
		catalog.MakeDescriptorIDSet(job.DescriptorIDs...),
		job.RunningStatus,
	)
}

func (d *deferredVisitor) UpdateSchemaChangerJob(
	ctx context.Context, op scop.UpdateSchemaChangerJob,
) error {
	return d.DeferredMutationStateUpdater.UpdateSchemaChangerJob(
		op.JobID,
		op.IsNonCancelable,
		op.RunningStatus,
		catalog.MakeDescriptorIDSet(op.DescriptorIDsToRemove...),
	)
}

func (i *immediateVisitor) SetJobStateOnDescriptor(
	ctx context.Context, op scop.SetJobStateOnDescriptor,
) error {
	mut, err := i.checkOutDescriptor(ctx, op.DescriptorID)
	if err != nil {
		return err
	}

	// TODO(ajwerner): This check here could be used as a check for a concurrent
	// attempt to place a schema change job on a descriptor. It might deserve
	// a special error that is not an assertion if we didn't choose to handle
	// this at a higher level.
	expected := op.State.JobID
	if op.Initialize {
		expected = jobspb.InvalidJobID
	}
	scs := mut.GetDeclarativeSchemaChangerState()
	if scs != nil {
		if op.Initialize || scs.JobID != expected {
			return errors.AssertionFailedf(
				"unexpected schema change job ID %d on table %d, expected %d",
				scs.JobID, op.DescriptorID, expected,
			)
		}
	}
	mut.SetDeclarativeSchemaChangerState(op.State.Clone())
	return nil
}

func (i *immediateVisitor) RemoveJobStateFromDescriptor(
	ctx context.Context, op scop.RemoveJobStateFromDescriptor,
) error {
	mut, err := i.checkOutDescriptor(ctx, op.DescriptorID)
	if err != nil {
		return err
	}
	existing := mut.GetDeclarativeSchemaChangerState()
	if existing == nil {
		return errors.AssertionFailedf(
			"expected schema change state with job ID %d on table %d, found none",
			op.JobID, op.DescriptorID,
		)
	} else if existing.JobID != op.JobID {
		return errors.AssertionFailedf(
			"unexpected schema change job ID %d on table %d, expected %d",
			existing.JobID, op.DescriptorID, op.JobID,
		)
	}
	mut.SetDeclarativeSchemaChangerState(nil)
	return nil
}
