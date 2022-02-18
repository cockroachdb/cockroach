// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/errors"
)

func (m *visitor) CreateSchemaChangerJob(
	ctx context.Context, job scop.CreateSchemaChangerJob,
) error {
	return m.s.AddNewSchemaChangerJob(job.JobID, job.Statements, job.Authorization, job.DescriptorIDs)
}

func (m *visitor) UpdateSchemaChangerJob(
	ctx context.Context, op scop.UpdateSchemaChangerJob,
) error {
	return m.s.UpdateSchemaChangerJob(op.JobID, op.IsNonCancelable)
}

func (m *visitor) SetJobStateOnDescriptor(
	ctx context.Context, op scop.SetJobStateOnDescriptor,
) error {
	mut, err := m.s.CheckOutDescriptor(ctx, op.DescriptorID)
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

func (m *visitor) RemoveJobStateFromDescriptor(
	ctx context.Context, op scop.RemoveJobStateFromDescriptor,
) error {
	{
		_, err := m.s.GetDescriptor(ctx, op.DescriptorID)

		// If we're clearing the status, we might have already deleted the
		// descriptor. Permit that by detecting the prior deletion and
		// short-circuiting.
		//
		// TODO(ajwerner): Ideally we'd model the clearing of the job dependency as
		// an operation which has to happen before deleting the descriptor. If that
		// were the case, this error would become unexpected.
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
	}

	mut, err := m.s.CheckOutDescriptor(ctx, op.DescriptorID)
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
