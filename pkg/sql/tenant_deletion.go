// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DropTenantByID implements the tree.TenantOperator interface.
func (p *planner) DropTenantByID(
	ctx context.Context, tenID uint64, synchronousImmediateDrop bool,
) error {
	if err := p.validateDropTenant(ctx); err != nil {
		return err
	}

	info, err := GetTenantRecordByID(ctx, p.InternalSQLTxn(), roachpb.MustMakeTenantID(tenID))
	if err != nil {
		return errors.Wrap(err, "destroying tenant")
	}
	return dropTenantInternal(
		ctx,
		p.ExecCfg().Settings,
		p.InternalSQLTxn(),
		p.ExecCfg().JobRegistry,
		p.extendedEvalCtx.jobs,
		p.User(),
		info,
		synchronousImmediateDrop,
	)
}

func (p *planner) validateDropTenant(ctx context.Context) error {
	if p.EvalContext().TxnReadOnly {
		return readOnlyError("DROP TENANT")
	}

	const op = "drop"
	if err := p.RequireAdminRole(ctx, "drop tenant"); err != nil {
		return err
	}
	return rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, op)
}

func dropTenantInternal(
	ctx context.Context,
	settings *cluster.Settings,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	sessionJobs *txnJobsCollection,
	user username.SQLUsername,
	info *descpb.TenantInfo,
	synchronousImmediateDrop bool,
) error {
	const op = "destroy"
	tenID := info.ID
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
	}

	if info.State == descpb.TenantInfo_DROP {
		return errors.Errorf("tenant %d is already in state DROP", tenID)
	}

	// Mark the tenant as dropping.
	//
	// Cancel any running replication job on this tenant record.
	// The GCJob will wait for this job to enter a terminal state.
	if info.TenantReplicationJobID != 0 {
		job, err := jobRegistry.LoadJobWithTxn(ctx, info.TenantReplicationJobID, txn)
		if err != nil {
			return errors.Wrap(err, "loading tenant replication job for cancelation")
		}
		if err := job.WithTxn(txn).CancelRequested(ctx); err != nil {
			return errors.Wrapf(err, "canceling tenant replication job %d", info.TenantReplicationJobID)
		}
	}

	// TODO(ssd): We may want to implement a job that waits out
	// any running sql pods before enqueing the GC job.
	info.State = descpb.TenantInfo_DROP
	info.DroppedName = info.Name
	info.Name = ""
	if err := UpdateTenantRecord(ctx, settings, txn, info); err != nil {
		return errors.Wrap(err, "destroying tenant")
	}

	jobID, err := createGCTenantJob(ctx, jobRegistry, txn, user, tenID, synchronousImmediateDrop)
	if err != nil {
		return errors.Wrap(err, "scheduling gc job")
	}
	if synchronousImmediateDrop {
		sessionJobs.addCreatedJobID(jobID)
	}
	return nil
}

// createGCTenantJob issues a job that asynchronously clears the tenant's
// data and removes its tenant record.
func createGCTenantJob(
	ctx context.Context,
	jobRegistry *jobs.Registry,
	txn isql.Txn,
	user username.SQLUsername,
	tenID uint64,
	dropImmediately bool,
) (jobspb.JobID, error) {
	// Queue a GC job that will delete the tenant data and finally remove the
	// row from `system.tenants`.
	gcDetails := jobspb.SchemaChangeGCDetails{}
	gcDetails.Tenant = &jobspb.SchemaChangeGCDetails_DroppedTenant{
		ID:       tenID,
		DropTime: timeutil.Now().UnixNano(),
	}
	progress := jobspb.SchemaChangeGCProgress{}
	if dropImmediately {
		progress.Tenant = &jobspb.SchemaChangeGCProgress_TenantProgress{
			Status: jobspb.SchemaChangeGCProgress_CLEARING,
		}
	}
	gcJobRecord := jobs.Record{
		Description:   fmt.Sprintf("GC for tenant %d", tenID),
		Username:      user,
		Details:       gcDetails,
		Progress:      progress,
		NonCancelable: true,
	}
	jobID := jobRegistry.MakeJobID()
	if _, err := jobRegistry.CreateJobWithTxn(
		ctx, gcJobRecord, jobID, txn,
	); err != nil {
		return 0, err
	}
	return jobID, nil
}
