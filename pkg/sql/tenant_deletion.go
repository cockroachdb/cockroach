// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DropTenantByID implements the tree.TenantOperator interface.
func (p *planner) DropTenantByID(
	ctx context.Context, tenID uint64, synchronousImmediateDrop, ignoreServiceMode bool,
) error {
	if p.SessionData().SafeUpdates {
		err := errors.Newf("DROP VIRTUAL CLUSTER causes irreversible data loss")
		err = errors.WithMessage(err, "rejected (via sql_safe_updates)")
		err = pgerror.WithCandidateCode(err, pgcode.Warning)
		return err
	}

	if p.EvalContext().TxnReadOnly {
		return readOnlyError("DROP VIRTUAL CLUSTER")
	}

	if err := rejectIfCantCoordinateMultiTenancy(p.execCfg.Codec, "drop", p.execCfg.Settings); err != nil {
		return err
	}

	if err := CanManageTenant(ctx, p); err != nil {
		return err
	}

	info, err := GetTenantRecordByID(ctx, p.InternalSQLTxn(), roachpb.MustMakeTenantID(tenID), p.ExecCfg().Settings)
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
		ignoreServiceMode,
	)
}

func dropTenantInternal(
	ctx context.Context,
	settings *cluster.Settings,
	txn isql.Txn,
	jobRegistry *jobs.Registry,
	sessionJobs *txnJobsCollection,
	user username.SQLUsername,
	info *mtinfopb.TenantInfo,
	synchronousImmediateDrop bool,
	ignoreServiceMode bool,
) error {
	const op = "destroy"
	tenID := info.ID
	if err := rejectIfSystemTenant(tenID, op); err != nil {
		return err
	}

	if ignoreServiceMode {
		// Compatibility with CC serverless use of
		// crdb_internal.destroy_tenant(): we want to disable the check
		// immediately below, as well as the additional check performed
		// inside UpdateTenantRecord() (via validateTenantInfo).
		info.ServiceMode = mtinfopb.ServiceModeNone
	}
	// We can only check the service mode after upgrading to a version
	// that supports the service mode column.
	if info.ServiceMode != mtinfopb.ServiceModeNone {
		return errors.WithHint(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"cannot drop tenant %q (%d) in service mode %v", info.Name, tenID, info.ServiceMode),
			"Use ALTER VIRTUAL CLUSTER STOP SERVICE before DROP VIRTUAL CLUSTER.")
	}

	if info.DataState == mtinfopb.DataStateDrop {
		return errors.Errorf("tenant %q (%d) is already in data state DROP", info.Name, tenID)
	}

	// Mark the tenant as dropping.
	//
	// Cancel any running replication job on this tenant record.
	// The GCJob will wait for this job to enter a terminal state.
	if info.PhysicalReplicationConsumerJobID != 0 {
		job, err := jobRegistry.LoadJobWithTxn(ctx, info.PhysicalReplicationConsumerJobID, txn)
		if err != nil {
			return errors.Wrap(err, "loading tenant replication job for cancelation")
		}
		if err := job.WithTxn(txn).CancelRequested(ctx); err != nil {
			return errors.Wrapf(err, "canceling tenant replication job %d", info.PhysicalReplicationConsumerJobID)
		}
	}

	// TODO(ssd): We may want to implement a job that waits out
	// any running sql pods before enqueing the GC job.
	info.DataState = mtinfopb.DataStateDrop
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
