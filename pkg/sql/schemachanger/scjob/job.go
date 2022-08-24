// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
)

func init() {
	jobs.RegisterConstructor(jobspb.TypeNewSchemaChange, func(
		job *jobs.Job, settings *cluster.Settings,
	) jobs.Resumer {
		return &newSchemaChangeResumer{
			job: job,
		}
	}, jobs.UsesTenantCostControl)
}

type newSchemaChangeResumer struct {
	job      *jobs.Job
	rollback bool
}

func (n *newSchemaChangeResumer) Resume(ctx context.Context, execCtxI interface{}) (err error) {
	return n.run(ctx, execCtxI)
}

func (n *newSchemaChangeResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, _ error,
) error {
	n.rollback = true
	return n.run(ctx, execCtx)
}

func (n *newSchemaChangeResumer) run(ctx context.Context, execCtxI interface{}) error {
	execCtx := execCtxI.(sql.JobExecContext)
	execCfg := execCtx.ExecCfg()
	if err := n.job.Update(ctx, nil /* txn */, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		return nil
	}); err != nil {
		// TODO(ajwerner): Detect transient errors and classify as retriable here or
		// in the jobs package.
		return err
	}
	// TODO(ajwerner): Wait for leases on all descriptors before starting to
	// avoid restarts.
	if err := execCfg.JobRegistry.CheckPausepoint("newschemachanger.before.exec"); err != nil {
		return err
	}
	payload := n.job.Payload()
	deps := scdeps.NewJobRunDependencies(
		execCfg.CollectionFactory,
		execCfg.DB,
		execCfg.IndexBackfiller,
		execCfg.IndexMerger,
		NewRangeCounter(execCfg.DB, execCfg.DistSQLPlanner),
		func(txn *kv.Txn) scexec.EventLogger {
			return sql.NewSchemaChangerEventLogger(txn, execCfg, 0)
		},
		execCfg.JobRegistry,
		n.job,
		execCfg.Codec,
		execCfg.Settings,
		execCfg.IndexValidator,
		func(ctx context.Context, descriptors *descs.Collection, txn *kv.Txn) scexec.DescriptorMetadataUpdater {
			return descmetadata.NewMetadataUpdater(ctx,
				execCfg.InternalExecutorFactory,
				descriptors,
				&execCfg.Settings.SV,
				txn,
				execCtx.SessionData(),
			)
		},
		execCfg.StatsRefresher,
		execCfg.DeclarativeSchemaChangerTestingKnobs,
		payload.Statement,
		execCtx.SessionData(),
		execCtx.ExtendedEvalContext().Tracing.KVTracingEnabled(),
	)

	err := scrun.RunSchemaChangesInJob(
		ctx,
		execCfg.DeclarativeSchemaChangerTestingKnobs,
		execCfg.Settings,
		deps,
		n.job.ID(),
		payload.DescriptorIDs,
		n.rollback,
	)
	// Return permanent errors back, otherwise we will try to retry
	if sql.IsPermanentSchemaChangeError(err) {
		return err
	}
	if err != nil {
		return jobs.MarkAsRetryJobError(err)
	}
	return nil
}
