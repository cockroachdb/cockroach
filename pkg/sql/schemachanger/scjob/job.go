// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scjob

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/descmetadata"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scrun"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
	job           *jobs.Job
	deps          scrun.JobRunDependencies
	rollbackCause error
}

var _ jobs.TraceableJob = (*newSchemaChangeResumer)(nil)

// ForceRealSpan implements the TraceableJob interface.
func (n *newSchemaChangeResumer) ForceRealSpan() bool {
	return true
}

// DumpTraceAfterRun implements the TraceableJob interface.
func (n *newSchemaChangeResumer) DumpTraceAfterRun() bool {
	return true
}

func (n *newSchemaChangeResumer) Resume(ctx context.Context, execCtxI interface{}) (err error) {
	return n.run(ctx, execCtxI)
}

func (n *newSchemaChangeResumer) OnFailOrCancel(
	ctx context.Context, execCtxI interface{}, err error,
) error {
	execCtx := execCtxI.(sql.JobExecContext)
	execCfg := execCtx.ExecCfg()
	// Permanent error has been hit, so there is no rollback
	// from here. Only if the status is reverting will these be
	// treated as fatal.
	if jobs.IsPermanentJobError(err) && n.job.State() == jobs.StateReverting {
		log.Warningf(ctx, "schema change will not rollback; permanent error detected: %v", err)
		return nil
	}
	n.rollbackCause = err

	// Clean up any protected timestamps as a last resort, in case the job
	// execution never did itself.
	if err := execCfg.ProtectedTimestampManager.Unprotect(ctx, n.job); err != nil {
		log.Warningf(ctx, "unable to revert protected timestamp %v", err)
	}
	return n.run(ctx, execCtx)
}

// CollectProfile writes the current phase's explain output, captured earlier,
// to the jobs_info table.
func (n *newSchemaChangeResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	p := execCtx.(sql.JobExecContext)
	return p.ExecCfg().InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		exOp := n.deps.GetExplain()
		filename := fmt.Sprintf("explain-phase.%s.txt", timeutil.Now().Format("20060102_150405.00"))
		return jobs.WriteExecutionDetailFile(ctx, filename, []byte(exOp), txn, n.job.ID())
	})
}

func (n *newSchemaChangeResumer) run(ctx context.Context, execCtxI interface{}) error {
	execCtx := execCtxI.(sql.JobExecContext)
	execCfg := execCtx.ExecCfg()
	if err := n.job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
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
	n.deps = scdeps.NewJobRunDependencies(
		execCfg.CollectionFactory,
		execCfg.InternalDB,
		execCfg.IndexBackfiller,
		execCfg.IndexSpanSplitter,
		execCfg.IndexMerger,
		NewRangeCounter(execCfg.DB, execCfg.DistSQLPlanner),
		func(txn isql.Txn) scrun.EventLogger {
			return sql.NewSchemaChangerRunEventLogger(txn, execCfg)
		},
		execCfg.JobRegistry,
		n.job,
		execCfg.Codec,
		execCfg.Settings,
		execCfg.Validator,
		func(ctx context.Context, descriptors *descs.Collection, txn isql.Txn) scexec.DescriptorMetadataUpdater {
			return descmetadata.NewMetadataUpdater(ctx,
				txn,
				descriptors,
				&execCfg.Settings.SV,
				execCtx.SessionData(),
			)
		},
		execCfg.StatsRefresher,
		execCfg.DeclarativeSchemaChangerTestingKnobs,
		payload.Statement,
		execCtx.SessionData(),
		execCtx.ExtendedEvalContext().Tracing.KVTracingEnabled(),
	)

	// If there are no descriptors left, then we can short circuit here.
	if len(payload.DescriptorIDs) == 0 {
		return nil
	}

	startTime := timeutil.FromUnixMicros(payload.StartedMicros)
	err := scrun.RunSchemaChangesInJob(
		ctx,
		execCfg.DeclarativeSchemaChangerTestingKnobs,
		n.deps,
		n.job.ID(),
		startTime,
		payload.DescriptorIDs,
		n.rollbackCause,
	)
	// Return permanent errors back, otherwise we will try to retry
	if sql.IsPermanentSchemaChangeError(err) {
		// If a descriptor can't be found, we additionally mark the error as a
		// permanent job error, so that non-cancelable jobs don't get retried. If a
		// descriptor has gone missing, it isn't likely to come back.
		// We also mark assertion errors as permanent job errors, since they are
		// never expected.
		if errors.IsAny(
			err, catalog.ErrDescriptorNotFound, catalog.ErrDescriptorDropped, catalog.ErrReferencedDescriptorNotFound,
		) || errors.HasAssertionFailure(err) {
			return jobs.MarkAsPermanentJobError(err)
		}
		return err
	}
	if err != nil {
		return jobs.MarkAsRetryJobError(err)
	}
	return nil
}
