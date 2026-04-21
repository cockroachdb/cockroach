// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnmode"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// resumeTransactionalLdr runs the transactional LDR ingestion loop.
func (r *logicalReplicationResumer) resumeTransactionalLdr(
	ctx context.Context, jobExecCtx sql.JobExecContext,
) error {
	return r.handleResumeError(ctx, jobExecCtx,
		r.resumeWithRetries(ctx, jobExecCtx, func() error {
			return r.runTxnCoordinator(ctx, jobExecCtx)
		}))
}

// runTxnCoordinator sets up and runs the transactional LDR coordinator.
func (r *logicalReplicationResumer) runTxnCoordinator(
	ctx context.Context, jobExecCtx sql.JobExecContext,
) error {
	client, err := r.getActiveClient(ctx, jobExecCtx.ExecCfg().InternalDB)
	if err != nil {
		return err
	}
	defer closeAndLog(ctx, client)

	if err := r.heartbeatAndCheckActive(ctx, client); err != nil {
		return err
	}

	planner := MakeLogicalReplicationPlanner(jobExecCtx, r.job, client)
	sourcePlan, err := planner.GetSourcePlan(ctx)
	if err != nil {
		return err
	}

	// TODO(jeffswenson): checkpoint partition URIs via
	// r.checkpointPartitionURIs once plan generation is added.

	// Build the DistSQL physical plan before starting concurrent work.
	flowPlan, planCtx, err := txnmode.PlanTxnReplication(ctx, r.job, jobExecCtx, sourcePlan)
	if err != nil {
		return errors.Wrap(err, "building DistSQL plan")
	}

	payload := r.job.Details().(jobspb.LogicalReplicationDetails)
	heartbeatInterval := func() time.Duration {
		return heartbeatFrequency.Get(&jobExecCtx.ExecCfg().Settings.SV)
	}
	heartbeatSender := streamclient.NewHeartbeatSender(
		ctx,
		client,
		streampb.StreamID(payload.StreamID),
		heartbeatInterval,
	)
	defer func() {
		_ = heartbeatSender.Stop()
	}()

	runFlow := func(ctx context.Context) error {
		return txnmode.RunDistSQLFlow(
			ctx, jobExecCtx, flowPlan, planCtx,
			r.job, heartbeatSender.FrontierUpdates,
		)
	}
	startHeartbeat := func(ctx context.Context) error {
		heartbeatSender.Start(ctx, timeutil.DefaultTimeSource{})
		return heartbeatSender.Wait()
	}

	return ctxgroup.GoAndWait(ctx, runFlow, startHeartbeat)
}
