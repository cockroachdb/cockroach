// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnapply"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/txnmode"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// resumeTransactionalLdr runs the transactional LDR ingestion loop, converging
// on the earliest unappliable transaction before pausing the job.
// The control loop has three outcomes per attempt:
//  1. The coordinator returns a txnapply.ReplicationError at timestamp T:
//     capture it as the candidate pause cause, shrink endTime to T, and retry.
//     The next attempt's mergefeed will skip events at and after T, so a
//     conflicting txn at T can never re-trigger the same error; instead, an
//     earlier conflict (if any) will surface.
//  2. The coordinator returns ErrEndTimeReached: every txn with timestamp
//     strictly less than endTime has been applied without surfacing a new
//     conflict, so the captured ReplicationError identifies the earliest
//     conflicting transaction. Pause the job with that error.
//  3. Any other error: propagate it; resumeWithRetries decides whether to
//     retry or surface the failure.
func (r *logicalReplicationResumer) resumeTransactionalLdr(
	ctx context.Context, jobExecCtx sql.JobExecContext,
) error {
	endTime := hlc.MaxTimestamp
	var capturedErr *txnapply.ReplicationError
	err := r.resumeWithRetries(ctx, jobExecCtx, func() error {
		err := r.runTxnCoordinator(ctx, jobExecCtx, endTime)
		switch {
		case errors.As(err, &capturedErr):
			endTime = capturedErr.Timestamp
			return err
		case errors.Is(err, txnmode.ErrEndTimeReached):
			if capturedErr == nil {
				return jobs.MarkAsPermanentJobError(errors.AssertionFailedf(
					"transactional LDR reached replication cutoff %s without a captured conflict error",
					endTime,
				))
			}
			return jobs.MarkPauseRequestError(capturedErr)
		default:
			return err
		}
	})
	return r.handleResumeError(ctx, jobExecCtx, err)
}

// runTxnCoordinator sets up and runs the transactional LDR coordinator.
func (r *logicalReplicationResumer) runTxnCoordinator(
	ctx context.Context, jobExecCtx sql.JobExecContext, endTime hlc.Timestamp,
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
	replicatedTime := replicatedTimeFromJob(r.job)
	flowPlan, planCtx, applierInstanceIDs, err :=
		txnmode.PlanTxnReplication(ctx, r.job, jobExecCtx, sourcePlan, replicatedTime, endTime)
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
			applierInstanceIDs, replicatedTime, endTime,
		)
	}
	startHeartbeat := func(ctx context.Context) error {
		heartbeatSender.Start(ctx, timeutil.DefaultTimeSource{})
		return heartbeatSender.Wait()
	}

	return ctxgroup.GoAndWait(ctx, runFlow, startHeartbeat)
}

// replicatedTimeFromJob returns the replicated time from job progress,
// falling back to the replication start time if no checkpoint exists.
func replicatedTimeFromJob(job *jobs.Job) hlc.Timestamp {
	payload := job.Payload().Details.(*jobspb.Payload_LogicalReplicationDetails).LogicalReplicationDetails
	progress := job.Progress().Details.(*jobspb.Progress_LogicalReplication).LogicalReplication
	if !progress.ReplicatedTime.IsEmpty() {
		return progress.ReplicatedTime
	}
	return payload.ReplicationStartTime
}
