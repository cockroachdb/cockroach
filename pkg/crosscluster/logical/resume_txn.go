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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	var capturedErr txnapply.ReplicationError
	return r.handleResumeError(ctx, jobExecCtx,
		r.resumeWithRetries(ctx, jobExecCtx, func() error {
			err := r.runTxnCoordinator(ctx, jobExecCtx, endTime)
			var replicationErr txnapply.ReplicationError
			switch {
			case errors.As(err, &replicationErr):
				capturedErr = replicationErr
				endTime = replicationErr.Timestamp
				return err
			case errors.Is(err, txnmode.ErrEndTimeReached):
				if capturedErr.Err == nil {
					return jobs.MarkAsPermanentJobError(errors.AssertionFailedf(
						"transactional LDR reached replication cutoff %s without a captured conflict error",
						endTime,
					))
				}
				return jobs.MarkPauseRequestError(capturedErr)
			default:
				return err
			}
		}))
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

	// TODO(jeffswenson): checkpoint partition URIs via
	// r.checkpointPartitionURIs once plan generation is added.

	heartbeatInterval := func() time.Duration {
		return heartbeatFrequency.Get(&jobExecCtx.ExecCfg().Settings.SV)
	}
	coordinator := txnmode.NewTxnLdrCoordinator(jobExecCtx, r.job, client, heartbeatInterval, endTime)
	return coordinator.Resume(ctx)
}
