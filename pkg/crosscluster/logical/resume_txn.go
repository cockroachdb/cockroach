// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
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

	// TODO(jeffswenson): checkpoint partition URIs via
	// r.checkpointPartitionURIs once plan generation is added.

	// TODO(jeffswenson): replace with txnmode.NewTxnLdrCoordinator once implemented.
	return jobs.MarkAsPermanentJobError(errors.WithHint(
		errors.New("transactional replication mode is not yet implemented"),
		"use MODE = 'immediate' instead"))
}
