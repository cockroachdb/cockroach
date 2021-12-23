// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

// Resume implements the jobs.Resumer interface.
func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) error {
	execCtx := execCtxI.(sql.JobExecContext)
	rc := execCtx.SpanConfigReconciliationJobDeps()

	// TODO(irfansharif): #73086 bubbles up retryable errors from the
	// reconciler/underlying watcher in the (very) unlikely event that it's
	// unable to generate incremental updates from the given timestamp (things
	// could've been GC-ed from underneath us). For such errors, instead of
	// failing this entire job, we should simply retry the reconciliation
	// process here. Not doing so is still fine, the spanconfig.Manager starts
	// the job all over again after some time, it's just that the checks for
	// failed jobs happen infrequently.

	if err := rc.Reconcile(ctx, hlc.Timestamp{}, func() error {
		// TODO(irfansharif): Stash this checkpoint somewhere and use it when
		// starting back up.
		_ = rc.Checkpoint()
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(context.Context, interface{}) error {
	return errors.AssertionFailedf("span config reconciliation job can never fail or be canceled")
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeAutoSpanConfigReconciliation,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		})
}
