// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package autoconfig

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
)

type autoConfigRunner struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*autoConfigRunner)(nil)

// OnFailOrCancel is a part of the Resumer interface.
func (r *autoConfigRunner) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	return nil
}

// Resume is part of the Resumer interface.
func (r *autoConfigRunner) Resume(ctx context.Context, execCtx interface{}) error {
	// The auto config runner is a forever running background job.
	// It's always safe to wind the SQL pod down whenever it's
	// running, something we indicate through the job's idle
	// status.
	r.job.MarkIdle(true)

	wait := make(chan struct{})

	// Note: even though the job registry cancels all running job upon
	// shutdown, we find some tests fail unless this job also does its
	// own wait.
	shutdownCh := execCtx.(sql.JobExecContext).ExecCfg().RPCContext.Stopper.ShouldQuiesce()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-shutdownCh:
		return context.Canceled

	case <-wait:
	}

	return nil
}

func init() {
	// Note: we disable tenant cost control because auto-config is used
	// by operators and should thus not incur costs (or performance
	// penalties) to tenants.
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &autoConfigRunner{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeAutoConfigRunner, createResumerFn,
		jobs.DisablesTenantCostControl)
}
