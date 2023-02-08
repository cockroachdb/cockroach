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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

type metricsPoller struct {
	job *jobs.Job
}

var _ jobs.Resumer = &metricsPoller{}

// OnFailOrCancel is a part of the Resumer interface.
func (mp *metricsPoller) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	return nil
}

// Resume is part of the Resumer interface.
func (mp *metricsPoller) Resume(ctx context.Context, execCtx interface{}) error {
	// The metrics polling job is a forever running background job. It's always
	// safe to wind the SQL pod down whenever it's running, something we
	// indicate through the job's idle status.
	mp.job.MarkIdle(true)

	exec := execCtx.(JobExecContext)
	return exec.ExecCfg().JobRegistry.PollMetricsTask(ctx)
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &metricsPoller{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypePollJobsStats, createResumerFn, jobs.UsesTenantCostControl)
}
