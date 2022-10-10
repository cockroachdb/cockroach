// Copyright 2022 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

type metricsPoller struct {
	job *jobs.Job
}

var _ jobs.Resumer = &metricsPoller{}

var metricsPollerRetryOptions = retry.Options{
	InitialBackoff: 5 * time.Millisecond,
	Multiplier:     2,
	MaxBackoff:     10 * time.Second,
}

// OnFailOrCancel is a part of the Resumer interface.
func (mp *metricsPoller) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	if jobErr != nil {
		log.Errorf(ctx, "failed polled stats job: %v", jobErr)
	}

	return nil
}

// Resume is part of the Resumer interface.
func (mp *metricsPoller) Resume(ctx context.Context, execCtx interface{}) error {
	exec := execCtx.(JobExecContext)
	var e error
	for r := retry.StartWithCtx(ctx, metricsPollerRetryOptions); r.Next(); {
		e = exec.ExecCfg().InternalExecutor.s.cfg.JobRegistry.PollMetricsTask(ctx)
	}
	return e
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &metricsPoller{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypePollJobsStats, createResumerFn, jobs.UsesTenantCostControl)
}
