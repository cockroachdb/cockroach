// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package upgrades contains the implementation of upgrades. It is imported
// by the server library.
//
// This package registers the upgrades with the upgrade package.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type mvccStatisticsUpdateJob struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*mvccStatisticsUpdateJob)(nil)

func (j *mvccStatisticsUpdateJob) Resume(ctx context.Context, execCtxI interface{}) (jobErr error) {
	// TODO(zachlite):
	// Delete samples older than configurable setting...
	// Collect span stats for tenant descriptors...
	// Write new samples...
	execCtx := execCtxI.(JobExecContext)
	stopper := execCtx.ExecCfg().DistSQLSrv.Stopper
	j.job.MarkIdle(true)

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-stopper.ShouldQuiesce():
			return nil
		}
	}
}

func (j *mvccStatisticsUpdateJob) OnFailOrCancel(
	ctx context.Context, _ interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(
			jobErr, "mvcc statistics update job is not cancelable",
		)
		log.Errorf(ctx, "%v", err)
	}
	return nil
}

func (j *mvccStatisticsUpdateJob) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeMVCCStatisticsUpdate,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &mvccStatisticsUpdateJob{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
}
