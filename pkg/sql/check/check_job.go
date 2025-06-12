// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package check

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type consistencyCheckResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = &consistencyCheckResumer{}

// Resume implements the Resumer interface
func (c *consistencyCheckResumer) Resume(ctx context.Context, execCtx interface{}) error {
	log.Infof(ctx, "starting CONSISTENCY CHECK job")

	if err := c.job.NoTxn().Update(ctx,
		func(_ isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			progress := md.Progress
			progress.Progress = &jobspb.Progress_FractionCompleted{
				FractionCompleted: 1,
			}
			ju.UpdateProgress(progress)
			return nil
		},
	); err != nil {
		return err
	}
	return nil
}

// OnFailOrCancel implements the Resumer interface
func (c *consistencyCheckResumer) OnFailOrCancel(
	ctx context.Context, execCtx interface{}, jobErr error,
) error {
	return nil
}

// CollectProfile implements the Resumer interface
func (c *consistencyCheckResumer) CollectProfile(ctx context.Context, execCtx interface{}) error {
	return nil
}

func init() {
	createResumerFn := func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &consistencyCheckResumer{job: job}
	}
	jobs.RegisterConstructor(jobspb.TypeConsistencyCheck, createResumerFn, jobs.UsesTenantCostControl)
}
