// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uamigrationjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type resumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = (*resumer)(nil)

var reconciliationJobCheckpointInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"spanconfig.reconciliation_job.checkpoint_interval",
	"the frequency at which the span config reconciliation job checkpoints itself",
	5*time.Second,
	settings.NonNegativeDuration,
)

// Resume implements the jobs.Resumer interface.
func (r *resumer) Resume(ctx context.Context, execCtxI interface{}) (jobErr error) {
  log.Infof(ctx, "ua migration job started")
	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *resumer) OnFailOrCancel(ctx context.Context, _ interface{}, _ error) error {
	if jobs.HasErrJobCanceled(errors.DecodeError(ctx, *r.job.Payload().FinalResumeError)) {
		return errors.AssertionFailedf("ua migration job cannot be canceled")
	}
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *resumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(jobspb.TypeUnifiedArchitectureMigration,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &resumer{job: job}
		},
	)
}
