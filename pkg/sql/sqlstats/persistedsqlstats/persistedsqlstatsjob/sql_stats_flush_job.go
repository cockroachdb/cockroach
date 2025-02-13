// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package persistedsqlstatsjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type sqlStatsFlushJob struct {
	job *jobs.Job
}

var _ jobs.Resumer = &sqlStatsFlushJob{}

// Resume implements the jobs.Resumer interface.
func (j *sqlStatsFlushJob) Resume(ctx context.Context, execCtxI interface{}) (jobErr error) {
	// TODO(kyle.wong): implement job
	return nil
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (r *sqlStatsFlushJob) OnFailOrCancel(ctx context.Context, _ interface{}, jobErr error) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(jobErr,
			"sql activity is not cancelable")
		log.Errorf(ctx, "%v", err)
	}
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (r *sqlStatsFlushJob) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeSQLStatsFlush,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &sqlStatsFlushJob{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
}
