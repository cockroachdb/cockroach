// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package sqlactivityjob implements the singleton "sql activity flush" job
// (jobspb.TypeSQLActivityFlush). The node holding its sqlliveness lease
// runs Resume.
package sqlactivityjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type sqlActivityFlushJob struct {
	job *jobs.Job
}

var _ jobs.Resumer = &sqlActivityFlushJob{}

// Resume implements the jobs.Resumer interface. The loop wakes every
// sql.stats.flush.interval and — if coordinated flushes are enabled —
// runs one coordinator cycle.
//
// Unlike the per-node flush loop we don't jitter the interval: the per-node
// loop jitters to avoid a thundering herd, but the coordinator job is a
// singleton so there's nothing to spread out.
//
// We re-read SQLStatsFlushInterval each iteration rather than reacting to
// SetOnChange. Setting changes are picked up on the next wakeup (worst
// case: one interval-period of latency).
func (j *sqlActivityFlushJob) Resume(ctx context.Context, execCtxI interface{}) error {
	log.Dev.Infof(ctx, "starting coordinated sql stats flush job")

	// Mark idle so a graceful shutdown isn't blocked on this job.
	j.job.MarkIdle(true)

	execCtx := execCtxI.(sql.JobExecContext)
	st := execCtx.ExecCfg().Settings
	stopper := execCtx.ExecCfg().DistSQLSrv.Stopper
	persistedStats := execCtx.ExecCfg().InternalDB.SQLStatsProvider()
	if persistedStats == nil {
		return errors.AssertionFailedf("sql activity flush job: persisted SQL stats provider is nil")
	}

	var timer timeutil.Timer
	defer timer.Stop()

	for {
		timer.Reset(persistedsqlstats.SQLStatsFlushInterval.Get(&st.SV))

		select {
		case <-timer.C:
		case <-stopper.ShouldQuiesce():
			return nil
		case <-ctx.Done():
			return nil
		}

		if !persistedsqlstats.CoordinatedFlushEnabled(ctx, st) {
			continue
		}

		if err := persistedStats.RunCoordinatedFlush(ctx, stopper); err != nil {
			log.Dev.Warningf(ctx, "coordinated sql stats flush: %v", err)
		}
	}
}

// OnFailOrCancel implements the jobs.Resumer interface.
func (j *sqlActivityFlushJob) OnFailOrCancel(
	ctx context.Context, _ interface{}, jobErr error,
) error {
	if jobs.HasErrJobCanceled(jobErr) {
		err := errors.NewAssertionErrorWithWrappedErrf(jobErr,
			"sql activity is not cancelable")
		log.Dev.Errorf(ctx, "%v", err)
	}
	return nil
}

// CollectProfile implements the jobs.Resumer interface.
func (j *sqlActivityFlushJob) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeSQLActivityFlush,
		func(job *jobs.Job, _ *cluster.Settings) jobs.Resumer {
			return &sqlActivityFlushJob{job: job}
		},
		jobs.DisablesTenantCostControl,
	)
}
