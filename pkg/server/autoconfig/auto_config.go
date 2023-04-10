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
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/acprovider"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/time/rate"
)

// autoConfigMaxNewRunnersPerSecond determines the rate at which the
// top level job can create per-environment runners.
var autoConfigMaxNewRunnersPerSecond = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"jobs.auto_config.new_runners_per_second.max_rate",
	"maximum rate at which the auto config subsystem can create new per-environment runners (set 0 to suspend job creation entirely)",
	// We keep this limit low because the tasks subsystem is intended
	// as a low-frequency, high-latency subsystem. The limit is more meant
	// as a throttle to prevent runaway creation of jobs.
	1,
)

// autoConfigMaxTasksPerSecond determines the rate at which each
// per-env runner can create new job tasks.
var autoConfigMaxTasksPerSecond = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"jobs.auto_config.new_tasks_per_second.max_rate",
	"maximum rate at which each task runner can create new jobs (set 0 to suspend job creation entirely)",
	// We keep this limit low because the tasks subsystem is intended
	// as a low-frequency, high-latency subsystem. The limit is more meant
	// as a throttle to prevent runaway creation of jobs.
	10,
)

// autoConfigRunner runs the job that accepts new auto configuration
// payloads and sequences the creation of individual jobs to execute
// them.
// The auto config payloads are run by the taskRunner defined in
// auto_config_task.go.
//
// Refer to the package-level documentation for more details.
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

// EnvironmentID aliases autoconfigpb.EnvironmentID
type EnvironmentID = autoconfigpb.EnvironmentID

// TaskID aliases autoconfigpb.TaskID
type TaskID = autoconfigpb.TaskID

// Resume is part of the Resumer interface.
func (r *autoConfigRunner) Resume(ctx context.Context, execCtx interface{}) error {
	// The auto config runner is a forever running background job.
	// It's always safe to wind the SQL pod down whenever it's
	// running, something we indicate through the job's idle
	// status.
	r.job.MarkIdle(true)

	exec := execCtx.(sql.JobExecContext)
	execCfg := exec.ExecCfg()

	// Provider gives us tasks to run.
	provider := getProvider(ctx, execCfg)

	// waitForEnvChange is the channel that indicates the set of
	// environments is updated.
	waitForEnvChange := provider.EnvUpdate()

	// limiter ensures we don't generate too many jobs per second.
	prevLimit := float64(0)
	var limiter *rate.Limiter

	for {
		// No tasks to create. Just wait until some tasks are delivered.
		log.Infof(ctx, "waiting for environment activation...")
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-waitForEnvChange:
		}

		for _, envID := range provider.ActiveEnvironments() {
			// Check if the rate limit has changed. If it has, make a new limiter.
			if curLimit := autoConfigMaxNewRunnersPerSecond.Get(&execCfg.Settings.SV); curLimit != prevLimit || limiter == nil {
				limiter = rate.NewLimiter(rate.Limit(curLimit), 1)
				prevLimit = curLimit
			}
			// Wait according to the configured rate limit.
			if err := limiter.Wait(ctx); err != nil {
				return err
			}

			// Ensure that the idleness is not changed while we call
			// `limiter.Wait`. This ensures that orchestration feels free to
			// stop this server if the rate limiter decided we should wait
			// (or if the operator decided to change the limit to 0).
			r.job.MarkIdle(false)
			if err := refreshEnvJob(ctx, execCfg, envID); err != nil {
				log.Warningf(ctx, "error refreshing environment %q: %v", envID, err)
			}
			r.job.MarkIdle(true)
		}
	}
}

func getProvider(ctx context.Context, execCfg *sql.ExecutorConfig) acprovider.Provider {
	provider := execCfg.AutoConfigProvider
	if provider == nil {
		panic(errors.AssertionFailedf("programming error: missing provider"))
	}
	log.Infof(ctx, "using provider with type %T", provider)
	return provider
}

func refreshEnvJob(ctx context.Context, execCfg *sql.ExecutorConfig, envID EnvironmentID) error {
	log.Infof(ctx, "refreshing runner job for environment %q", envID)
	jobID := execCfg.JobRegistry.MakeJobID()
	var jobCreated bool
	if err := execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Do we already have a job for this environment?
		row, err := txn.QueryRowEx(ctx,
			"get-env-runner-job", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			`SELECT id FROM system.jobs WHERE job_type = $1 AND created_by_type = $2`,
			jobspb.TypeAutoConfigEnvRunner.String(),
			makeEnvRunnerJobCreatedKey(envID))
		if err != nil {
			return err
		}
		if row != nil {
			log.Infof(ctx, "found existing job %v for environment %q", row[0], envID)
			return nil
		}

		// The job did not exist yet. Create it now.
		if err := createEnvRunnerJob(ctx, txn, execCfg.JobRegistry, jobID, envID); err != nil {
			return errors.Wrapf(err, "creating job %d for env %q", jobID, envID)
		}
		jobCreated = true
		return nil
	}); err != nil {
		return err
	}
	if jobCreated {
		log.Infof(ctx, "created job %d for env %q", jobID, envID)
		// Start the job immediately. This speeds up the application
		// of initial configuration tasks.
		execCfg.JobRegistry.NotifyToResume(ctx, jobID)
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
	jobs.RegisterConstructor(jobspb.TypeAutoConfigRunner, createResumerFn, jobs.DisablesTenantCostControl)
}
