// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	intervalBaseSettingKey     = "jobs.registry.interval.base"
	adoptIntervalSettingKey    = "jobs.registry.interval.adopt"
	cancelIntervalSettingKey   = "jobs.registry.interval.cancel"
	gcIntervalSettingKey       = "jobs.registry.interval.gc"
	maxAdoptionsPerLoopKey     = "jobs.registry.max_adoptions_per_loop"
	retentionTimeSettingKey    = "jobs.retention_time"
	cancelUpdateLimitKey       = "jobs.cancel_update_limit"
	debugPausePointsSettingKey = "jobs.debug.pausepoints"
	metricsPollingIntervalKey  = "jobs.metrics.interval.poll"
	claimQueryTimeoutKey       = "jobs.registry.claim_query.timeout"
)

const (
	// defaultAdoptInterval is the default adopt interval.
	defaultAdoptInterval = 30 * time.Second

	// defaultMaxAdoptionsPerLoop is the default maximum number of jobs a node
	// can adopt in one adoption loop.
	defaultMaxAdoptionsPerLoop = 10

	// defaultCancelInterval is the default cancel interval.
	defaultCancelInterval = 10 * time.Second

	// defaultGcInterval is the default GC Interval.
	defaultGcInterval = 1 * time.Hour

	// defaultIntervalBase is the default interval base.
	defaultIntervalBase = 1.0

	// defaultRetentionTime is the default duration for which terminal jobs are
	// kept in the records.
	defaultRetentionTime = 14 * 24 * time.Hour

	// defaultCancellationsUpdateLimit is the default number of jobs that can be
	// updated when canceling jobs concurrently from dead sessions.
	defaultCancellationsUpdateLimit int64 = 1000

	// defaultPollForMetricsInterval is the default interval to poll the jobs
	// table for metrics.
	defaultPollForMetricsInterval = 30 * time.Second
)

var (
	intervalBaseSetting = settings.RegisterFloatSetting(
		settings.ApplicationLevel,
		intervalBaseSettingKey,
		"the base multiplier for other intervals such as adopt, cancel, and gc",
		defaultIntervalBase,
		settings.PositiveFloat,
	)

	adoptIntervalSetting = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		adoptIntervalSettingKey,
		"the interval at which a node (a) claims some of the pending jobs and "+
			"(b) restart its already claimed jobs that are in running or reverting "+
			"states but are not running",
		defaultAdoptInterval,
		settings.PositiveDuration,
	)

	maxAdoptionsPerLoop = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		maxAdoptionsPerLoopKey,
		"the maximum number of jobs a node can adopt in one adoption loop",
		defaultMaxAdoptionsPerLoop,
		settings.PositiveInt,
	)

	cancelIntervalSetting = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		cancelIntervalSettingKey,
		"the interval at which a node cancels the jobs belonging to the known "+
			"dead sessions",
		defaultCancelInterval,
		settings.PositiveDuration,
	)

	// PollJobsMetricsInterval is the interval at which a tenant in the cluster
	// will poll the jobs table for metrics
	PollJobsMetricsInterval = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		metricsPollingIntervalKey,
		"the interval at which a node in the cluster will poll the jobs table for metrics",
		defaultPollForMetricsInterval,
		settings.PositiveDuration,
	)

	gcIntervalSetting = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		gcIntervalSettingKey,
		"the interval a node deletes expired job records that have exceeded their "+
			"retention duration",
		defaultGcInterval,
		settings.PositiveDuration,
	)

	// RetentionTimeSetting wraps "jobs.retention_timehelpers_test.go".
	RetentionTimeSetting = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		retentionTimeSettingKey,
		"the amount of time for which records for completed jobs are retained",
		defaultRetentionTime,
		settings.PositiveDuration,
		settings.WithPublic)

	cancellationsUpdateLimitSetting = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		cancelUpdateLimitKey,
		"the number of jobs that can be updated when canceling jobs concurrently from dead sessions",
		defaultCancellationsUpdateLimit,
		settings.NonNegativeInt,
	)

	debugPausepoints = settings.RegisterStringSetting(
		settings.ApplicationLevel,
		debugPausePointsSettingKey,
		"the list, comma separated, of named pausepoints currently enabled for debugging",
		"",
	)

	claimQueryTimeout = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		claimQueryTimeoutKey,
		"the timeout for the claim query used when adopting jobs",
		time.Minute,
		settings.PositiveDuration,
	)
)

// jitter adds a small jitter in the given duration.
func jitter(dur time.Duration) time.Duration {
	const jitter = 1.0 / 6.0
	jitterFraction := 1 + (2*rand.Float64()-1)*jitter // 1 + [-1/6, +1/6)
	return time.Duration(float64(dur) * jitterFraction)
}

// loopController controls the execution of a job at specific intervals. The
// interval is controlled through a cluster setting. The structure consists
// of a timer to execute the job at regular intervals and a notification
// channel, updated, to update the timer through onUpdate() when
// the cluster setting is changed. After each execution, onExecute() is called
// to reset the timer. The structure internally keeps track of the last run of the job
// using lastRun, which is updated in onExecute().
//
// Common usage pattern:
//
//	lc := makeLoopController(...)
//	defer lc.cleanup()
//	for {
//	  select {
//	  case <- lc.update:
//	    lc.onUpdate() or lc.onUpdateWithBound()
//	  case <- lc.timer.C:
//	    executeJob()
//	    lc.onExecute() or lc.onExecuteWithBound
//	  }
//	}
type loopController struct {
	timer   timeutil.Timer
	lastRun time.Time
	updated chan struct{}
	// getInterval returns the value of the associated cluster setting.
	getInterval func() time.Duration
}

// makeLoopController returns a structure that controls the execution of a job
// at regular intervals. The structure's cleanup method should be deferred to
// execute before destroying the instantiated structure.
func makeLoopController(
	r *Registry, s *settings.DurationSetting, overrideKnob *time.Duration,
) loopController {
	lc := loopController{
		lastRun: timeutil.Now(),
		updated: make(chan struct{}, 1),
		getInterval: func() time.Duration {
			return r.GetLoopInterval(s, overrideKnob)
		},
	}

	// onChange sends a notification on updated channel to notify a change in the
	// associated cluster setting.
	onChange := func(ctx context.Context) {
		select {
		case lc.updated <- struct{}{}:
		default:
		}
	}

	// register onChange() to get a notification when the cluster is updated.
	s.SetOnChange(&r.settings.SV, onChange)
	intervalBaseSetting.SetOnChange(&r.settings.SV, onChange)

	lc.timer.Reset(jitter(lc.getInterval()))
	return lc
}

// onUpdate is called when the associated interval setting gets updated.
func (lc *loopController) onUpdate() {
	lc.timer.Reset(timeutil.Until(lc.lastRun.Add(jitter(lc.getInterval()))))
}

// onExecute is called after the associated job is executed.
func (lc *loopController) onExecute() {
	lc.lastRun = timeutil.Now()
	lc.timer.Reset(jitter(lc.getInterval()))
}

// cleanup stops the loop controller's timer.
func (lc *loopController) cleanup() {
	lc.timer.Stop()
}
