// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const intervalBaseSettingKey = "jobs.registry.interval.base"
const adoptIntervalSettingKey = "jobs.registry.interval.adopt"
const cancelIntervalSettingKey = "jobs.registry.interval.cancel"
const gcIntervalSettingKey = "jobs.registry.interval.gc"
const retentionTimeSettingKey = "jobs.retention_time"
const cancelUpdateLimitKey = "jobs.cancel_update_limit"

// defaultAdoptInterval is the default adopt interval.
var defaultAdoptInterval = 30 * time.Second

// defaultCancelInterval is the default cancel interval.
var defaultCancelInterval = 10 * time.Second

// defaultGcInterval is the default GC Interval.
var defaultGcInterval = 1 * time.Hour

// defaultIntervalBase is the default interval base.
var defaultIntervalBase = 1.0

// defaultRetentionTime is the default duration for which terminal jobs are
// kept in the records.
var defaultRetentionTime = 14 * 24 * time.Hour

// defaultCancellationsUpdateLimit is the default number of jobs that can be
// updated when canceling jobs concurrently from dead sessions.
var defaultCancellationsUpdateLimit int64 = 1000

var (
	intervalBaseSetting = settings.RegisterFloatSetting(
		intervalBaseSettingKey,
		"the base multiplier for other intervals such as adopt, cancel, and gc",
		defaultIntervalBase,
		settings.PositiveFloat,
	)

	adoptIntervalSetting = settings.RegisterDurationSetting(
		adoptIntervalSettingKey,
		"the interval at which a node (a) claims some of the pending jobs and "+
			"(b) restart its already claimed jobs that are in running or reverting "+
			"states but are not running",
		defaultAdoptInterval,
		settings.PositiveDuration,
	)

	cancelIntervalSetting = settings.RegisterDurationSetting(
		cancelIntervalSettingKey,
		"the interval at which a node cancels the jobs belonging to the known "+
			"dead sessions",
		defaultCancelInterval,
		settings.PositiveDuration,
	)

	gcIntervalSetting = settings.RegisterDurationSetting(
		gcIntervalSettingKey,
		"the interval a node deletes expired job records that have exceeded their "+
			"retention duration",
		defaultGcInterval,
		settings.PositiveDuration,
	)

	retentionTimeSetting = settings.RegisterDurationSetting(
		retentionTimeSettingKey,
		"the amount of time to retain records for completed jobs before",
		defaultRetentionTime,
		settings.PositiveDuration,
	).WithPublic()

	cancellationsUpdateLimitSetting = settings.RegisterIntSetting(
		cancelUpdateLimitKey,
		"the number of jobs that can be updated when canceling jobs concurrently from dead sessions",
		defaultCancellationsUpdateLimit,
		settings.NonNegativeInt,
	)
)

// jitter adds a small jitter in the given duration.
func jitter(dur time.Duration) time.Duration {
	const jitter = 1 / 6
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
//  lc, cleanup := makeLoopController(...)
//  defer cleanup()
//  for {
//    select {
//    case <- lc.update:
//      lc.onUpdate() or lc.onUpdateWithBound()
//    case <- lc.timer.C:
//      executeJob()
//      lc.onExecute() or lc.onExecuteWithBound
//    }
//  }
//
type loopController struct {
	timer   *timeutil.Timer
	lastRun time.Time
	updated chan struct{}
	// getInterval returns the value of the associated cluster setting.
	getInterval func() time.Duration
}

// makeLoopController returns a structure that controls the execution of a job
// at regular intervals. Moreover, it returns a cleanup function that should be
// deferred to execute before destroying the instantiated structure.
func makeLoopController(
	st *cluster.Settings, s *settings.DurationSetting, overrideKnob *time.Duration,
) (loopController, func()) {
	lc := loopController{
		timer:   timeutil.NewTimer(),
		lastRun: timeutil.Now(),
		updated: make(chan struct{}, 1),
		// getInterval returns the value of the associated cluster setting. If
		// overrideKnob is not nil, it overrides the cluster setting.
		getInterval: func() time.Duration {
			if overrideKnob != nil {
				return *overrideKnob
			}
			return time.Duration(intervalBaseSetting.Get(&st.SV) * float64(s.Get(&st.SV)))
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
	s.SetOnChange(&st.SV, onChange)
	intervalBaseSetting.SetOnChange(&st.SV, onChange)

	lc.timer.Reset(jitter(lc.getInterval()))
	return lc, func() { lc.timer.Stop() }
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
