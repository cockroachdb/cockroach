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
const gcIntervalSettingKey = "jobs.retention_time"

// defaultAdoptInterval is the default adopt interval
var defaultAdoptInterval = 30 * time.Second

// defaultCancelInterval is the default cancel interval
var defaultCancelInterval = 10 * time.Second

// defaultGcInterval is the default GC Interval
var defaultGcInterval = 1 * time.Hour

// defaultIntervalBase is the default interval base
var defaultIntervalBase = 1.0

var (
	intervalBaseSetting = settings.RegisterFloatSetting(
		intervalBaseSettingKey,
		"the base multiplier for other intervals such as adopt, cancel, and gc.",
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
		"the duration for which the records of completed jobs are retained",
		defaultGcInterval,
		settings.PositiveDuration,
	).WithPublic()
)

// jitter adds a small jitter in the given duration
func jitter(dur time.Duration) time.Duration {
	const jitter = 1 / 6
	jitterFraction := 1 + (2*rand.Float64()-1)*jitter // 1 + [-1/6, +1/6)
	return time.Duration(float64(dur) * jitterFraction)
}

// jobLoopController structure contains fields to control registry jobs
type jobLoopController struct {
	timer       *timeutil.Timer
	lastRun     time.Time
	updated     chan struct{}
	getInterval func() time.Duration
}

func makeJobLoopController(
	st *cluster.Settings, s *settings.DurationSetting, overrideKnob *time.Duration,
) (jobLoopController, func()) {
	lc := jobLoopController{
		timer:   timeutil.NewTimer(),
		lastRun: timeutil.Now(),
		updated: make(chan struct{}, 1),
		getInterval: func() time.Duration {
			if overrideKnob != nil {
				return *overrideKnob
			}
			return jitter(time.Duration(intervalBaseSetting.Get(&st.SV) * float64(s.Get(&st.SV))))
		},
	}

	onUpdate := func(ctx context.Context) {
		select {
		case lc.updated <- struct{}{}:
		default:
		}
	}

	s.SetOnChange(&st.SV, onUpdate)
	intervalBaseSetting.SetOnChange(&st.SV, onUpdate)

	lc.timer.Reset(lc.getInterval())
	return lc, func() { lc.timer.Stop() }
}

// onUpdate is called when the associated interval setting gets updated
func (lc *jobLoopController) onUpdate() {
	lc.timer.Reset(timeutil.Until(lc.lastRun.Add(lc.getInterval())))
}

// onExecute is called after the associated job is executed
func (lc *jobLoopController) onExecute() {
	lc.timer.Read = true
	lc.lastRun = timeutil.Now()
	lc.timer.Reset(lc.getInterval())
}
