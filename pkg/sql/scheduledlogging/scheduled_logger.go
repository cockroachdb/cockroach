// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scheduledlogging

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// ScheduledLogEmitterType is a type identifying the names of supported log emitters.
type ScheduledLogEmitterType string

const (
	// CaptureIndexUsageStatsEmitterType is a log emitter responsible for
	// emitting index usage statistic logs to the telemetry channel.
	CaptureIndexUsageStatsEmitterType ScheduledLogEmitterType = "capture-index-usage-stats-emitter"
)

// ScheduledLogEmitter is an interface defining expected methods from types
// that intend to emit logs on a scheduled interval.
type ScheduledLogEmitter interface {
	Emit(ctx context.Context, ie sqlutil.InternalExecutor, stopper *stop.Stopper) error
	Interval(cs *cluster.Settings) time.Duration
	IsEnabled(cs *cluster.Settings) bool
	IsRunning() bool
}

// TODO(thomas): not sure if this requires mutex protection.
var emitters = make(map[ScheduledLogEmitterType]ScheduledLogEmitter)

// RegisterLogEmitter registers a ScheduledLogEmitter for a given
// ScheduledLogEmitterType. This is intended to be used in init.
func RegisterLogEmitter(emitterName ScheduledLogEmitterType, emitter ScheduledLogEmitter) {
	if _, ok := emitters[emitterName]; ok {
		log.Info(context.Background(), string(emitterName)+" already registered")
		return
	}
	emitters[emitterName] = emitter
}

// LoggingScheduler is responsible for starting log emitters. LoggingScheduler
// also supplies an internal executor to be used by the log emitters.
type LoggingScheduler struct {
	DB *kv.DB
	cs *cluster.Settings
	ie sqlutil.InternalExecutor
}

// StartScheduledLogging creates a LoggingScheduler and starts all log
// emitters.
func StartScheduledLogging(
	ctx context.Context,
	stopper *stop.Stopper,
	db *kv.DB,
	cs *cluster.Settings,
	ie sqlutil.InternalExecutor,
) {
	scheduler := LoggingScheduler{
		DB: db,
		cs: cs,
		ie: ie,
	}
	scheduler.startLogEmitters(ctx, stopper)
}

func (s *LoggingScheduler) startLogEmitters(ctx context.Context, stopper *stop.Stopper) {
	for emitType, emitter := range emitters {
		emitType := emitType
		emitter := emitter
		taskName := fmt.Sprintf("scheduling-%s", string(emitType))
		_ = stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {

			for timer := time.NewTimer(emitter.Interval(s.cs)); ; timer.Reset(emitter.Interval(s.cs)) {
				select {
				case <-stopper.ShouldQuiesce():
					timer.Stop()
					return
				case <-timer.C:
					if !emitter.IsEnabled(s.cs) {
						continue
					}
					// If the emitter is still running from its previous schedule, skip
					// the current schedule.
					if emitter.IsRunning() {
						log.Infof(ctx, "skipped schedule for %s\n", string(emitType))
						continue
					}

					err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
						return emitter.Emit(ctx, s.ie, stopper)
					})
					if err != nil {
						log.Errorf(ctx, "error executing %s: %+v", string(emitType), err)
					}
				}
			}
		})
	}
}
