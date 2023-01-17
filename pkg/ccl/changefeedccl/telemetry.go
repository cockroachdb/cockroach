// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type sinkTelemetryData struct {
	emittedBytes *atomic.Int64
}

func makeSinkTelemetryData() *sinkTelemetryData {
	return &sinkTelemetryData{
		emittedBytes: &atomic.Int64{},
	}
}

type periodicTelemetryLogger struct {
	ctx               context.Context
	sinkTelemetryData *sinkTelemetryData
	job               *jobs.Job
	changefeedDetails eventpb.CommonChangefeedEventDetails
	settings          *cluster.Settings

	mu struct {
		syncutil.Mutex
		lastEmitTime int64
	}
}

type telemetryLogger interface {
	// recordEmittedBytes records the number of emitted bytes without
	// publishing logs.
	recordEmittedBytes(numBytes int)

	// maybeFlushLogs flushes buffered metrics to logs depending
	// on the semantics of the implementation.
	maybeFlushLogs()

	// flushLogs flushes buffered metrics to logs.
	flushLogs()
}

var _ telemetryLogger = (*periodicTelemetryLogger)(nil)

func makePeriodicTelemetryLogger(
	ctx context.Context, jobID jobspb.JobID, flowCtx *execinfra.FlowCtx,
) (*periodicTelemetryLogger, error) {
	job, err := flowCtx.Cfg.JobRegistry.LoadJob(ctx, jobID)
	if err != nil {
		return nil, err
	}

	return &periodicTelemetryLogger{
		ctx:               ctx,
		job:               job,
		changefeedDetails: getCommonChangefeedEventDetails(ctx, job.Details().(jobspb.ChangefeedDetails), job.Payload().Description),
		sinkTelemetryData: makeSinkTelemetryData(),
		settings:          flowCtx.Cfg.Settings,
	}, nil
}

// recordEmittedBytes implements the telemetryLogger interface.
func (ptl *periodicTelemetryLogger) recordEmittedBytes(numBytes int) {
	ptl.sinkTelemetryData.emittedBytes.Add(int64(numBytes))
}

func (ptl *periodicTelemetryLogger) resetEmittedBytes() int {
	return int(ptl.sinkTelemetryData.emittedBytes.Swap(0))
}

func (ptl *periodicTelemetryLogger) getInterval() (loggingIntervalNanos int64, enabled bool) {
	loggingPeriod := ContinuousTelemetryInterval.Get(&ptl.settings.SV).Nanoseconds()
	if loggingPeriod < 0 {
		return -1, false
	}
	return loggingPeriod, true
}

// recordEmittedBytes implements the telemetryLogger interface.
func (ptl *periodicTelemetryLogger) maybeFlushLogs() {
	loggingInterval, enabled := ptl.getInterval()
	if !enabled {
		return
	}

	currentTime := timeutil.Now().UnixNano()
	// This is a barrier to ensure that only one goroutine writes logs in
	// case multiple goroutines call this function at the same time.
	if !func() bool {
		ptl.mu.Lock()
		defer ptl.mu.Unlock()
		if currentTime < ptl.mu.lastEmitTime+loggingInterval {
			return false
		}
		ptl.mu.lastEmitTime = currentTime
		return true
	}() {
		return
	}

	continuousTelemetryEvent := &eventpb.ChangefeedEmittedBytes{
		CommonChangefeedEventDetails: ptl.changefeedDetails,
		JobId:                        int64(ptl.job.ID()),
		EmittedBytes:                 int32(ptl.resetEmittedBytes()),
		LoggingInterval:              loggingInterval,
	}
	log.StructuredEvent(ptl.ctx, continuousTelemetryEvent)
}

func (ptl *periodicTelemetryLogger) flushLogs() {
	if interval, enabled := ptl.getInterval(); enabled {
		continuousTelemetryEvent := &eventpb.ChangefeedEmittedBytes{
			CommonChangefeedEventDetails: ptl.changefeedDetails,
			JobId:                        int64(ptl.job.ID()),
			EmittedBytes:                 int32(ptl.resetEmittedBytes()),
			LoggingInterval:              interval,
			Closing:                      true,
		}
		log.StructuredEvent(ptl.ctx, continuousTelemetryEvent)
	}
}

// ContinuousTelemetryInterval determines the interval at which each node emits telemetry events
// during the lifespan of each enterprise changefeed.
var ContinuousTelemetryInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.telemetry.continuous_logging.interval",
	"determines the interval at which each node emits continuous telemetry events"+
		" during the lifespan of every enterprise changefeed; setting a negative value disables",
	24*time.Hour,
)
