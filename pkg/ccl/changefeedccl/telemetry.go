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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type sinkTelemetryData struct {
	emittedBytes atomic.Int64
}

type periodicTelemetryLogger struct {
	ctx               context.Context
	sinkTelemetryData sinkTelemetryData
	job               *jobs.Job
	changefeedDetails eventpb.CommonChangefeedEventDetails
	settings          *cluster.Settings

	lastEmitTime atomic.Int64
}

type telemetryLogger interface {
	// recordEmittedBytes records the number of emitted bytes without
	// publishing logs.
	recordEmittedBytes(numBytes int)

	// maybeFlushLogs flushes buffered metrics to logs depending
	// on the semantics of the implementation.
	maybeFlushLogs()

	// close flushes buffered metrics to logs.
	close()
}

var _ telemetryLogger = (*periodicTelemetryLogger)(nil)

func makePeriodicTelemetryLogger(
	ctx context.Context, job *jobs.Job, s *cluster.Settings,
) (*periodicTelemetryLogger, error) {
	return &periodicTelemetryLogger{
		ctx:               ctx,
		job:               job,
		changefeedDetails: getCommonChangefeedEventDetails(ctx, job.Details().(jobspb.ChangefeedDetails), job.Payload().Description),
		sinkTelemetryData: sinkTelemetryData{},
		settings:          s,
	}, nil
}

// recordEmittedBytes implements the telemetryLogger interface.
func (ptl *periodicTelemetryLogger) recordEmittedBytes(numBytes int) {
	ptl.sinkTelemetryData.emittedBytes.Add(int64(numBytes))
}

func (ptl *periodicTelemetryLogger) resetEmittedBytes() int64 {
	return ptl.sinkTelemetryData.emittedBytes.Swap(0)
}

// recordEmittedBytes implements the telemetryLogger interface.
func (ptl *periodicTelemetryLogger) maybeFlushLogs() {
	loggingInterval := ContinuousTelemetryInterval.Get(&ptl.settings.SV).Nanoseconds()
	if loggingInterval == 0 {
		return
	}

	currentTime := timeutil.Now().UnixNano()
	// This is a barrier to ensure that only one goroutine writes logs in
	// case multiple goroutines call this function at the same time.
	// This prevents a burst of telemetry events from being needlessly
	// logging the same data.
	lastEmit := ptl.lastEmitTime.Load()
	if currentTime < lastEmit+loggingInterval {
		return
	}
	if !ptl.lastEmitTime.CompareAndSwap(lastEmit, currentTime) {
		return
	}

	continuousTelemetryEvent := &eventpb.ChangefeedEmittedBytes{
		CommonChangefeedEventDetails: ptl.changefeedDetails,
		JobId:                        int64(ptl.job.ID()),
		EmittedBytes:                 ptl.resetEmittedBytes(),
		LoggingInterval:              loggingInterval,
	}
	log.StructuredEvent(ptl.ctx, continuousTelemetryEvent)
}

func (ptl *periodicTelemetryLogger) close() {
	loggingInterval := ContinuousTelemetryInterval.Get(&ptl.settings.SV).Nanoseconds()
	if loggingInterval == 0 {
		return
	}

	continuousTelemetryEvent := &eventpb.ChangefeedEmittedBytes{
		CommonChangefeedEventDetails: ptl.changefeedDetails,
		JobId:                        int64(ptl.job.ID()),
		EmittedBytes:                 ptl.resetEmittedBytes(),
		LoggingInterval:              loggingInterval,
		Closing:                      true,
	}
	log.StructuredEvent(ptl.ctx, continuousTelemetryEvent)
}

func wrapMetricsRecorderWithTelemetry(
	ctx context.Context, job *jobs.Job, s *cluster.Settings, mb metricsRecorder,
) (*telemetryMetricsRecorder, error) {
	logger, err := makePeriodicTelemetryLogger(ctx, job, s)
	if err != nil {
		return &telemetryMetricsRecorder{}, err
	}
	return &telemetryMetricsRecorder{
		telemetryLogger: logger,
		inner:           mb,
	}, nil
}

type telemetryMetricsRecorder struct {
	telemetryLogger *periodicTelemetryLogger
	inner           metricsRecorder
}

var _ metricsRecorder = (*telemetryMetricsRecorder)(nil)

func (r *telemetryMetricsRecorder) close() {
	r.telemetryLogger.close()
}

func (r *telemetryMetricsRecorder) recordMessageSize(sz int64) {
	r.inner.recordMessageSize(sz)
}

func (r *telemetryMetricsRecorder) recordInternalRetry(numMessages int64, reducedBatchSize bool) {
	r.inner.recordInternalRetry(numMessages, reducedBatchSize)
}

func (r *telemetryMetricsRecorder) recordOneMessage() recordOneMessageCallback {
	return func(mvcc hlc.Timestamp, bytes int, compressedBytes int) {
		r.inner.recordOneMessage()(mvcc, bytes, compressedBytes)
		r.telemetryLogger.recordEmittedBytes(bytes)
		r.telemetryLogger.maybeFlushLogs()
	}
}

func (r *telemetryMetricsRecorder) recordEmittedBatch(
	startTime time.Time, numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int,
) {
	r.inner.recordEmittedBatch(startTime, numMessages, mvcc, bytes, compressedBytes)
	r.telemetryLogger.recordEmittedBytes(bytes)
	r.telemetryLogger.maybeFlushLogs()
}

func (r *telemetryMetricsRecorder) recordResolvedCallback() func() {
	return r.inner.recordResolvedCallback()
}

func (r *telemetryMetricsRecorder) recordFlushRequestCallback() func() {
	return r.inner.recordFlushRequestCallback()
}

func (r *telemetryMetricsRecorder) getBackfillCallback() func() func() {
	return r.inner.getBackfillCallback()
}

func (r *telemetryMetricsRecorder) getBackfillRangeCallback() func(int64) (func(), func()) {
	return r.inner.getBackfillRangeCallback()
}

func (r *telemetryMetricsRecorder) recordSizeBasedFlush() {
	r.inner.recordSizeBasedFlush()
}

func (r *telemetryMetricsRecorder) recordParallelIOQueueLatency(latency time.Duration) {
	r.inner.recordParallelIOQueueLatency(latency)
}

func (r *telemetryMetricsRecorder) recordSinkIOInflightChange(delta int64) {
	r.inner.recordSinkIOInflightChange(delta)
}

// ContinuousTelemetryInterval determines the interval at which each node emits telemetry events
// during the lifespan of each enterprise changefeed.
var ContinuousTelemetryInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"changefeed.telemetry.continuous_logging.interval",
	"determines the interval at which each node emits continuous telemetry events"+
		" during the lifespan of every enterprise changefeed; setting a zero value disables",
	24*time.Hour,
	settings.NonNegativeDuration,
)
