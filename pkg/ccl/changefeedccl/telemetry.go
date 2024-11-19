// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/timers"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cidr"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/rcrowley/go-metrics"
)

type sinkTelemetryData struct {
	emittedBytes    atomic.Int64
	emittedMessages atomic.Int64
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

	// recordEmittedMessages records the number of emitted messages without
	// publishing logs.
	recordEmittedMessages(numMessages int)

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
		changefeedDetails: makeCommonChangefeedEventDetails(ctx, job.Details().(jobspb.ChangefeedDetails), job.Payload().Description, job.ID()),
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

// recordEmittedMessages implements the telemetryLogger interface.
func (ptl *periodicTelemetryLogger) recordEmittedMessages(numMessages int) {
	ptl.sinkTelemetryData.emittedMessages.Add(int64(numMessages))
}

func (ptl *periodicTelemetryLogger) resetEmittedMessages() int64 {
	return ptl.sinkTelemetryData.emittedMessages.Swap(0)
}

// recordEmittedBytes implements the telemetryLogger interface.
func (ptl *periodicTelemetryLogger) maybeFlushLogs() {
	loggingInterval := continuousTelemetryInterval.Get(&ptl.settings.SV).Nanoseconds()
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
		EmittedBytes:                 ptl.resetEmittedBytes(),
		EmittedMessages:              ptl.resetEmittedMessages(),
		LoggingInterval:              loggingInterval,
	}
	log.StructuredEvent(ptl.ctx, severity.INFO, continuousTelemetryEvent)
}

func (ptl *periodicTelemetryLogger) close() {
	loggingInterval := continuousTelemetryInterval.Get(&ptl.settings.SV).Nanoseconds()
	if loggingInterval == 0 {
		return
	}

	continuousTelemetryEvent := &eventpb.ChangefeedEmittedBytes{
		CommonChangefeedEventDetails: ptl.changefeedDetails,
		EmittedBytes:                 ptl.resetEmittedBytes(),
		EmittedMessages:              ptl.resetEmittedMessages(),
		LoggingInterval:              loggingInterval,
		Closing:                      true,
	}
	log.StructuredEvent(ptl.ctx, severity.INFO, continuousTelemetryEvent)
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

func (r *telemetryMetricsRecorder) makeCloudstorageFileAllocCallback() func(delta int64) {
	return r.inner.makeCloudstorageFileAllocCallback()
}

func (r *telemetryMetricsRecorder) recordInternalRetry(numMessages int64, reducedBatchSize bool) {
	r.inner.recordInternalRetry(numMessages, reducedBatchSize)
}

func (r *telemetryMetricsRecorder) recordOneMessage() recordOneMessageCallback {
	return func(mvcc hlc.Timestamp, bytes int, compressedBytes int) {
		r.inner.recordOneMessage()(mvcc, bytes, compressedBytes)
		r.telemetryLogger.recordEmittedBytes(bytes)
		r.telemetryLogger.recordEmittedMessages(1)
		r.telemetryLogger.maybeFlushLogs()
	}
}

func (r *telemetryMetricsRecorder) recordEmittedBatch(
	startTime time.Time, numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int,
) {
	r.inner.recordEmittedBatch(startTime, numMessages, mvcc, bytes, compressedBytes)
	r.telemetryLogger.recordEmittedBytes(bytes)
	r.telemetryLogger.recordEmittedMessages(numMessages)
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

func (r *telemetryMetricsRecorder) recordSinkIOInflightChange(delta int64) {
	r.inner.recordSinkIOInflightChange(delta)
}

func (r *telemetryMetricsRecorder) newParallelIOMetricsRecorder() parallelIOMetricsRecorder {
	return r.inner.newParallelIOMetricsRecorder()
}

func (r *telemetryMetricsRecorder) getKafkaThrottlingMetrics(
	settings *cluster.Settings,
) metrics.Histogram {
	return r.inner.getKafkaThrottlingMetrics(settings)
}

func (r *telemetryMetricsRecorder) netMetrics() *cidr.NetMetrics {
	return r.inner.netMetrics()
}

func (r *telemetryMetricsRecorder) timers() *timers.ScopedTimers {
	return r.inner.timers()
}

// continuousTelemetryInterval determines the interval at which each node emits telemetry events
// during the lifespan of each enterprise changefeed.
var continuousTelemetryInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.telemetry.continuous_logging.interval",
	"determines the interval at which each node emits continuous telemetry events"+
		" during the lifespan of every enterprise changefeed; setting a zero value disables",
	24*time.Hour,
	settings.NonNegativeDuration,
)
