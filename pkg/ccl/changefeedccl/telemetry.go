// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type sinkTelemetryData struct {
	emittedBytes    atomic.Int64
	emittedMessages atomic.Int64
}

type periodicTelemetryLogger struct {
	ctx               context.Context
	sinkTelemetryData sinkTelemetryData
	changefeedDetails eventpb.CommonChangefeedEventDetails
	settings          *cluster.Settings

	lastEmitTime atomic.Int64
}

type telemetryLogger interface {
	// incEmittedCounters increments the counters for emitted messages and bytes
	// without publishing logs.
	incEmittedCounters(numMessages int, numBytes int)

	// maybeFlushLogs flushes buffered metrics to logs depending
	// on the semantics of the implementation.
	maybeFlushLogs()

	// close flushes buffered metrics to logs.
	close()
}

var _ telemetryLogger = (*periodicTelemetryLogger)(nil)

func makePeriodicTelemetryLogger(
	ctx context.Context,
	details jobspb.ChangefeedDetails,
	description string,
	jobID jobspb.JobID,
	s *cluster.Settings,
) (*periodicTelemetryLogger, error) {
	return &periodicTelemetryLogger{
		ctx:               ctx,
		changefeedDetails: makeCommonChangefeedEventDetails(ctx, details, description, jobID),
		sinkTelemetryData: sinkTelemetryData{},
		settings:          s,
	}, nil
}

// incEmittedCounters implements the telemetryLogger interface.
func (ptl *periodicTelemetryLogger) incEmittedCounters(numMessages, numBytes int) {
	ptl.sinkTelemetryData.emittedMessages.Add(int64(numMessages))
	ptl.sinkTelemetryData.emittedBytes.Add(int64(numBytes))
}

func (ptl *periodicTelemetryLogger) resetEmittedBytes() int64 {
	return ptl.sinkTelemetryData.emittedBytes.Swap(0)
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
	ctx context.Context,
	details jobspb.ChangefeedDetails,
	description string,
	jobID jobspb.JobID,
	s *cluster.Settings,
	mb metricsRecorder,
	knobs TestingKnobs,
) (*telemetryMetricsRecorder, error) {
	var logger telemetryLogger
	logger, err := makePeriodicTelemetryLogger(ctx, details, description, jobID, s)
	if err != nil {
		return &telemetryMetricsRecorder{}, err
	}
	if knobs.WrapTelemetryLogger != nil {
		logger = knobs.WrapTelemetryLogger(logger)
	}
	return &telemetryMetricsRecorder{
		metricsRecorder: mb,
		telemetryLogger: logger,
	}, nil
}

type telemetryMetricsRecorder struct {
	metricsRecorder
	telemetryLogger telemetryLogger
}

var _ metricsRecorder = (*telemetryMetricsRecorder)(nil)

func (r *telemetryMetricsRecorder) close() {
	r.telemetryLogger.close()
}

func (r *telemetryMetricsRecorder) recordOneMessage() recordOneMessageCallback {
	return func(mvcc hlc.Timestamp, bytes int, compressedBytes int) {
		r.metricsRecorder.recordOneMessage()(mvcc, bytes, compressedBytes)
		r.telemetryLogger.incEmittedCounters(1 /* numMessages */, bytes)
		r.telemetryLogger.maybeFlushLogs()
	}
}

func (r *telemetryMetricsRecorder) recordEmittedBatch(
	startTime time.Time, numMessages int, mvcc hlc.Timestamp, bytes int, compressedBytes int,
) {
	r.metricsRecorder.recordEmittedBatch(startTime, numMessages, mvcc, bytes, compressedBytes)
	r.telemetryLogger.incEmittedCounters(numMessages, bytes)
	r.telemetryLogger.maybeFlushLogs()
}

// continuousTelemetryInterval determines the interval at which each node emits
// periodic telemetry events during the lifespan of each changefeed.
var continuousTelemetryInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"changefeed.telemetry.continuous_logging.interval",
	"determines the interval at which each node emits continuous telemetry events"+
		" during the lifespan of every changefeed; setting a zero value disables logging",
	24*time.Hour,
	settings.NonNegativeDuration,
)
