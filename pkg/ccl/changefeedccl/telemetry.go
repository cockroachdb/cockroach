// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

// sinkTelemetry is used to track metrics destined to be
// logged as telemetry.
type sinkTelemetryData struct {
	emittedBytes *atomic.Int64
}

func makeSinkTelemetryData() sinkTelemetryData {
	return sinkTelemetryData{
		emittedBytes: &atomic.Int64{},
	}
}

// incEmittedBytes atomically adds numBytes to the running count of emitted bytes.
func (std *sinkTelemetryData) incEmittedBytes(numBytes int) {
	std.emittedBytes.Add(int64(numBytes))
}

// resetEmittedBytes atomically resets running count of emitted bytes and
// returns the old count.
func (std *sinkTelemetryData) resetEmittedBytes() int {
	return int(std.emittedBytes.Swap(0))
}

const ongoingTelemetryLogPeriod = 10 * time.Hour

type periodicTelemetryLogger struct {
	sinkTelemetry sinkTelemetryData
	lastResolved  hlc.Timestamp
}

func makePeriodicTelemetryLogger() periodicTelemetryLogger {
	return periodicTelemetryLogger{
		sinkTelemetry: makeSinkTelemetryData(),
	}
}

func (ptl *periodicTelemetryLogger) maybeLogTelemetry(
	ctx context.Context,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	stmt string,
	newResolved hlc.Timestamp,
	knobs TestingKnobs,
) {

	if knobs.LogTelemetryEveryTime || newResolved.WallTime-ptl.lastResolved.WallTime > ongoingTelemetryLogPeriod.Nanoseconds() {
		changefeedEventDetails := getCommonChangefeedEventDetails(ctx, details, stmt)
		continuousTelemetryEvent := &eventpb.ChangefeedEmittedBytes{
			CommonChangefeedEventDetails: changefeedEventDetails,
			Id:                           int64(jobID),
			EmittedBytes:                 int32(ptl.sinkTelemetry.resetEmittedBytes()),
		}
		log.StructuredEvent(ctx, continuousTelemetryEvent)
	}
}

func logChangefeedCreateTelemetry(ctx context.Context, jr *jobs.Record) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if jr != nil {
		changefeedDetails := jr.Details.(jobspb.ChangefeedDetails)
		changefeedEventDetails = getCommonChangefeedEventDetails(ctx, changefeedDetails, jr.Description)
	}

	createChangefeedEvent := &eventpb.CreateChangefeed{
		CommonChangefeedEventDetails: changefeedEventDetails,
	}

	log.StructuredEvent(ctx, createChangefeedEvent)
}

func logChangefeedFailedTelemetry(
	ctx context.Context, job *jobs.Job, failureType changefeedbase.FailureType,
) {
	var changefeedEventDetails eventpb.CommonChangefeedEventDetails
	if job != nil {
		changefeedDetails := job.Details().(jobspb.ChangefeedDetails)
		changefeedEventDetails = getCommonChangefeedEventDetails(ctx, changefeedDetails, job.Payload().Description)
	}

	changefeedFailedEvent := &eventpb.ChangefeedFailed{
		CommonChangefeedEventDetails: changefeedEventDetails,
		FailureType:                  failureType,
	}

	log.StructuredEvent(ctx, changefeedFailedEvent)
}

func getCommonChangefeedEventDetails(
	ctx context.Context, details jobspb.ChangefeedDetails, description string,
) eventpb.CommonChangefeedEventDetails {
	opts := details.Opts

	sinkType := "core"
	if details.SinkURI != `` {
		parsedSink, err := url.Parse(details.SinkURI)
		if err != nil {
			log.Warningf(ctx, "failed to parse sink for telemetry logging: %v", err)
		}
		sinkType = parsedSink.Scheme
	}

	var initialScan string
	initialScanType, initialScanSet := opts[changefeedbase.OptInitialScan]
	_, initialScanOnlySet := opts[changefeedbase.OptInitialScanOnly]
	_, noInitialScanSet := opts[changefeedbase.OptNoInitialScan]
	if initialScanSet && initialScanType == `` {
		initialScan = `yes`
	} else if initialScanSet && initialScanType != `` {
		initialScan = initialScanType
	} else if initialScanOnlySet {
		initialScan = `only`
	} else if noInitialScanSet {
		initialScan = `no`
	}

	var resolved string
	resolvedValue, resolvedSet := opts[changefeedbase.OptResolvedTimestamps]
	if !resolvedSet {
		resolved = "no"
	} else if resolvedValue == `` {
		resolved = "yes"
	} else {
		resolved = resolvedValue
	}

	changefeedEventDetails := eventpb.CommonChangefeedEventDetails{
		Description: description,
		SinkType:    sinkType,
		// TODO: Rename this field to NumTargets.
		NumTables:   int32(AllTargets(details).Size),
		Resolved:    resolved,
		Format:      opts[changefeedbase.OptFormat],
		InitialScan: initialScan,
	}

	return changefeedEventDetails
}

func failureTypeForStartupError(err error) changefeedbase.FailureType {
	if errors.Is(err, context.Canceled) { // Occurs for sinkless changefeeds
		return changefeedbase.ConnectionClosed
	} else if isTagged, tag := changefeedbase.IsTaggedError(err); isTagged {
		return tag
	}
	return changefeedbase.OnStartup
}
