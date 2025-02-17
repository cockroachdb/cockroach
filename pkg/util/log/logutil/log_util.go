// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logutil

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/redact"
)

// LogJobCompletion publishes an eventpb.RecoveryEvent about a successful or
// failed import, backup or restore job.
func LogJobCompletion(
	ctx context.Context,
	eventType eventpb.RecoveryEventType,
	jobID jobspb.JobID,
	success bool,
	jobErr error,
	numRows int64,
) {
	var redactedErr redact.RedactableString
	if jobErr != nil {
		redactedErr = redact.Sprint(jobErr)
	}
	status := jobs.StateSucceeded
	if !success {
		if jobs.HasErrJobCanceled(jobErr) {
			status = jobs.StateCanceled
		} else {
			status = jobs.StateFailed
		}
	}

	event := &eventpb.RecoveryEvent{
		RecoveryType: eventType,
		JobID:        uint64(jobID),
		ResultStatus: string(status),
		ErrorText:    redactedErr,
		NumRows:      numRows,
	}

	log.StructuredEvent(ctx, severity.INFO, event)
}

// LogEventsWithDelay logs an eventpb.EventPayload at provided
// delay duration to avoid exceeding the 10 log-line per second limit per node
// on the telemetry logging pipeline.
func LogEventsWithDelay(
	ctx context.Context, events []logpb.EventPayload, stopper *stop.Stopper, delay time.Duration,
) {
	// Log the first event immediately.
	timer := time.NewTimer(0 * time.Second)
	defer timer.Stop()
	for len(events) > 0 {
		select {
		case <-stopper.ShouldQuiesce():
			return
		case <-timer.C:
			event := events[0]
			log.StructuredEvent(ctx, severity.INFO, event)
			events = events[1:]
			// Apply a delay to subsequent events.
			timer.Reset(delay)
		}
	}
}
