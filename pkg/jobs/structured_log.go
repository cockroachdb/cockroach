// Copyright 2024 The Cockroach Authors.
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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

type JobEventType string
type JobEventLevel int

// JobEvent holds the details of an event emitted from the Jobs subsystem to be
// logged via structured logging.
type JobEvent struct {
	// ID of the job the event belongs to
	JobID jobspb.JobID `json:"job_id"`
	// EventLevel indicating the severity of the event
	EventLevel JobEventLevel `json:"event_level"`
	// Type of the job event being logged
	EventType JobEventType `json:"event_type"`
	// Time the job event took place
	EventTime time.Time `json:"event_time"`
	// A human-readable summary of the job event
	EventSummary string `json:"event_summary"`
	// Structured data with details about the job event
	EventDetails any `json:"event_details"`
}

// JobStateChangeDetails holds details specific to a job state change event.
type JobStateChangeDetails struct {
	// Status that a job has transitioned into
	NewStatus Status `json:"new_status"`
	// Status that a job has transitioned from
	PreviousStatus Status `json:"previous_status,omitempty"`
	// number representing how many times the job has been run
	RunNum int `json:"run_number"`
	// Any error that may have been produced in a job's lifecycle
	Error redact.RedactableString `json:"error,omitempty"`
	// The FinalResumeError that may exist in a job's progress field
	FinalResumeError redact.RedactableString `json:"final_resume_error,omitempty"`
}

const StateChange JobEventType = "state_change"
const (
	JobEventLevelNone JobEventLevel = iota
	JobEventLevelError
	JobEventLevelWarn
	JobEventLevelInfo
	JobEventLevelDebug
)

// Logs job state change using structured logging, if job is not nil.
func maybeLogStateChangeStructured(ctx context.Context, job *Job, status Status, jobErr error) {
	if job == nil {
		return
	}
	payload := job.Payload()
	runNum := job.getRunStats().NumRuns
	details := JobStateChangeDetails{
		NewStatus:      status,
		PreviousStatus: job.Status(),
		RunNum:         runNum,
	}

	if jobErr != nil {
		// TODO(kyle.wong): is jobErr.Error() already a redactable string, or do we need to
		// 	wrap the whole error in redaction tags?
		details.Error = redact.RedactableString(jobErr.Error())
	}

	if payload.FinalResumeError != nil {
		details.FinalResumeError = redact.RedactableString(payload.FinalResumeError.String())
	}

	out := &JobEvent{
		JobID:        job.ID(),
		EventLevel:   JobEventLevelInfo,
		EventType:    StateChange,
		EventTime:    timeutil.Now(),
		EventSummary: fmt.Sprintf("state changed to: %s", status),
		EventDetails: details,
	}

	log.Structured(ctx, log.JOB_EVENT, out)
}
