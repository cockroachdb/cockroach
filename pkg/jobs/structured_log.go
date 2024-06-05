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

const (
	StateChange JobEventType = "state_change"
)

type JobEvent struct {
	JobID        jobspb.JobID `json:"job_id"`
	EventType    JobEventType `json:"event_type"`
	EventTime    time.Time    `json:"event_time"`
	EventSummary string       `json:"event_summary"`
	EventDetails any          `json:"event_details"`
}

type JobStateChangeDetails struct {
	NewStatus        Status                  `json:"new_status"`
	PreviousStatus   Status                  `json:"previous_status,omitempty"`
	RunNum           int                     `json:"run_number"`
	Error            redact.RedactableString `json:"error,omitempty"`
	FinalResumeError redact.RedactableString `json:"final_resume_error,omitempty"`
}

func maybeLogStateChangeStructured(ctx context.Context, job *Job, status Status, jobErr error) {
	if job == nil {
		return
	}
	payload := job.Payload()
	runNum := job.getRunStats().NumRuns
	// TODO (kyle.wong) how do we get previous status?
	details := JobStateChangeDetails{
		NewStatus:      status,
		PreviousStatus: job.Status(),
		RunNum:         runNum,
	}

	if jobErr != nil {
		// TODO(abarganier): properly redact
		details.Error = redact.RedactableString(jobErr.Error())
	}

	if payload.FinalResumeError != nil {
		details.FinalResumeError = redact.RedactableString(payload.FinalResumeError.String())
	}

	out := &JobEvent{
		JobID:        job.ID(),
		EventType:    StateChange,
		EventTime:    timeutil.Now(),
		EventSummary: fmt.Sprintf("state changed to: %s", status),
		EventDetails: details,
	}

	log.Structured(ctx, log.JOB_EVENT, out)
}
