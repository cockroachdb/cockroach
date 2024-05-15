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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

type ProgressHighWatermark struct {
	WallTime int64 `json:"wall_time"`
	Logical  int32 `json:"logical"`
}

type ProgressFractional struct {
	FractionCompleted float32 `json:"fraction_completed;omitempty"`
}

type JobStateChange struct {
	JobID   jobspb.JobID `json:"job_id"`
	JobType string       `json:"job_type"`
	Status  Status       `json:"status"`
	// TODO(abarganier): we need to consider redaction for this string field.
	RunningStatus        redact.RedactableString `json:"running_status,omitempty"`
	StateChangeTimeNanos int64                   `json:"state_change_time_nanos"`
	ModifiedMicros       int64                   `json:"modified_micros"`
	// TODO(abarganier): Figure out redactable errors.
	// TODO(abarganier): Payload struct has error, resume_errors, cleanup_errors, final_resume_error,
	// do we need to include these?
	Error redact.RedactableString `json:"error,omitempty"`
	// TODO(abarganier): Should we include the RetriableExecutionFailures from the Payload?
	ProgressWatermark  *ProgressHighWatermark  `json:"progress_watermark,omitempty"`
	ProgressFractional *ProgressFractional     `json:"progress_fractional,omitempty"`
	PauseReason        redact.RedactableString `json:"pause_reason,omitempty"`
	// TODO(abarganier) should we include the Session?
}

func maybeLogStateChangeStructured(ctx context.Context, job *Job, status Status, jobErr error) {
	if job == nil {
		return
	}
	payload := job.Payload()
	progress := job.Progress()
	out := &JobStateChange{
		JobID:   job.ID(),
		JobType: payload.Type().String(),
		Status:  status,
		// TODO(abarganier): properly redact
		RunningStatus:        redact.RedactableString(progress.RunningStatus),
		StateChangeTimeNanos: timeutil.Now().UnixNano(),
		ModifiedMicros:       progress.ModifiedMicros,
		// TODO(abarganier): properly redact
		PauseReason: redact.RedactableString(payload.PauseReason),
	}
	if jobErr != nil {
		// TODO(abarganier): properly redact
		out.Error = redact.RedactableString(jobErr.Error())
	}
	switch p := progress.Progress.(type) {
	case *jobspb.Progress_FractionCompleted:
		out.ProgressFractional = &ProgressFractional{FractionCompleted: p.FractionCompleted}
	case *jobspb.Progress_HighWater:
		out.ProgressWatermark = &ProgressHighWatermark{
			WallTime: p.HighWater.WallTime,
			Logical:  p.HighWater.Logical,
		}
	}
	log.Structured(ctx, log.StructuredMeta{EventType: log.JOB_STATE_CHANGE}, out)
}
