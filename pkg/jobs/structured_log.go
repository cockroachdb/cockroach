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

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/obs/logstream"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

// TODO[kyle.wong] should we have a job run id or job run number or something?
type JobStateChange struct {
	JobID                  jobspb.JobID            `json:"job_id"`
	JobType                string                  `json:"job_type"`
	Status                 Status                  `json:"status"`
	StateChangeTimeNanos   int64                   `json:"state_change_time_nanos"`
	ProgressModifiedMicros int64                   `json:"progress_modified_micros"`
	ProgressFractional     float32                 `json:"progress_fractional,omitempty"`
	ProgressWatermark      int64                   `json:"progress_watermark,omitempty"`
	Error                  redact.RedactableString `json:"error,omitempty"`
	FinalResumeError       redact.RedactableString `json:"final_resume_error,omitempty"`
}

func maybeLogStateChangeStructured(ctx context.Context, job *Job, status Status, jobErr error) {
	if job == nil {
		return
	}
	payload := job.Payload()
	progress := job.Progress()
	out := &JobStateChange{
		JobID:                  job.ID(),
		JobType:                payload.Type().String(),
		Status:                 status,
		StateChangeTimeNanos:   timeutil.Now().UnixNano(),
		ProgressModifiedMicros: progress.ModifiedMicros,
	}

	if jobErr != nil {
		// TODO(abarganier): properly redact
		out.Error = redact.RedactableString(jobErr.Error())
	}

	if payload.FinalResumeError != nil {
		out.FinalResumeError = redact.RedactableString(payload.FinalResumeError.String())
	}

	switch p := progress.Progress.(type) {
	case *jobspb.Progress_FractionCompleted:
		out.ProgressFractional = p.FractionCompleted
	case *jobspb.Progress_HighWater:
		out.ProgressWatermark = p.HighWater.WallTime
	}

	log.Structured(ctx, log.StructuredMeta{EventType: log.JOB_STATE_CHANGE}, out)
}

type JobStateChangeProcessor struct {
	r *Registry
}

func (js JobStateChangeProcessor) Process(ctx context.Context, j *JobStateChange) error {
	override := sessiondata.NodeUserSessionDataOverride
	override.Database = catconstants.SystemDatabaseName
	insertStmt := `
		INSERT INTO system.job_state_history (job_id, job_type, status, state_change_time, progress_modified_time, 
		                                      progress_fractional, progress_watermark, error, final_resume_error) 
		VALUES ($1, $2, $3, $4 :: TIMESTAMP, $5 :: TIMESTAMP, $6, $7 :: TIMESTAMP, $8, $9)`
	if js.r.db == nil {
		fmt.Println("DB IS NIL, continuing")
		return nil
	}
	return js.r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := txn.ExecEx(ctx, "job-state-history-insert", txn.KV(), override, insertStmt,
			[]interface{}{j.JobID, j.JobType, j.Status, j.StateChangeTimeNanos, j.ProgressModifiedMicros,
				j.ProgressFractional, j.ProgressWatermark, j.Error, j.FinalResumeError})
		return err
	})
}

func InitJobStateLogProcessor(ctx context.Context, stopper *stop.Stopper, r *Registry) {
	processor := logstream.NewStructuredLogProcessor[*JobStateChange](&JobStateChangeProcessor{r: r})
	logstream.RegisterProcessor(ctx, stopper, log.JOB_STATE_CHANGE, processor)
}

var _ logstream.LogProcessor[*JobStateChange] = (*JobStateChangeProcessor)(nil)
