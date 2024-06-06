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
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/obs/logstream"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
		// TODO(kyle.wong): is jobErr.Error() already a redactable string, or do we need to
		// 	wrap the whole error in redaction tags?
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

type JobStateChangeProcessor struct {
	r *Registry
}

func (js JobStateChangeProcessor) Process(ctx context.Context, e any) error {
	if js.r.db == nil {
		return nil
	}

	j, ok := e.(*JobEvent)

	if !ok {
		panic(errors.AssertionFailedf("Unexpected event type provided to JobStateChangeProcessor: %#v", e))
	}

	return js.r.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {

		values := []interface{}{j.JobID, j.EventType, j.EventTime, j.EventSummary}
		eventDetailsBytes, er := json.Marshal(j.EventDetails)
		if er != nil {
			panic(errors.AssertionFailedf("unable to marshal event details: %v", e))
		}
		values = append(values, string(eventDetailsBytes))

		insertStmt := `
		INSERT INTO system.job_events (job_id, event_type, event_time, event_summary, event_details) 
		VALUES ($1, $2, $3, $4, $5 :: JSONB)`
		override := sessiondata.NodeUserSessionDataOverride
		override.Database = catconstants.SystemDatabaseName
		_, err := txn.ExecEx(ctx, "job-state-history-insert", txn.KV(), override, insertStmt, values...)
		return err
	})
}

func InitJobStateLogProcessor(ctx context.Context, stopper *stop.Stopper, r *Registry) {
	logstream.RegisterProcessor(ctx, stopper, log.JOB_EVENT, &JobStateChangeProcessor{r: r})
}

var _ logstream.Processor = (*JobStateChangeProcessor)(nil)
