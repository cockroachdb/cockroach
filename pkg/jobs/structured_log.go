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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TODO[kyle.wong] should we have a job run id or job run number or something?
type JobStateChange struct {
	JobID                jobspb.JobID `json:"job_id"`
	JobType              string       `json:"job_type"`
	Status               Status       `json:"status"`
	StateChangeTimestamp int64        `json:"state_change_timestamp"`
}

func maybeLogStateChangeStructured(ctx context.Context, job *Job, status Status) {
	if job == nil {
		return
	}
	payload := job.Payload()
	out := &JobStateChange{
		JobID:                job.ID(),
		JobType:              payload.Type().String(),
		Status:               status,
		StateChangeTimestamp: timeutil.Now().UnixNano(),
	}
	log.Structured(ctx, log.StructuredMeta{EventType: log.JOB_STATE_CHANGE}, out)
}

func JobStateChangeProcessor(ctx context.Context, j *JobStateChange) error {
	// TODO: define a new systems table and persist job state changes to said table.
	fmt.Println(j)
	return nil
}

func InitJobStateLogProcessor(ctx context.Context, stopper *stop.Stopper) {
	processor := logstream.NewStructuredLogProcessor[*JobStateChange](JobStateChangeProcessor)
	logstream.RegisterProcessor(ctx, stopper, log.JOB_STATE_CHANGE, processor)
}
