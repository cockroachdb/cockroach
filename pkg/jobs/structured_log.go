// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// LogStateChangeStructured job state change using structured logging, if job is not nil.
func LogStateChangeStructured(
	ctx context.Context,
	id jobspb.JobID,
	jobType string,
	payload *jobspb.Payload,
	prevState State,
	state State,
) {
	// TODO (msbutler): check with obs about changing proto names.
	out := eventpb.StatusChange{
		JobID:          int64(id),
		JobType:        jobType,
		Description:    redact.Sprintf("state changed to: %s", state),
		PreviousStatus: string(prevState),
		NewStatus:      string(state),
	}

	if payload != nil {
		if payload.FinalResumeError != nil {
			if finalResumeError := errors.DecodeError(ctx, *payload.FinalResumeError); finalResumeError != nil {
				out.FinalResumeErr = finalResumeError.Error()
			}
		}

		if payload.Error != "" {
			out.Error = payload.Error
		}
	}

	log.StructuredEventDepth(ctx, severity.INFO, 1, &out)
}
