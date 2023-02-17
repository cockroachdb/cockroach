// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logutil

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
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
	status := jobs.StatusSucceeded
	if !success {
		if jobs.HasErrJobCanceled(jobErr) {
			status = jobs.StatusCanceled
		} else {
			status = jobs.StatusFailed
		}
	}

	event := &eventpb.RecoveryEvent{
		RecoveryType: eventType,
		JobID:        uint64(jobID),
		ResultStatus: string(status),
		ErrorText:    redactedErr,
		NumRows:      numRows,
	}

	log.StructuredEvent(ctx, event)
}
