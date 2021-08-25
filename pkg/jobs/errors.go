// Copyright 2021 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/errors"
)

// errRetryJobSentinel exists so the errors returned from MarkAsRetryJobError can
// be marked with it, allowing more robust detection of retry errors even if
// they are wrapped, etc. This was originally introduced to deal with injected
// retry errors from testing knobs.
var errRetryJobSentinel = errors.New("retriable job error")

// MarkAsRetryJobError marks an error as a retriable job error which
// indicates that the registry should retry the job.
func MarkAsRetryJobError(err error) error {
	return errors.Mark(err, errRetryJobSentinel)
}

// Registry does not retry a job that fails due to a permanent error.
var errJobPermanentSentinel = errors.New("permanent job error")

// MarkAsPermanentJobError marks an error as a permanent job error, which indicates
// Registry to not retry the job when it fails due to this error.
func MarkAsPermanentJobError(err error) error {
	return errors.Mark(err, errJobPermanentSentinel)
}

// IsPermanentJobError checks whether the given error is a permanent error.
func IsPermanentJobError(err error) bool {
	return errors.Is(err, errJobPermanentSentinel)
}

// InvalidStatusError is the error returned when the desired operation is
// invalid given the job's current status.
type InvalidStatusError struct {
	id     jobspb.JobID
	status Status
	op     string
	err    string
}

func (e *InvalidStatusError) Error() string {
	if e.err != "" {
		return fmt.Sprintf("cannot %s %s job (id %d, err: %q)", e.op, e.status, e.id, e.err)
	}
	return fmt.Sprintf("cannot %s %s job (id %d)", e.op, e.status, e.id)
}

// SimplifyInvalidStatusError unwraps an *InvalidStatusError into an error
// message suitable for users. Other errors are returned as passed.
func SimplifyInvalidStatusError(err error) error {
	if ierr := (*InvalidStatusError)(nil); errors.As(err, &ierr) {
		return errors.Errorf("job %s", ierr.status)
	}
	return err
}
