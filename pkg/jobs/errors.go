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
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// IsPauseSelfError checks whether the given error is a
// PauseRequestError.
func IsPauseSelfError(err error) bool {
	return errors.Is(err, errPauseSelfSentinel)
}

// errPauseSelfSentinel exists so the errors returned from PauseRequestErr can
// be marked with it.
var errPauseSelfSentinel = errors.New("job requested it be paused")

// MarkPauseRequestError marks an error as a pause request job error, which
// indicates to the Registry that the job would like to be paused rather than
// failing.
func MarkPauseRequestError(reason error) error {
	return errors.Mark(reason, errPauseSelfSentinel)
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

// retriableExecutionError captures metadata about retriable errors encountered
// during the execution of a job. These errors propagate information to be
// stored in the payload in Payload.RetriableExecutionErrorLog.
type retriableExecutionError struct {
	instanceID base.SQLInstanceID
	start, end time.Time
	status     Status
	cause      error
}

func newRetriableExecutionError(
	instanceID base.SQLInstanceID, status Status, start, end time.Time, cause error,
) *retriableExecutionError {
	return &retriableExecutionError{
		instanceID: instanceID,
		status:     status,
		start:      start,
		end:        end,
		cause:      cause,
	}
}

// Error makes retriableExecutionError and error.
func (e *retriableExecutionError) Error() string {
	return formatRetriableExecutionFailure(
		e.instanceID, e.status, e.start, e.end, e.cause,
	)

}

func formatRetriableExecutionFailure(
	instanceID base.SQLInstanceID, status Status, start, end time.Time, cause error,
) string {
	mustTimestamp := func(ts time.Time) *tree.DTimestamp {
		ret, _ := tree.MakeDTimestamp(ts, time.Microsecond)
		return ret
	}
	return fmt.Sprintf(
		"%s execution from %v to %v on %d failed: %v",
		status,
		mustTimestamp(start),
		mustTimestamp(end),
		instanceID,
		cause,
	)
}

// Cause exposes the underlying error.
func (e *retriableExecutionError) Cause() error { return e.cause }

// Unwrap exposes the underlying error.
func (e *retriableExecutionError) Unwrap() error { return e.cause }

// Format formats the error.
func (e *retriableExecutionError) Format(s fmt.State, verb rune) { errors.FormatError(e, s, verb) }

// SafeFormatError formats the error safely.
func (e *retriableExecutionError) SafeFormatError(p errors.Printer) error {
	if p.Detail() {
		p.Printf("retriable execution error")
	}
	return e.cause
}

func (e *retriableExecutionError) toRetriableExecutionFailure(
	ctx context.Context, maxErrorSize int,
) *jobspb.RetriableExecutionFailure {
	// If the cause is too large, we format it, losing all structure, and retain
	// a prefix.
	ef := &jobspb.RetriableExecutionFailure{
		Status:               string(e.status),
		ExecutionStartMicros: timeutil.ToUnixMicros(e.start),
		ExecutionEndMicros:   timeutil.ToUnixMicros(e.end),
		InstanceID:           e.instanceID,
	}
	if encodedCause := errors.EncodeError(ctx, e.cause); encodedCause.Size() < maxErrorSize {
		ef.Error = &encodedCause
	} else {
		formatted := e.cause.Error()
		if len(formatted) > maxErrorSize {
			formatted = formatted[:maxErrorSize]
		}
		ef.TruncatedError = formatted
	}
	return ef
}
