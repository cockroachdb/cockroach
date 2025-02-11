// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// errRetryJobSentinel exists so the errors returned from MarkAsRetryJobError can
// be marked with it, allowing more robust detection of retry errors even if
// they are wrapped, etc. This was originally introduced to deal with injected
// retry errors from testing knobs.
var errRetryJobSentinel = errors.New("retriable job error")

// MarkAsRetryJobError marks an error as a retriable job error which
// indicates that the registry should retry the job.
// Note that if a job is _not_ in the NonCancelable state, it will _only_ be
// retried if the error has been marked as a retry job error.
func MarkAsRetryJobError(err error) error {
	return errors.Mark(err, errRetryJobSentinel)
}

// IsRetryJobError checks whether the given error is a job retry error.
func IsRetryJobError(err error) bool {
	return errors.Is(err, errRetryJobSentinel)
}

// Registry does not retry a job that fails due to a permanent error.
var errJobPermanentSentinel = errors.New("permanent job error")

// MarkAsPermanentJobError marks an error as a permanent job error, which
// indicates Registry to not retry the job when it fails due to this error.
// Note that if a job is in the NonCancelable state, it will always be retried
// _unless_ the error has been marked as permanent job error.
func MarkAsPermanentJobError(err error) error {
	return errors.Mark(err, errJobPermanentSentinel)
}

// IsPermanentJobError checks whether the given error is a permanent error.
func IsPermanentJobError(err error) bool {
	return errors.Is(err, errJobPermanentSentinel)
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

// IsPauseSelfError checks whether the given error is a
// PauseRequestError.
func IsPauseSelfError(err error) bool {
	return errors.Is(err, errPauseSelfSentinel)
}

// PauseRequestExplained is a prose used to wrap and explain a pause-request error.
const PauseRequestExplained = "pausing due to error; use RESUME JOB to try to proceed once the issue is resolved, or CANCEL JOB to rollback"

// errJobLeaseNotHeld is a marker error for returning from a job execution if it
// knows or finds out it no longer has a job lease.
var errJobLeaseNotHeld = errors.New("job lease not held")

// InvalidStateError is the error returned when the desired operation is
// invalid given the job's current state.
type InvalidStateError struct {
	id    jobspb.JobID
	state State
	op    string
	err   string
}

func (e *InvalidStateError) Error() string {
	if e.err != "" {
		return fmt.Sprintf("cannot %s %s job (id %d, err: %q)", e.op, e.state, e.id, e.err)
	}
	return fmt.Sprintf("cannot %s %s job (id %d)", e.op, e.state, e.id)
}

// SimplifyInvalidStateError unwraps an *InvalidStateError into an error
// message suitable for users. Other errors are returned as passed.
func SimplifyInvalidStateError(err error) error {
	if ierr := (*InvalidStateError)(nil); errors.As(err, &ierr) {
		return errors.Errorf("job %s", ierr.state)
	}
	return err
}

// retriableExecutionError captures metadata about retriable errors encountered
// during the execution of a job. These errors propagate information to be
// stored in the payload in Payload.RetriableExecutionErrorLog.
type retriableExecutionError struct {
	instanceID base.SQLInstanceID
	start, end time.Time
	state      State
	cause      error
}

func newRetriableExecutionError(
	instanceID base.SQLInstanceID, state State, end time.Time, cause error,
) *retriableExecutionError {
	return &retriableExecutionError{
		instanceID: instanceID,
		state:      state,
		end:        end,
		cause:      cause,
	}
}

// Error makes retriableExecutionError and error.
func (e *retriableExecutionError) Error() string {
	return formatRetriableExecutionFailure(
		e.instanceID, e.state, e.start, e.end, e.cause,
	)

}

func formatRetriableExecutionFailure(
	instanceID base.SQLInstanceID, state State, start, end time.Time, cause error,
) string {
	mustTimestamp := func(ts time.Time) *tree.DTimestamp {
		ret, _ := tree.MakeDTimestamp(ts, time.Microsecond)
		return ret
	}
	return fmt.Sprintf(
		"%s execution from %v to %v on %d failed: %v",
		state,
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
