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

// retryJobErrorSentinel exists so the errors returned from MarkAsRetryJobError can
// be marked with it, allowing more robust detection of retry errors even if
// they are wrapped, etc. This was originally introduced to deal with injected
// retry errors from testing knobs.
var retryJobErrorSentinel = errors.New("retriable job error")

// MarkAsRetryJobError marks an error as a retriable job error which
// indicates that the registry should retry the job.
func MarkAsRetryJobError(err error) error {
	return errors.Mark(err, retryJobErrorSentinel)
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
	instanceID base.SQLInstanceID, start, end time.Time, status Status, cause error,
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
	mustTimestamp := func(ts time.Time) *tree.DTimestamp {
		ret, _ := tree.MakeDTimestamp(ts, time.Microsecond)
		return ret
	}
	return fmt.Sprintf(
		"%s execution from %v to %v on %d failed: %v",
		e.status,
		mustTimestamp(e.start),
		mustTimestamp(e.end),
		e.instanceID,
		e.cause,
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
	ctx context.Context,
) *jobspb.RetriableExecutionFailure {
	ee := errors.EncodeError(ctx, e.cause)
	return &jobspb.RetriableExecutionFailure{
		Status:               string(e.status),
		ExecutionStartMicros: timeutil.ToUnixMicros(e.start),
		ExecutionEndMicros:   timeutil.ToUnixMicros(e.end),
		InstanceID:           e.instanceID,
		Error:                &ee,
	}
}
