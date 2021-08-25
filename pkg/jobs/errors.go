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
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errbase"
	github_com_gogo_protobuf_proto "github.com/gogo/protobuf/proto"
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

// RetriableExecutionError captures metadata about retriable errors encountered
// during the execution of a job. These errors are stored in the payload in the
// ResumeErrors field.
type RetriableExecutionError struct {
	md    jobspb.ExecutionFailure
	cause error
}

func newRetriableExecutionError(
	instanceID base.SQLInstanceID, now, start time.Time, status Status, cause error,
) *RetriableExecutionError {
	return &RetriableExecutionError{
		md: jobspb.ExecutionFailure{
			InstanceID:           instanceID,
			Status:               string(status),
			ExecutionStartMicros: start.UnixNano() / 1000,
			ExecutionEndMicros:   now.UnixNano() / 1000,
		},
		cause: cause,
	}
}

// Error makes RetriableExecutionError and error.
func (e *RetriableExecutionError) Error() string {
	return fmt.Sprintf(
		"%s execution from %v to %v on %d failed: %v",
		e.md.Status, e.Start(), e.End(), e.md.InstanceID, e.cause,
	)
}

// Start returns the time at which this execution started.
func (e *RetriableExecutionError) Start() *tree.DTimestamp {
	return microsToTime(e.md.ExecutionStartMicros)
}

// End returns the time at which this execution concluded.
func (e *RetriableExecutionError) End() *tree.DTimestamp {
	return microsToTime(e.md.ExecutionEndMicros)
}

func microsToTime(micros int64) *tree.DTimestamp {
	ts, _ := tree.MakeDTimestamp(
		time.Unix(0, micros*time.Microsecond.Nanoseconds()),
		time.Microsecond,
	)
	return ts
}

// Cause exposes the underlying error.
func (w *RetriableExecutionError) Cause() error { return w.cause }

// Unwrap exposes the underlying error.
func (w *RetriableExecutionError) Unwrap() error { return w.cause }

// Format formats the error.
func (w *RetriableExecutionError) Format(s fmt.State, verb rune) { errbase.FormatError(w, s, verb) }

// SafeFormatError formats the error safely.
func (w *RetriableExecutionError) SafeFormatError(p errbase.Printer) error {
	if p.Detail() {
		p.Printf("assertion failure")
	}
	return w.cause
}

func init() {
	k := errors.GetTypeKey((*RetriableExecutionError)(nil))
	errors.RegisterWrapperEncoder(k, func(
		ctx context.Context, err error,
	) (msgPrefix string, safeDetails []string, payload github_com_gogo_protobuf_proto.Message) {
		e := err.(*RetriableExecutionError)
		return "", nil, &e.md
	})
	errors.RegisterWrapperDecoder(k, func(
		ctx context.Context, cause error, msgPrefix string, safeDetails []string, payload github_com_gogo_protobuf_proto.Message,
	) error {
		return &RetriableExecutionError{
			md:    *payload.(*jobspb.ExecutionFailure),
			cause: cause,
		}
	})
}
