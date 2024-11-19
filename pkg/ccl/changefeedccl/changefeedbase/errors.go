// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/errors"
)

// FailureType is the reason for the changefeed failure that maps to the
// failure_type values we emit in the ChangefeedFailed telemetry events
type FailureType = string

const (
	// ConnectionClosed means the user disconnected from a core changefeed
	ConnectionClosed FailureType = "connection_closed"

	// UserInput applies to errors in the create statement prior to job creation
	UserInput FailureType = "user_input"

	// OnStartup applies to all non-user-input errors during Planning
	OnStartup FailureType = "on_startup"

	// UnknownError applies to all errors not otherwise categorized
	UnknownError FailureType = "unknown_error"
)

// Used for categorizing errors in logging
type taggedError struct {
	wrapped error
	tag     string
}

// MarkTaggedError wraps the given error with an extra tag string property to be
// read afterwards in cases such as logging
func MarkTaggedError(e error, tag string) error {
	return &taggedError{wrapped: e, tag: tag}
}

// IsTaggedError returns whether or not the top level error is a TaggedError
// along with its tag. This does not work if the tagged error is wrapped.
func IsTaggedError(err error) (bool, string) {
	if err == nil {
		return false, ""
	}
	if tagged := (*taggedError)(nil); errors.As(err, &tagged) {
		return true, tagged.tag
	}
	return false, ""
}

// Error implements the error interface.
func (e *taggedError) Error() string {
	return e.wrapped.Error()
}

// Cause implements the github.com/pkg/errors.causer interface.
func (e *taggedError) Cause() error { return e.wrapped }

// Unwrap implements the github.com/golang/xerrors.Wrapper interface, which is
// planned to be moved to the stdlib in go 1.13.
func (e *taggedError) Unwrap() error { return e.wrapped }

type terminalError struct{}

func (e *terminalError) Error() string {
	return "terminal changefeed error"
}

// TODO(yevgeniy): retryableError and all machinery related
// to MarkRetryableError maybe removed once 23.1 is released.
type retryableError struct{}

func (e *retryableError) Error() string {
	return "retryable changefeed error"
}

// WithTerminalError decorates underlying error to indicate
// that the error is a terminal changefeed error.
func WithTerminalError(cause error) error {
	if cause == nil {
		return nil
	}
	return errors.Mark(cause, &terminalError{})
}

// MarkRetryableError wraps the given error, marking it as retryable.s
func MarkRetryableError(cause error) error {
	if cause == nil {
		return nil
	}
	return errors.Mark(cause, &retryableError{})
}

type drainHelper interface {
	IsDraining() bool
}

// AsTerminalError determines if the cause error is a terminal changefeed
// error.  Returns non-nil error if changefeed should terminate with the
// returned error.
func AsTerminalError(ctx context.Context, dh drainHelper, cause error) (termErr error) {
	if cause == nil {
		return nil
	}

	if err := ctx.Err(); err != nil {
		// If context has been cancelled, we must respect that; this happens
		// if, e.g. this changefeed is being cancelled.
		return err
	}

	if dh.IsDraining() {
		// This node is being drained. It's safe to propagate this error (to the
		// job registry) since job registry should not be able to commit this error
		// to the jobs table; but to be safe, make sure this error is marked as jobs
		// retryable error to ensure that some other node retries this changefeed.
		return jobs.MarkAsRetryJobError(cause)
	}

	// GC TTL errors are always fatal.
	if errors.HasType(cause, (*kvpb.BatchTimestampBeforeGCError)(nil)) {
		return WithTerminalError(cause)
	}

	// Explicitly marked terminal errors are terminal.
	if errors.Is(cause, &terminalError{}) {
		return cause
	}

	// Assertion failures are terminal.
	if errors.HasAssertionFailure(cause) {
		return cause
	}

	// All other errors retry.
	return nil
}

// ErrNodeDraining indicates that this node is being drained.
var ErrNodeDraining = errors.New("node draining")
