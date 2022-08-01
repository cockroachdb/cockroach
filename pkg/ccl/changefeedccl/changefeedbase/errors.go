// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedbase

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/jobs/joberror"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/flowinfra"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

const retryableErrorString = "retryable changefeed error"

type retryableError struct {
	// A schema change may result in a changefeed returning retryableError,
	// which can signal the changefeed to restart.
	// boundaryTimestamp can be returned inside this error so
	// the changefeed knows where to restart from. Note this is
	// only useful for sinkless/core changefeeds because they do not have
	// the ability to read/write their state to jobs tables during restarts.
	boundaryTimestamp hlc.Timestamp
	wrapped           error
}

// MarkRetryableError wraps the given error, marking it as retryable to
// changefeeds.
func MarkRetryableError(e error) error {
	return &retryableError{wrapped: e}
}

// MarkRetryableErrorWithTimestamp wraps the given error, marks it as
// retryable, and attaches a timestamp to the error.
func MarkRetryableErrorWithTimestamp(e error, ts hlc.Timestamp) error {
	return &retryableError{boundaryTimestamp: ts, wrapped: e}
}

// Error implements the error interface.
func (e *retryableError) Error() string {
	return fmt.Sprintf("%s: %s", retryableErrorString, e.wrapped.Error())
}

// Cause implements the github.com/pkg/errors.causer interface.
func (e *retryableError) Cause() error { return e.wrapped }

// Unwrap implements the github.com/golang/xerrors.Wrapper interface, which is
// planned to be moved to the stdlib in go 1.13.
func (e *retryableError) Unwrap() error { return e.wrapped }

// IsRetryableError returns true if the supplied error, or any of its parent
// causes, is a IsRetryableError.
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.HasType(err, (*retryableError)(nil)) {
		return true
	}

	// TODO(knz): this is a bad implementation. Make it go away
	// by avoiding string comparisons.

	errStr := err.Error()
	if strings.Contains(errStr, retryableErrorString) {
		// If a RetryableError occurs on a remote node, DistSQL serializes it such
		// that we can't recover the structure and we have to rely on this
		// unfortunate string comparison.
		return true
	}

	return (joberror.IsDistSQLRetryableError(err) ||
		flowinfra.IsNoInboundStreamConnectionError(err) ||
		errors.HasType(err, (*roachpb.NodeUnavailableError)(nil)) ||
		errors.Is(err, sql.ErrPlanChanged))
}

// MaybeGetRetryableErrorTimestamp will get the timestamp of an error if
// the error is a retryableError and the timestamp field is populated.
func MaybeGetRetryableErrorTimestamp(err error) (timestamp hlc.Timestamp, ok bool) {
	if retryableErr := (*retryableError)(nil); errors.As(err, &retryableErr) {
		if !retryableErr.boundaryTimestamp.IsEmpty() {
			return retryableErr.boundaryTimestamp, true
		}
	}
	return hlc.Timestamp{}, false
}

// MaybeStripRetryableErrorMarker performs some minimal attempt to clean the
// RetryableError marker out. This won't do anything if the RetryableError
// itself has been wrapped, but that's okay, we'll just have an uglier string.
func MaybeStripRetryableErrorMarker(err error) error {
	// The following is a hack to work around the error cast linter.
	// What we're doing here is really not kosher; this function
	// has no business in assuming that the retryableError{} wrapper
	// has not been wrapped already. We could even expect that
	// it gets wrapped in the common case.
	// TODO(knz): Remove/replace this.
	if reflect.TypeOf(err) == retryableErrorType {
		err = errors.UnwrapOnce(err)
	}
	return err
}

var retryableErrorType = reflect.TypeOf((*retryableError)(nil))
