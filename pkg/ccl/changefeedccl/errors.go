// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/errors"
)

const retryableErrorString = "retryable changefeed error"

// retryableError is deprecated, but used to maintain backwards
// compatibility with 20.2 nodes.
// TODO(pbardea): Remove in 21.2, and use the helpers in utilccl/errors.go
// instead.
type retryableError struct {
	wrapped error
}

// markRetryableError wraps the given error, marking it as retryable to
// changefeeds.
// TODO(pbardea): Remove in 21.2, and use utilccl.MarkRetryableError instead.
func markRetryableError(e error) error {
	// Wrap all these errors with a more generic job retry error.
	e = utilccl.MarkRetryableError(e)
	return &retryableError{wrapped: e}
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

// isChangefeedRetryableError returns true if the supplied error, or any of its
// parent causes, is a isChangefeedRetryableError. This should be used in
// utilccl.RetryDistSQLFlowCustomRetryable, and should eventually be removed in
// 21.2, when no more errors will be marked with this changefeed specific error.
func isChangefeedRetryableError(err error) bool {
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

	return false
}

// maybeStripRetryableErrorMarker performs some minimal attempt to clean the
// RetryableError marker out. This won't do anything if the RetryableError
// itself has been wrapped, but that's okay, we'll just have an uglier string.
func maybeStripRetryableErrorMarker(err error) error {
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
