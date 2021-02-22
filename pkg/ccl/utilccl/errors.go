// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package utilccl

import (
	"fmt"
	"strings"
)

const retryableJobsFlowError = "retryable jobs error"

type retryableError struct {
	wrapped error
}

// MarkRetryableError wraps the given error, marking it as retryable to
// jobs.
func MarkRetryableError(e error) error {
	return &retryableError{wrapped: e}
}

// Error implements the error interface.
func (e *retryableError) Error() string {
	return fmt.Sprintf("%s: %s", retryableJobsFlowError, e.wrapped.Error())
}

// Cause implements the github.com/pkg/errors.causer interface.
func (e *retryableError) Cause() error { return e.wrapped }

// Unwrap implements the github.com/golang/xerrors.Wrapper interface, which is
// planned to be moved to the stdlib in go 1.13.
func (e *retryableError) Unwrap() error { return e.wrapped }

// IsDistSQLRetryableError returns true if the supplied error, or any of its parent
// causes, is a IsDistSQLRetryableError.
func IsDistSQLRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// TODO(knz): this is a bad implementation. Make it go away
	// by avoiding string comparisons.

	errStr := err.Error()
	if strings.Contains(errStr, `rpc error`) {
		// When a crdb node dies, any DistSQL flows with processors scheduled on
		// it get an error with "rpc error" in the message from the call to
		// `(*DistSQLPlanner).Run`.
		return true
	}
	return false
}
