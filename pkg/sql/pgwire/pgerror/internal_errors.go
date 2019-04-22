// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgerror

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// This file provides facilities to track internal errors.
//
// At a high level the model adopted is the following:
// - at the point an internal error is created, the constructor
//   below will capture the stack trace. The stack strace
//   is stored in the error.
// - the error flows through the system.
// - at every point where the error flows out of the system,
//   the function log.ReportInternalError() is called on it.
//   This ensures an error report is uploaded.
//
// Note: the main point where an error flows out of the
// system is towards a SQL client. However there are also
// "internal" executions. The top level caller where
// such an error can be provided (e.g. distsql servers)
// should also be responsible for log.ReportInternalError().
//
// We store the collected stack trace as a string, because:
//
// - log.ReportInternalError() wants a log.Stacktrace
//   object as input.
// - however, we want to collect the stack trace at the point the
//   error is generated, not in log.ReportInternalError().
// - we can't store the log.Stacktrace object directly
//   in the pgerror.Error object, because that has to
//   be protobuf-encodable: pgerrors "flow" through distsql
//   across a cluster. log.Stacktrace is not protobuf-encodable.

const assertionErrorHint = `You have encountered an unexpected error inside CockroachDB.

Please check https://github.com/cockroachdb/cockroach/issues to check
whether this problem is already tracked. If you cannot find it there,
please report the error with details at:

    https://github.com/cockroachdb/cockroach/issues/new/choose

If you would rather not post publicly, please contact us directly at:

    support@cockroachlabs.com

The Cockroach Labs team appreciates your feedback.
`

// AssertionFailedf creates an internal error.
func AssertionFailedf(format string, args ...interface{}) *Error {
	return AssertionFailedWithDepthf(1, format, args...)
}

const internalErrorPrefix = "internal error: "

// AssertionFailedWithDepthf creates an internal error.
func AssertionFailedWithDepthf(depth int, format string, args ...interface{}) *Error {
	err := NewWithDepthf(depth+1, CodeInternalError, internalErrorPrefix+format, args...)
	st := log.NewStackTrace(depth + 1)

	// Internal details, reportable, must not contain PII.
	err.SafeDetail = []*Error_SafeDetail{{
		SafeMessage:       log.ReportablesToSafeError(depth+1, format, args).Error(),
		EncodedStackTrace: log.EncodeStackTrace(st),
	}}

	// User-visible details and hints, may contain PII.
	err.Detail = fmt.Sprintf("stack trace:\n%s", log.PrintStackTrace(st))
	err.Hint = assertionErrorHint
	return err
}

// NewAssertionErrorWithWrappedErrf wraps an error (which may be a pg error)
// and turns it into an assertion error. Both details from the original error
// and the context of the caller are preserved.
func NewAssertionErrorWithWrappedErrf(err error, format string, args ...interface{}) error {
	retErr := WrapWithDepthf(1, err, CodeInternalError, format, args...)
	pgErr, ok := GetPGCause(retErr)
	if ok {
		return pgErr
	}
	// The wrap was refused (it's a special error - eg a retry error). Force an assertion error.
	return AssertionFailedWithDepthf(1, "%v", err)
}

// NewInternalTrackingError instantiates an error
// meant for use with telemetry.ReportError directly.
//
// Do not use this! Convert uses to AssertionFailedf or similar
// above.
func NewInternalTrackingError(issue int, detail string) *Error {
	key := fmt.Sprintf("#%d.%s", issue, detail)
	err := AssertionFailedWithDepthf(1, key)
	err.TelemetryKey = key
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}
