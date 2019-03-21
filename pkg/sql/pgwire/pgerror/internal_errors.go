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

// assertionErrorHint gets added as hint on internal errors created
// via NewAssertionErrorf or NewAssertionErrorWithWrappedErrf.
const assertionErrorHint = `You have encountered an unexpected error inside CockroachDB.

Please check https://github.com/cockroachdb/cockroach/issues to check
whether this problem is already tracked. If you cannot find it there,
please report the error with details at:

    https://github.com/cockroachdb/cockroach/issues/new/choose

If you would rather not post publicly, please contact us directly at:

    support@cockroachlabs.com

The Cockroach Labs team appreciates your feedback.
`

// NewAssertionErrorf creates an internal error.
func NewAssertionErrorf(format string, args ...interface{}) error {
	return NewAssertionErrorWithDepthf(1, format, args...)
}

// NewAssertionErrorWithDepthf creates an internal error.
// The call stack is collected at "depth" levels higher in the call stack.
func NewAssertionErrorWithDepthf(depth int, format string, args ...interface{}) error {
	var err error = NewErrorWithDepthf(depth+1, CodeInternalError, format, args...)
	st := log.NewStackTrace(depth + 1)
	err = internalWithSafeDetailf(depth+1, err, st, format, args...)
	err = WithDetailf(err, "stack trace:\n%s", log.PrintStackTrace(st))
	err = WithHintf(err, assertionErrorHint)
	return err
}

// InternalErrorPrefix is prepended inside internal errors in this
// package, or when an error stack is flattened into a Error
// inside Flatten().
const InternalErrorPrefix = "internal error: "

// NewAssertionErrorWithWrappedErrf wraps an error (which may be a pg
// error) and decorates it as an internal error. The result implements
// the errors.Causer() interface to retrieve the original error object.
func NewAssertionErrorWithWrappedErrf(err error, format string, args ...interface{}) error {
	st := log.NewStackTrace(1)
	err = NewInternalErrorWrapper(err, CodeInternalError)
	err = internalWithSafeDetailf(1, err, st, format, args...)
	err = WithMessagef(err, format, args...)
	err = WithDetailf(err, "stack trace:\n%s", log.PrintStackTrace(st))
	err = WithHintf(err, assertionErrorHint)
	return err
}

// NewInternalTrackingError instantiates an error
// meant for use with telemetry.ReportError directly.
//
// Do not use this! Convert uses to NewAssertionErrorf or similar
// above.
func NewInternalTrackingError(issue int, detail string) error {
	key := fmt.Sprintf("#%d.%s", issue, detail)
	err := NewAssertionErrorWithDepthf(1, key)
	err = &withTelemetryKey{cause: err, key: key}
	err = &withHint{cause: err, hint: fmt.Sprintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)}
	return err
}

// NewStatementCompletionUnknownError creates an error with the corresponding pg
// code. This is used to inform the client that it's unknown whether a statement
// succeeded or not. Of particular interest to clients is when this error is
// returned for a statement outside of a transaction or for a COMMIT/RELEASE
// SAVEPOINT - there manual inspection may be necessary to check whether the
// statement/transaction committed. When this is returned for other
// transactional statements, the transaction has been rolled back (like it is
// for any errors).
//
// NOTE(andrei): When introducing this error, I haven't verified the exact
// conditions under which Postgres returns this code, nor its relationship to
// code CodeTransactionResolutionUnknownError. I couldn't find good
// documentation on these codes.
func NewStatementCompletionUnknownError(err error) error {
	return &withDefaultCode{cause: err, code: CodeStatementCompletionUnknownError}
}

// TxnRetryMsgPrefix is the prefix inserted in an error message when flattened
// to a retry error. It's also inserted by NewRetryError().
const TxnRetryMsgPrefix = "restart transaction"

// NewInternalErrorWrapper instantiates a wrapInternalError.
// It is exported for use in package sqlerror.
func NewInternalErrorWrapper(cause error, code string) error {
	if code == "" {
		return cause
	}
	return &withInternalError{cause: cause, internalErrorCode: code}
}

// NewUnknownPayloadWrapper instantiates a withUnknownErrorPayload.
// It is exported for use in package sqlerror.
func NewUnknownPayloadWrapper(cause error, payloadType string) error {
	if payloadType == "" {
		return cause
	}
	return &withUnknownErrorPayload{cause: cause, payloadType: payloadType}
}
