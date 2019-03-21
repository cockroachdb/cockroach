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

package sqlerror

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// NewAssertionErrorWithDepthf is the "advanced" version of the function in package pgerror.
func NewAssertionErrorWithDepthf(depth int, format string, args ...interface{}) error {
	var err error = pgerror.NewErrorWithDepthf(depth+1, pgerror.CodeInternalError, format, args...)
	st := log.NewStackTrace(depth + 1)
	err = internalWithSafeDetailf(depth+1, err, st, format, args...)
	err = WithDetailf(err, "stack trace:\n%s", log.PrintStackTrace(st))
	err = WithHintf(err, pgerror.AssertionErrorHint)
	return err
}

// NewAssertionErrorWithWrappedErrf is the "advanced" version of the function in package pgerror.
func NewAssertionErrorWithWrappedErrf(err error, format string, args ...interface{}) error {
	st := log.NewStackTrace(1)
	err = &withInternalError{cause: err, internalErrorCode: pgerror.CodeInternalError}
	err = internalWithSafeDetailf(1, err, st, format, args...)
	err = WithMessagef(err, format, args...)
	err = WithDetailf(err, "stack trace:\n%s", log.PrintStackTrace(st))
	err = WithHintf(err, pgerror.AssertionErrorHint)
	return err
}

func init() {
	// Override the base constructors with our advanced version.
	pgerror.NewAssertionErrorWithDepthf = NewAssertionErrorWithDepthf
	pgerror.NewAssertionErrorWithWrappedErrf = NewAssertionErrorWithWrappedErrf
}

// NewAssertionErrorf aliases the function in pgerror for convenience.
var NewAssertionErrorf = pgerror.NewAssertionErrorf

func isInternalErrorLeaf(err error) (bool, string) {
	switch e := err.(type) {
	case *withUnknownErrorPayload:
		return true, pgerror.CodeInternalError
	case *withInternalError:
		return true, e.internalErrorCode
	case *pgerror.Error:
		return e.Code == pgerror.CodeInternalError, e.Code
	}
	return false, ""
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
	return &withCode{cause: err, code: pgerror.CodeStatementCompletionUnknownError}
}

// TxnRetryMsgPrefix is the prefix inserted in an error message when flattened
// to a retry error. It's also inserted by NewRetryError().
const TxnRetryMsgPrefix = "restart transaction"
