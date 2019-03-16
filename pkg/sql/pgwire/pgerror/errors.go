// Copyright 2016 The Cockroach Authors.
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
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ error = &Error{}

func (pg *Error) Error() string {
	return pg.Message
}

// FullError can be used when the hint and/or detail are to be tested.
func FullError(err error) string {
	var errString string
	if pqErr, ok := err.(*pq.Error); ok {
		errString = formatMsgHintDetail("pq: ", pqErr.Message, pqErr.Hint, pqErr.Detail)
	} else if pg, ok := GetPGCause(err); ok {
		errString = formatMsgHintDetail("", err.Error(), pg.Hint, pg.Detail)
	} else {
		errString = err.Error()
	}
	return errString
}

func formatMsgHintDetail(prefix, msg, hint, detail string) string {
	var b strings.Builder
	b.WriteString(prefix)
	b.WriteString(msg)
	if hint != "" {
		b.WriteString("\nHINT: ")
		b.WriteString(hint)
	}
	if detail != "" {
		b.WriteString("\nDETAIL: ")
		b.WriteString(detail)
	}
	return b.String()
}

// NewErrorWithDepthf creates an Error and extracts the context
// information at the specified depth level.
func NewErrorWithDepthf(depth int, code string, format string, args ...interface{}) *Error {
	srcCtx := makeSrcCtx(depth + 1)
	return &Error{
		Message: fmt.Sprintf(format, args...),
		Code:    code,
		Source:  &srcCtx,
	}
}

// NewError creates an Error.
func NewError(code string, msg string) *Error {
	return NewErrorWithDepthf(1, code, "%s", msg)
}

// NewErrorf creates an Error with a format string.
func NewErrorf(code string, format string, args ...interface{}) *Error {
	return NewErrorWithDepthf(1, code, format, args...)
}

// NewDangerousStatementErrorf creates a new Error for "rejected dangerous statements".
func NewDangerousStatementErrorf(format string, args ...interface{}) *Error {
	var buf bytes.Buffer
	buf.WriteString("rejected: ")
	fmt.Fprintf(&buf, format, args...)
	buf.WriteString(" (sql_safe_updates = true)")
	return NewErrorWithDepthf(1, CodeWarningError, "%s", buf.String())
}

// NewWrongNumberOfPreparedStatements creates new an Error for trying to prepare
// a query string containing more than one statement.
func NewWrongNumberOfPreparedStatements(n int) *Error {
	return NewErrorWithDepthf(1, CodeInvalidPreparedStatementDefinitionError,
		"prepared statement had %d statements, expected 1", n)
}

// SetHintf annotates an Error object with a hint.
func (pg *Error) SetHintf(f string, args ...interface{}) *Error {
	pg.Hint = fmt.Sprintf(f, args...)
	return pg
}

// SetDetailf annotates an Error object with details.
func (pg *Error) SetDetailf(f string, args ...interface{}) *Error {
	pg.Detail = fmt.Sprintf(f, args...)
	return pg
}

// ResetSource resets the Source field of the Error object
// with the details on the depth-level caller of ResetSource.
func (pg *Error) ResetSource(depth int) {
	srcCtx := makeSrcCtx(depth + 1)
	pg.Source = &srcCtx
}

// makeSrcCtx creates a Error_Source value with contextual information
// about the caller at the requested depth.
func makeSrcCtx(depth int) Error_Source {
	f, l, fun := caller.Lookup(depth + 1)
	return Error_Source{File: f, Line: int32(l), Function: fun}
}

// GetPGCause returns an unwrapped Error.
func GetPGCause(err error) (*Error, bool) {
	switch pgErr := errors.Cause(err).(type) {
	case *Error:
		return pgErr, true

	default:
		return nil, false
	}
}

// UnimplementedWithIssueErrorf constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssueErrorf(issue int, format string, args ...interface{}) *Error {
	err := NewErrorWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: "+format, args...)
	err.TelemetryKey = fmt.Sprintf("#%d", issue)
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueError constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssueError(issue int, msg string) *Error {
	err := NewErrorWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: %s", msg)
	err.TelemetryKey = fmt.Sprintf("#%d", issue)
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueDetailError constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>.detail" in tracking.
// This is useful when we need an extra axis of information to drill down into.
func UnimplementedWithIssueDetailError(issue int, detail, msg string) *Error {
	err := NewErrorWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: %s", msg)
	if detail == "" {
		err.TelemetryKey = fmt.Sprintf("#%d", issue)
	} else {
		err.TelemetryKey = fmt.Sprintf("#%d.%s", issue, detail)
	}
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueDetailErrorf is like the above
// but supports message formatting.
func UnimplementedWithIssueDetailErrorf(
	issue int, detail, format string, args ...interface{},
) *Error {
	err := NewErrorWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: "+format, args...)
	if detail == "" {
		err.TelemetryKey = fmt.Sprintf("#%d", issue)
	} else {
		err.TelemetryKey = fmt.Sprintf("#%d.%s", issue, detail)
	}
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueHintError constructs an error with the given
// message, hint, and a link to the passed issue. Recorded as "#<issue>"
// in tracking.
func UnimplementedWithIssueHintError(issue int, msg, hint string) *Error {
	err := NewErrorWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: %s", msg)
	err.TelemetryKey = fmt.Sprintf("#%d", issue)
	return err.SetHintf("%s\nSee: https://github.com/cockroachdb/cockroach/issues/%d", hint, issue)
}

const unimplementedErrorHint = `This feature is not yet implemented in CockroachDB.

Please check https://github.com/cockroachdb/cockroach/issues to check
whether this feature is already tracked. If you cannot find it there,
please report this error with reproduction steps at:

    https://github.com/cockroachdb/cockroach/issues/new/choose

If you would rather not post publicly, please contact us directly at:

    support@cockroachlabs.com

The Cockroach Labs team appreciates your feedback.
`

// Unimplemented constructs an unimplemented feature error.
//
// `feature` is used for tracking, and is not included when the error printed.
func Unimplemented(feature, msg string, args ...interface{}) *Error {
	return UnimplementedWithDepth(1, feature, msg, args...)
}

// UnimplementedWithDepth constructs an implemented feature error,
// tracking the context at the specified depth.
func UnimplementedWithDepth(depth int, feature, msg string, args ...interface{}) *Error {
	err := NewErrorWithDepthf(depth+1, CodeFeatureNotSupportedError, msg, args...)
	err.TelemetryKey = feature
	err.Hint = unimplementedErrorHint
	return err
}

// Wrapf wraps an error into a pgerror. See
// the doc on WrapWithDepthf for details.
func Wrapf(err error, code, format string, args ...interface{}) *Error {
	return WrapWithDepthf(1, err, code, format, args...)
}

// Wrapf wraps an error into a pgerror. If the code is not
// CodeInternalError, the code is used only if the original error was
// not a pgerror already. If the code is CodeInternalError, the code
// overrides the original error code and the result becomes an
// internal error.
func WrapWithDepthf(depth int, err error, code, format string, args ...interface{}) *Error {
	var pgErr Error
	origErr, ok := GetPGCause(err)
	if ok {
		// Copy the error. We can't use the existing error directly
		// because it may be a global (const) object and we want to modify
		// it below.
		pgErr = *origErr
		pgErr.Message = fmt.Sprintf(format, args...) + fmt.Sprintf(": %s", pgErr.Message)
	} else {
		pgErr = Error{
			Code:    code,
			Message: fmt.Sprintf(format, args...) + fmt.Sprintf(": %v", err),
		}

		// Keep the safe message, if any.
		msg := log.Redact(err)

		// Keep the stack trace if one was available in the original
		// non-Error error (e.g. when constructed via errors.Wrap).
		type stackTracer interface {
			StackTrace() errors.StackTrace
		}
		if e, ok := err.(stackTracer); ok {
			tr := e.StackTrace()
			// TODO(knz): convert e.StackTrace() to a log.StackTrace then
			// populate EncodedStackTrace instead.
			msg = fmt.Sprintf("%s\noriginal error stack trace:\n%+v", msg, tr)
		}
		pgErr.SafeDetail = []*Error_SafeDetail{&Error_SafeDetail{
			SafeMessage: msg,
		}}
	}

	// We'll want to report what we can from the provided context.
	st := log.NewStackTrace(depth + 1)
	pgErr.SafeDetail = append(pgErr.SafeDetail, &Error_SafeDetail{
		SafeMessage:       log.ReportablesToSafeError(depth+1, format, args).Error(),
		EncodedStackTrace: log.EncodeStackTrace(st),
	})

	// If wrapping a non-internal error using the internal error code,
	// the internal error prevails.
	if code == CodeInternalError {
		// Override the result to become an internal error.
		pgErr.Code = code
		pgErr.Hint = assertionErrorHint

		// Add the stack trace to the user-visible details as well.
		var buf bytes.Buffer
		if pgErr.Detail != "" {
			fmt.Fprintf(&buf, "%s\n", pgErr.Detail)
		}
		fmt.Fprintf(&buf, "stack trace: %s", log.PrintStackTrace(st))
		pgErr.Detail = buf.String()
	}
	return &pgErr
}

// Format implements the fmt.Formatter interface.
func (pg *Error) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		switch {
		case s.Flag('+'):
			if pg.Source != nil {
				fmt.Fprintf(s, "%s:%d in %s(): ", pg.Source.File, pg.Source.Line, pg.Source.Function)
			}
			fmt.Fprintf(s, "(%s) %s", pg.Code, pg.Message)
			for _, d := range pg.SafeDetail {
				fmt.Fprintf(s, "\n-- detail --\n%s", d.SafeMessage)
				if d.EncodedStackTrace != "" {
					if st, ok := log.DecodeStackTrace(d.EncodedStackTrace); ok {
						fmt.Fprintf(s, "\n%s", log.PrintStackTrace(st))
					}
				}
			}
		default:
			fmt.Fprintf(s, "(%s) %s", pg.Code, pg.Message)
		}
	case 's':
		fmt.Fprintf(s, "%s", pg.Message)
	}
}
