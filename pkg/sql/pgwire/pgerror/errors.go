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
	"runtime"
	"strings"

	"github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// NewErrorWithDepth creates an Error with context extracted from the
// specified depth.
func NewErrorWithDepth(depth int, code string, msg string) *Error {
	return NewErrorWithDepthf(depth+1, code, "%s", msg)
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
	err := NewErrorWithDepthf(1, CodeInternalError, "internal error: "+format, args...)
	err.InternalCommand = captureTrace()
	err.Detail = err.InternalCommand
	err.Hint = assertionErrorHint
	return err
}

func captureTrace() string {
	var pc [50]uintptr
	n := runtime.Callers(3, pc[:])
	frames := runtime.CallersFrames(pc[:n])
	var buf bytes.Buffer
	sep := ""
	for {
		frame, more := frames.Next()
		if !more {
			break
		}
		file := frame.File
		if index := strings.LastIndexByte(file, '/'); index >= 0 {
			file = file[index+1:]
		}
		fmt.Fprintf(&buf, "%s%s:%d", sep, file, frame.Line)
		sep = ","
	}
	return buf.String()
}

// UnimplementedWithIssueErrorf constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssueErrorf(issue int, format string, args ...interface{}) error {
	err := NewErrorWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: "+format, args...)
	err.InternalCommand = fmt.Sprintf("#%d", issue)
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueError constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssueError(issue int, msg string) error {
	err := NewErrorWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: %s", msg)
	err.InternalCommand = fmt.Sprintf("#%d", issue)
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueDetailError constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>.detail" in tracking.
// This is useful when we need an extra axis of information to drill down into.
func UnimplementedWithIssueDetailError(issue int, detail, msg string) error {
	err := NewErrorWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: %s", msg)
	err.InternalCommand = fmt.Sprintf("#%d.%s", issue, detail)
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueHintError constructs an error with the given
// message, hint, and a link to the passed issue. Recorded as "#<issue>"
// in tracking.
func UnimplementedWithIssueHintError(issue int, msg, hint string) error {
	err := NewErrorWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: %s", msg)
	err.InternalCommand = fmt.Sprintf("#%d", issue)
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
	err.InternalCommand = feature
	err.Hint = unimplementedErrorHint
	return err
}

// Wrap wraps an error into a pgerror. The code is used
// if the original error was not a pgerror already. The errContext
// string is used to populate the InternalCommand. If InternalCommand
// already exists, the errContext is prepended.
func Wrap(err error, code, errContext string) error {
	var pgErr Error
	origErr, ok := GetPGCause(err)
	if ok {
		// Copy the error. We can't use the existing error directly
		// because it may be a global (const) object and we want to modify
		// it below.
		pgErr = *origErr
	} else {
		pgErr = Error{
			Code: code,
			// Keep the stack trace if one was available in the original
			// non-Error error (e.g. when constructed via errors.Wrap).
			InternalCommand: log.ErrorSource(err),
		}
	}

	// Prepend the context to the existing message.
	prefix := errContext + ": "
	pgErr.Message = prefix + err.Error()

	// Prepend the context also to the internal command, to ensure it
	// goes to telemetry.
	if pgErr.InternalCommand != "" {
		pgErr.InternalCommand = prefix + pgErr.InternalCommand
	} else {
		pgErr.InternalCommand = errContext
	}
	return &pgErr
}
