// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package pgerror

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

var _ error = &Error{}

// Error implements the error interface.
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

// NewWithDepthf creates an Error and extracts the context
// information at the specified depth level.
func NewWithDepthf(depth int, code string, format string, args ...interface{}) *Error {
	srcCtx := makeSrcCtx(depth + 1)
	return &Error{
		Message: fmt.Sprintf(format, args...),
		Code:    code,
		Source:  &srcCtx,
	}
}

// New creates an Error.
func New(code string, msg string) *Error {
	return NewWithDepthf(1, code, "%s", msg)
}

// Newf creates an Error with a format string.
func Newf(code string, format string, args ...interface{}) *Error {
	return NewWithDepthf(1, code, format, args...)
}

// DangerousStatementf creates a new Error for "rejected dangerous statements".
func DangerousStatementf(format string, args ...interface{}) *Error {
	var buf bytes.Buffer
	buf.WriteString("rejected: ")
	fmt.Fprintf(&buf, format, args...)
	buf.WriteString(" (sql_safe_updates = true)")
	return NewWithDepthf(1, CodeWarningError, "%s", buf.String())
}

// WrongNumberOfPreparedStatements creates new an Error for trying to prepare
// a query string containing more than one statement.
func WrongNumberOfPreparedStatements(n int) *Error {
	return NewWithDepthf(1, CodeInvalidPreparedStatementDefinitionError,
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

// UnimplementedWithIssuef constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssuef(issue int, format string, args ...interface{}) *Error {
	err := NewWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: "+format, args...)
	err.TelemetryKey = fmt.Sprintf("#%d", issue)
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssue constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssue(issue int, msg string) *Error {
	err := NewWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: %s", msg)
	err.TelemetryKey = fmt.Sprintf("#%d", issue)
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueDetail constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>.detail" in tracking.
// This is useful when we need an extra axis of information to drill down into.
func UnimplementedWithIssueDetail(issue int, detail, msg string) *Error {
	err := NewWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: %s", msg)
	if detail == "" {
		err.TelemetryKey = fmt.Sprintf("#%d", issue)
	} else {
		err.TelemetryKey = fmt.Sprintf("#%d.%s", issue, detail)
	}
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueDetailf is like the above
// but supports message formatting.
func UnimplementedWithIssueDetailf(issue int, detail, format string, args ...interface{}) *Error {
	err := NewWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: "+format, args...)
	if detail == "" {
		err.TelemetryKey = fmt.Sprintf("#%d", issue)
	} else {
		err.TelemetryKey = fmt.Sprintf("#%d.%s", issue, detail)
	}
	return err.SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueHint constructs an error with the given
// message, hint, and a link to the passed issue. Recorded as "#<issue>"
// in tracking.
func UnimplementedWithIssueHint(issue int, msg, hint string) *Error {
	err := NewWithDepthf(1, CodeFeatureNotSupportedError, "unimplemented: %s", msg)
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

// Unimplemented constructs an unimplemented feature error with a format string.
//
// `feature` is used for tracking, and is not included when the error is printed.
func Unimplemented(feature, msg string) *Error {
	return UnimplementedWithDepthf(1, feature, "%s", msg)
}

// Unimplementedf constructs an unimplemented feature error.
//
// `feature` is used for tracking, and is not included when the error is printed.
func Unimplementedf(feature, format string, args ...interface{}) *Error {
	return UnimplementedWithDepthf(1, feature, format, args...)
}

// UnimplementedWithDepthf constructs an implemented feature error,
// tracking the context at the specified depth.
func UnimplementedWithDepthf(depth int, feature, format string, args ...interface{}) *Error {
	err := NewWithDepthf(depth+1, CodeFeatureNotSupportedError, "unimplemented: "+format, args...)
	err.TelemetryKey = feature
	err.Hint = unimplementedErrorHint
	return err
}

var _ fmt.Formatter = &Error{}

// Format implements the fmt.Formatter interface.
//
// %v/%s prints the rror as usual.
// %#v adds the pg error code at the beginning.
// %+v prints all the details, including the embedded stack traces.
func (pg *Error) Format(s fmt.State, verb rune) {
	switch {
	case verb == 'v' && s.Flag('+'):
		// %+v prints all details.
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
		return
	case verb == 'v' && s.Flag('#'):
		// %#v spells out the code as prefix.
		fmt.Fprintf(s, "(%s) %s", pg.Code, pg.Message)
	case verb == 'v':
		fallthrough
	case verb == 's':
		fmt.Fprintf(s, "%s", pg.Message)
	case verb == 'q':
		fmt.Fprintf(s, "%q", pg.Message)
	}
}

// IsSQLRetryableError returns true if err is retryable. This is true
// for errors that show a connection issue or an issue with the node
// itself. This can occur when a node is restarting or is unstable in
// some other way. Note that retryable errors may occur event in cases
// where the SQL execution ran to completion.
//
// TODO(bdarnell): Why are RPC errors in this list? These should
// generally be retried on the server side or transformed into
// ambiguous result errors ("connection reset/refused" are needed for
// the pgwire connection, but anything RPC-related should be handled
// within the cluster).
// TODO(knz): This should really use the errors library. Investigate
// how to get rid of the error message comparison.
func IsSQLRetryableError(err error) bool {
	// Don't forget to update the corresponding test when making adjustments
	// here.
	errString := FullError(err)
	matched, merr := regexp.MatchString(
		"(no inbound stream connection|connection reset by peer|connection refused|failed to send RPC|rpc error: code = Unavailable|EOF|result is ambiguous)",
		errString)
	if merr != nil {
		return false
	}
	return matched
}
