// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgerror

import (
	"bytes"
	goErr "errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/util/stacktrace"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

var _ error = (*Error)(nil)
var _ errors.ErrorHinter = (*Error)(nil)
var _ errors.ErrorDetailer = (*Error)(nil)
var _ errors.SafeDetailer = (*Error)(nil)
var _ fmt.Formatter = (*Error)(nil)

// Error implements the error interface.
func (pg *Error) Error() string { return pg.Message }

// ErrorHint implements the hintdetail.ErrorHinter interface.
func (pg *Error) ErrorHint() string { return pg.Hint }

// ErrorDetail implements the hintdetail.ErrorDetailer interface.
func (pg *Error) ErrorDetail() string { return pg.Detail }

// SafeDetails implements the errbase.SafeDetailer interface.
// TODO(knz): this is provided for compatibility with 19.1 nodes.
// Remove in 19.3, together with the "SafeDetail" proto field.
func (pg *Error) SafeDetails() []string {
	details := make([]string, len(pg.SafeDetail))
	for i, d := range pg.SafeDetail {
		details[i] = d.SafeMessage + d.EncodedStackTrace
	}
	return details
}

// FullError can be used when the hint and/or detail are to be tested.
func FullError(err error) string {
	var errString string
	if pqErr, ok := err.(*pq.Error); ok {
		errString = formatMsgHintDetail("pq: ", pqErr.Message, pqErr.Hint, pqErr.Detail)
	} else {
		pg := Flatten(err)
		errString = formatMsgHintDetail("", err.Error(), pg.Hint, pg.Detail)
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

// NewWithDepthf creates an error with a pg code and extracts the context
// information at the specified depth level.
func NewWithDepthf(depth int, code string, format string, args ...interface{}) error {
	err := errors.NewWithDepthf(1+depth, format, args...)
	err = WithCandidateCode(err, code)
	return err
}

// New creates an error with a code.
func New(code string, msg string) error {
	err := errors.NewWithDepth(1, msg)
	err = WithCandidateCode(err, code)
	return err
}

// Newf creates an Error with a format string.
func Newf(code string, format string, args ...interface{}) error {
	err := errors.NewWithDepthf(1, format, args...)
	err = WithCandidateCode(err, code)
	return err
}

// Noticef generates a Notice with a format string.
func Noticef(format string, args ...interface{}) error {
	err := errors.NewWithDepthf(1, format, args...)
	err = WithCandidateCode(err, pgcode.SuccessfulCompletion)
	return err
}

// DangerousStatementf creates a new error for "rejected dangerous
// statements".
func DangerousStatementf(format string, args ...interface{}) error {
	var buf bytes.Buffer
	buf.WriteString("rejected: ")
	fmt.Fprintf(&buf, format, args...)
	buf.WriteString(" (sql_safe_updates = true)")
	err := goErr.New(buf.String())
	err = errors.WithSafeDetails(err, format, args...)
	err = WithCandidateCode(err, pgcode.Warning)
	return err
}

// WrongNumberOfPreparedStatements creates new an Error for trying to prepare
// a query string containing more than one statement.
func WrongNumberOfPreparedStatements(n int) error {
	err := errors.NewWithDepthf(1, "prepared statement had %d statements, expected 1", errors.Safe(n))
	err = WithCandidateCode(err, pgcode.InvalidPreparedStatementDefinition)
	return err
}

var _ fmt.Formatter = &Error{}

// Format implements the fmt.Formatter interface.
//
// %v/%s prints the error as usual.
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
				st, err := stacktrace.DecodeStackTrace(d.EncodedStackTrace)
				if err != nil {
					fmt.Fprintf(s, "unable to encode stack trace: %+v", err)
				} else {
					fmt.Fprintf(s, "\n%s", stacktrace.PrintStackTrace(st))
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
