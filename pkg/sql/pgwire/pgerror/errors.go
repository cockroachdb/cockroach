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

	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/pkg/errors"
)

var _ error = &Error{}

func (pg *Error) Error() string {
	return pg.Message
}

// NewError creates an Error.
func NewError(code string, msg string) *Error {
	return NewErrorf(code, "%s", msg)
}

// NewErrorf creates an Error with a format string.
func NewErrorf(code string, format string, args ...interface{}) *Error {
	srcCtx := makeSrcCtx(1)
	return &Error{
		Message: fmt.Sprintf(format, args...),
		Code:    code,
		Source:  &srcCtx,
	}
}

// NewDangerousStatementErrorf creates a new Error for "rejected dangerous statements".
func NewDangerousStatementErrorf(format string, args ...interface{}) *Error {
	var buf bytes.Buffer
	buf.WriteString("rejected: ")
	fmt.Fprintf(&buf, format, args...)
	buf.WriteString(" (sql_safe_updates = true)")
	return NewError(CodeWarningError, buf.String())
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

// UnimplementedWithIssueErrorf constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssueErrorf(issue int, msg string, args ...interface{}) error {
	feature := fmt.Sprintf("#%d", issue)
	var buf bytes.Buffer
	buf.WriteString("unimplemented: ")
	fmt.Fprintf(&buf, msg, args...)
	return Unimplemented(
		feature, buf.String(),
	).SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// UnimplementedWithIssueError constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssueError(issue int, msg string) error {
	feature := fmt.Sprintf("#%d", issue)
	return Unimplemented(
		feature, "unimplemented: "+msg,
	).SetHintf("See: https://github.com/cockroachdb/cockroach/issues/%d", issue)
}

// Unimplemented constructs an unimplemented feature error.
//
// `feature` is used for tracking, and is not included when the error printed.
func Unimplemented(feature, msg string) *Error {
	err := NewError(CodeFeatureNotSupportedError, msg)
	err.InternalCommand = feature
	return err
}
