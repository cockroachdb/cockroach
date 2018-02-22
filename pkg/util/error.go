// Copyright 2017 The Cockroach Authors.
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

package util

import (
	"fmt"

	"github.com/pkg/errors"
)

// UnexpectedWithIssueErr indicates an error with an associated Github issue.
// It's supposed to be used for conditions that would otherwise be checked by
// assertions, except that they fail and we need the public's help for tracking
// it down.
// The error message will invite users to report repros.
//
// Modeled after pgerror.Unimplemented.
type UnexpectedWithIssueErr struct {
	issue int
	msg   string
}

// UnexpectedWithIssueErrorf constructs an UnexpectedWithIssueError with the
// provided issue and formatted message.
func UnexpectedWithIssueErrorf(issue int, format string, args ...interface{}) error {
	return UnexpectedWithIssueErr{
		issue: issue,
		msg:   fmt.Sprintf(format, args...),
	}
}

// Error implements the error interface.
func (e UnexpectedWithIssueErr) Error() string {
	var fmtMsg string
	if e.msg != "" {
		fmtMsg = fmt.Sprintf(": %s", e.msg)
	}
	return fmt.Sprintf("unexpected error%s (we've been trying to track this particular issue down; "+
		"please report your reproduction at "+
		"https://github.com/cockroachdb/cockroach/issues/%d)", fmtMsg, e.issue)
}

// ErrorSource attempts to return the file:line where `i` was created if `i` has
// that information (i.e. if it is an errors.withStack). Returns "" otherwise.
func ErrorSource(i interface{}) string {
	type stackTracer interface {
		StackTrace() errors.StackTrace
	}
	if e, ok := i.(stackTracer); ok {
		tr := e.StackTrace()
		if len(tr) > 0 {
			return fmt.Sprintf("%v", tr[0]) // prints file:line
		}
	}
	return ""
}
