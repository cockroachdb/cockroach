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

package errorutil

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// UnexpectedWithIssueErr indicates an error with an associated Github issue.
// It's supposed to be used for conditions that would otherwise be checked by
// assertions, except that they fail and we need the public's help for tracking
// it down.
// The error message will invite users to report repros.
//
// Modeled after pgerror.Unimplemented.
type UnexpectedWithIssueErr struct {
	issue   int
	msg     string
	safeMsg string
}

// UnexpectedWithIssueErrorf constructs an UnexpectedWithIssueError with the
// provided issue and formatted message.
func UnexpectedWithIssueErrorf(issue int, format string, args ...interface{}) error {
	return UnexpectedWithIssueErr{
		issue:   issue,
		msg:     fmt.Sprintf(format, args...),
		safeMsg: log.ReportablesToSafeError(2 /* depth */, format, args).Error(),
	}
}

// Error implements the error interface.
func (e UnexpectedWithIssueErr) Error() string {
	return fmt.Sprintf("unexpected error: %s\nWe've been trying to track this particular issue down. "+
		"Please report your reproduction at "+
		"https://github.com/cockroachdb/cockroach/issues/%d "+
		"unless that issue seems to have been resolved "+
		"(in which case you might want to update crdb to a newer version).",
		e.msg, e.issue)
}

// SafeMessage implements the SafeMessager interface.
func (e UnexpectedWithIssueErr) SafeMessage() string {
	return fmt.Sprintf("issue #%d: %s", e.issue, e.safeMsg)
}

// SendReport creates a Sentry report about the error, if the settings allow.
// The format string will be reproduced ad litteram in the report; the arguments
// will be sanitized.
func (e UnexpectedWithIssueErr) SendReport(ctx context.Context, sv *settings.Values) {
	log.SendCrashReport(ctx, sv, 1 /* depth */, "%s", []interface{}{e})
}
