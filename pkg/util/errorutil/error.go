// Copyright 2017 The Cockroach Authors.
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
		safeMsg: log.ReportablesToSafeError(1 /* depth */, format, args).Error(),
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
	log.SendCrashReport(ctx, sv, 1 /* depth */, "%s", []interface{}{e}, log.ReportTypeError)
}
