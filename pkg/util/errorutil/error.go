// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package errorutil

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

// UnexpectedWithIssueErrorf indicates an error with an associated Github issue.
// It's supposed to be used for conditions that would otherwise be checked by
// assertions, except that they fail and we need the public's help for tracking
// it down.
// The error message will invite users to report repros.
func UnexpectedWithIssueErrorf(issue int, format string, args ...interface{}) error {
	err := errors.Newf(format, args...)
	err = errors.Wrap(err, "unexpected error")
	err = errors.WithSafeDetails(err, "issue #%d", errors.Safe(issue))
	err = errors.WithHint(err,
		fmt.Sprintf("We've been trying to track this particular issue down. "+
			"Please report your reproduction at "+
			"https://github.com/cockroachdb/cockroach/issues/%d "+
			"unless that issue seems to have been resolved "+
			"(in which case you might want to update crdb to a newer version).",
			issue))
	return err
}

// SendReport creates a Sentry report about the error, if the settings allow.
// The format string will be reproduced ad litteram in the report; the arguments
// will be sanitized.
func SendReport(ctx context.Context, sv *settings.Values, err error) {
	if !logcrash.ShouldSendReport(sv) {
		return
	}
	event, extraDetails := errors.BuildSentryReport(err)
	logcrash.SendReport(ctx, logcrash.ReportTypeError, event, extraDetails)
}
