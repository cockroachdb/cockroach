// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errorutil

import (
	"fmt"

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
