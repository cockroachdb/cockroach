// Copyright 2019 The Cockroach Authors.
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

package errutil

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/hintdetail"
	"github.com/cockroachdb/cockroach/pkg/errors/issuelink"
	"github.com/cockroachdb/cockroach/pkg/errors/safedetails"
	"github.com/cockroachdb/cockroach/pkg/errors/telemetrykeys"
	"github.com/cockroachdb/cockroach/pkg/errors/withstack"
)

// This file re-implements the unimplemented primitives
// from the original pgerror package, using
// the primitives from the library instead.

// Unimplemented constructs an unimplemented feature error.
func Unimplemented(feature, msg string) error {
	return unimplementedInternal(1 /*depth*/, 0 /*issue*/, feature /*detail*/, false /*format*/, msg)
}

// Unimplementedf constructs an unimplemented feature error.
// The message is formatted.
func Unimplementedf(feature, format string, args ...interface{}) error {
	return UnimplementedWithDepthf(1, feature, format, args...)
}

// UnimplementedWithDepthf constructs an implemented feature error,
// tracking the context at the specified depth.
func UnimplementedWithDepthf(depth int, feature, format string, args ...interface{}) error {
	return unimplementedInternal(1 /*depth*/, 0 /*issue*/, feature /*detail*/, true /*format*/, format, args...)
}

// UnimplementedWithIssue constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssue(issue int, msg string) error {
	return unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, false /*format*/, msg)
}

// UnimplementedWithIssuef constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func UnimplementedWithIssuef(issue int, format string, args ...interface{}) error {
	return unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, true /*format*/, format, args...)
}

// UnimplementedWithIssueHint constructs an error with the given
// message, hint, and a link to the passed issue. Recorded as "#<issue>"
// in tracking.
func UnimplementedWithIssueHint(issue int, msg, hint string) error {
	err := unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, false /*format*/, msg)
	err = hintdetail.WithHint(err, hint)
	return err
}

// UnimplementedWithIssueDetail constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>.detail" in tracking.
// This is useful when we need an extra axis of information to drill down into.
func UnimplementedWithIssueDetail(issue int, detail, msg string) error {
	return unimplementedInternal(1 /*depth*/, issue, detail, false /*format*/, msg)
}

// UnimplementedWithIssueDetailf is like UnimplementedWithIssueDetail
// but the message is formatted.
func UnimplementedWithIssueDetailf(issue int, detail, format string, args ...interface{}) error {
	return unimplementedInternal(1 /*depth*/, issue, detail, true /*format*/, format, args...)
}

func unimplementedInternal(
	depth, issue int, detail string, format bool, msg string, args ...interface{},
) error {
	// Create the issue link.
	link := issuelink.IssueLink{Detail: detail}
	if issue > 0 {
		link.IssueURL = makeURL(issue)
	}

	// Instantiate the base error.
	var err error
	if format {
		err = issuelink.UnimplementedErrorf(link, "unimplemented: "+msg, args...)
		err = safedetails.WithSafeDetails(err, msg, args...)
	} else {
		err = issuelink.UnimplementedError(link, "unimplemented: "+msg)
	}
	// Decorate with a stack trace.
	err = withstack.WithStackDepth(err, 1+depth)

	if issue > 0 {
		// There is an issue number. Decorate with a telemetry annotation.
		var key string
		if detail == "" {
			key = fmt.Sprintf("#%d", issue)
		} else {
			key = fmt.Sprintf("#%d.%s", issue, detail)
		}
		err = telemetrykeys.WithTelemetry(err, key)
	} else if detail != "" {
		// No issue but a detail string. It's an indication to also
		// perform telemetry.
		err = telemetrykeys.WithTelemetry(err, detail)
	}
	return err
}

func makeURL(issue int) string {
	return fmt.Sprintf("https://github.com/cockroachdb/cockroach/issues/%d", issue)
}
