// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package unimplemented

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

// This file re-implements the unimplemented primitives from the
// original pgerror package, using the primitives from the errors
// library instead.

// New constructs an unimplemented feature error.
func New(feature, msg string) error {
	return unimplementedInternal(1 /*depth*/, 0 /*issue*/, feature /*detail*/, false /*format*/, msg)
}

// Newf constructs an unimplemented feature error.
// The message is formatted.
func Newf(feature, format string, args ...interface{}) error {
	return NewWithDepthf(1, feature, format, args...)
}

// NewWithDepthf constructs an implemented feature error,
// tracking the context at the specified depth.
func NewWithDepthf(depth int, feature, format string, args ...interface{}) error {
	return unimplementedInternal(depth+1 /*depth*/, 0 /*issue*/, feature /*detail*/, true /*format*/, format, args...)
}

// NewWithIssue constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func NewWithIssue(issue int, msg string) error {
	return unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, false /*format*/, msg)
}

// NewWithIssuef constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
func NewWithIssuef(issue int, format string, args ...interface{}) error {
	return unimplementedInternal(1 /*depth*/, issue, "" /*detail*/, true /*format*/, format, args...)
}

// NewWithIssueDetail constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>.detail" in tracking.
// This is useful when we need an extra axis of information to drill down into.
func NewWithIssueDetail(issue int, detail, msg string) error {
	return unimplementedInternal(1 /*depth*/, issue, detail, false /*format*/, msg)
}

// NewWithIssueDetailf is like NewWithIssueDetail but the message is formatted.
func NewWithIssueDetailf(issue int, detail, format string, args ...interface{}) error {
	return unimplementedInternal(1 /*depth*/, issue, detail, true /*format*/, format, args...)
}

func unimplementedInternal(
	depth, issue int, detail string, format bool, msg string, args ...interface{},
) error {
	// Create the issue link.
	link := errors.IssueLink{Detail: detail}
	if issue > 0 {
		link.IssueURL = build.MakeIssueURL(issue)
	}

	// Instantiate the base error.
	var err error
	if format {
		err = errors.UnimplementedErrorf(link, "unimplemented: "+msg, args...)
		err = errors.WithSafeDetails(err, msg, args...)
	} else {
		err = errors.UnimplementedError(link, "unimplemented: "+msg)
	}
	// Decorate with a stack trace.
	err = errors.WithStackDepth(err, 1+depth)

	if issue > 0 {
		// There is an issue number. Decorate with a telemetry annotation.
		var key string
		if detail == "" {
			key = fmt.Sprintf("#%d", issue)
		} else {
			key = fmt.Sprintf("#%d.%s", issue, detail)
		}
		err = errors.WithTelemetry(err, key)
	} else if detail != "" {
		// No issue but a detail string. It's an indication to also
		// perform telemetry.
		err = errors.WithTelemetry(err, detail)
	}

	// Wrap with the corresponding PG error for unimplemented.
	err = pgerror.WithCandidateCode(err, pgcode.FeatureNotSupported)
	return err
}
