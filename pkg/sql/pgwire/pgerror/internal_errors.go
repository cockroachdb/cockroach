// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgerror

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/errors"
)

// This file provides facilities to track internal errors.

// NewInternalTrackingError instantiates an error
// meant for use with telemetry.ReportError directly.
//
// Do not use this! Convert uses to AssertionFailedf or similar
// above.
func NewInternalTrackingError(issue int, detail string) error {
	key := fmt.Sprintf("#%d.%s", issue, detail)
	err := errors.AssertionFailedWithDepthf(1, "%s", errors.Safe(key))
	err = errors.WithTelemetry(err, key)
	err = errors.WithIssueLink(err, errors.IssueLink{IssueURL: build.MakeIssueURL(issue)})
	return err
}
