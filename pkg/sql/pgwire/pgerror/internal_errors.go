// Copyright 2019 The Cockroach Authors.
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

package pgerror

import (
	"fmt"

	"github.com/cockroachdb/errors"
)

// This file provides facilities to track internal errors.

// AssertionFailedf creates an internal error.
//
// TODO(knz): this function exists as a transition until the callers
// are modified to use the errors package directly.
var AssertionFailedf = errors.AssertionFailedf

// AssertionFailedWithDepthf creates an internal error.
//
// TODO(knz): this function exists as a transition until the callers
// are modified to use the errors package directly.
var AssertionFailedWithDepthf = errors.AssertionFailedWithDepthf

// NewAssertionErrorWithWrappedErrf creates an internal error
// and attaches (but masks) the provided error cause.
var NewAssertionErrorWithWrappedErrf = errors.NewAssertionErrorWithWrappedErrf

// NewInternalTrackingError instantiates an error
// meant for use with telemetry.ReportError directly.
//
// Do not use this! Convert uses to AssertionFailedf or similar
// above.
func NewInternalTrackingError(issue int, detail string) error {
	key := fmt.Sprintf("#%d.%s", issue, detail)
	err := AssertionFailedWithDepthf(1, "%s", errors.Safe(key))
	err = errors.WithTelemetry(err, key)
	err = errors.WithIssueLink(err, errors.IssueLink{IssueURL: fmt.Sprintf("https://github.com/cockroachdb/cockroach/issues/%d", issue)})
	return err
}
