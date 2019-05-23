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

package pgerror

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors"
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
