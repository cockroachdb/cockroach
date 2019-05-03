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

package errors

import "github.com/cockroachdb/cockroach/pkg/errors/errutil"

// New creates an error with a simple error message.
// A stack trace is retained.
var New func(msg string) error = errutil.New

// NewWithDepth is like New() except the depth to capture the stack
// trace is configurable.
var NewWithDepth func(depth int, msg string) error = errutil.NewWithDepth

// Newf creates an error with a formatted error message.
// A stack trace is retained.
var Newf func(format string, args ...interface{}) error = errutil.Newf

// NewWithDepthf is like Newf() except the depth to capture the stack
// trace is configurable.
var NewWithDepthf func(depth int, format string, args ...interface{}) error = errutil.NewWithDepthf

// Errorf aliases Newf and is provided for compatibility with other packages.
var Errorf func(format string, args ...interface{}) error = errutil.Errorf

// WithMessage wraps an error with a simple error message prefix.
var WithMessage func(err error, msg string) error = errutil.WithMessage

// WithMessagef wraps an error with a simple error message prefix.
var WithMessagef func(err error, format string, args ...interface{}) error = errutil.WithMessagef

// Wrap wraps an error with a message prefix.
// A stack trace is retained.
var Wrap func(err error, msg string) error = errutil.Wrap

// WrapWithDepth is like Wrap except the depth to capture the stack
// trace is configurable.
var WrapWithDepth func(depth int, err error, msg string) error = errutil.WrapWithDepth

// Wrapf wraps an error with a formatted message prefix.  A stack
// trace is also retained. If the format is empty, no prefix is added,
// but the extra arguments are still processed for reportable strings.
var Wrapf func(err error, format string, args ...interface{}) error = errutil.Wrapf

// WrapWithDepthf is like Wrapf except the depth to capture the stack
// trace is configurable.
var WrapWithDepthf func(depth int, err error, format string, args ...interface{}) error = errutil.WrapWithDepthf

// Unimplemented constructs an unimplemented feature error.
var Unimplemented func(feature, msg string) error = errutil.Unimplemented

// Unimplementedf constructs an unimplemented feature error.
// The message is formatted.
var Unimplementedf func(feature, format string, args ...interface{}) error = errutil.Unimplementedf

// UnimplementedWithDepthf constructs an implemented feature error,
// tracking the context at the specified depth.
var UnimplementedWithDepthf func(depth int, feature, format string, args ...interface{}) error = errutil.UnimplementedWithDepthf

// UnimplementedWithIssue constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
var UnimplementedWithIssue func(issue int, msg string) error = errutil.UnimplementedWithIssue

// UnimplementedWithIssuef constructs an error with the formatted message
// and a link to the passed issue. Recorded as "#<issue>" in tracking.
var UnimplementedWithIssuef func(issue int, format string, args ...interface{}) error = errutil.UnimplementedWithIssuef

// UnimplementedWithIssueHint constructs an error with the given
// message, hint, and a link to the passed issue. Recorded as "#<issue>"
// in tracking.
var UnimplementedWithIssueHint func(issue int, msg, hint string) error = errutil.UnimplementedWithIssueHint

// UnimplementedWithIssueDetail constructs an error with the given message
// and a link to the passed issue. Recorded as "#<issue>.detail" in tracking.
// This is useful when we need an extra axis of information to drill down into.
var UnimplementedWithIssueDetail func(issue int, detail, msg string) error = errutil.UnimplementedWithIssueDetail

// UnimplementedWithIssueDetailf is like UnimplementedWithIssueDetail
// but the message is formatted.
var UnimplementedWithIssueDetailf func(issue int, detail, format string, args ...interface{}) error = errutil.UnimplementedWithIssueDetailf

// AssertionFailedf creates an internal error.
var AssertionFailedf func(format string, args ...interface{}) error = errutil.AssertionFailedf

// AssertionFailedWithDepthf creates an internal error
// with a stack trace collected at the specified depth.
var AssertionFailedWithDepthf func(depth int, format string, args ...interface{}) error = errutil.AssertionFailedWithDepthf

// NewAssertionErrorWithWrappedErrf wraps an error and turns it into
// an assertion error. Both details from the original error and the
// context of the caller are preserved. The original error is not
// visible as cause any more.
var NewAssertionErrorWithWrappedErrf func(origErr error, format string, args ...interface{}) error = errutil.NewAssertionErrorWithWrappedErrf
