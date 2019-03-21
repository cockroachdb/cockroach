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

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Wrap wraps an error into a pgerror. See
// the doc on WrapWithDepthf for details.
func Wrap(err error, code, msg string) error {
	return WrapWithDepthf(1, err, code, "%s", msg)
}

// Wrapf wraps an error into a pgerror. See
// the doc on WrapWithDepthf for details.
func Wrapf(err error, code, format string, args ...interface{}) error {
	return WrapWithDepthf(1, err, code, format, args...)
}

// WrapWithDepthf wraps an error. The result implements the
// errors.Cause() interface to retrieve the original error object.
// Use the Flatten() function to get a simple Error object instead,
// suitable for sending to a SQL client.
// The callback trace collected starts at "depth" levels higher in the call stack.
func WrapWithDepthf(depth int, err error, code, format string, args ...interface{}) error {
	if err == nil {
		// Shortcut: although the functions below do the same, this takes less time.
		return nil
	}

	// Add a message prefix.
	err = WithMessagef(err, format, args...)

	// If there's a default code, add it.
	if code != "" && code != CodeUncategorizedError {
		err = WithDefaultCode(err, code)
	}

	// For internal errors and if verbosity is sufficiently high, add
	// more reportable details.
	if errCode := GetCode(err, code); errCode == CodeInternalError || log.V(2) {
		st := log.NewStackTrace(depth + 1)
		err = internalWithSafeDetailf(depth+1, err, st, format, args...)
		err = WithDetailf(err, "stack trace:\n%s", log.PrintStackTrace(st))
	} else {
		// Otherwise, simply add the source code context.
		err = WithSource(depth+1, err)
	}

	return err
}

// WithMessage adds a message.
// Nothing is wrapped if the message is empty.
func WithMessage(err error, msg string) error {
	if err == nil || msg == "" {
		return err
	}
	return &withMessage{cause: err, msg: msg}
}

// WithMessagef adds a message.
// Nothing is wrapped if the message ends up empty.
func WithMessagef(err error, format string, args ...interface{}) error {
	if err == nil || (format == "" && len(args) == 0) {
		// No error, or no message to add. Do nothing.
		// This ensures e.g. that Wrap(err, "") doesn't produce an error
		// message of the form "pq: : something".
		return err
	}
	return &withMessage{cause: err, msg: fmt.Sprintf(format, args...)}
}
