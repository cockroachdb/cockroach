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

import "github.com/pkg/errors"

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
//
// The default implementation of this (below) is simple and relies on
// errors' simple Wrapf() to avoid a dependency cyle.
//
// When the full functionality is activated by importing package
// sqlerrors, the code given as argument is added as fallback if the
// wrapped error didn't have a code already. Use NewAssertionErrorf /
// NewAsertionErrorWithWrappedErrf to enforce a CodeInternalError
// regardless of the enclosed error.
var WrapWithDepthf func(depth int, err error, code, format string, args ...interface{}) error

func init() {
	WrapWithDepthf = defaultWrapWithDepthf
}

func defaultWrapWithDepthf(depth int, err error, code, format string, args ...interface{}) error {
	return errors.Wrapf(err, format, args...)
}
