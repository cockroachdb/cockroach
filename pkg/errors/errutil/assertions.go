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
	"github.com/cockroachdb/cockroach/pkg/errors/assert"
	"github.com/cockroachdb/cockroach/pkg/errors/barriers"
)

// AssertionFailedf creates an internal error.
func AssertionFailedf(format string, args ...interface{}) error {
	return AssertionFailedWithDepthf(1, format, args...)
}

// AssertionFailedWithDepthf creates an internal error
// with a stack trace collected at the specified depth.
func AssertionFailedWithDepthf(depth int, format string, args ...interface{}) error {
	err := NewWithDepthf(1+depth, format, args...)
	err = assert.WithAssertionFailure(err)
	return err
}

// NewAssertionErrorWithWrappedErrf wraps an error and turns it into
// an assertion error. Both details from the original error and the
// context of the caller are preserved. The original error is not
// visible as cause any more. The original error message is preserved.
func NewAssertionErrorWithWrappedErrf(origErr error, format string, args ...interface{}) error {
	err := barriers.Handled(origErr)
	err = WrapWithDepthf(1, err, format, args...)
	err = assert.WithAssertionFailure(err)
	return err
}
