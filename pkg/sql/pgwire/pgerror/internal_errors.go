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
	"github.com/pkg/errors"
)

// AssertionErrorHint gets added as hint on internal errors created
// via NewAssertionErrorf or NewAssertionErrorWithWrappedErrf.
const AssertionErrorHint = `You have encountered an unexpected error inside CockroachDB.

Please check https://github.com/cockroachdb/cockroach/issues to check
whether this problem is already tracked. If you cannot find it there,
please report the error with details at:

    https://github.com/cockroachdb/cockroach/issues/new/choose

If you would rather not post publicly, please contact us directly at:

    support@cockroachlabs.com

The Cockroach Labs team appreciates your feedback.
`

// NewAssertionErrorf creates an internal error.
func NewAssertionErrorf(format string, args ...interface{}) error {
	return NewAssertionErrorWithDepthf(1, format, args...)
}

// InternalErrorPrefix is prepended inside internal errors in this
// package, or when an error stack is flattened into a pgerror.Error
// inside sqlerror.Flatten().
const InternalErrorPrefix = "internal error: "

// NewAssertionErrorWithDepthf creates an internal error.
//
// Note: this is initially set to defaultNewAssertionErrorWithDepthf() until
// package sqlerror is initialized.
var NewAssertionErrorWithDepthf func(depth int, format string, args ...interface{}) error

// NewAssertionErrorWithWrappedErrf wraps an error (which may be a pg
// error) and decorates it as an internal error. The result implements
// the errors.Causer() interface to retrieve the original error object.
//
// Note: this is initially set to defaultNewAssertionErrorWithWrappedErrf() until
// package sqlerror is initialized.
var NewAssertionErrorWithWrappedErrf func(err error, format string, args ...interface{}) error

func init() {
	NewAssertionErrorWithDepthf = defaultNewAssertionErrorWithDepthf
	NewAssertionErrorWithWrappedErrf = defaultNewAssertionErrorWithWrappedErrf
}

// defaultNewAssertionErrorWithDepthf is used as implementation of
// NewAssertionErrorWithDepthf until package sqlerror is initialized.
func defaultNewAssertionErrorWithDepthf(depth int, format string, args ...interface{}) error {
	err := NewErrorWithDepthf(depth+1, CodeInternalError, InternalErrorPrefix+format, args...)
	st := log.NewStackTrace(depth + 1)
	err.Detail = fmt.Sprintf("stack trace:\n%s", log.PrintStackTrace(st))
	err.Hint = AssertionErrorHint
	return err
}

// defaultNewAssertionErrorWithWrappedErrf is used as implementation of
// NewAssertionErrorWithWrappedErrf until package sqlerror is initialized.
func defaultNewAssertionErrorWithWrappedErrf(err error, format string, args ...interface{}) error {
	return errors.Wrapf(err, InternalErrorPrefix+format, args...)
}
