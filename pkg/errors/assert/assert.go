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

package assert

import (
	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/errors/stdstrings"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// WithAssertionFailure decorates the error with an assertion failure marker.
// This is not intended to be used directly (see AssertionFailed() for
// further decoration).
func WithAssertionFailure(err error) error {
	if err == nil {
		return nil
	}
	return &withAssertionFailure{cause: err}
}

// HasAssertionFailure returns true if the error or any of its causes
// is an assertion failure annotation.
func HasAssertionFailure(err error) bool {
	_, ok := markers.If(err, func(err error) (v interface{}, ok bool) {
		v, ok = err.(*withAssertionFailure)
		return
	})
	return ok
}

// IsAssertionFailure returns true if the error (not its causes) is an
// assertion failure annotation. Consider using markers.If or
// HasAssertionFailure to test both the error and its causes.
func IsAssertionFailure(err error) bool {
	_, ok := err.(*withAssertionFailure)
	return ok
}

type withAssertionFailure struct {
	cause error
}

// ErrorHint implements the hintdetail.ErrorHinter interface.
func (w *withAssertionFailure) ErrorHint() string {
	return AssertionErrorHint + stdstrings.IssueReferral
}

// AssertionErrorHint is the hint emitted upon assertion failures.
const AssertionErrorHint = `You have encountered an unexpected error inside CockroachDB.`

func (w *withAssertionFailure) Error() string { return w.cause.Error() }
func (w *withAssertionFailure) Cause() error  { return w.cause }
func (w *withAssertionFailure) Unwrap() error { return w.cause }

func decodeAssertFailure(cause error, _ string, _ []string, _ protoutil.SimpleMessage) error {
	return &withAssertionFailure{cause: cause}
}

func init() {
	errbase.RegisterWrapperDecoder(errbase.FullTypeName(&withAssertionFailure{}), decodeAssertFailure)
}
