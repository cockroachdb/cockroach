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

import "github.com/cockroachdb/cockroach/pkg/errors/assert"

// WithAssertionFailure decorates the error with an assertion failure marker.
// This is not intended to be used directly (see AssertionFailed() for
// further decoration).
var WithAssertionFailure func(err error) error = assert.WithAssertionFailure

// HasAssertionFailure returns true if the error or any of its causes
// is an assertion failure annotation.
var HasAssertionFailure func(err error) bool = assert.HasAssertionFailure

// IsAssertionFailure returns true if the error (not its causes) is an
// assertion failure annotation. Consider using markers.If or
// HasAssertionFailure to test both the error and its causes.
var IsAssertionFailure func(err error) bool = assert.IsAssertionFailure
