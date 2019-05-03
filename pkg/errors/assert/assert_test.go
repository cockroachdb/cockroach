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

package assert_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/assert"
	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/pkg/errors"
)

func TestAssert(t *testing.T) {
	tt := testutils.T{T: t}

	baseErr := errors.New("world")
	err := errors.Wrap(assert.WithAssertionFailure(baseErr), "hello")

	tt.Check(markers.Is(err, baseErr))

	tt.Check(assert.HasAssertionFailure(err))

	if _, ok := markers.If(err, func(err error) (interface{}, bool) { return nil, assert.IsAssertionFailure(err) }); !ok {
		t.Error("woops")
	}

	tt.CheckEqual(err.Error(), "hello: world")

	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	tt.Check(markers.Is(newErr, baseErr))

	tt.Check(assert.HasAssertionFailure(newErr))

	if _, ok := markers.If(newErr, func(err error) (interface{}, bool) { return nil, assert.IsAssertionFailure(err) }); !ok {
		t.Error("woops")
	}

	tt.CheckEqual(newErr.Error(), "hello: world")
}
