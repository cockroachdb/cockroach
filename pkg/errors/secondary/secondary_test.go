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

package secondary_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/errors/secondary"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
)

// This test demonstrates that a secondary error annotation
// does not reveal the secondary error as a cause.
func TestHideSecondaryError(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := errors.New("hello")
	err1 := errors.Wrap(origErr, "world")

	// Assertion: without the annotation, the cause can be identified.
	tt.Assert(markers.Is(err1, origErr))

	// This test: the secondary error is not visible as cause.
	err := secondary.WithSecondaryError(errors.New("other"), err1)
	tt.Check(!markers.Is(err, origErr))
}

// This test demonstrates that the secondary error details
// are preserved, even when they go through the network.
func TestSecondaryErrorMaskedDetails(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := errors.New("hello original")

	b := secondary.WithSecondaryError(errors.New("message hidden"), origErr)

	// Assertion: the original message is hidden.
	tt.Assert(!strings.Contains(b.Error(), "original"))

	// This test: the details are available when printing the error details.
	errV := fmt.Sprintf("%+v", b)
	tt.Check(strings.Contains(errV, "original"))

	// Simulate a network traversal.
	enc := errbase.EncodeError(b)
	newB := errbase.DecodeError(enc)

	t.Logf("decoded: %# v", pretty.Formatter(newB))

	// The original message is hidden.
	tt.Check(!strings.Contains(b.Error(), "original"))

	// The cause is still hidden.
	tt.Check(!markers.Is(newB, origErr))

	// However the cause's details are still visible.
	errV = fmt.Sprintf("%+v", newB)
	tt.Check(strings.Contains(errV, "original"))
}
