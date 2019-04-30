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

package barriers_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/barriers"
	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/pkg/errors"
)

// This test demonstrates that a barrier hides it causes.
func TestHideCause(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := errors.New("hello")
	err1 := errors.Wrap(origErr, "world")

	// Assertion: without barriers, the cause can be identified.
	tt.Assert(markers.Is(err1, origErr))

	// This test: a barrier hides the cause.
	err := barriers.Handled(err1)
	tt.Check(!markers.Is(err, origErr))
}

// This test demonstrates how the message is preserved (or not) depending
// on how the barrier is constructed.
func TestBarrierMessage(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := errors.New("hello")

	b1 := barriers.Handled(origErr)
	tt.CheckEqual(b1.Error(), origErr.Error())

	b2 := barriers.HandledWithMessage(origErr, "woo")
	tt.CheckEqual(b2.Error(), "woo")
}

// This test demonstrates that the original error details
// are preserved through barriers, even when they go through the network.
func TestBarrierMaskedDetails(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := errors.New("hello friends")

	b := barriers.HandledWithMessage(origErr, "message hidden")

	// Assertion: the friends message is hidden.
	tt.Assert(!strings.Contains(b.Error(), "friends"))

	// This test: the details are available when printing the error details.
	errV := fmt.Sprintf("%+v", b)
	tt.Check(strings.Contains(errV, "friends"))

	// Simulate a network traversal.
	enc := errbase.EncodeError(b)
	newB := errbase.DecodeError(enc)

	// The friends message is hidden.
	tt.Check(!strings.Contains(b.Error(), "friends"))

	// The cause is still hidden.
	tt.Check(!markers.Is(newB, origErr))

	// However the cause's details are still visible.
	errV = fmt.Sprintf("%+v", newB)
	tt.Check(strings.Contains(errV, "friends"))
}

// This test exercises HandledWithMessagef.
func TestHandledWithMessagef(t *testing.T) {
	tt := testutils.T{T: t}

	origErr := errors.New("hello friends")

	b1 := barriers.HandledWithMessage(origErr, "woo woo")
	b2 := barriers.HandledWithMessagef(origErr, "woo %s", "woo")

	tt.CheckEqual(b1.Error(), b2.Error())
}
