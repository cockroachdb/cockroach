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

package safedetails_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/errors/markers"
	"github.com/cockroachdb/cockroach/pkg/errors/safedetails"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func TestDetailCapture(t *testing.T) {
	origErr := errors.New("hello world")

	err := safedetails.WithSafeDetails(origErr, "bye %s %s", safedetails.Safe("planet"), "and universe")

	t.Logf("here's the error:\n%+v", err)

	subTest := func(t *testing.T, err error) {
		tt := testutils.T{T: t}

		// The cause is preserved.
		tt.Check(markers.Is(err, origErr))

		// The message is unchanged by the wrapper.
		tt.CheckEqual(err.Error(), "hello world")

		// The unsafe string is hidden.
		errV := fmt.Sprintf("%+v", err)
		tt.Check(!strings.Contains(errV, "and universe"))

		// The safe string is preserved.
		tt.Check(strings.Contains(errV, "planet"))

		// The format string is preserved.
		tt.Check(strings.Contains(errV, "bye %s %s"))
	}

	// Check the error properties locally.
	t.Run("local", func(t *testing.T) {
		subTest(t, err)
	})

	// Same tests, across the network.
	enc := errbase.EncodeError(err)
	newErr := errbase.DecodeError(enc)

	t.Run("remote", func(t *testing.T) {
		subTest(t, newErr)
	})
}
