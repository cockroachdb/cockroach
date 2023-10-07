// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package assertion

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// disableFatal sets shouldFatal to false, resetting it when the test completes.
func disableFatal(t *testing.T) {
	old := shouldFatal
	t.Cleanup(func() {
		shouldFatal = old
	})
	shouldFatal = false
}

// onReport sets up a MaybeSendReport handler, resetting it when the test completes.
func onReport(t *testing.T, fn func(error)) {
	old := MaybeSendReport
	t.Cleanup(func() {
		MaybeSendReport = old
	})
	MaybeSendReport = func(ctx context.Context, err error) {
		fn(err)
	}
}

// TestFail tests that assertion failures behave correctly.
func TestFail(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableFatal(t)

	// Set up a custom MaybeSendReport handler for the duration of the test.
	var reported error
	onReport(t, func(err error) {
		reported = err
	})

	// Failed returns an assertion error, and notifies Sentry.
	ctx := context.Background()
	err := Failed(ctx, "foo: %s", "bar")
	require.Error(t, err)
	require.True(t, errors.HasAssertionFailure(err))
	require.Error(t, reported)
	require.Same(t, reported, err)

	// Format args are redacted.
	require.Equal(t, err.Error(), "foo: bar")
	require.EqualValues(t, redact.Sprint(err), "foo: ‹bar›")

	// The error includes a stack trace, but strips the Failed frame.
	require.Contains(t, fmt.Sprintf("%+v", err), ".TestFail")
	require.NotContains(t, fmt.Sprintf("%+v", err), "assertion.Failed")

	// We don't test the fatal handling, since it can't easily be tested, and the
	// logic is trivial.
}

// TestSettings tests that various settings are initialized correctly.
func TestSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.Equal(t, shouldFatal, !build.IsRelease())
	require.Equal(t, ExpensiveEnabled, util.RaceEnabled)
}

// TestPanic tests that Panic() panics when fatal assertions are disabled.
func TestPanic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	disableFatal(t)

	// Set up a custom MaybeSendReport handler for the duration of the test.
	var reported error
	onReport(t, func(err error) {
		reported = err
	})

	// Catch and inspect the panic error.
	var panicErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					panicErr = e
				}
			}
		}()
		Panic(context.Background(), "foo: %s", "bar")
	}()

	require.Error(t, panicErr)
	require.True(t, errors.HasAssertionFailure(panicErr))
	require.Error(t, reported)
	require.Equal(t, panicErr, reported)
}
