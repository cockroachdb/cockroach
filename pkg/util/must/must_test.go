// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package must_test

import (
	"context"
	"fmt"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/must"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// noopFail sets must.OnFail to a noop, and returns a cleanup function
// to be called when the test completes.
func noopFail() func() {
	must.OnFail = func(context.Context, error) {}
	return func() {
		must.OnFail = nil
	}
}

// TestFail tests that assertion failures behave correctly.
func TestFail(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer noopFail()()

	// Fail returns an assertion error.
	ctx := context.Background()
	err := must.Fail(ctx, "foo: %s", "bar")
	require.Error(t, err)
	require.True(t, errors.HasAssertionFailure(err))

	// Format args are redacted.
	require.Equal(t, err.Error(), "foo: bar")
	require.EqualValues(t, redact.Sprint(err), "foo: ‹bar›")

	// The error includes a stack trace, but strips the must.Fail frame.
	require.Contains(t, fmt.Sprintf("%+v", err), ".TestFail")
	require.NotContains(t, fmt.Sprintf("%+v", err), "must.Fail")

	// FatalAssertions is false in release builds, true otherwise.
	require.Equal(t, must.FatalAssertions, !build.IsRelease())

	// We don't test the fatal handling, since it can't easily be tested, and the
	// logic is trivial.
}

// TestExpensive tests that Expensive only runs under race builds.
func TestExpensive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer noopFail()()

	// Expensive should only run under the race tag. When it does run, it returns
	// the error from the closure.
	errExpensive := errors.New("expensive")
	var called bool
	err := must.Expensive(func() error {
		called = true
		return errExpensive
	})

	if util.RaceEnabled {
		require.True(t, called)
		require.ErrorIs(t, err, errExpensive)
	} else {
		require.False(t, called)
		require.NoError(t, err)
	}
}

// TestAssertions tests that various assertions fire correctly.
func TestAssertions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer noopFail()()

	ctx := context.Background()
	const format = "message with %s"
	const arg = "argument"
	p1, p2 := &hlc.Timestamp{Logical: 1}, &hlc.Timestamp{Logical: 1}
	pNil := (*hlc.Timestamp)(nil)

	testcases := map[string][]struct {
		err      error
		expectOK bool
	}{
		"True": {
			{must.True(ctx, true, format, arg), true},
			{must.True(ctx, false, format, arg), false},
		},

		"False": {
			{must.False(ctx, false, format, arg), true},
			{must.False(ctx, true, format, arg), false},
		},

		"Equal": {
			{must.Equal(ctx, true, true, format, arg), true},
			{must.Equal(ctx, true, false, format, arg), false},
			{must.Equal(ctx, 1, 1, format, arg), true},
			{must.Equal(ctx, 0, 1, format, arg), false},
			{must.Equal(ctx, 3.14, 3.14, format, arg), true},
			{must.Equal(ctx, 3.14, 2.717, format, arg), false},
			{must.Equal(ctx, "a", "a", format, arg), true},
			{must.Equal(ctx, "a", "b", format, arg), false},
			{must.Equal(ctx, hlc.Timestamp{Logical: 1}, hlc.Timestamp{Logical: 1}, format, arg), true},
			{must.Equal(ctx, hlc.Timestamp{Logical: 1}, hlc.Timestamp{Logical: 2}, format, arg), false},
			{must.Equal(ctx, p1, p1, format, arg), true},
			{must.Equal(ctx, p1, p2, format, arg), false},
			{must.Equal(ctx, pNil, pNil, format, arg), true},
			{must.Equal(ctx, pNil, p2, format, arg), false},
		},

		"NotEqual": {
			{must.NotEqual(ctx, true, false, format, arg), true},
			{must.NotEqual(ctx, true, true, format, arg), false},
			{must.NotEqual(ctx, 0, 1, format, arg), true},
			{must.NotEqual(ctx, 1, 1, format, arg), false},
			{must.NotEqual(ctx, 3.14, 2.717, format, arg), true},
			{must.NotEqual(ctx, 3.14, 3.14, format, arg), false},
			{must.NotEqual(ctx, "a", "b", format, arg), true},
			{must.NotEqual(ctx, "a", "a", format, arg), false},
			{must.NotEqual(ctx, hlc.Timestamp{Logical: 1}, hlc.Timestamp{Logical: 2}, format, arg), true},
			{must.NotEqual(ctx, hlc.Timestamp{Logical: 1}, hlc.Timestamp{Logical: 1}, format, arg), false},
			{must.NotEqual(ctx, p1, p2, format, arg), true},
			{must.NotEqual(ctx, p1, p1, format, arg), false},
			{must.NotEqual(ctx, pNil, p2, format, arg), true},
			{must.NotEqual(ctx, pNil, pNil, format, arg), false},
		},

		"Greater": {
			{must.Greater(ctx, 1, 0, format, arg), true},
			{must.Greater(ctx, 1, 1, format, arg), false},
			{must.Greater(ctx, 0, 1, format, arg), false},
			{must.Greater(ctx, 3.14, 2.717, format, arg), true},
			{must.Greater(ctx, 3.14, 3.14, format, arg), false},
			{must.Greater(ctx, 2.717, 3.14, format, arg), false},
			{must.Greater(ctx, "b", "a", format, arg), true},
			{must.Greater(ctx, "aa", "a", format, arg), true},
			{must.Greater(ctx, "b", "b", format, arg), false},
			{must.Greater(ctx, "a", "b", format, arg), false},
		},

		"GreaterOrEqual": {
			{must.GreaterOrEqual(ctx, 1, 0, format, arg), true},
			{must.GreaterOrEqual(ctx, 1, 1, format, arg), true},
			{must.GreaterOrEqual(ctx, 0, 1, format, arg), false},
			{must.GreaterOrEqual(ctx, 3.14, 2.717, format, arg), true},
			{must.GreaterOrEqual(ctx, 3.14, 3.14, format, arg), true},
			{must.GreaterOrEqual(ctx, 2.717, 3.14, format, arg), false},
			{must.GreaterOrEqual(ctx, "b", "a", format, arg), true},
			{must.GreaterOrEqual(ctx, "aa", "a", format, arg), true},
			{must.GreaterOrEqual(ctx, "b", "b", format, arg), true},
			{must.GreaterOrEqual(ctx, "a", "b", format, arg), false},
		},

		"Less": {
			{must.Less(ctx, 0, 1, format, arg), true},
			{must.Less(ctx, 1, 1, format, arg), false},
			{must.Less(ctx, 1, 0, format, arg), false},
			{must.Less(ctx, 2.717, 3.14, format, arg), true},
			{must.Less(ctx, 3.14, 3.14, format, arg), false},
			{must.Less(ctx, 3.14, 2.717, format, arg), false},
			{must.Less(ctx, "a", "b", format, arg), true},
			{must.Less(ctx, "a", "aa", format, arg), true},
			{must.Less(ctx, "b", "b", format, arg), false},
			{must.Less(ctx, "b", "a", format, arg), false},
		},

		"LessOrEqual": {
			{must.LessOrEqual(ctx, 0, 1, format, arg), true},
			{must.LessOrEqual(ctx, 1, 1, format, arg), true},
			{must.LessOrEqual(ctx, 1, 0, format, arg), false},
			{must.LessOrEqual(ctx, 2.717, 3.14, format, arg), true},
			{must.LessOrEqual(ctx, 3.14, 3.14, format, arg), true},
			{must.LessOrEqual(ctx, 3.14, 2.717, format, arg), false},
			{must.LessOrEqual(ctx, "a", "b", format, arg), true},
			{must.LessOrEqual(ctx, "a", "aa", format, arg), true},
			{must.LessOrEqual(ctx, "b", "b", format, arg), true},
			{must.LessOrEqual(ctx, "b", "a", format, arg), false},
		},

		"EqualBytes": {
			{must.EqualBytes(ctx, []byte(nil), nil, format, arg), true},
			{must.EqualBytes(ctx, []byte("foo"), nil, format, arg), false},
			{must.EqualBytes(ctx, []byte("foo"), []byte("foo"), format, arg), true},
			{must.EqualBytes(ctx, []byte("foo"), []byte("bar"), format, arg), false},
			{must.EqualBytes(ctx, []byte{1, 2, 3}, []byte{4, 5, 6}, format, arg), false},
			{must.EqualBytes(ctx, "", "", format, arg), true},
			{must.EqualBytes(ctx, "foo", "", format, arg), false},
			{must.EqualBytes(ctx, "foo", "foo", format, arg), true},
			{must.EqualBytes(ctx, "foo", "bar", format, arg), false},
			{must.EqualBytes(ctx, keys.NodeLivenessKey(1), keys.NodeLivenessKey(1), format, arg), true},
			{must.EqualBytes(ctx, keys.NodeLivenessKey(1), keys.NodeLivenessKey(2), format, arg), false},
		},

		"NotEqualBytes": {
			{must.NotEqualBytes(ctx, []byte(nil), nil, format, arg), false},
			{must.NotEqualBytes(ctx, []byte("foo"), nil, format, arg), true},
			{must.NotEqualBytes(ctx, []byte("foo"), []byte("foo"), format, arg), false},
			{must.NotEqualBytes(ctx, []byte("foo"), []byte("bar"), format, arg), true},
			{must.NotEqualBytes(ctx, []byte{1, 2, 3}, []byte{4, 5, 6}, format, arg), true},
			{must.NotEqualBytes(ctx, "", "", format, arg), false},
			{must.NotEqualBytes(ctx, "foo", "", format, arg), true},
			{must.NotEqualBytes(ctx, "foo", "foo", format, arg), false},
			{must.NotEqualBytes(ctx, "foo", "bar", format, arg), true},
			{must.NotEqualBytes(ctx, keys.NodeLivenessKey(1), keys.NodeLivenessKey(1), format, arg), false},
			{must.NotEqualBytes(ctx, keys.NodeLivenessKey(1), keys.NodeLivenessKey(2), format, arg), true},
		},

		"PrefixBytes": {
			{must.PrefixBytes(ctx, []byte(nil), nil, format, arg), true},
			{must.PrefixBytes(ctx, []byte{}, nil, format, arg), true},
			{must.PrefixBytes(ctx, []byte("foo"), nil, format, arg), true},
			{must.PrefixBytes(ctx, []byte("foo"), []byte{}, format, arg), true},
			{must.PrefixBytes(ctx, []byte("foo"), []byte("f"), format, arg), true},
			{must.PrefixBytes(ctx, []byte("foo"), []byte("foo"), format, arg), true},
			{must.PrefixBytes(ctx, []byte("foo"), []byte("foobar"), format, arg), false},
			{must.PrefixBytes(ctx, []byte("foo"), []byte("bar"), format, arg), false},
			{must.PrefixBytes(ctx, []byte(""), []byte("bar"), format, arg), false},
			{must.PrefixBytes(ctx, []byte{1, 2, 3}, []byte{7, 8, 9}, format, arg), false},
			{must.PrefixBytes(ctx, "foo", "f", format, arg), true},
			{must.PrefixBytes(ctx, "foo", "foo", format, arg), true},
			{must.PrefixBytes(ctx, "foo", "bar", format, arg), false},
			{must.PrefixBytes(ctx, keys.NodeLivenessKey(1), keys.NodeLivenessPrefix, format, arg), true},
			{must.PrefixBytes(ctx, keys.NodeLivenessKey(1), keys.LocalPrefix, format, arg), false},
		},

		"NotPrefixBytes": {
			{must.NotPrefixBytes(ctx, []byte(nil), nil, format, arg), false},
			{must.NotPrefixBytes(ctx, []byte{}, nil, format, arg), false},
			{must.NotPrefixBytes(ctx, []byte("foo"), nil, format, arg), false},
			{must.NotPrefixBytes(ctx, []byte("foo"), []byte{}, format, arg), false},
			{must.NotPrefixBytes(ctx, []byte("foo"), []byte("f"), format, arg), false},
			{must.NotPrefixBytes(ctx, []byte("foo"), []byte("foo"), format, arg), false},
			{must.NotPrefixBytes(ctx, []byte("foo"), []byte("foobar"), format, arg), true},
			{must.NotPrefixBytes(ctx, []byte("foo"), []byte("bar"), format, arg), true},
			{must.NotPrefixBytes(ctx, []byte(""), []byte("bar"), format, arg), true},
			{must.NotPrefixBytes(ctx, []byte{1, 2, 3}, []byte{1, 2, 3}, format, arg), false},
			{must.NotPrefixBytes(ctx, "foo", "f", format, arg), false},
			{must.NotPrefixBytes(ctx, "foo", "foo", format, arg), false},
			{must.NotPrefixBytes(ctx, "foo", "bar", format, arg), true},
			{must.NotPrefixBytes(ctx, keys.NodeLivenessKey(1), keys.NodeLivenessPrefix, format, arg), false},
			{must.NotPrefixBytes(ctx, keys.NodeLivenessKey(1), keys.LocalPrefix, format, arg), true},
		},

		"Error": {
			{must.Error(ctx, errors.New("error"), format, arg), true},
			{must.Error(ctx, nil, format, arg), false},
		},

		"NoError": {
			{must.NoError(ctx, nil, format, arg), true},
			{must.NoError(ctx, errors.New("boom"), format, arg), false},
		},

		"Len": {
			{must.Len(ctx, []int{1, 2, 3}, 3, format, arg), true},
			{must.Len(ctx, []int(nil), 0, format, arg), true},
			{must.Len(ctx, []int{1, 2, 3}, 2, format, arg), false},
		},

		"Contains": {
			{must.Contains(ctx, []int{1, 2, 3}, 1, format, arg), true},
			{must.Contains(ctx, []int{1, 2, 3}, 0, format, arg), false},
			{must.Contains(ctx, []int(nil), 1, format, arg), false},
			{must.Contains(ctx, []int{}, 1, format, arg), false},
			{must.Contains(ctx, []string{"foo", "bar"}, "baz", format, arg), false},
		},

		"NotContains": {
			{must.NotContains(ctx, []int{1, 2, 3}, 1, format, arg), false},
			{must.NotContains(ctx, []int{1, 2, 3}, 0, format, arg), true},
			{must.NotContains(ctx, []int(nil), 1, format, arg), true},
			{must.NotContains(ctx, []int{}, 1, format, arg), true},
			{must.NotContains(ctx, []string{"foo", "bar"}, "bar", format, arg), false},
		},

		"Empty": {
			{must.Empty(ctx, []int{}, format, arg), true},
			{must.Empty(ctx, []int(nil), format, arg), true},
			{must.Empty(ctx, []int{1, 2, 3}, format, arg), false},
		},

		"NotEmpty": {
			{must.NotEmpty(ctx, []int{1, 2, 3}, format, arg), true},
			{must.NotEmpty(ctx, []int{}, format, arg), false},
			{must.NotEmpty(ctx, []int(nil), format, arg), false},
		},

		"Nil": {
			{must.Nil(ctx, (*hlc.Timestamp)(nil), format, arg), true},
			{must.Nil(ctx, &hlc.Timestamp{}, format, arg), false},
		},

		"NotNil": {
			{must.NotNil(ctx, &hlc.Timestamp{}, format, arg), true},
			{must.NotNil(ctx, (*hlc.Timestamp)(nil), format, arg), false},
		},

		"Zero": {
			{must.Zero(ctx, false, format, arg), true},
			{must.Zero(ctx, true, format, arg), false},
			{must.Zero(ctx, 0, format, arg), true},
			{must.Zero(ctx, 1, format, arg), false},
			{must.Zero(ctx, 0.0, format, arg), true},
			{must.Zero(ctx, 0.1, format, arg), false},
			{must.Zero(ctx, "", format, arg), true},
			{must.Zero(ctx, "a", format, arg), false},
			{must.Zero(ctx, hlc.Timestamp{}, format, arg), true},
			{must.Zero(ctx, hlc.Timestamp{Logical: 1}, format, arg), false},
			{must.Zero(ctx, (*hlc.Timestamp)(nil), format, arg), true},
			{must.Zero(ctx, &hlc.Timestamp{}, format, arg), false},
		},

		"NotZero": {
			{must.NotZero(ctx, true, format, arg), true},
			{must.NotZero(ctx, false, format, arg), false},
			{must.NotZero(ctx, 1, format, arg), true},
			{must.NotZero(ctx, 0, format, arg), false},
			{must.NotZero(ctx, 0.1, format, arg), true},
			{must.NotZero(ctx, 0.0, format, arg), false},
			{must.NotZero(ctx, "a", format, arg), true},
			{must.NotZero(ctx, "", format, arg), false},
			{must.NotZero(ctx, hlc.Timestamp{Logical: 1}, format, arg), true},
			{must.NotZero(ctx, hlc.Timestamp{}, format, arg), false},
			{must.NotZero(ctx, &hlc.Timestamp{}, format, arg), true},
			{must.NotZero(ctx, (*hlc.Timestamp)(nil), format, arg), false},
		},

		"Same": {
			{must.Same(ctx, p1, p1, format, arg), true},
			{must.Same(ctx, pNil, pNil, format, arg), true},
			{must.Same(ctx, p1, p2, format, arg), false},
			{must.Same(ctx, p1, pNil, format, arg), false},
		},

		"NotSame": {
			{must.NotSame(ctx, p1, p2, format, arg), true},
			{must.NotSame(ctx, p1, pNil, format, arg), true},
			{must.NotSame(ctx, p1, p1, format, arg), false},
			{must.NotSame(ctx, pNil, pNil, format, arg), false},
		},
	}

	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	for name, tcs := range testcases {
		t.Run(name, w.Run(t, name, func(t *testing.T) string {
			var output string
			for _, tc := range tcs {
				t.Run("", func(t *testing.T) {
					if tc.expectOK {
						require.NoError(t, tc.err)
						return
					}

					require.Error(t, tc.err)
					require.True(t, errors.HasAssertionFailure(tc.err))

					// Format args are included but properly redacted.
					require.Contains(t, redact.Sprint(tc.err), redact.Sprintf(format, arg))

					// The error includes a stack trace, but strips any frames from the must package.
					require.Contains(t, fmt.Sprintf("%+v", tc.err), ".TestAssertions")
					require.NotContains(t, fmt.Sprintf("%+v", tc.err), "must.")

					// Output and check error output.
					errString := string(redact.Sprint(tc.err))

					// Replace any hex memory addresses with 0xf00, since they'll change
					// for each run.
					reHex := regexp.MustCompile(`0x[0-9a-f]{8,}`)
					errString = reHex.ReplaceAllString(errString, `0xf00`)

					output += errString + "\n"
				})
			}
			return output
		}))
	}
}

// TestPanicOn tests that PanicOn panics when expected.
func TestPanicOn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer noopFail()()

	ctx := context.Background()

	must.PanicOn(must.True(ctx, true, "message")) // success is noop

	require.Panics(t, func() {
		must.PanicOn(must.True(ctx, false, "message"))
	})
}

// TestFatalOn tests that FatalOn doesn't fatal on success. We can't easily test
// that it fatals.
func TestFatalOn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer noopFail()()

	ctx := context.Background()

	must.FatalOn(ctx, must.True(ctx, true, "message")) // success is noop

	// We don't test failures, because it fatals.
}
