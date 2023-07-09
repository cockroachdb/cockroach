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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
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

	// In test builds, Fail() panics. We don't test the fatal/release handling,
	// since it can't easily be tested, and the logic is trivial.
	if buildutil.CrdbTestBuild {
		must.OnFail = nil
		require.Panics(t, func() {
			_ = must.Fail(ctx, "foo: %s", "bar")
		})
	}
}

// TestExpensive tests that Expensive only runs under invariants or race builds.
func TestExpensive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer noopFail()()

	// Expensive should only run under the invariants or race tags.
	var called bool
	must.Expensive(func() {
		called = true
	})

	if util.RaceEnabled || buildutil.Invariants {
		require.True(t, called)
	} else {
		require.False(t, called)
	}
}

func TestAssertions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer noopFail()()

	ctx := context.Background()
	const format = "foo: %s"
	const arg = "bar"

	ok := func(t *testing.T, err error) {
		t.Helper()
		require.NoError(t, err)
	}

	fail := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)
		require.True(t, errors.HasAssertionFailure(err))

		// Format args are included but properly redacted.
		require.Contains(t, redact.Sprint(err), redact.Sprintf(format, arg))

		// The error includes a stack trace, but strips any frames from the must package.
		require.Contains(t, fmt.Sprintf("%+v", err), ".TestAssertions")
		require.NotContains(t, fmt.Sprintf("%+v", err), "must.")
	}

	t.Run("True", func(t *testing.T) {
		ok(t, must.True(ctx, true, format, arg))
		fail(t, must.True(ctx, false, format, arg))
	})

	t.Run("False", func(t *testing.T) {
		ok(t, must.False(ctx, false, format, arg))
		fail(t, must.False(ctx, true, format, arg))
	})

	t.Run("Equal", func(t *testing.T) {
		ok(t, must.Equal(ctx, true, true, format, arg))
		fail(t, must.Equal(ctx, true, false, format, arg))

		ok(t, must.Equal(ctx, 1, 1, format, arg))
		fail(t, must.Equal(ctx, 0, 1, format, arg))

		ok(t, must.Equal(ctx, 3.14, 3.14, format, arg))
		fail(t, must.Equal(ctx, 3.14, 2.717, format, arg))

		ok(t, must.Equal(ctx, "a", "a", format, arg))
		fail(t, must.Equal(ctx, "a", "b", format, arg))

		ok(t, must.Equal(ctx, hlc.Timestamp{Logical: 1}, hlc.Timestamp{Logical: 1}, format, arg))
		fail(t, must.Equal(ctx, hlc.Timestamp{Logical: 1}, hlc.Timestamp{Logical: 2}, format, arg))

		p1, p2 := &hlc.Timestamp{Logical: 1}, &hlc.Timestamp{Logical: 1}
		ok(t, must.Equal(ctx, p1, p1, format, arg))
		fail(t, must.Equal(ctx, p1, p2, format, arg))
	})

	t.Run("NotEqual", func(t *testing.T) {
		ok(t, must.NotEqual(ctx, true, false, format, arg))
		fail(t, must.NotEqual(ctx, true, true, format, arg))

		ok(t, must.NotEqual(ctx, 0, 1, format, arg))
		fail(t, must.NotEqual(ctx, 1, 1, format, arg))

		ok(t, must.NotEqual(ctx, 3.14, 2.717, format, arg))
		fail(t, must.NotEqual(ctx, 3.14, 3.14, format, arg))

		ok(t, must.NotEqual(ctx, "a", "b", format, arg))
		fail(t, must.NotEqual(ctx, "a", "a", format, arg))

		ok(t, must.NotEqual(ctx, hlc.Timestamp{Logical: 1}, hlc.Timestamp{Logical: 2}, format, arg))
		fail(t, must.NotEqual(ctx, hlc.Timestamp{Logical: 1}, hlc.Timestamp{Logical: 1}, format, arg))

		p1, p2 := &hlc.Timestamp{Logical: 1}, &hlc.Timestamp{Logical: 1}
		ok(t, must.NotEqual(ctx, p1, p2, format, arg))
		fail(t, must.NotEqual(ctx, p1, p1, format, arg))
	})

	t.Run("Greater", func(t *testing.T) {
		ok(t, must.Greater(ctx, 1, 0, format, arg))
		fail(t, must.Greater(ctx, 1, 1, format, arg))
		fail(t, must.Greater(ctx, 0, 1, format, arg))

		ok(t, must.Greater(ctx, 3.14, 2.717, format, arg))
		fail(t, must.Greater(ctx, 3.14, 3.14, format, arg))
		fail(t, must.Greater(ctx, 2.717, 3.14, format, arg))

		ok(t, must.Greater(ctx, "b", "a", format, arg))
		ok(t, must.Greater(ctx, "aa", "a", format, arg))
		fail(t, must.Greater(ctx, "b", "b", format, arg))
		fail(t, must.Greater(ctx, "a", "b", format, arg))
	})

	t.Run("GreaterOrEqual", func(t *testing.T) {
		ok(t, must.GreaterOrEqual(ctx, 1, 0, format, arg))
		ok(t, must.GreaterOrEqual(ctx, 1, 1, format, arg))
		fail(t, must.GreaterOrEqual(ctx, 0, 1, format, arg))

		ok(t, must.GreaterOrEqual(ctx, 3.14, 2.717, format, arg))
		ok(t, must.GreaterOrEqual(ctx, 3.14, 3.14, format, arg))
		fail(t, must.GreaterOrEqual(ctx, 2.717, 3.14, format, arg))

		ok(t, must.GreaterOrEqual(ctx, "b", "a", format, arg))
		ok(t, must.GreaterOrEqual(ctx, "aa", "a", format, arg))
		ok(t, must.GreaterOrEqual(ctx, "b", "b", format, arg))
		fail(t, must.GreaterOrEqual(ctx, "a", "b", format, arg))
	})

	t.Run("Less", func(t *testing.T) {
		ok(t, must.Less(ctx, 0, 1, format, arg))
		fail(t, must.Less(ctx, 1, 1, format, arg))
		fail(t, must.Less(ctx, 1, 0, format, arg))

		ok(t, must.Less(ctx, 2.717, 3.14, format, arg))
		fail(t, must.Less(ctx, 3.14, 3.14, format, arg))
		fail(t, must.Less(ctx, 3.14, 2.717, format, arg))

		ok(t, must.Less(ctx, "a", "b", format, arg))
		ok(t, must.Less(ctx, "a", "aa", format, arg))
		fail(t, must.Less(ctx, "b", "b", format, arg))
		fail(t, must.Less(ctx, "b", "a", format, arg))
	})

	t.Run("LessOrEqual", func(t *testing.T) {
		ok(t, must.LessOrEqual(ctx, 0, 1, format, arg))
		ok(t, must.LessOrEqual(ctx, 1, 1, format, arg))
		fail(t, must.LessOrEqual(ctx, 1, 0, format, arg))

		ok(t, must.LessOrEqual(ctx, 2.717, 3.14, format, arg))
		ok(t, must.LessOrEqual(ctx, 3.14, 3.14, format, arg))
		fail(t, must.LessOrEqual(ctx, 3.14, 2.717, format, arg))

		ok(t, must.LessOrEqual(ctx, "a", "b", format, arg))
		ok(t, must.LessOrEqual(ctx, "a", "aa", format, arg))
		ok(t, must.LessOrEqual(ctx, "b", "b", format, arg))
		fail(t, must.LessOrEqual(ctx, "b", "a", format, arg))
	})

	t.Run("Error", func(t *testing.T) {
		ok(t, must.Error(ctx, errors.New("error"), format, arg))
		fail(t, must.Error(ctx, nil, format, arg))
	})

	t.Run("NoError", func(t *testing.T) {
		ok(t, must.NoError(ctx, nil, format, arg))
		fail(t, must.NoError(ctx, errors.New("error"), format, arg))
	})

	t.Run("Len", func(t *testing.T) {
		ok(t, must.Len(ctx, []int{1, 2, 3}, 3, format, arg))
		ok(t, must.Len(ctx, []int(nil), 0, format, arg))
		fail(t, must.Len(ctx, []int{1, 2, 3}, 2, format, arg))
	})

	t.Run("Empty", func(t *testing.T) {
		ok(t, must.Empty(ctx, []int{}, format, arg))
		ok(t, must.Empty(ctx, []int(nil), format, arg))
		fail(t, must.Empty(ctx, []int{1, 2, 3}, format, arg))
	})

	t.Run("NotEmpty", func(t *testing.T) {
		ok(t, must.NotEmpty(ctx, []int{1, 2, 3}, format, arg))
		fail(t, must.NotEmpty(ctx, []int{}, format, arg))
		fail(t, must.NotEmpty(ctx, []int(nil), format, arg))
	})

	t.Run("Nil", func(t *testing.T) {
		ok(t, must.Nil(ctx, (*hlc.Timestamp)(nil), format, arg))
		fail(t, must.Nil(ctx, &hlc.Timestamp{}, format, arg))
	})

	t.Run("NotNil", func(t *testing.T) {
		ok(t, must.NotNil(ctx, &hlc.Timestamp{}, format, arg))
		fail(t, must.NotNil(ctx, (*hlc.Timestamp)(nil), format, arg))
	})

	t.Run("Zero", func(t *testing.T) {
		ok(t, must.Zero(ctx, false, format, arg))
		fail(t, must.Zero(ctx, true, format, arg))

		ok(t, must.Zero(ctx, 0, format, arg))
		fail(t, must.Zero(ctx, 1, format, arg))

		ok(t, must.Zero(ctx, 0.0, format, arg))
		fail(t, must.Zero(ctx, 0.1, format, arg))

		ok(t, must.Zero(ctx, "", format, arg))
		fail(t, must.Zero(ctx, "a", format, arg))

		ok(t, must.Zero(ctx, hlc.Timestamp{}, format, arg))
		fail(t, must.Zero(ctx, hlc.Timestamp{Logical: 1}, format, arg))

		ok(t, must.Zero(ctx, (*hlc.Timestamp)(nil), format, arg))
		fail(t, must.Zero(ctx, &hlc.Timestamp{}, format, arg))
	})

	t.Run("NotZero", func(t *testing.T) {
		ok(t, must.NotZero(ctx, true, format, arg))
		fail(t, must.NotZero(ctx, false, format, arg))

		ok(t, must.NotZero(ctx, 1, format, arg))
		fail(t, must.NotZero(ctx, 0, format, arg))

		ok(t, must.NotZero(ctx, 0.1, format, arg))
		fail(t, must.NotZero(ctx, 0.0, format, arg))

		ok(t, must.NotZero(ctx, "a", format, arg))
		fail(t, must.NotZero(ctx, "", format, arg))

		ok(t, must.NotZero(ctx, hlc.Timestamp{Logical: 1}, format, arg))
		fail(t, must.NotZero(ctx, hlc.Timestamp{}, format, arg))

		ok(t, must.NotZero(ctx, &hlc.Timestamp{}, format, arg))
		fail(t, must.NotZero(ctx, (*hlc.Timestamp)(nil), format, arg))
	})

	t.Run("Same", func(t *testing.T) {
		p1, p2 := &hlc.Timestamp{Logical: 1}, &hlc.Timestamp{Logical: 1}
		pNil := (*hlc.Timestamp)(nil)
		ok(t, must.Same(ctx, p1, p1, format, arg))
		ok(t, must.Same(ctx, pNil, pNil, format, arg))
		fail(t, must.Same(ctx, p1, p2, format, arg))
		fail(t, must.Same(ctx, p1, pNil, format, arg))
	})

	t.Run("NotSame", func(t *testing.T) {
		p1, p2 := &hlc.Timestamp{Logical: 1}, &hlc.Timestamp{Logical: 1}
		pNil := (*hlc.Timestamp)(nil)
		ok(t, must.NotSame(ctx, p1, p2, format, arg))
		ok(t, must.NotSame(ctx, p1, pNil, format, arg))
		fail(t, must.NotSame(ctx, p1, p1, format, arg))
		fail(t, must.NotSame(ctx, pNil, pNil, format, arg))
	})
}
