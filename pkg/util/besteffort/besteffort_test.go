// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package besteffort

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestWarningAndError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testErr := errors.New("test error")

	operations := []struct {
		name string
		fn   func(context.Context, string, func(context.Context) error)
	}{
		{"Warning", Warning},
		{"Error", Error},
	}

	for _, tc := range operations {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("failure panics by default", func(t *testing.T) {
				defer TestForbidSkip("panic")()
				require.Panics(t, func() {
					tc.fn(ctx, "panic", func(ctx context.Context) error {
						return testErr
					})
				})
			})

			t.Run("TestAllowFailure prevents panic", func(t *testing.T) {
				defer TestForbidSkip("op-name")()
				defer TestAllowFailure("op-name")()
				require.NotPanics(t, func() {
					tc.fn(ctx, "op-name", func(ctx context.Context) error {
						return testErr
					})
				})
			})

			t.Run("TestAllowFailure cleanup restores panic", func(t *testing.T) {
				defer TestForbidSkip("op-name")()

				forbidFailures := TestAllowFailure("op-name")
				require.NotPanics(t, func() {
					tc.fn(ctx, "op-name", func(ctx context.Context) error {
						return testErr
					})
				})

				forbidFailures()
				require.Panics(t, func() {
					tc.fn(ctx, "op-name", func(ctx context.Context) error {
						return testErr
					})
				})
			})

			t.Run("TestForbidSkip ensures execution", func(t *testing.T) {
				defer TestForbidSkip("forbid")()
				for i := 0; i < 20; i++ {
					executed := false
					tc.fn(ctx, "forbid", func(ctx context.Context) error {
						executed = true
						return nil
					})
					require.True(t, executed)
				}
			})

			t.Run("skipping without TestForbidSkip", func(t *testing.T) {
				executedCount := 0
				iterations := 100

				for i := 0; i < iterations; i++ {
					tc.fn(ctx, "skip", func(ctx context.Context) error {
						executedCount++
						return nil
					})
				}

				// With 50% skip probability, should execute sometimes but not always
				require.Greater(t, executedCount, 0)
				require.Less(t, executedCount, iterations)
			})
		})
	}
}
