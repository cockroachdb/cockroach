// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package retry

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestDynamicRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeClusterSettings()
	states := map[string]Options{
		"initial": {
			MaxRetries: 3,
		},
		"secondary": {
			MaxRetries: 5,
		},
		"tertiary": {
			MaxRetries: 7,
		},
	}

	alwaysErr := func() (int, error) {
		return 0, errors.New("always error")
	}
	immediateSuccess := func() (int, error) {
		return 42, nil
	}
	succeedAfterN := func(n int) func() (int, error) {
		attempts := 0
		return func() (int, error) {
			attempts++
			if attempts > n {
				return 42, nil
			}
			return 0, errors.New("still failing")
		}
	}

	testcases := []struct {
		name             string
		constructor      func(r *DynamicRetry[int])
		do               func() (int, error)
		expectErr        bool
		expectedAttempts int
	}{
		{
			name:             "immediate success",
			do:               immediateSuccess,
			expectedAttempts: 1,
		},
		{
			name:             "initial state only, consume retries",
			do:               alwaysErr,
			expectErr:        true,
			expectedAttempts: 4,
		},
		{
			name: "transition to second after exhausting first",
			do:   alwaysErr,
			constructor: func(r *DynamicRetry[int]) {
				r.WithStrategy(func(
					r *DynamicRetryState, _ int, _ error,
				) RetryTransition {
					if r.CurrentAttempt() == 3 {
						return NewState("secondary")
					}
					return Continue()
				})
			},
			expectedAttempts: 10,
			expectErr:        true,
		},
		{
			name: "succeed within the third state",
			do:   succeedAfterN(10),
			constructor: func(r *DynamicRetry[int]) {
				r.WithStrategy(func(
					r *DynamicRetryState, _ int, _ error,
				) RetryTransition {
					if r.CurrentAttempt() == 3 {
						return NewState("secondary")
					}
					if r.CurrentAttempt() == 9 {
						return NewState("tertiary")
					}
					return Continue()
				})
			},
			expectedAttempts: 11,
		},
		{
			name: "fast fail before exhausting retries",
			do:   alwaysErr,
			constructor: func(r *DynamicRetry[int]) {
				r.WithStrategy(func(
					r *DynamicRetryState, _ int, _ error,
				) RetryTransition {
					if r.CurrentAttempt() == 1 {
						return FastFail(errors.New("fast fail invoked"))
					}
					return Continue()
				})
			},
			expectErr:        true,
			expectedAttempts: 2,
		},
		{
			name: "reset before failure",
			do:   alwaysErr,
			constructor: func(r *DynamicRetry[int]) {
				r.WithStrategy(func(
					r *DynamicRetryState, _ int, _ error,
				) RetryTransition {
					if r.CurrentAttempt() == 3 {
						return Reset()
					}
					return Continue()
				})
			},
			expectErr:        true,
			expectedAttempts: 8,
		},
		{
			name: "composite test with all transitions",
			do:   succeedAfterN(17),
			constructor: func(r *DynamicRetry[int]) {
				r.WithStrategy(func(
					r *DynamicRetryState, _ int, _ error,
				) RetryTransition {
					// Consume all retries in initial state, then transition to secondary.
					if r.CurrentAttempt() == 3 {
						return NewState("secondary")
					}

					// Reset once in the secondary state.
					if r.CurrentAttempt() == 9 {
						return Reset()
					}

					// After consuming all retries in secondary state twice, transition to
					// tertiary.
					if r.CurrentAttempt() == 15 {
						return NewState("tertiary")
					}

					// Fast fail immediately in the tertiary state.
					if r.CurrentAttempt() == 16 {
						return FastFail(errors.New("fast fail in tertiary state"))
					}

					return Continue()
				})
			},
			expectErr:        true,
			expectedAttempts: 17,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewDynamicRetry[int]("test-operation", &st.SV).WithStates(states)
			if tc.constructor != nil {
				tc.constructor(r)
			}
			_, err := r.Do(tc.do, "initial")
			if tc.expectErr {
				require.Error(t, err)
			}
			require.Equal(t, tc.expectedAttempts, r.CurrentAttempt()+1)
		})
	}
}

func TestDynamicRetryContext(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st := cluster.MakeClusterSettings()
	states := map[string]Options{
		"only": {
			MaxRetries: 5,
		},
	}
	r := NewDynamicRetry[int]("test-operation", &st.SV).
		WithStates(states).
		OnContinue(func(d *DynamicRetryState, _ int, _ error) error {
			if d.CurrentAttempt() == 2 {
				cancel()
			}
			return nil
		})

	_, err := r.DoCtx(ctx, func(_ context.Context) (int, error) {
		return 0, errors.New("always error")
	}, "only")

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, 3, r.CurrentAttempt()+1)
}
