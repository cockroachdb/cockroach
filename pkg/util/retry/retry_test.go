// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package retry

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRetryExceedsMaxBackoff(t *testing.T) {
	opts := Options{
		InitialBackoff: time.Microsecond * 10,
		MaxBackoff:     time.Microsecond * 100,
		Multiplier:     2,
		MaxRetries:     10,
	}

	r := Start(opts)
	r.opts.RandomizationFactor = 0
	for i := 0; i < 10; i++ {
		d := r.retryIn()
		if d > opts.MaxBackoff {
			t.Fatalf("expected backoff less than max-backoff: %s vs %s", d, opts.MaxBackoff)
		}
		r.currentAttempt++
	}
}

func TestRetryExceedsMaxAttempts(t *testing.T) {
	opts := Options{
		InitialBackoff: time.Microsecond * 10,
		MaxBackoff:     time.Second,
		Multiplier:     2,
		MaxRetries:     1,
	}

	attempts := 0
	for r := Start(opts); r.Next(); attempts++ {
	}

	if expAttempts := opts.MaxRetries + 1; attempts != expAttempts {
		t.Errorf("expected %d attempts, got %d attempts", expAttempts, attempts)
	}
}

func TestRetryExceedsMaxDuration(t *testing.T) {
	timeSource := timeutil.NewManualTime(time.Now())
	opts := Options{
		InitialBackoff:      time.Millisecond * 10,
		MaxBackoff:          time.Millisecond * 10,
		Multiplier:          1,
		RandomizationFactor: -1,
		MaxDuration:         time.Millisecond * 100,
		PreemptivelyCancel:  true,
		Clock:               timeSource,
	}
	const expectedRetries = 10
	r := Start(opts)
	r.backingOffHook = func(backoff time.Duration) {
		timeSource.Advance(backoff)
	}
	var retries int
	for r.Reset(); r.Next(); retries++ {
	}
	require.Equal(t, expectedRetries, retries, "expected %d retries, got %d", expectedRetries, retries)
}

func TestRetryReset(t *testing.T) {
	t.Run("attempt-based retry", func(t *testing.T) {
		opts := Options{
			InitialBackoff: time.Microsecond * 10,
			MaxBackoff:     time.Second,
			Multiplier:     2,
			MaxRetries:     1,
		}

		expAttempts := opts.MaxRetries + 1

		attempts := 0
		// Backoff loop has 1 allowed retry; we always call Reset, so
		// just make sure we get to 2 attempts and then break.
		for r := Start(opts); r.Next(); attempts++ {
			if attempts == expAttempts {
				break
			}
			r.Reset()
		}
		if attempts != expAttempts {
			t.Errorf("expected %d attempts, got %d", expAttempts, attempts)
		}
	})

	t.Run("duration-based retry", func(t *testing.T) {
		timeSource := timeutil.NewManualTime(time.Now())
		maxDuration := time.Microsecond * 100
		opts := Options{
			InitialBackoff: time.Microsecond * 10,
			MaxBackoff:     time.Second,
			Multiplier:     2,
			MaxDuration:    maxDuration,
			Clock:          timeSource,
		}

		var attempts int
		expAttempts := 3
		for r := Start(opts); r.Next(); attempts++ {
			if attempts == expAttempts {
				break
			}
			// Each attempt takes the entire max duration, so without reset, we would
			// expect only 1 attempt.
			timeSource.Advance(maxDuration)
			r.Reset()
		}
		if attempts != expAttempts {
			t.Errorf("expected %d attempts, got %d", expAttempts, attempts)
		}
	})
}

func TestRetryStop(t *testing.T) {
	closer := make(chan struct{})

	opts := Options{
		InitialBackoff: time.Second,
		MaxBackoff:     time.Second,
		Multiplier:     2,
		Closer:         closer,
	}

	var attempts int

	// Create a retry loop which will never stop without stopper.
	for r := Start(opts); r.Next(); attempts++ {
		go close(closer)
		// Don't race the stopper, just wait for it to do its thing.
		<-opts.Closer
	}

	if expAttempts := 1; attempts != expAttempts {
		t.Errorf("expected %d attempts, got %d", expAttempts, attempts)
	}
}

func TestRetryNextCh(t *testing.T) {
	runTest := func(t *testing.T, opts Options, expectedAttempts int) {
		t.Helper()
		var attempt int
		timeSource := timeutil.NewManualTime(time.Now())
		opts.Clock = timeSource
		opts.RandomizationFactor = -1 // Disable randomization for deterministic tests.

		r := Start(opts)
		r.backingOffHook = func(backoff time.Duration) {
			timeSource.Advance(backoff)
		}
		for r.Reset(); attempt < expectedAttempts+1; attempt++ {
			start := timeSource.Now()
			ch := r.NextCh()
			ok := <-ch
			measuredBackoff := timeSource.Since(start)

			if attempt == 0 {
				require.True(t, ok, "expected permitted attempt on first attempt")
				require.Zero(t, measuredBackoff, "expected first attempt to start instantly")
			} else if attempt < expectedAttempts {
				require.True(t, ok, "expected permitted attempt on attempt #%d", attempt+1)
				require.GreaterOrEqual(
					t, measuredBackoff, opts.InitialBackoff,
					"expected non-zero backoff at least greater than initial backoff",
				)
			} else {
				require.False(t, ok, "expected blocked attempt on attempts exceeding the limit")
			}
		}
	}

	t.Run("attempt-based retry", func(t *testing.T) {
		opts := Options{
			InitialBackoff: time.Millisecond,
			Multiplier:     2,
			MaxRetries:     1,
		}
		runTest(t, opts, opts.MaxRetries+1 /* expectedAttempts */)
	})

	t.Run("duration-based retry", func(t *testing.T) {
		opts := Options{
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond * 2,
			Multiplier:     2,
			// Requires to be set because the manual clock is always advanced based on
			// the backoff duration, so the duration expiration timer can tie with the
			// backoff timer.
			PreemptivelyCancel: true,
			MaxDuration:        time.Millisecond * 4,
		}
		runTest(t, opts, 3 /* expectedAttempts */)
	})
}

func TestRetryWithMaxAttempts(t *testing.T) {
	expectedErr := errors.New("placeholder")
	attempts := 0
	noErrFunc := func() error {
		attempts++
		return nil
	}
	errWithAttemptsCounterFunc := func() error {
		attempts++
		return expectedErr
	}
	errorUntilAttemptNumFunc := func(until int) func() error {
		return func() error {
			attempts++
			if attempts == until {
				return nil
			}
			return expectedErr
		}
	}
	cancelCtx, cancelCtxFunc := context.WithCancel(context.Background())
	closeCh := make(chan struct{})

	testCases := []struct {
		desc string

		ctx                    context.Context
		opts                   Options
		preWithMaxAttemptsFunc func()
		retryFunc              func() error
		maxAttempts            int

		// Due to channel races with select, we can allow a range of number of attempts.
		minNumAttempts  int
		maxNumAttempts  int
		expectedErrText string
	}{
		{
			desc: "succeeds when no errors are ever given",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Microsecond * 10,
				MaxBackoff:     time.Microsecond * 20,
				Multiplier:     2,
				MaxRetries:     1,
			},
			retryFunc:   noErrFunc,
			maxAttempts: 3,

			minNumAttempts: 1,
			maxNumAttempts: 1,
		},
		{
			desc: "succeeds after one faked error",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Microsecond * 10,
				MaxBackoff:     time.Microsecond * 20,
				Multiplier:     2,
				MaxRetries:     1,
			},
			retryFunc:   errorUntilAttemptNumFunc(1),
			maxAttempts: 3,

			minNumAttempts: 1,
			maxNumAttempts: 1,
		},
		{
			desc: "errors when max_attempts=3 is exhausted",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Microsecond * 10,
				MaxBackoff:     time.Microsecond * 20,
				Multiplier:     2,
				MaxRetries:     1,
			},
			retryFunc:   errWithAttemptsCounterFunc,
			maxAttempts: 3,

			minNumAttempts:  3,
			maxNumAttempts:  3,
			expectedErrText: expectedErr.Error(),
		},
		{
			desc: "errors when max_attempts=1 is exhausted",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Microsecond * 10,
				MaxBackoff:     time.Microsecond * 20,
				Multiplier:     2,
				MaxRetries:     0,
			},
			retryFunc:   errWithAttemptsCounterFunc,
			maxAttempts: 1,

			minNumAttempts:  1,
			maxNumAttempts:  1,
			expectedErrText: expectedErr.Error(),
		},
		{
			desc: "errors with context that is canceled",
			ctx:  cancelCtx,
			opts: Options{
				InitialBackoff: time.Microsecond * 10,
				MaxBackoff:     time.Microsecond * 20,
				Multiplier:     2,
				MaxRetries:     1,
			},
			retryFunc:   errWithAttemptsCounterFunc,
			maxAttempts: 3,
			preWithMaxAttemptsFunc: func() {
				cancelCtxFunc()
			},

			minNumAttempts:  1,
			maxNumAttempts:  3,
			expectedErrText: "did not run function due to context completion: context canceled",
		},
		{
			desc: "errors with opt.Closer that is closed",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Microsecond * 10,
				MaxBackoff:     time.Microsecond * 20,
				Multiplier:     2,
				MaxRetries:     1,
				Closer:         closeCh,
			},
			retryFunc:   errWithAttemptsCounterFunc,
			maxAttempts: 3,
			preWithMaxAttemptsFunc: func() {
				close(closeCh)
			},

			minNumAttempts:  1,
			maxNumAttempts:  3,
			expectedErrText: "did not run function due to closed opts.Closer",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			attempts = 0
			if tc.preWithMaxAttemptsFunc != nil {
				tc.preWithMaxAttemptsFunc()
			}
			err := WithMaxAttempts(tc.ctx, tc.opts, tc.maxAttempts, tc.retryFunc)
			if tc.expectedErrText != "" {
				// Error can be either the expected error or the error timeout, as
				// channels can race.
				require.Truef(
					t,
					err.Error() == tc.expectedErrText || err.Error() == expectedErr.Error(),
					"expected %s or %s, got %s",
					tc.expectedErrText,
					expectedErr.Error(),
					err.Error(),
				)
			} else {
				require.NoError(t, err)
			}
			require.GreaterOrEqual(t, attempts, tc.minNumAttempts)
			require.LessOrEqual(t, attempts, tc.maxNumAttempts)
		})
	}
}

func TestRetryWithMaxDuration(t *testing.T) {
	expectedErr := errors.New("placeholder")
	noErrFunc := func(int) error {
		return nil
	}
	errFunc := func(int) error {
		return expectedErr
	}
	// 0-based attempts counter
	errorUntilAttemptNumFunc := func(until int) func(int) error {
		return func(attemptNum int) error {
			if attemptNum == until {
				return nil
			}
			return expectedErr
		}
	}
	cancelCtx, cancelCtxFunc := context.WithCancel(context.Background())
	closeCh := make(chan struct{})

	type testcase struct {
		name              string
		ctx               context.Context
		opts              Options
		retryFunc         func(attemptNum int) error
		preRetryFunc      func()
		expectedTimeSpent time.Duration
		// For cases where the amount of time spent is not deterministic, we can set
		// an upper bound instead (e.g. for context or closer).
		maxExpectedTimeSpent time.Duration
		expectedErr          bool
		skipUnderDuress      bool
	}

	testCases := []testcase{
		{
			name: "succeeds when no errors are ever given",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 20,
				Multiplier:     2,
				MaxDuration:    time.Millisecond * 50,
			},
			retryFunc:         noErrFunc,
			expectedTimeSpent: 0,
		},
		{
			name: "succeeds after one faked error",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 20,
				Multiplier:     2,
				MaxDuration:    time.Millisecond * 50,
			},
			retryFunc:         errorUntilAttemptNumFunc(1),
			expectedTimeSpent: time.Millisecond,
		},
		{
			name: "errors when max duration is exhausted",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 2,
				Multiplier:     2,
				MaxDuration:    time.Millisecond * 50,
			},
			retryFunc:         errFunc,
			expectedTimeSpent: time.Millisecond * 50,
			expectedErr:       true,
		},
		{
			name: "preemptively ends loop if backoff exceeds max duration",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff:     time.Millisecond,
				MaxBackoff:         time.Millisecond * 100,
				Multiplier:         100,
				MaxDuration:        time.Millisecond * 50,
				PreemptivelyCancel: true,
			},
			retryFunc:         errFunc,
			expectedTimeSpent: time.Millisecond,
			expectedErr:       true,
		},
		{
			name: "errors with context that is canceled",
			ctx:  cancelCtx,
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 2,
				Multiplier:     2,
				MaxDuration:    time.Millisecond * 50,
			},
			retryFunc: errFunc,
			preRetryFunc: func() {
				cancelCtxFunc()
			},
			maxExpectedTimeSpent: time.Millisecond * 20,
			expectedErr:          true,
			// Under duress, closing a context will not necessarily stop the retry
			// loop immediately, so we skip this test under duress.
			skipUnderDuress: true,
		},
		{
			name: "errors with opt.Closer that is closed",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 2,
				Multiplier:     2,
				MaxDuration:    time.Millisecond * 50,
				Closer:         closeCh,
			},
			retryFunc: errFunc,
			preRetryFunc: func() {
				close(closeCh)
			},
			maxExpectedTimeSpent: time.Millisecond * 20,
			expectedErr:          true,
			// Under duress, closing a channel will not necessarily stop the retry
			// loop immediately, so we skip this test under duress.
			skipUnderDuress: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipUnderDuress && skip.Duress() {
				skip.UnderDuress(t, "skipping test under duress: %s", tc.name)
			}

			timeSource := timeutil.NewManualTime(time.Now())
			tc.opts.Clock = timeSource
			// Disable randomization for deterministic tests.
			tc.opts.RandomizationFactor = -1

			if tc.preRetryFunc != nil {
				tc.preRetryFunc()
			}

			start := timeSource.Now()
			expectedMaxEndTime := start.Add(tc.opts.MaxDuration)

			r := StartWithCtx(tc.ctx, tc.opts)
			// Simulate advancing time by manually advancing by the backoff duration
			// up to the expected maximum end time.
			r.backingOffHook = func(backoff time.Duration) {
				nextTime := timeSource.Now().Add(backoff)
				if nextTime.After(expectedMaxEndTime) {
					nextTime = expectedMaxEndTime
				}
				timeSource.AdvanceTo(nextTime)
			}

			var err error
			for r.Reset(); r.Next(); {
				if err = tc.retryFunc(r.CurrentAttempt()); err == nil {
					break
				}
			}

			if tc.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.expectedTimeSpent != 0 {
				require.Equal(
					t, tc.expectedTimeSpent, timeSource.Since(start), "expected time does not match actual spent time",
				)
			}

			if tc.maxExpectedTimeSpent != 0 {
				require.LessOrEqual(
					t, timeSource.Since(start), tc.maxExpectedTimeSpent,
					"expected time spent to be less than or equal to max expected time spent",
				)
			}
		})
	}
}

func TestRetryWithMaxDurationAndMaxRetries(t *testing.T) {
	t.Run("max retries hit before max duration", func(t *testing.T) {
		opts := Options{
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond * 20,
			Multiplier:     2,
			MaxDuration:    time.Millisecond * 50,
			MaxRetries:     3,
		}

		var attempts int
		start := time.Now()
		for r := Start(opts); r.Next(); attempts++ {
			if attempts == opts.MaxRetries+1 {
				break
			}
		}
		elapsed := time.Since(start)

		require.Equal(
			t, opts.MaxRetries+1, attempts, "expected %d attempts, got %d", opts.MaxRetries+1, attempts,
		)
		require.Less(
			t, elapsed, opts.MaxDuration, "expected elapsed time to be less than max duration",
		)
	})

	t.Run("max duration hit before max retries", func(t *testing.T) {
		opts := Options{
			InitialBackoff: time.Millisecond,
			MaxBackoff:     time.Millisecond,
			MaxDuration:    time.Millisecond * 20,
			MaxRetries:     30,
		}

		var attempts int
		start := time.Now()
		for r := Start(opts); r.Next(); attempts++ {
		}
		elapsed := time.Since(start)

		require.Less(
			t, attempts, opts.MaxRetries+1, "expected attempts to not have exhausted max retries", attempts,
		)
		require.GreaterOrEqual(
			t, elapsed, opts.MaxDuration-2*time.Millisecond, "expected elapsed time to have exhausted max duration",
		)
	})
}
