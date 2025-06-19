// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package retry

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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
	opts := Options{
		InitialBackoff: time.Microsecond * 10,
		MaxBackoff:     time.Microsecond * 100,
		Multiplier:     2,
		MaxDuration:    time.Millisecond * 100,
	}

	var mu syncutil.Mutex
	var attempts int
	var maxAttempts int

	// Once max duration has elapsed, no more attempts should be made, so we
	// record the current number of attempts at the time of the timeout.
	go func() {
		time.AfterFunc(opts.MaxDuration, func() {
			mu.Lock()
			defer mu.Unlock()
			maxAttempts = attempts
		})
	}()

	for r := Start(opts); r.Next(); attempts++ {
		time.Sleep(time.Microsecond * 10)
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts != maxAttempts {
		t.Errorf(
			"expected retry loop to stop after %d attempts, but it ran for %d attempts",
			maxAttempts, attempts,
		)
	}
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
		maxDuration := time.Microsecond * 100
		opts := Options{
			InitialBackoff: time.Microsecond * 10,
			MaxBackoff:     time.Second,
			Multiplier:     2,
			MaxDuration:    maxDuration,
		}

		var attempts int
		expAttempts := 3
		for r := Start(opts); r.Next(); attempts++ {
			if attempts == expAttempts {
				break
			}
			// Each attempt takes the entire max duration, so without reset, we would
			// expect only 1 attempt.
			time.Sleep(maxDuration)
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
	runTest := func(opts Options, expectedAttempts int) {
		var attempt int
		for r := Start(opts); attempt < expectedAttempts+1; attempt++ {
			ch := r.NextCh()
			start := time.Now()
			ok := <-ch
			measuredBackoff := time.Since(start)

			if attempt == 0 {
				require.True(t, ok, "expected permitted attempt on first attempt")
				require.LessOrEqual(
					t, measuredBackoff, time.Microsecond,
					"expected first attempt to start nearly instantly",
				)
			} else if attempt < expectedAttempts {
				require.True(t, ok, "expected permitted attempt on attempt #%d", attempt+1)
				require.GreaterOrEqual(
					// Add some tolerance to the backoff measurement
					t, measuredBackoff, opts.InitialBackoff*3/4,
					"expected backoff to be at least initial backoff of attempt #%d",
					attempt,
				)
			} else {
				require.False(t, ok, "expected blocked attempt on third attempt")
			}
		}
	}

	t.Run("attempt-based retry", func(t *testing.T) {
		opts := Options{
			InitialBackoff: time.Millisecond,
			Multiplier:     2,
			MaxRetries:     1,
		}
		runTest(opts, 2)
	})

	t.Run("duration-based retry", func(t *testing.T) {
		opts := Options{
			InitialBackoff: time.Millisecond,
			Multiplier:     2,
			MaxDuration:    time.Millisecond * 4,
		}
		runTest(opts, 3)
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
	noErrFunc := func() error {
		return nil
	}
	errFunc := func() error {
		return expectedErr
	}
	var attempts int
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
		desc              string
		ctx               context.Context
		opts              Options
		preRetryFunc      func()
		retryFunc         func() error
		expectedTimeSpent time.Duration
		minTimeSpent      time.Duration
		maxTimeSpent      time.Duration
		expectedErr       bool
	}{
		{
			desc: "succeeds when no errors are ever given",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 20,
				Multiplier:     2,
				MaxDuration:    time.Millisecond * 50,
			},
			retryFunc:    noErrFunc,
			maxTimeSpent: time.Microsecond,
		},
		{
			desc: "succeeds after one faked error",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 20,
				Multiplier:     2,
				MaxDuration:    time.Millisecond * 50,
			},
			retryFunc:         errorUntilAttemptNumFunc(2),
			expectedTimeSpent: time.Millisecond,
		},
		{
			desc: "errors when max duration is exhausted",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 20,
				Multiplier:     2,
				MaxDuration:    time.Millisecond * 50,
			},
			retryFunc:         errFunc,
			expectedTimeSpent: time.Millisecond * 50,
			expectedErr:       true,
		},
		{
			desc: "max duration is not exceeded even if backoff would exceed it",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 100,
				Multiplier:     100,
				MaxDuration:    time.Millisecond * 50,
			},
			retryFunc:         errFunc,
			expectedTimeSpent: time.Millisecond * 50,
			expectedErr:       true,
		},
		{
			desc: "errors with context that is canceled",
			ctx:  cancelCtx,
			opts: Options{
				InitialBackoff: time.Millisecond,
				MaxBackoff:     time.Millisecond * 20,
				Multiplier:     2,
				MaxDuration:    time.Millisecond * 50,
			},
			retryFunc: errFunc,
			preRetryFunc: func() {
				cancelCtxFunc()
			},
			expectedTimeSpent: time.Microsecond,
			expectedErr:       true,
		},
		{
			desc: "errors with opt.Closer that is closed",
			ctx:  context.Background(),
			opts: Options{
				InitialBackoff:      time.Microsecond * 10,
				MaxBackoff:          time.Microsecond * 20,
				Multiplier:          2,
				RandomizationFactor: 0.05,
				MaxDuration:         time.Millisecond * 50,
				Closer:              closeCh,
			},
			retryFunc:         errFunc,
			expectedTimeSpent: time.Microsecond,
			preRetryFunc: func() {
				close(closeCh)
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			attempts = 0
			if tc.preRetryFunc != nil {
				tc.preRetryFunc()
			}
			startTime := time.Now()
			var err error
			for r := StartWithCtx(tc.ctx, tc.opts); r.Next(); {
				if err = tc.retryFunc(); err == nil {
					break
				}
			}
			actualEndTime := time.Now()
			if tc.expectedErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.expectedTimeSpent != 0 {
				expectedEndTime := startTime.Add(tc.expectedTimeSpent)
				// Allow for a 5% tolerance, with at least a 1ms tolerance.
				tolerance := max(
					time.Millisecond,
					time.Duration(float64(tc.expectedTimeSpent)*0.05),
				)
				require.WithinDuration(
					t, expectedEndTime, actualEndTime, tolerance,
					"expected time spent to be roughly %s ± %s, got %s",
					tc.expectedTimeSpent, tolerance, actualEndTime.Sub(startTime),
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
			MaxBackoff:     time.Millisecond * 20,
			Multiplier:     2,
			MaxDuration:    time.Millisecond * 50,
			MaxRetries:     10,
		}

		var attempts int
		start := time.Now()
		for r := Start(opts); r.Next(); attempts++ {
			if time.Since(start) >= opts.MaxDuration {
				break
			}
		}
		elapsed := time.Since(start)

		require.Less(
			t, attempts, opts.MaxRetries+1, "expected attempts to not have exhausted max retries", attempts,
		)
		require.GreaterOrEqual(
			t, elapsed, opts.MaxDuration, "expected elapsed time to have exhausted max duration",
		)
	})
}
