// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package retry

import (
	"context"
	"testing"
	"time"

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

func TestRetryReset(t *testing.T) {
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
	var attempts int

	opts := Options{
		InitialBackoff: time.Millisecond,
		Multiplier:     2,
		MaxRetries:     1,
	}
	for r := Start(opts); attempts < 3; attempts++ {
		c := r.NextCh()
		if r.currentAttempt != attempts {
			t.Errorf("expected attempt=%d; got %d", attempts, r.currentAttempt)
		}
		switch attempts {
		case 0:
			if c == nil {
				t.Errorf("expected non-nil NextCh() on first attempt")
			}
			if _, ok := <-c; ok {
				t.Errorf("expected closed (immediate) NextCh() on first attempt")
			}
		case 1:
			if c == nil {
				t.Errorf("expected non-nil NextCh() on second attempt")
			}
			if _, ok := <-c; !ok {
				t.Errorf("expected open (delayed) NextCh() on first attempt")
			}
		case 2:
			if c != nil {
				t.Errorf("expected nil NextCh() on third attempt")
			}
		default:
			t.Fatalf("unexpected attempt %d", attempts)
		}
	}
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
			desc: "errors when max attempts is exhausted",
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
