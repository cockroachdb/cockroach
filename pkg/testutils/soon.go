// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// DefaultSucceedsSoonDuration is the maximum amount of time unittests
	// will wait for a condition to become true. See SucceedsSoon().
	DefaultSucceedsSoonDuration = 45 * time.Second

	// RaceSucceedsSoonDuration is the maximum amount of time
	// unittests will wait for a condition to become true when
	// running with the race detector enabled.
	RaceSucceedsSoonDuration = DefaultSucceedsSoonDuration * 5
)

// SucceedsSoon fails the test (with t.Fatal) unless the supplied function runs
// without error within a preset maximum duration. The function is invoked
// immediately at first and then successively with an exponential backoff
// starting at 1ns and ending at DefaultSucceedsSoonDuration (or
// RaceSucceedsSoonDuration if race is enabled).
func SucceedsSoon(t TestFataler, fn func() error) {
	t.Helper()
	SucceedsWithin(t, fn, SucceedsSoonDuration())
}

// SucceedsSoonError returns an error unless the supplied function runs without
// error within a preset maximum duration. The function is invoked immediately
// at first and then successively with an exponential backoff starting at 1ns
// and ending at DefaultSucceedsSoonDuration (or RaceSucceedsSoonDuration if
// race is enabled).
func SucceedsSoonError(fn func() error) error {
	return SucceedsWithinError(fn, SucceedsSoonDuration())
}

// SucceedsWithin fails the test (with t.Fatal) unless the supplied
// function runs without error within the given duration. The function
// is invoked immediately at first and then successively with an
// exponential backoff starting at 1ns and ending at duration. On
// timeout, a goroutine dump is written to a file alongside the test
// output to aid in debugging what the system was doing instead of
// reaching the expected state.
func SucceedsWithin(t TestFataler, fn func() error, duration time.Duration) {
	t.Helper()
	if err := SucceedsWithinError(fn, duration); err != nil {
		if f, l, _, ok := errors.GetOneLineSource(err); ok {
			err = errors.Wrapf(err, "from %s:%d", f, l)
		}
		dumpFile := WriteGoroutineDump()
		t.Fatalf("condition failed to evaluate within %s: %s\n\ngoroutine dump: %s",
			duration, err, dumpFile)
	}
}

// SucceedsWithinError returns an error unless the supplied function
// runs without error within the given duration. The function is
// invoked immediately at first and then successively with an
// exponential backoff starting at 1ns and ending at duration.
func SucceedsWithinError(fn func() error, duration time.Duration) error {
	tBegin := timeutil.Now()
	wrappedFn := func() error {
		err := fn()
		if timeutil.Since(tBegin) > 3*time.Second && err != nil {
			log.Dev.InfofDepth(context.Background(), 4, "SucceedsSoon: %v", err)
		}
		return err
	}
	return retry.ForDuration(duration, wrappedFn)
}

func SucceedsSoonDuration() time.Duration {
	if util.RaceEnabled || syncutil.DeadlockEnabled {
		return RaceSucceedsSoonDuration
	}
	return DefaultSucceedsSoonDuration
}

// Soon retries the fn closure until it succeeds (no assertion failures) or the
// SucceedsSoonDuration elapses, failing the test on timeout.
//
// The fn receives a *RetryT that supports both testify assert and require
// idioms, with the following semantics:
//   - assert failures are collected, and cause a retry when the closure returns
//   - require failures (FailNow) abort the closure and cause a retry
//
// Within the closure, use rt (RetryT) for "retryable" assertions, and the outer
// t for assertions that must fail the test immediately:
//
//	testutils.Soon(t, func(rt *testutils.RetryT) {
//		x, y, err := someCall()
//		require.NoError(t, err)    // fatal: unexpected error / bug
//		assert.Equal(rt, 123, x)   // retryable, keep going if fails
//		require.Equal(rt, 456, y)  // retryable, value should converge
//	})
//
// Panics within the closure escape to the caller.
func Soon(t TestFataler, fn func(t *RetryT)) {
	t.Helper()
	SoonDuration(t, fn, SucceedsSoonDuration())
}

// SoonDuration is like Soon but with a custom timeout duration.
func SoonDuration(t TestFataler, fn func(t *RetryT), duration time.Duration) {
	t.Helper()
	if err := soonErr(fn, duration); err != nil {
		dumpFile := WriteGoroutineDump()
		t.Fatalf(
			"condition failed to evaluate within %s: %s\n\ngoroutine dump: %s",
			duration, err, dumpFile,
		)
	}
}

func soonErr(fn func(t *RetryT), duration time.Duration) error {
	return SucceedsWithinError(func() (retErr error) {
		var rt RetryT
		defer func() {
			if r := recover(); r == nil {
				return
			} else if _, ok := r.(retryPanic); !ok {
				panic(r) // it was a real panic
			} else if retErr = rt.toError(); retErr == nil {
				// No errors collected, FailNow was called directly.
				retErr = errors.New("FailNow called")
			}
		}()
		fn(&rt)
		return rt.toError()
	}, duration)
}

// RetryT implements testify's TestingT interface (Errorf, FailNow, Helper) and
// collects assertion failures as errors. FailNow calls, and assertion failures
// collected via Errorf, cause the enclosing Soon* call to retry the closure.
type RetryT struct {
	errors []string
}

// Helper is a no-op to satisfy testify's TestingT interface.
func (r *RetryT) Helper() {}

// Errorf records an assertion failure message.
func (r *RetryT) Errorf(format string, args ...interface{}) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

// FailNow panics with a sentinel retryPanic value. This stops the current
// closure execution and lets Soon* catch the panic and retry.
func (r *RetryT) FailNow() {
	panic(retryPanic{})
}

func (r *RetryT) toError() error {
	if len(r.errors) == 0 {
		return nil
	}
	return errors.Newf("%s", strings.Join(r.errors, "\n"))
}

// retryPanic is a sentinel value used by RetryT.FailNow to abort the current
// attempt via panic/recover without killing the test goroutine. This lets
// require.* calls trigger a retry instead of a fatal exit.
type retryPanic struct{}
