// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
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

// SucceedsSoon fails the test (with t.Fatal) unless the supplied
// function runs without error within a preset maximum duration. The
// function is invoked immediately at first and then successively with
// an exponential backoff starting at 1ns and ending at around 1s.
func SucceedsSoon(t TB, fn func() error) {
	t.Helper()
	SucceedsWithin(t, fn, succeedsSoonDuration())
}

// RequireSoon is the same as SucceedsSoon but also passes in a
// TB instance that doesn't fail t if FailNow is called on it, so
// that the function can use other helpers like require.
//
// RequireSoon(t, func(t testutils.TB) error {
//		require.Equal(t, expected, got)
//		return nil
// }
//
// is equivalent to
//
// SucceedsSoon(t, func() error {
//		if expected != got {
//			return errors.Newf("Expected %s got %s", expected, got)
//		}
//		return nil
// })
func RequireSoon(t TB, fn func(retryableT TB) error) {
	t.Helper()
	sentinel := newSentinelWrapper(t)
	SucceedsSoon(t, func() (err error) {
		defer func() {
			if p := recover(); p != nil && p != errRetryablePanic {
				panic(p)
			}
			if err == nil {
				err = sentinel.err
			}
			sentinel.err = nil
		}()
		return fn(sentinel)
	})
}

// SucceedsSoonError returns an error unless the supplied function runs without
// error within a preset maximum duration. The function is invoked immediately
// at first and then successively with an exponential backoff starting at 1ns
// and ending at around 1s.
func SucceedsSoonError(fn func() error) error {
	return SucceedsWithinError(fn, succeedsSoonDuration())
}

// SucceedsWithin fails the test (with t.Fatal) unless the supplied
// function runs without error within the given duration. The function
// is invoked immediately at first and then successively with an
// exponential backoff starting at 1ns and ending at around 1s.
func SucceedsWithin(t TB, fn func() error, duration time.Duration) {
	t.Helper()
	if err := SucceedsWithinError(fn, duration); err != nil {
		t.Fatalf("condition failed to evaluate within %s: %s", duration, err)
	}
}

// SucceedsWithinError returns an error unless the supplied function
// runs without error within the given duration. The function is
// invoked immediately at first and then successively with an
// exponential backoff starting at 1ns and ending at around 1s.
func SucceedsWithinError(fn func() error, duration time.Duration) error {
	tBegin := timeutil.Now()
	wrappedFn := func() error {
		err := fn()
		if timeutil.Since(tBegin) > 3*time.Second && err != nil {
			log.InfofDepth(context.Background(), 4, "SucceedsSoon: %v", err)
		}
		return err
	}
	return retry.ForDuration(duration, wrappedFn)
}

func succeedsSoonDuration() time.Duration {
	if util.RaceEnabled {
		return RaceSucceedsSoonDuration
	}
	return DefaultSucceedsSoonDuration
}

var errRetryablePanic error = errors.New("retryable panic")

type sentinelWrapper struct {
	t   TB
	err error
}

func newSentinelWrapper(t TB) *sentinelWrapper {
	return &sentinelWrapper{t: t}
}

// Fatal implements the TB interface.
func (s *sentinelWrapper) Fatal(args ...interface{}) {
	s.Helper()
	if len(args) > 0 {
		s.err = errors.New(fmt.Sprint(args...))
	} else {
		s.err = errors.New("Fatal() was called")
	}
	s.FailNow()
}

// Fatalf implements the TB interface.
func (s *sentinelWrapper) Fatalf(format string, args ...interface{}) {
	s.Helper()
	s.Errorf(format, args...)
	s.FailNow()
}

// Errorf implements the TB interface.
func (s *sentinelWrapper) Errorf(format string, args ...interface{}) {
	s.Helper()
	s.err = fmt.Errorf(format, args...)
}

// FailNow implements the TB interface.
func (s *sentinelWrapper) FailNow() {
	s.Helper()
	panic(errRetryablePanic)
}

// Helper implements the TB interface.
func (s *sentinelWrapper) Helper() {
	s.t.Helper()
}
