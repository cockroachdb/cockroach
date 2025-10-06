// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"context"
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
// exponential backoff starting at 1ns and ending at duration.
func SucceedsWithin(t TestFataler, fn func() error, duration time.Duration) {
	t.Helper()
	if err := SucceedsWithinError(fn, duration); err != nil {
		if f, l, _, ok := errors.GetOneLineSource(err); ok {
			err = errors.Wrapf(err, "from %s:%d", f, l)
		}
		t.Fatalf("condition failed to evaluate within %s: %s", duration, err)
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
			log.InfofDepth(context.Background(), 4, "SucceedsSoon: %v", err)
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
