// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
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
		dumpFile := writeGoroutineDump()
		t.Fatalf("condition failed to evaluate within %s: %s\n\ngoroutine dump: %s",
			duration, err, dumpFile)
	}
}

// writeGoroutineDump writes a goroutine stack dump to a file in the test
// output directory and returns the file path. If the file cannot be created
// or written, it returns a description of the error.
func writeGoroutineDump() string {
	f, err := os.CreateTemp(datapathutils.DebuggableTempDir(), "goroutine_dump_*.txt")
	if err != nil {
		return fmt.Sprintf("<failed to create dump file: %v>", err)
	}
	defer f.Close()
	if _, err := f.Write(allstacks.Get()); err != nil {
		return fmt.Sprintf("<failed to write dump to %s: %v>", f.Name(), err)
	}
	return f.Name()
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
