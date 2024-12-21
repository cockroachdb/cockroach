// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"context"
	"os"
	"time"

	"github.com/DataExMachina-dev/side-eye-go/sideeye"
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
		maybeCaptureSideEyeSnapshot(t)
		t.Fatalf("condition failed to evaluate within %s: %s", duration, err)
	}
}

var sideEyeToken = os.Getenv("SIDE_EYE_API_TOKEN")

// maybeCaptureSideEyeSnapshot captures a Side-Eye snapshot if the
// SIDE_EYE_TOKEN env var is set. If the snapshot is captured, the snapshot's
// URL is logged. Snapshots are captured with a 30s timeout.
func maybeCaptureSideEyeSnapshot(t TestFataler) {
	// If the Side-Eye token is not set, don't do anything.
	if sideEyeToken == "" {
		log.Infof(context.Background(), "not capturing Side-Eye snapshot for timed-out test; SIDE_EYE_API_TOKEN env var not set")
		return
	}

	var name string
	if t, ok := t.(TestNamedFatalerLogger); ok {
		name = t.Name()
	} else {
		name = "unknown SucceedsWithin test"
	}
	ctx := context.Background()
	snapshotCtx, cancel := context.WithTimeoutCause(
		ctx, 30*time.Second, errors.New("timed out waiting for Side-Eye snapshot"),
	)
	defer cancel()
	snapshotURL, err := sideeye.CaptureSelfSnapshot(snapshotCtx, name, sideeye.WithEnvironment("unit tests"))
	if err != nil {
		if errors.As(err, &sideeye.BinaryStrippedError{}) {
			log.Infof(ctx, "failed to capture Side-Eye snapshot because the binary is stripped of debug info; "+
				"if running with `go test` instead of bazel, use `go test -o test.out` "+
				"for creating a non-stripped binary")
		}

		if ctx.Err() != nil {
			err = context.Cause(ctx)
		}
		log.Warningf(ctx, "failed to capture Side-Eye snapshot: %s", err)
	} else {
		log.Infof(ctx, "captured Side-Eye snapshot: %s", snapshotURL)
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
