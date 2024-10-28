// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package testutils

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/DataExMachina-dev/side-eye-go/sideeyeclient"
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

type succeedsWithinOpts struct {
	sideEye     bool
	sideEyeName string
}

func (o *succeedsWithinOpts) applyOpts(opts []succeedsWithinOpt) *succeedsWithinOpts {
	for _, f := range opts {
		f(o)
	}
	return o
}

type succeedsWithinOpt func(*succeedsWithinOpts)

func WithSideEye() succeedsWithinOpt {
	return func(o *succeedsWithinOpts) {
		o.sideEye = true
	}
}

func withSideEyeName(name string) succeedsWithinOpt {
	return func(o *succeedsWithinOpts) {
		o.sideEyeName = name
	}
}

// SucceedsSoon fails the test (with t.Fatal) unless the supplied function runs
// without error within a preset maximum duration. The function is invoked
// immediately at first and then successively with an exponential backoff
// starting at 1ns and ending at DefaultSucceedsSoonDuration (or
// RaceSucceedsSoonDuration if race is enabled).
func SucceedsSoon(t TestFataler, fn func() error, opts ...succeedsWithinOpt) {
	t.Helper()
	SucceedsWithin(t, fn, SucceedsSoonDuration())
}

// SucceedsSoonError returns an error unless the supplied function runs without
// error within a preset maximum duration. The function is invoked immediately
// at first and then successively with an exponential backoff starting at 1ns
// and ending at DefaultSucceedsSoonDuration (or RaceSucceedsSoonDuration if
// race is enabled).
func SucceedsSoonError(fn func() error, opts ...succeedsWithinOpt) error {
	return SucceedsWithinError(fn, SucceedsSoonDuration(), opts...)
}

// SucceedsWithin fails the test (with t.Fatal) unless the supplied
// function runs without error within the given duration. The function
// is invoked immediately at first and then successively with an
// exponential backoff starting at 1ns and ending at duration.
func SucceedsWithin(t TestFataler, fn func() error, duration time.Duration, optFs ...succeedsWithinOpt) {
	t.Helper()
	if t, ok := t.(TestNamedFatalerLogger); ok {
		optFs = append(optFs, withSideEyeName(t.Name()))
	}
	if err := SucceedsWithinError(fn, duration, optFs...); err != nil {
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
func SucceedsWithinError(fn func() error, duration time.Duration, optFs ...succeedsWithinOpt) error {
	opts := new(succeedsWithinOpts).applyOpts(optFs)
	tBegin := timeutil.Now()
	wrappedFn := func() error {
		err := fn()
		if timeutil.Since(tBegin) > 3*time.Second && err != nil {
			log.InfofDepth(context.Background(), 4, "SucceedsSoon: %v", err)
		}
		if opts.sideEye {
			url, capErr := sideEyeCapture(opts.sideEyeName)
			if capErr != nil {
				return errors.Join(err, capErr)
			}
			return errors.Wrap(err, fmt.Sprintf("side-eye snapshot: %s", url))
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

var sideEyeClient = sync.OnceValues(func() (*sideeyeclient.SideEyeClient, error) {
	tok, ok := os.LookupEnv("SIDE_EYE_API_TOKEN")
	if !ok {
		return nil, nil
	}
	client, err := sideeyeclient.NewSideEyeClient(sideeyeclient.WithApiToken(tok))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create side-eye client")
	}
	return client, nil
})

func sideEyeCapture(name string) (string, error) {
	client, err := sideEyeClient()
	if err != nil {
		return "", err
	}
	if client == nil {
		// Side-Eye is not configured. Maybe we're not on CI.
		return "", nil
	}

	// TODO: add hostname or something? how do we get this info / what do we call this?
	envName := fmt.Sprintf("testutils-soon_%s_%s", name, timeutil.Now().Format("2006-01-02T15:04:05"))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	res, err := client.CaptureSnapshot(ctx, envName)
	if err != nil {
		return "", errors.Wrap(err, "failed to capture snapshot")
	}
	if len(res.ProcessErrors) > 0 {
		return "", errors.Newf("failed to capture snapshot with partial errors: %+v", res.ProcessErrors)
	}

	return res.SnapshotURL, nil
}
