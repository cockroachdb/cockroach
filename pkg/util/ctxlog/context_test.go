// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ctxlog

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRunWithTimeout(t *testing.T) {
	ctx := context.Background()
	err := timeutil.RunWithTimeout(ctx, "foo", 1, func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatal("RunWithTimeout shouldn't return a timeout error if nobody touched the context.")
	}

	err = timeutil.RunWithTimeout(ctx, "foo", 1, func(ctx context.Context) error {
		time.Sleep(15 * time.Millisecond)
		return ctx.Err()
	})
	require.Error(t, err)
	baseExpectedMsg := "operation \"foo\" timed out after ..ms \\(given timeout 1ns\\)"
	expectedMsg := baseExpectedMsg + ": context deadline exceeded"
	require.Regexp(t, expectedMsg, err.Error())
	var netError net.Error
	if !errors.As(err, &netError) {
		t.Fatal("RunWithTimeout should return a net.Error")
	}
	//lint:ignore SA1019 grandfathered test code
	if !netError.Timeout() || !netError.Temporary() {
		t.Fatal("RunWithTimeout should return a timeout and temporary error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunWithTimeout should return an error with a DeadlineExceeded cause")
	}

	err = timeutil.RunWithTimeout(ctx, "foo", 1, func(ctx context.Context) error {
		time.Sleep(15 * time.Millisecond)
		return errors.Wrap(ctx.Err(), "custom error")
	})
	require.Error(t, err)
	expExtended := baseExpectedMsg + ": custom error: context deadline exceeded"
	require.Regexp(t, expExtended, err.Error())
	if !errors.As(err, &netError) {
		t.Fatal("RunWithTimeout should return a net.Error")
	}
	//lint:ignore SA1019 grandfathered test code
	if !netError.Timeout() || !netError.Temporary() {
		t.Fatal("RunWithTimeout should return a timeout and temporary error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunWithTimeout should return an error with a DeadlineExceeded cause")
	}
}

// TestRunWithTimeoutWithoutDeadlineExceeded ensures that when a timeout on the
// context occurs but the underlying error does not have
// context.DeadlineExceeded as its Cause (perhaps due to serialization) the
// returned error is still a TimeoutError. In this case however the underlying
// cause should be the returned error and not context.DeadlineExceeded.
func TestRunWithTimeoutWithoutDeadlineExceeded(t *testing.T) {
	ctx := context.Background()
	notContextDeadlineExceeded := errors.Handled(context.DeadlineExceeded)
	err := timeutil.RunWithTimeout(ctx, "foo", 1, func(ctx context.Context) error {
		<-ctx.Done()
		return notContextDeadlineExceeded
	})
	var netError net.Error
	if !errors.As(err, &netError) {
		t.Fatal("RunWithTimeout should return a net.Error")
	}
	//lint:ignore SA1019 grandfathered test code
	if !netError.Timeout() || !netError.Temporary() {
		t.Fatal("RunWithTimeout should return a timeout and temporary error")
	}
	if !errors.Is(err, notContextDeadlineExceeded) {
		t.Fatalf("RunWithTimeout should return an error caused by the underlying " +
			"returned error")
	}
}

func TestRunWithTimeoutAfterDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := timeutil.RunWithTimeout(ctx, "test", time.Second, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	require.Error(t, err)
	require.Regexp(t,
		`operation "test" timed out after \d+m?s \(given timeout 1s\): context deadline exceeded`,
		err.Error())
}
