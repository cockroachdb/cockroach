// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contextutil

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestRunWithTimeout(t *testing.T) {
	ctx := context.TODO()
	err := RunWithTimeout(ctx, "foo", 1, func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatal("RunWithTimeout shouldn't return a timeout error if nobody touched the context.")
	}

	err = RunWithTimeout(ctx, "foo", 1, func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return ctx.Err()
	})
	expectedMsg := "operation \"foo\" timed out after 1ns"
	if err.Error() != expectedMsg {
		t.Fatalf("expected %s, actual %s", expectedMsg, err.Error())
	}
	netError, ok := err.(net.Error)
	if !ok {
		t.Fatal("RunWithTimeout should return a net.Error")
	}
	if !netError.Timeout() || !netError.Temporary() {
		t.Fatal("RunWithTimeout should return a timeout and temporary error")
	}
	if errors.Cause(err) != context.DeadlineExceeded {
		t.Fatalf("RunWithTimeout should return an error with a DeadlineExceeded cause")
	}

	err = RunWithTimeout(ctx, "foo", 1, func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return errors.Wrap(ctx.Err(), "custom error")
	})
	expExtended := expectedMsg + ": custom error: context deadline exceeded"
	if err.Error() != expExtended {
		t.Fatalf("expected %s, actual %s", expExtended, err.Error())
	}
	netError, ok = err.(net.Error)
	if !ok {
		t.Fatal("RunWithTimeout should return a net.Error")
	}
	if !netError.Timeout() || !netError.Temporary() {
		t.Fatal("RunWithTimeout should return a timeout and temporary error")
	}
	if errors.Cause(err) != context.DeadlineExceeded {
		t.Fatalf("RunWithTimeout should return an error with a DeadlineExceeded cause")
	}
}

// TestRunWithTimeoutWithoutDeadlineExceeded ensures that when a timeout on the
// context occurs but the underlying error does not have
// context.DeadlineExceeded as its Cause (perhaps due to serialization) the
// returned error is still a TimeoutError. In this case however the underlying
// cause should be the returned error and not context.DeadlineExceeded.
func TestRunWithTimeoutWithoutDeadlineExceeded(t *testing.T) {
	ctx := context.TODO()
	notContextDeadlineExceeded := errors.New(context.DeadlineExceeded.Error())
	err := RunWithTimeout(ctx, "foo", 1, func(ctx context.Context) error {
		<-ctx.Done()
		return notContextDeadlineExceeded
	})
	netError, ok := err.(net.Error)
	if !ok {
		t.Fatal("RunWithTimeout should return a net.Error")
	}
	if !netError.Timeout() || !netError.Temporary() {
		t.Fatal("RunWithTimeout should return a timeout and temporary error")
	}
	if errors.Cause(err) != notContextDeadlineExceeded {
		t.Fatalf("RunWithTimeout should return an error caused by the underlying " +
			"returned error")
	}
}

func TestCancelWithReason(t *testing.T) {
	ctx := context.Background()

	var cancel CancelWithReasonFunc
	ctx, cancel = WithCancelReason(ctx)

	e := errors.New("hodor")
	go func() {
		cancel(e)
	}()

	<-ctx.Done()

	expected := "context canceled"
	found := ctx.Err().Error()
	assert.Equal(t, expected, found)
	assert.Equal(t, e, GetCancelReason(ctx))
}
