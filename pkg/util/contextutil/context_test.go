// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package contextutil

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
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
	if err.Error() != expectedMsg {
		t.Fatalf("expected %s, actual %s", expectedMsg, err.Error())
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
