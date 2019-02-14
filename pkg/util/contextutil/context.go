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
	"fmt"
	"runtime/debug"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// WithCancel adds an info log to context.WithCancel's CancelFunc.
func WithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	return wrap(context.WithCancel(parent))
}

func wrap(ctx context.Context, cancel context.CancelFunc) (context.Context, context.CancelFunc) {
	if !log.V(1) {
		return ctx, cancel
	}
	return ctx, func() {
		if log.V(2) {
			log.InfofDepth(ctx, 1, "canceling context:\n%s", debug.Stack())
		} else if log.V(1) {
			log.InfofDepth(ctx, 1, "canceling context")
		}
		cancel()
	}
}

type TimeoutError struct {
	operation string
	duration  time.Duration
	deadline  time.Time
}

func (t TimeoutError) Error() string {
	if !t.deadline.IsZero() {
		return fmt.Sprintf("operation %q missed deadline of %s", t.operation, t.deadline)
	}
	return fmt.Sprintf("operation %q timed out after %s", t.operation, t.duration)
}
func (TimeoutError) Timeout() bool   { return true }
func (TimeoutError) Temporary() bool { return true }
func (TimeoutError) Cause() error {
	// This ensures that people looking for DeadlineExceeded in particular still
	// see it.
	return context.DeadlineExceeded
}

// RunWithTimeout runs a function with a timeout, the same way you'd do with
// context.WithTimeout. It improve the opaque error messages returned by
// WithTimeout by augmenting them with the op string that is passed in.
func RunWithTimeout(
	ctx context.Context, op string, timeout time.Duration, fn func(ctx context.Context) error,
) error {
	if timeout <= 0 {
		return fn(ctx)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := fn(ctx)
	if err == context.DeadlineExceeded || ctx.Err() == context.DeadlineExceeded {
		return TimeoutError{
			operation: op,
			duration:  timeout,
		}
	}
	return err
}

// RunWithDeadline runs a function with a deadline, the same way you'd do with
// context.WithDeadline. It improve the opaque error messages returned by
// WithDeadline by augmenting them with the op string that is passed in.
func RunWithDeadline(
	ctx context.Context, op string, d time.Time, fn func(ctx context.Context) error,
) error {
	ctx, cancel := context.WithDeadline(ctx, d)
	defer cancel()
	err := fn(ctx)
	if err == context.DeadlineExceeded || ctx.Err() == context.DeadlineExceeded {
		return TimeoutError{
			operation: op,
			deadline:  d,
		}
	}
	return err
}
