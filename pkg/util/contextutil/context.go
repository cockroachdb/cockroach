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
	"net"
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

// TimeoutError is a wrapped ContextDeadlineExceeded error. It indicates that
// an operation didn't complete within its designated timeout.
type TimeoutError struct {
	operation string
	duration  time.Duration
	cause     error
}

func (t TimeoutError) Error() string {
	return fmt.Sprintf("operation %q timed out after %s", t.operation, t.duration)
}

// Timeout implements net.Error.
func (TimeoutError) Timeout() bool { return true }

// Temporary implements net.Error.
func (TimeoutError) Temporary() bool { return true }

// Cause implements Causer.
func (t TimeoutError) Cause() error {
	// This ensures that people looking for DeadlineExceeded in particular still
	// see it.
	if t.cause == nil {
		return context.DeadlineExceeded
	}
	return t.cause
}

// We implement net.Error the same way that context.DeadlineExceeded does, so
// that people looking for net.Error attributes will still find them.
var _ net.Error = TimeoutError{}

// RunWithTimeout runs a function with a timeout, the same way you'd do with
// context.WithTimeout. It improves the opaque error messages returned by
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
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		return TimeoutError{
			operation: op,
			duration:  timeout,
			cause:     err,
		}
	}
	return err
}
