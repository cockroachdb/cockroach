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
	"fmt"
	"net"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// WithCancel adds an info log to context.WithCancel's CancelFunc. Prefer using
// WithCancelReason when possible.
func WithCancel(parent context.Context) (context.Context, context.CancelFunc) {
	return wrap(context.WithCancel(parent))
}

// reasonKey is a marker struct that's used to save the reason a context was
// canceled.
type reasonKey struct{}

// CancelWithReasonFunc is a context.CancelFunc that also passes along an error
// that is the reason for cancellation.
type CancelWithReasonFunc func(reason error)

// WithCancelReason adds a CancelFunc to this context, returning a new
// cancellable context and a CancelWithReasonFunc, which is like
// context.CancelFunc, except it also takes a "reason" error. The context that
// is canceled with this CancelWithReasonFunc will immediately be updated to
// contain this "reason". The reason can be retrieved with GetCancelReason.
// This function doesn't change the deadline of a context if it already exists.
func WithCancelReason(ctx context.Context) (context.Context, CancelWithReasonFunc) {
	val := new(atomic.Value)
	ctx = context.WithValue(ctx, reasonKey{}, val)
	ctx, cancel := wrap(context.WithCancel(ctx))
	return ctx, func(reason error) {
		val.Store(reason)
		cancel()
	}
}

// GetCancelReason retrieves the cancel reason for a context that has been
// created via WithCancelReason. The reason will be nil if the context was not
// created with WithCancelReason, or if the context has not been canceled yet.
// Otherwise, the reason will be the error that the context's
// CancelWithReasonFunc was invoked with.
func GetCancelReason(ctx context.Context) error {
	i := ctx.Value(reasonKey{})
	switch t := i.(type) {
	case *atomic.Value:
		return t.Load().(error)
	}
	return nil
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

var _ error = (*TimeoutError)(nil)
var _ fmt.Formatter = (*TimeoutError)(nil)
var _ errors.Formatter = (*TimeoutError)(nil)

// We implement net.Error the same way that context.DeadlineExceeded does, so
// that people looking for net.Error attributes will still find them.
var _ net.Error = (*TimeoutError)(nil)

func (t *TimeoutError) Error() string { return fmt.Sprintf("%v", t) }

// Format implements fmt.Formatter.
func (t *TimeoutError) Format(s fmt.State, verb rune) { errors.FormatError(t, s, verb) }

// FormatError implements errors.Formatter.
func (t *TimeoutError) FormatError(p errors.Printer) error {
	p.Printf("operation %q timed out after %s", t.operation, t.duration)
	if errors.UnwrapOnce(t.cause) != nil {
		// If there were details (wrappers, stack trace etc.) ensure
		// they get printed.
		return t.cause
	}
	// We omit the "context deadline exceeded" suffix in the common case.
	return nil
}

// Timeout implements net.Error.
func (*TimeoutError) Timeout() bool { return true }

// Temporary implements net.Error.
func (*TimeoutError) Temporary() bool { return true }

// Cause implements Causer.
func (t *TimeoutError) Cause() error {
	return t.cause
}

// RunWithTimeout runs a function with a timeout, the same way you'd do with
// context.WithTimeout. It improves the opaque error messages returned by
// WithTimeout by augmenting them with the op string that is passed in.
func RunWithTimeout(
	ctx context.Context, op string, timeout time.Duration, fn func(ctx context.Context) error,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	err := fn(ctx)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = &TimeoutError{
			operation: op,
			duration:  timeout,
			cause:     err,
		}
	}
	return err
}
