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
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// contextWithStacktrace overrides Err to annotate context.DeadlineExceeded and
// context.Canceled errors with a stacktrace.
// See: https://github.com/cockroachdb/cockroach/issues/95794
type contextWithStacktrace struct {
	context.Context

	mu struct {
		syncutil.Mutex
		capturedErr error
	}
}

// capturedErr returns the error (wrapped with errors.WithStack) that is
// "captured" on the first call to Err() that returns a non-nil error.
// The stacktrace of this error is from the point it was first "detected".
// If Err() is called on a child context created under contextWithStacktrace,
// an error will not be captured.
func (ctx *contextWithStacktrace) capturedErr() error {
	// Populate capturedErr via Err() in case it is nil.
	_ = ctx.Err()
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.mu.capturedErr
}

// Err implements the context.Context interface.
func (ctx *contextWithStacktrace) Err() error {
	err := ctx.Context.Err()
	if err == nil {
		return nil
	}
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	// Capture first non-nil result from Context.Err().
	if ctx.mu.capturedErr == nil {
		ctx.mu.capturedErr = errors.Wrap(err, "captured context error")
	}
	return err
}

// RunWithTimeout runs a function with a timeout, the same way you'd do with
// context.WithTimeout. It improves the opaque error messages returned by
// WithTimeout by augmenting them with the op string that is passed in.
func RunWithTimeout(
	ctx context.Context, op string, timeout time.Duration, fn func(ctx context.Context) error,
) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	ctxWithStacktrace := &contextWithStacktrace{Context: ctx}
	defer cancel()
	start := timeutil.Now()
	err := fn(ctxWithStacktrace)
	if err != nil && errors.Is(ctxWithStacktrace.Err(), context.DeadlineExceeded) {
		err = &TimeoutError{
			operation: op,
			timeout:   timeout,
			took:      timeutil.Since(start),
			// err may have been customized so combine it with capturedErr
			// to retain both the customization and the stacktrace.
			cause: errors.CombineErrors(err, ctxWithStacktrace.capturedErr()),
		}
	}
	return err
}
