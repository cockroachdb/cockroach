// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type errCancelKey struct{}

// WithErrCancel returns a cancelable context that whose cancellation function
// takes an error. While that error will *not* be returned from `ctx.Err`, the
// package-level method `Err` will return the error (annotated with
// errors.WithStackDepth) for the returned context and its descendants.
func WithErrCancel(parent context.Context) (context.Context, func(error)) {
	wrappedCtx, wrappedCancel := context.WithCancel(parent)
	ctx := &errCancelCtx{
		Context: wrappedCtx,
	}
	return ctx, func(err error) {
		if err == nil {
			err = context.Canceled
		}
		err = errors.WithStackDepth(err, 1 /* depth */)
		defer wrappedCancel() // actually cancel after we've populated our ctx's err
		ctx.mu.Lock()
		defer ctx.mu.Unlock()
		ctx.err = err
	}
}

// Err returns an error associated to the Context. This is nil unless the
// Context is canceled, and will match `ctx.Err()` for contexts that were
// not derived from a since-canceled parent created via WithErrCancel.
//
// However, for a Context derived from a since-canceled parent created via
// WithErrCancel, Err returns the error passed to the cancellation function,
// wrapped in an `errors.WithStackDepth` that identifies the caller of the
// `cancel(err)` call. When the Context passed to `Err` has multiple such
// parents, the "most distant" one is returned, under the assumption that
// it provides the original reason for the context chain's cancellation.
//
// See ExampleWithErrCancel for an example.
func Err(ctx context.Context) error {
	err := ctx.Err()
	if err == nil {
		return nil
	}
	for {
		// Walk to the closest errCancelCtx parent.
		c, ok := ctx.Value(errCancelKey{}).(*errCancelCtx)
		if !ok {
			// None found, done.
			break
		}
		ctx = c.Context

		// If it's canceled, remember the error.
		if extErr := c.getErr(); extErr != nil {
			err = extErr
		}

		// Keep walking.
	}
	return err
}

type errCancelCtx struct {
	context.Context
	mu  syncutil.Mutex
	err error // protected by mu
}

func (ctx *errCancelCtx) getErr() error {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()
	return ctx.err
}

func (ctx *errCancelCtx) Value(key interface{}) interface{} {
	if key == (errCancelKey{}) {
		return ctx
	}
	return ctx.Context.Value(key)
}
