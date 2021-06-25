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
	"sync"

	"github.com/cockroachdb/errors"
)

type extErrKey struct{}

// WithExtCancel is like `context.WithCancel` but allows the caller to supply
// a hook that is called as part of cancel(). The return value of the hook
// is exposed via the ExtCanceled and ExtErr methods.
func WithExtCancel(
	parent context.Context, hook func(...interface{}) interface{},
) (context.Context, func(...interface{})) {
	wrappedCtx, wrappedCancel := context.WithCancel(parent)
	ctx := &extCancelCtx{
		Context: wrappedCtx,
	}
	return ctx, func(args ...interface{}) {
		defer wrappedCancel() // actually cancel after we've populated our ctx from hook
		ctx.ext.Lock()
		defer ctx.ext.Unlock()
		if !ctx.ext.canceled {
			ctx.ext.canceled = true
			ctx.ext.val = hook(args...)
		}
	}
}

// WithCallerCancel returns a context that, if cancelled, returns an
// `errors.WithStack` from ExtErr, where the stack identifies the caller
// of the cancel function.
func WithCallerCancel(parent context.Context) (context.Context, func(...interface{})) {
	return WithExtCancel(parent, func(...interface{}) interface{} {
		return errors.WithStackDepth(context.Canceled, 2 /* depth */)
	})
}

// ExtCanceled will return the value of the hook function supplied to the
// nearest canceled parent context created via WithExtContext(). It is an
// extension of `(Context).Err` that is allowed to return arbitrary values.
// Will fall back to `ctx.Err()` on nil returns.
func ExtCanceled(ctx context.Context) interface{} {
	v := ctx.Value(extErrKey{})
	if v == nil {
		v = ctx.Err()
	}
	return v
}

// ExtErr is like ExtCanceled, but returns the value from the nearest canceled
// parent context if it is a non-nil error, and falling back to ctx.Error()
// otherwise.
func ExtErr(ctx context.Context) error {
	err, _ := ExtCanceled(ctx).(error)
	if err == nil {
		err = ctx.Err()
	}
	return err
}

type extCancelCtx struct {
	context.Context
	ext struct {
		sync.Mutex
		canceled bool
		val      interface{}
	}
}

func (ctx *extCancelCtx) Value(key interface{}) interface{} {
	var v interface{}
	if key == (extErrKey{}) {
		ctx.ext.Lock()
		if ctx.ext.canceled {
			v = ctx.ext.val
		}
		ctx.ext.Unlock()
	}
	if v == nil {
		v = ctx.Context.Value(key)
	}
	return v
}
