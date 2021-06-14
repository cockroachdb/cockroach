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
	"time"
)

// WithoutCancel returns a context that doesn't inherit the cancellation of its
// parent, and so it can never be canceled. Aside from the cancellation, the
// returned context inherits all values from the parent.
func WithoutCancel(ctx context.Context) context.Context {
	if ctx.Done() == nil {
		// If ctx can't be canceled, there's no reason to allocate a new context.
		return ctx
	}
	return &uncanceledContext{ctx: ctx}
}

// InheritsCancellation returns true if child inherits the cancellation of
// parent.
//
// child is assumed to be derived directly or indirectly from parent. It's
// illegal to call InheritsCancellation with unrelated contexts.
//
// If parent is not a cancelable context, InheritsCancellation might return true
// even if there is a WithoutCancel context in between child and parent. This is
// because WithoutCancel is a no-op when called on a non-cancelable context.
//
// It is assumed that the only way for a child to not inherit a parent's
// cancellation is when there's a context produced by WithoutCancel() in the
// chain, between child and parent. In other words, we're assuming that there's
// no other types other than uncanceledCtx which override Done().
func InheritsCancellation(child, parent context.Context) bool {
	barrier1 := getUncanceledParent(child)
	if barrier1 == nil {
		// There's no barriers anywhere, so the child must be inheriting from the
		// parent.
		return true
	}
	barrier2 := getUncanceledParent(parent)
	// There's two cases to consider:
	// 1. barrier -> parent -> child
	// 2. [other barrier] -> parent -> barrier -> child
	// In case 1, the child inherits from the parent. In case 2, it doesn't.
	return barrier1 == barrier2
}

// uncanceledContext is an implementation of context.Context that ignores the
// cancellation of the parent.
type uncanceledContext struct {
	ctx context.Context
}

var _ context.Context = &uncanceledContext{}

// Value is part of the Context interface.
//
// Value recognizes cancellationBarrierKey and returns the receiver. This is so
// that children contexts can get a reference to a parent uncanceledContext, for
// the purposes of InheritsCancelation. Since we want to integrate with any
// implementation con context.Context, the only way for a parent to provide
// information to a child is through the Value method.
func (c *uncanceledContext) Value(key interface{}) interface{} {
	if key == cancellationBarrierKey {
		return c
	}
	return c.ctx.Value(key)
}

// Done is part of the Context interface. Always returns nil.
//
// NB: uncanceledContext has to remain the only Context implementation in the
// codebase to override Done() because InheritsCancellation relies on there not
// being other cancellation "barriers".
func (*uncanceledContext) Done() <-chan struct{} {
	return nil
}

// Err is part the Context interface. Always returns nil.
func (*uncanceledContext) Err() error {
	return nil
}

// Deadline overrides the Context method.  Always returns false.
func (*uncanceledContext) Deadline() (deadline time.Time, ok bool) {
	return time.Time{}, false
}

// cancellationBarrierKey is the key for which a uncanceledContext returns
// itself.
var cancellationBarrierKey = new(int)

// getUncanceledParent returns the innermost uncanceledContext amongst ctx's
// parents. Returns nil if there's no such context on the chain.
func getUncanceledParent(ctx context.Context) *uncanceledContext {
	c := ctx.Value(cancellationBarrierKey)
	if c == nil {
		return nil
	}
	return c.(*uncanceledContext)
}
