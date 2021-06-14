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
	"fmt"
	"time"
)

// WithTag returns a context tagged with the provided tag. Calling Tagged(tag)
// on the returned context or on a child context will return true.
func WithTag(ctx context.Context, tag interface{}) context.Context {
	// We use our own crdbCtx instead of simply context.WithValue() because
	// crdbCtx lets us retrieve a reference to itself when queried by tag. That
	// helps the CancellationParents option for Tagged().
	c := newCrdbCtx(ctx)
	c.tag = tag
	return c
}

// WithoutCancel returns a context that doesn't inherit the cancellation of its
// parent, and so it can never be canceled. Aside from the cancellation, the
// returned context inherits all values from the parent.
func WithoutCancel(ctx context.Context) context.Context {
	c := newCrdbCtx(ctx)
	c.cancelBarrier = true
	return c
}

// CtxWalkOption enumerates the options for Tagged().
type CtxWalkOption int

const (
	// AllParents instructs Tagged() to consider all of the ctx's ancestors.
	AllParents CtxWalkOption = iota
	// CancellationParents instructs Tagged() to only consider ctx's ancestors
	// whose cancellation is inherited by ctx.
	CancellationParents
)

// Tagged returns true if ctx is, or inherits from, a context produced by
// WithTag(key). key is compared through structural equality to the tags of the
// contexts in ctx's chain.
//
// If the CancellationParents option is specified, then false is returned even if
// a suitably-tagged context is found, but the cancellation of that context
// is no longer inherited by ctx (because a WithoutCancelation() was used
// somewhere on the chain from the tagged context to ctx).
//
// This is useful, for example, for tagging contexts that get canceled on
// certain events (e.g. stopper quiescence) and then, in child tasks, checking
// whether we're still running inside such a context.
func Tagged(ctx context.Context, key interface{}, opt CtxWalkOption) bool {
	p, ok := ctx.Value(key).(*crdbCtx)
	if !ok {
		return false
	}
	if opt == AllParents {
		return true
	}

	if opt != CancellationParents {
		panic(fmt.Sprintf("unexpected opt value: %v", opt))
	}

	uncParent := getCancellationBarrierParent(ctx)
	if uncParent == nil {
		return true
	}

	// The caller is using the CancelationParents options and the ctx chain has
	// both a crdbCtx (with the right key), and an uncanceledContext. There are two cases:
	//
	// 1. The chain looks like (from parent to child): ... -> crdbCtx -> ... -> uncanceledCtx -> ... -> ctx.
	//    In this case, we want to return nil because the uncanceledCtx terminates
	//    the cancellation of crdbCtx (i.e. ctx doesn't inherit crdbCtx's cancelation).
	// 2. The chain looks like (from parent to child): ... -> uncanceledCtx -> ... -> crdbCtx -> ... -> ctx.
	//    In this case, we want to return the crdbCtx, since ctx inherits its
	//    cancellation. We're making an assumption here that there's no other way
	//    to prevent the inheritance of cancellation in a context chain other than
	//    by using WithoutCancel (i.e. that there's no other types other than
	//    crdbCtx which override Done()).
	//
	// We're going discriminate the relative positions of crdbCtx and
	// uncanceledCtx by checking if the crdbCtx comes before the uncanceledCtx.
	pp := uncParent.Value(key).(*crdbCtx)
	if pp == nil || pp != p {
		// If we either haven't found any crdbCtx within uncParent's parents, or we
		// found a different one than p, then we must be in case 2 - uncanceledCtx
		// sits in front of crdbCtx.
		return true
	}
	return false
}

// crdbCtx is an implementation of context.Context that provides additional
// functionality.
type crdbCtx struct {
	ctx context.Context
	// tag is the context's tag, as set by WithTag.
	tag interface{}
	// cancelBarrier, makes this context, and any children, not inherit the
	// parent's cancellation. See WithoutCancel.
	cancelBarrier bool
}

var _ context.Context = &crdbCtx{}

func newCrdbCtx(ctx context.Context) *crdbCtx {
	return &crdbCtx{
		ctx: ctx,
	}
}

// Value is part of the Context interface.
func (c *crdbCtx) Value(key interface{}) interface{} {
	if key == c.tag {
		return c
	}
	if c.cancelBarrier && key == cancellationBarrierKey {
		return c
	}
	return c.ctx.Value(key)
}

// Done is part of the Context interface.
//
// NB: uncanceledContext has to remain the only Context implementation in the
// codebase to override Done(), because Tagged, when ran with the
// CancellationParents option, relies on there not being other cancellation
// "barriers".
func (c *crdbCtx) Done() <-chan struct{} {
	if c.cancelBarrier {
		return nil
	}
	return c.ctx.Done()
}

// Err is part the Context interface.
func (c *crdbCtx) Err() error {
	if c.cancelBarrier {
		return nil
	}
	return c.ctx.Err()
}

// Deadline overrides the Context method.
func (c *crdbCtx) Deadline() (deadline time.Time, ok bool) {
	if c.cancelBarrier {
		return time.Time{}, false
	}
	return c.ctx.Deadline()
}

// cancellationBarrierKey is the key for which a crdbCtx that acts as a
// cancellation barrier returns itself.
var cancellationBarrierKey = new(int)

// getUncanceledParent returns the innermost uncanceledContext amongst ctx's
// parents. Returns nil if there's no such context on the chain.
func getCancellationBarrierParent(ctx context.Context) *crdbCtx {
	c := ctx.Value(cancellationBarrierKey)
	if c == nil {
		return nil
	}
	return c.(*crdbCtx)
}
