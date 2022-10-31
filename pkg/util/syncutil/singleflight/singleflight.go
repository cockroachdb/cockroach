// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// This code originated in Go's internal/singleflight package.

// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
	"go.opentelemetry.io/otel/attribute"
)

// call is an in-flight or completed singleflight.Do call
type call struct {
	wg sync.WaitGroup

	// These fields are written once before the WaitGroup is done
	// and are only read after the WaitGroup is done.
	val interface{}
	err error

	// These fields are read and written with the singleflight
	// mutex held before the WaitGroup is done, and are read but
	// not written after the WaitGroup is done.
	dups    int
	waiters []*futureImpl
}

type waiter struct {
	c  chan<- Result
	sp *tracing.Span
}

// Group represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
type Group struct {
	// opName is used as the operation name of the spans produced by this Group
	// for every flight.
	opName string
	// tagName represents the name of the tag containing the key for each flight.
	// If not set, the spans do not get such a tag.
	tagName string
	mu      syncutil.Mutex   // protects m
	m       map[string]*call // lazily initialized
}

const NoTags = ""

// NewGroup creates a Group.
//
// opName will be used as the operation name of the spans produced by this Group
// for every flight.
// tagName will be used as the name of the span tag containing the key for each
// flight span. If NoTags is passed, the spans do not get such tags.
func NewGroup(opName, tagName string) *Group {
	return &Group{opName: opName, tagName: tagName}
}

// Result holds the results of Do, so they can be passed
// on a channel.
type Result struct {
	Val    interface{}
	Err    error
	Shared bool
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// The return value shared indicates whether v was given to multiple callers.
//
// NOTE: If fn responds to ctx cancelation by interrupting the work and
// returning an error, canceling ctx might propagate such error to other callers
// if a flight's leader's ctx is canceled. See DoChan for more control over this
// behavior.
func (g *Group) Do(
	ctx context.Context, key string, fn func(context.Context) (interface{}, error),
) (v interface{}, shared bool, err error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		log.Eventf(ctx, "waiting on singleflight %s:%s owned by another leader...", g.opName, key)
		c.wg.Wait()
		log.Eventf(ctx, "waiting on singleflight %s:%s owned by another leader... done", g.opName, key)
		return c.val, true, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	g.doCall(ctx,
		c, key,
		DoOpts{
			Stop:               nil,
			InheritCancelation: true,
		},
		fn)
	return c.val, c.dups > 0, c.err
}

// DoOpts groups options for the DoChan() method.
type DoOpts struct {
	// Stop, if not nil, is used both to create the flight in a stopper task and
	// to cancel the flight's ctx on stopper quiescence.
	Stop *stop.Stopper
	// InheritCancelation controls whether the cancelation of the caller's ctx
	// affects the flight. If set, the flight closure gets the caller's ctx. If
	// not set, the closure runs in a different ctx which does not inherit the
	// caller's cancelation. It is common to not want the flight to inherit the
	// caller's cancelation, so that a canceled leader does not propagate an error
	// to everybody else that joined the sane flight.
	InheritCancelation bool
}

// Future represents the result of the DoChan() call.
type Future interface {
	// ReaderClose indicates that the reader is no longer waiting on this Future.
	// Readers that do not wait on C() are required to call ReaderClose() in order
	// to prevent the singleflight from using DoChan()'s tracing span after the
	// span is finished by the caller. In other words, the span used for the
	// DoChan() call needs to live until either <-C() returns the result, or
	// ReaderClose() is called.
	//
	// ReaderClose() can be called after C(), although that is not necessary. It's
	// often a good idea to `defer future.ReaderClose()` immediately after a
	// DoChan() call.
	ReaderClose()

	// C() returns the channel on which the result of the DoChan call will be
	// delivered.
	C() <-chan Result
}

// futureImpl implements Future, providing the expoted reading interface, and
// the internal writing interface. The futureImpl captures a tracing span, which
// is released by ReaderClose().
type futureImpl struct {
	c  chan Result
	mu struct {
		syncutil.Mutex
		sp *tracing.Span
	}
}

var _ Future = &futureImpl{}

func newFutureImpl(sp *tracing.Span) *futureImpl {
	f := &futureImpl{
		c: make(chan Result, 1),
	}
	f.mu.sp = sp
	return f
}

// ReaderClose is part of the Future interface.
func (f *futureImpl) ReaderClose() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mu.sp = nil
}

// ReaderClose is part of the Future interface.
func (f *futureImpl) C() <-chan Result {
	return f.c
}

func (f *futureImpl) writerClose(r Result, groupName, key string) {
	f.mu.Lock()
	if f.mu.sp != nil {
		f.mu.sp.Recordf("finished waiting on singleflight %s:%s. err: %v", groupName, key, r.Err)
	}
	f.mu.Unlock()
	f.c <- r
}

// DoChan is like Do but returns a Future that will receive the results when
// they are ready. The method also returns a boolean specifying whether the
// caller's fn function will be called or not. This return value lets callers
// identify a unique "leader" for a flight.
//
// ReaderClose() must be called on the returned Future if the caller does not
// wait for the Future's result.
//
// opts controls details about how the flight is to run.
//
// NOTE: DoChan makes it possible to initiate or join a flight while holding a
// lock without holding it for the duration of the flight. A common usage
// pattern is:
// 1. Check some datastructure to see if it contains the value you're looking
// for.
// 2. If it doesn't, initiate or join a flight to produce it.
//
// Step one is expected to be done while holding a lock. Modifying the
// datastructure in the callback is expected to need to take the same lock. Once
// a caller proceeds to step two, it likely wants to keep the lock until
// DoChan() returned a channel, in order to ensure that a flight is only started
// before any modifications to the datastructure occurred (relative to the state
// observed in step one). Were the lock to be released before calling DoChan(),
// a previous flight might modify the datastructure before our flight began.
//
// In addition to the above, another reason for using DoChan over Do is so that
// the caller can be canceled while not propagating the cancelation to other
// innocent callers that joined the same flight; the caller can listen to its
// cancelation in parallel to the result channel. See DoOpts.InheritCancelation.
func (g *Group) DoChan(
	ctx context.Context, key string, opts DoOpts, fn func(context.Context) (interface{}, error),
) (Future, bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}

	if c, ok := g.m[key]; ok {
		c.dups++
		waiter := newFutureImpl(tracing.SpanFromContext(ctx))
		c.waiters = append(c.waiters, waiter)
		g.mu.Unlock()
		log.Eventf(ctx, "joining singleflight %s:%s owned by another leader", g.opName, key)
		return waiter, false
	}
	waiter := newFutureImpl(nil /* sp - the leader does not keep track of the span because it doesn't need to log*/)
	c := &call{waiters: []*futureImpl{waiter}}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	go g.doCall(ctx, c, key, opts, fn)

	return waiter, true
}

// doCall handles the single call for a key.
func (g *Group) doCall(
	ctx context.Context,
	c *call,
	key string,
	opts DoOpts,
	fn func(ctx context.Context) (interface{}, error),
) {
	// Prepare the ctx for the call.
	ctx, sp := tracing.ChildSpan(ctx, g.opName)
	if g.tagName != "" {
		sp.SetTag(g.tagName, attribute.StringValue(key))
	}
	defer sp.Finish()
	if !opts.InheritCancelation {
		// Copy the log tags and the span.
		ctx = logtags.AddTags(context.Background(), logtags.FromContext(ctx))
		ctx = tracing.ContextWithSpan(ctx, sp)
	}
	if opts.Stop != nil {
		var cancel func()
		ctx, cancel = opts.Stop.WithCancelOnQuiesce(ctx)
		defer cancel()
	}

	if opts.Stop != nil {
		if err := opts.Stop.RunTask(ctx, g.opName+":"+key, func(ctx context.Context) {
			c.val, c.err = fn(ctx)
		}); err != nil {
			c.err = err
		}
	} else {
		c.val, c.err = fn(ctx)
	}
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	res := Result{c.val, c.err, c.dups > 0}
	for _, waiter := range c.waiters {
		waiter.writerClose(res, g.opName, key)
	}
	g.mu.Unlock()
}

var _ = (*Group).Forget

// Forget tells the singleflight to forget about a key.  Future calls
// to Do for this key will call the function rather than waiting for
// an earlier call to complete.
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}

// NumCalls returns the number of in-flight calls for a given key.
func (g *Group) NumCalls(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.m[key]; ok {
		return c.dups + 1
	}
	return 0
}
