// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-golang.txt.

// This code originated in Go's internal/singleflight package.

// Package singleflight provides a duplicate function call suppression
// mechanism.
package singleflight

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"go.opentelemetry.io/otel/attribute"
)

// call is an in-flight or completed singleflight.Do/DoChan call.
type call struct {
	opName, key string
	// c is closed when the call completes, signaling all waiters.
	c chan struct{}

	mu struct {
		syncutil.Mutex
		// sp is the tracing span of the flight leader. Nil if the leader does not
		// have a span. Once this span is finished, its recording is captured as
		// `rec` below. This span's recording might get copied into the traces of
		// other flight members. A non-leader caller with a recording trace will
		// enable recording on this span dynamically.
		sp *tracing.Span
	}

	/////////////////////////////////////////////////////////////////////////////
	// These fields are written once before the channel is closed and are only
	// read after the channel is closed.
	/////////////////////////////////////////////////////////////////////////////
	val interface{}
	err error
	// rec is the call's recording, if any of the callers that joined the call
	// requested the trace to be recorded. It is set once mu.sp is set to nil,
	// which happens before c is closed. rec is only read after c is closed.
	rec tracing.Trace

	/////////////////////////////////////////////////////////////////////////////
	// These fields are read and written with the singleflight mutex held before
	// the channel is closed, and are read but not written after the channel is
	// closed.
	/////////////////////////////////////////////////////////////////////////////
	dups int
}

func newCall(opName, key string, sp *tracing.Span) *call {
	c := &call{
		opName: opName,
		key:    key,
		c:      make(chan struct{}),
	}
	c.mu.sp = sp
	return c
}

func (c *call) maybeStartRecording(mode tracingpb.RecordingType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.sp.RecordingType() < mode {
		c.mu.sp.SetRecordingType(mode)
	}
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

// NoTags can be passed to NewGroup as the tagName to indicate that the tracing
// spans created for operations should not have the operation key as a tag. In
// particular, in cases where a single dummy key is used with a Group, having it
// as a tag is not necessary.
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
	// Val represents the call's return value.
	Val interface{}
	// Err represents the call's error, if any.
	Err error
	// Shared is set if the result has been shared with multiple callers.
	Shared bool
	// Leader is set if the caller was the flight leader (the caller that
	// triggered the flight).
	Leader bool
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
		c.maybeStartRecording(tracing.SpanFromContext(ctx).RecordingType())
		g.mu.Unlock()
		log.Eventf(ctx, "waiting on singleflight %s:%s owned by another leader. Starting to record the leader's flight.", g.opName, key)

		// Block on the call.
		<-c.c
		log.Eventf(ctx, "waiting on singleflight %s:%s owned by another leader... done", g.opName, key)
		// Get the call's result through result() so that the call's trace gets
		// imported into ctx.
		res := c.result(ctx, false /* leader */)
		return res.Val, true, res.Err
	}

	// Open a child span for the flight. Note that this child span might outlive
	// its parent if the caller doesn't wait for the result of this flight. It's
	// common for the caller to not always wait, particularly if
	// opts.InheritCancelation == false (i.e. if the caller can be canceled
	// independently of the flight).
	ctx, sp := tracing.ChildSpan(ctx, g.opName)
	if g.tagName != "" {
		sp.SetTag(g.tagName, attribute.StringValue(key))
	}
	c := newCall(g.opName, key, sp) // c takes ownership of sp
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

// Future is the return type of the DoChan() call.
type Future struct {
	call   *call
	leader bool
}

func makeFuture(c *call, leader bool) Future {
	return Future{
		call:   c,
		leader: leader,
	}
}

// C returns the channel on which the result of the DoChan call will be
// delivered.
func (f Future) C() <-chan struct{} {
	if f.call == nil {
		return nil
	}
	return f.call.c
}

// WaitForResult delivers the flight's result. If called before the call is done
// (i.e. before channel returned by C() is closed), then it will block;
// canceling ctx unblocks it.
//
// If the ctx has a recording tracing span, and if the context passed to DoChan
// also had a recording span in it (commonly because the same ctx was used),
// then the recording of the flight will be ingested into ctx even if this
// caller was not the flight's leader.
func (f Future) WaitForResult(ctx context.Context) Result {
	if f.call == nil {
		panic("WaitForResult called on empty Future")
	}
	return f.call.result(ctx, f.leader)
}

// Reset resets the future
func (f *Future) Reset() {
	*f = Future{}
}

// result returns the call's results. It will block until the call completes.
func (c *call) result(ctx context.Context, leader bool) Result {
	// Wait for the call to finish.
	select {
	// Give priority to c.c to ensure that a context error is not returned if the
	// call is done.
	case <-c.c:
	default:
		select {
		case <-c.c:
		case <-ctx.Done():
			op := fmt.Sprintf("%s:%s", c.opName, c.key)
			if !leader {
				log.Eventf(ctx, "waiting for singleflight interrupted: %v", ctx.Err())
				// If we're recording, copy over the call's trace.
				sp := tracing.SpanFromContext(ctx)
				if sp.RecordingType() != tracingpb.RecordingOff {
					// We expect the call to still be ongoing, so c.rec is not yet
					// populated. So, we'll look in c.mu.sp, and get the span's recording
					// so far. This is racy with the leader finishing and resetting
					// c.mu.sp, in which case we'll get nothing; we ignore that
					// possibiltity as unlikely, making this best-effort.
					rec := func() tracing.Trace {
						c.mu.Lock()
						defer c.mu.Unlock()
						return c.mu.sp.GetTraceRecording(sp.RecordingType())
					}()
					sp.ImportTrace(rec)
				}
			}
			return Result{
				Val:    nil,
				Err:    errors.Wrapf(ctx.Err(), "interrupted during singleflight %s", op),
				Shared: false,
				Leader: leader}
		}
	}

	// If we got here, c.c has been closed, so we can access c.rec.
	if !leader {
		// If we're recording, copy over the call's trace.
		sp := tracing.SpanFromContext(ctx)
		if sp.RecordingType() != tracingpb.RecordingOff {
			if rec := c.rec; !rec.Empty() {
				tracing.SpanFromContext(ctx).ImportTrace(rec.PartialClone())
			}
		}
	}

	return Result{
		Val:    c.val,
		Err:    c.err,
		Shared: c.dups > 0,
		Leader: leader,
	}
}

// DoChan is like Do but returns a Future that will receive the results when
// they are ready. The method also returns a boolean specifying whether the
// caller's fn function will be called or not. This return value lets callers
// identify a unique "leader" for a flight.
//
// Close() must be called on the returned Future if the caller does not
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
		c.maybeStartRecording(tracing.SpanFromContext(ctx).RecordingType())

		g.mu.Unlock()
		log.Eventf(ctx, "joining singleflight %s:%s owned by another leader", g.opName, key)
		return makeFuture(c, false /* leader */), false
	}

	// Open a child span for the flight. Note that this child span might outlive
	// its parent if the caller doesn't wait for the result of this flight. It's
	// common for the caller to not always wait, particularly if
	// opts.InheritCancelation == false (i.e. if the caller can be canceled
	// independently of the flight).
	ctx, sp := tracing.ChildSpan(ctx, g.opName)
	if g.tagName != "" {
		sp.SetTag(g.tagName, attribute.StringValue(key))
	}
	c := newCall(g.opName, key, sp) // c takes ownership of sp
	g.m[key] = c
	g.mu.Unlock()

	go g.doCall(ctx, c, key, opts, fn)

	return makeFuture(c, true /* leader */), true
}

// doCall handles the single call for a key. At the end of the call, c.c is
// closed, signaling the waiters (if any).
//
// doCall takes ownership of c.sp, which will be finished before returning.
func (g *Group) doCall(
	ctx context.Context,
	c *call,
	key string,
	opts DoOpts,
	fn func(ctx context.Context) (interface{}, error),
) {
	// Prepare the ctx for the call.
	if !opts.InheritCancelation {
		// Copy the log tags and the span.
		ctx = logtags.AddTags(context.Background(), logtags.FromContext(ctx))
		c.mu.Lock()
		sp := c.mu.sp
		c.mu.Unlock()
		ctx = tracing.ContextWithSpan(ctx, sp)
	}
	if opts.Stop != nil {
		var cancel func()
		ctx, cancel = opts.Stop.WithCancelOnQuiesce(ctx)
		defer cancel()

		if err := opts.Stop.RunTask(ctx, g.opName+":"+key, func(ctx context.Context) {
			c.val, c.err = fn(ctx)
		}); err != nil {
			c.err = err
		}
	} else {
		c.val, c.err = fn(ctx)
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.m, key)
	{
		c.mu.Lock()
		sp := c.mu.sp
		// Prevent other flyers from observing a finished span.
		c.mu.sp = nil
		c.rec = sp.FinishAndGetTraceRecording(sp.RecordingType())
		c.mu.Unlock()
	}
	// Publish the results to all waiters.
	close(c.c)
}

var _ = (*Group).Forget

// Forget tells the singleflight to forget about a key.  Future calls
// to Do for this key will call the function rather than waiting for
// an earlier call to complete.
func (g *Group) Forget(key string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.m, key)
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
